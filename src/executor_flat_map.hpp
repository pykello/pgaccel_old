#pragma once
#include "executor_state.hpp"
#include <limits>
#include <type_traits>

namespace pgaccel
{
namespace
{
// Original insert-only flat lookup table. Slots contain a 32-bit hash tag and
// a one-based payload index. Payload indexes remain stable across vector growth;
// references do not. Both arrays allocate through the query resource.
template <class Key, class Value, class Hash, class Equal>
class FlatMap
{
  public:
    using Entry = std::pair<Key, Value>;
    static constexpr uint32_t missing = UINT32_MAX;

  private:
    std::pmr::vector<uint64_t> slots;
    std::pmr::vector<Entry> entries;
    Hash hash;
    Equal equal;

    void grow(size_t capacity)
    {
        if (capacity > slots.max_size())
            throw std::runtime_error("Query memory budget exceeded (flat map capacity)");
        std::pmr::vector<uint64_t> replacement(capacity, 0, slots.get_allocator());
        for (size_t i = 0; i < entries.size(); ++i)
        {
            uint64_t h = hash(entries[i].first);
            size_t position = h & (capacity - 1);
            while (replacement[position])
                position = (position + 1) & (capacity - 1);
            replacement[position] = (h & 0xffffffff00000000ULL) | uint64_t(i + 1);
        }
        slots.swap(replacement);
    }

  public:
    FlatMap(std::pmr::memory_resource &memory, Hash hasher = {}, Equal equality = {})
        : slots(&memory), entries(&memory), hash(hasher), equal(equality)
    {
        static_assert(std::is_nothrow_move_constructible_v<Entry>);
    }
    FlatMap(FlatMap &&) noexcept = default;
    FlatMap(const FlatMap &) = delete;
    FlatMap &operator=(const FlatMap &) = delete;
    size_t size() const { return entries.size(); }
    Entry &at(uint32_t index) { return entries[index]; }
    const Entry &at(uint32_t index) const { return entries[index]; }

    template <class Factory>
    std::pair<uint32_t, bool> get(const Key &key, Factory initial)
    {
        const uint64_t h = hash(key);
        const uint64_t tag = h & 0xffffffff00000000ULL;
        if (slots.empty())
            grow(16);
        size_t position = h & (slots.size() - 1);
        while (uint64_t slot = slots[position])
        {
            uint32_t index = uint32_t(slot) - 1;
            if ((slot & 0xffffffff00000000ULL) == tag && equal(entries[index].first, key))
                return {index, false};
            position = (position + 1) & (slots.size() - 1);
        }
        if (entries.size() >= UINT32_MAX)
            throw std::runtime_error("Query memory budget exceeded (flat map entry indexes)");
        if (entries.size() + 1 > slots.size() - slots.size() / 4)
        {
            if (slots.size() > slots.max_size() / 2)
                throw std::runtime_error("Query memory budget exceeded (flat map capacity)");
            grow(slots.size() * 2);
            position = h & (slots.size() - 1);
            while (slots[position])
                position = (position + 1) & (slots.size() - 1);
        }
        const uint32_t index = entries.size();
        entries.emplace_back(key, initial());
        slots[position] = tag | uint64_t(index + 1);
        return {index, true};
    }
    uint32_t find(const Key &key) const
    {
        if (slots.empty()) return missing;
        const uint64_t h = hash(key), tag = h & 0xffffffff00000000ULL;
        size_t position = h & (slots.size() - 1);
        while (uint64_t slot = slots[position])
        {
            uint32_t index = uint32_t(slot) - 1;
            if ((slot & 0xffffffff00000000ULL) == tag && equal(entries[index].first, key))
                return index;
            position = (position + 1) & (slots.size() - 1);
        }
        return missing;
    }
    template <class F> void forEach(F function)
    {
        for (auto &entry : entries)
            function(entry.first, entry.second);
    }
    void release()
    {
        std::pmr::vector<Entry> emptyEntries(entries.get_allocator());
        entries.swap(emptyEntries);
        std::pmr::vector<uint64_t> emptySlots(slots.get_allocator());
        slots.swap(emptySlots);
    }
};
} // namespace
} // namespace pgaccel
