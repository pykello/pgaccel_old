#pragma once
#include <cstdint>
#include <functional>
#include <memory_resource>
#include <span>
#include <stdexcept>
#include <vector>

namespace pgaccel {
// Original insert-only interner. The stable ID is the contiguous key index;
// hash slots carry a fingerprint and one-based index, never a key pointer.
template <class Key, class Hash, class Equal = std::equal_to<Key>>
class ColumnIntern {
    std::pmr::vector<uint64_t> slots;
    std::pmr::vector<Key> values;
    Hash hash;
    Equal equal;
    void grow(size_t capacity) {
        if (capacity > slots.max_size()) throw std::runtime_error("Dictionary capacity exceeded");
        std::pmr::vector<uint64_t> replacement(capacity, 0, slots.get_allocator());
        for (size_t i = 0; i < values.size(); ++i) {
            uint64_t h = hash(values[i]);
            size_t position = h & (capacity - 1);
            while (replacement[position]) position = (position + 1) & (capacity - 1);
            replacement[position] = (h & 0xffffffff00000000ULL) | uint64_t(i + 1);
        }
        slots.swap(replacement);
    }
  public:
    static constexpr uint32_t missing = UINT32_MAX;
    explicit ColumnIntern(std::pmr::memory_resource &memory, Hash h = {}, Equal e = {})
        : slots(&memory), values(&memory), hash(h), equal(e) {}
    std::span<const Key> keys() const { return values; }
    uint32_t find(const Key &key) const {
        if (slots.empty()) return missing;
        uint64_t h = hash(key), tag = h & 0xffffffff00000000ULL;
        size_t position = h & (slots.size() - 1);
        while (uint64_t slot = slots[position]) {
            uint32_t id = uint32_t(slot) - 1;
            if ((slot & 0xffffffff00000000ULL) == tag && equal(values[id], key)) return id;
            position = (position + 1) & (slots.size() - 1);
        }
        return missing;
    }
    uint32_t intern(const Key &key) {
        if (slots.empty()) grow(16);
        uint64_t h = hash(key), tag = h & 0xffffffff00000000ULL;
        size_t position = h & (slots.size() - 1);
        while (uint64_t slot = slots[position]) {
            uint32_t id = uint32_t(slot) - 1;
            if ((slot & 0xffffffff00000000ULL) == tag && equal(values[id], key)) return id;
            position = (position + 1) & (slots.size() - 1);
        }
        if (values.size() >= UINT32_MAX) throw std::runtime_error("Dictionary ID capacity exceeded");
        if (values.size() + 1 > slots.size() - slots.size() / 4) {
            if (slots.size() > slots.max_size() / 2) throw std::runtime_error("Dictionary capacity exceeded");
            grow(slots.size() * 2);
            position = h & (slots.size() - 1);
            while (slots[position]) position = (position + 1) & (slots.size() - 1);
        }
        uint32_t id = values.size();
        values.push_back(key);
        slots[position] = tag | uint64_t(id + 1);
        return id;
    }
};
} // namespace pgaccel
