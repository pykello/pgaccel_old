#pragma once
#include "executor_state.hpp"
#include <cassert>
#include <utility>

namespace pgaccel
{
namespace
{
struct IntegerHash
{
    size_t operator()(uint64_t value) const
    {
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27;
        value *= 0x94d049bb133111ebULL;
        return value ^ (value >> 31);
    }
};

// Exact integer keys with no per-entry allocation. Zero has a separate flag,
// so every 64-bit pattern remains a valid key. Growth commits only after the
// replacement allocation succeeds and all existing keys have been relocated.
class IntegerSet
{
    std::pmr::vector<uint64_t> slots;
    size_t nonzero = 0;
    bool zero = false;

    void grow(size_t capacity)
    {
        if (capacity > slots.max_size())
            throw std::runtime_error("Query memory budget exceeded (integer set capacity)");
        std::pmr::vector<uint64_t> replacement(capacity, 0, slots.get_allocator());
        for (uint64_t value : slots)
        {
            if (!value)
                continue;
            size_t index = IntegerHash{}(value) & (capacity - 1);
            while (replacement[index])
                index = (index + 1) & (capacity - 1);
            replacement[index] = value;
        }
        slots.swap(replacement);
    }

  public:
    explicit IntegerSet(std::pmr::memory_resource &memory) : slots(&memory) {}
    IntegerSet(IntegerSet &&other) noexcept
        : slots(std::move(other.slots)), nonzero(std::exchange(other.nonzero, 0)),
          zero(std::exchange(other.zero, false)) {}
    IntegerSet(const IntegerSet &) = delete;
    IntegerSet &operator=(const IntegerSet &) = delete;

    size_t size() const { return nonzero + zero; }
    bool empty() const { return !size(); }
    void swap(IntegerSet &other)
    {
        // Every set in an execution uses the same query resource.
        assert(slots.get_allocator() == other.slots.get_allocator());
        slots.swap(other.slots);
        std::swap(nonzero, other.nonzero);
        std::swap(zero, other.zero);
    }
    void release()
    {
        std::pmr::vector<uint64_t> empty(slots.get_allocator());
        slots.swap(empty);
        nonzero = 0;
        zero = false;
    }
    bool insert(uint64_t value) { return insert(value, IntegerHash{}(value)); }
    bool insert(uint64_t value, uint64_t hash)
    {
        if (!value)
            return !std::exchange(zero, true);
        if (slots.empty())
            grow(16);
        size_t mask = slots.size() - 1;
        size_t index = hash & mask;
        while (slots[index])
        {
            if (slots[index] == value)
                return false;
            index = (index + 1) & mask;
        }
        if (nonzero + 1 > slots.size() - slots.size() / 4)
        {
            if (slots.size() > slots.max_size() / 2)
                throw std::runtime_error("Query memory budget exceeded (integer set capacity)");
            grow(slots.size() * 2);
            mask = slots.size() - 1;
            index = hash & mask;
            while (slots[index])
                index = (index + 1) & mask;
        }
        slots[index] = value;
        ++nonzero;
        return true;
    }
    template <class F> void forEach(F function) const
    {
        if (zero)
            function(0);
        for (uint64_t value : slots)
            if (value)
                function(value);
    }
};
} // namespace
} // namespace pgaccel
