#pragma once
#include "executor_value.hpp"
#include "executor_integer_set.hpp"
#include <bit>

namespace pgaccel {
namespace {
// Exact logical values: inline singleton, compact integers, or general cells.
// Promotion preserves signed 128-bit values and string ownership.
class ExactSet {
    enum Mode { EMPTY, SINGLE, INTEGERS, GENERAL } mode = EMPTY;
    std::pmr::memory_resource &memory;
    uint64_t first = 0;
    IntegerSet integers;
    using General = std::pmr::unordered_set<Cell, Hash>;
    std::shared_ptr<General> general;
  public:
    explicit ExactSet(std::pmr::memory_resource &resource) : memory(resource), integers(resource) {}
    size_t size() const {
        switch (mode) {
        case EMPTY: return 0;
        case SINGLE: return 1;
        case INTEGERS: return integers.size();
        case GENERAL: return general->size();
        }
        return 0;
    }
    template <class F> void forEach(F function) const {
        switch (mode) {
        case EMPTY: break;
        case SINGLE: function(Cell{Wide(std::bit_cast<int64_t>(first))}); break;
        case INTEGERS:
            integers.forEach([&](uint64_t value) { function(Cell{Wide(std::bit_cast<int64_t>(value))}); });
            break;
        case GENERAL:
            for (const auto &value : *general) function(value);
        }
    }
    void insert(const Cell &value) {
        if (mode == GENERAL) { general->insert(value); return; }
        if (!value.string && value.number >= INT64_MIN && value.number <= INT64_MAX) {
            uint64_t number = uint64_t(value.number);
            if (mode == EMPTY) { first = number; mode = SINGLE; return; }
            if (mode == SINGLE) {
                if (number == first) return;
                integers.insert(first);
                integers.insert(number);
                mode = INTEGERS;
            } else integers.insert(number);
            return;
        }
        auto replacement = std::allocate_shared<General>(std::pmr::polymorphic_allocator<General>(&memory));
        forEach([&](const Cell &old) { replacement->insert(old); });
        replacement->insert(value);
        general = std::move(replacement);
        integers.release();
        mode = GENERAL;
    }
};
} // namespace
} // namespace pgaccel
