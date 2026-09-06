#pragma once
#include "executor_state.hpp"
#include <immintrin.h>
#include <span>

namespace pgaccel {
namespace {
// Exact set with a bounded integer bitmap when key density permits it.
// The hash representation remains available for strings and wide domains.
struct MembershipSet {
    std::pmr::unordered_set<Cell, Hash> values;
    std::pmr::vector<uint64_t> bits;
    int scale;
    int64_t first = 0, last = -1;
    explicit MembershipSet(Budget &budget, int s)
        : values(&budget), bits(&budget), scale(s) {}
    // Construct directly from a numeric child output, without hash nodes.
    void assignNumbers(std::span<const Wide> numbers, int sourceScale, Budget &budget) {
        values.clear(); bits.clear(); first = 0; last = -1;
        if (numbers.empty()) return;
        Wide factor = Power(scale - sourceScale);
        Wide lo = Multiply(numbers.front(), factor), hi = lo;
        for (Wide number : numbers) {
            Wide value = Multiply(number, factor);
            lo = std::min(lo, value); hi = std::max(hi, value);
        }
        if (lo >= INT64_MIN && hi <= INT64_MAX) {
            Wide width = hi - lo + 1;
            uint64_t words = (uint64_t(width) + 63) / 64;
            if (width <= (Wide(1) << 29) && width <= Wide(numbers.size()) * 2048 &&
                words * sizeof(uint64_t) <= (budget.limit - budget.used.load()) / 2) {
                bits.assign(words, 0); first = int64_t(lo); last = int64_t(hi);
                for (Wide number : numbers) {
                    uint64_t offset = uint64_t(Multiply(number, factor) - lo);
                    bits[offset / 64] |= uint64_t(1) << (offset % 64);
                }
                return;
            }
        }
        for (Wide number : numbers) values.insert(Cell{Multiply(number, factor)});
    }
    void compact(Budget &budget) {
        if (values.empty()) return;
        Wide lo = INT64_MAX, hi = INT64_MIN;
        for (const auto &v : values) {
            if (v.string || v.number < INT64_MIN || v.number > INT64_MAX) return;
            lo = std::min(lo, v.number); hi = std::max(hi, v.number);
        }
        Wide width = hi - lo + 1;
        if (width > (Wide(1) << 29) || width > Wide(values.size()) * 2048) return;
        uint64_t words = (uint64_t(width) + 63) / 64;
        if (words * sizeof(uint64_t) > (budget.limit - budget.used.load()) / 2) return;
        bits.resize(words);
        first = int64_t(lo); last = int64_t(hi);
        for (const auto &v : values) {
            uint64_t offset = uint64_t(v.number - lo);
            bits[offset / 64] |= uint64_t(1) << (offset % 64);
        }
        values.clear(); values.rehash(0);
    }
    bool contains(const Cell &value) const {
        if (bits.empty()) return values.contains(value);
        if (value.string || value.number < first || value.number > last) return false;
        uint64_t offset = uint64_t(value.number - first);
        return bits[offset / 64] >> (offset % 64) & 1;
    }
    void intersect(const RowGroup &rg, const ColumnRef &column, bool anti,
                   bool avx, uint8_t *bitmap) const {
        auto data = rg.columns[column.columnIdx].get();
        int row = 0;
        int adjustment = scale - Scale(column.Type().get());
        auto type = column.Type()->type_num();
        if (avx && !bits.empty() && !adjustment && first >= INT32_MIN && last <= INT32_MAX &&
            (type == INT32_TYPE || type == INT64_TYPE) &&
            data->type == ColumnDataBase::RAW_COLUMN_DATA &&
            static_cast<const RawColumnDataBase *>(data)->bytesPerValue == 4) {
            auto keys = reinterpret_cast<const int32_t *>(static_cast<const RawColumnDataBase *>(data)->values);
            auto lo = _mm512_set1_epi32(first), hi = _mm512_set1_epi32(last);
            for (; row + 16 <= rg.size; row += 16) {
                uint16_t previous; memcpy(&previous, bitmap + row / 8, 2);
                if (!previous) continue;
                auto key = _mm512_loadu_si512(keys + row);
                auto delta = _mm512_sub_epi32(key, lo);
                uint16_t valid = previous & _mm512_cmpge_epi32_mask(key, lo) & _mm512_cmple_epi32_mask(key, hi);
                auto words = _mm512_mask_i32gather_epi32(_mm512_setzero_si512(), valid,
                    _mm512_srli_epi32(delta, 5), bits.data(), 4);
                uint16_t found = valid & _mm512_test_epi32_mask(_mm512_srlv_epi32(words,
                    _mm512_and_si512(delta, _mm512_set1_epi32(31))), _mm512_set1_epi32(1));
                uint16_t selected = previous & (anti ? uint16_t(~found) : found);
                memcpy(bitmap + row / 8, &selected, 2);
            }
        }
        for (; row < rg.size; ++row) {
            uint8_t bit = uint8_t(1) << (row % 8);
            if (!(bitmap[row / 8] & bit)) continue;
            auto key = Read(rg, column, row);
            if (!key.string) key.number = Multiply(key.number, Power(adjustment));
            if (contains(key) == anti) bitmap[row / 8] &= ~bit;
        }
    }
};
}
}
