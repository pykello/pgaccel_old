#pragma once
#include "executor_state.hpp"
#include <array>

namespace pgaccel {
namespace {
// Required inner equalities permit semijoin reduction without changing
// duplicate multiplicity. Keep discovery bounded to tiny integer domains.
void FilterSmallJoinDomains(const QueryDesc &q,
                           std::vector<std::pmr::vector<uint64_t>> &rows,
                           Budget &budget) {
    if (!q.leftJoins.empty() || !q.existenceJoins.empty() || q.tables.size() < 2) return;
    struct Edge { const ColumnRef *build, *probe; };
    uint64_t remaining = budget.limit - budget.used.load();
    if (remaining < 4096 || q.predicates.size() > (remaining - 512) / (2 * sizeof(Edge))) return;
    Scratch scratch(budget, 512);
    std::pmr::vector<Edge> edges(&budget); edges.reserve(2 * q.predicates.size());
    auto integer = [&](const ColumnRef &column) {
        auto type = column.Type()->type_num();
        return column.tableIdx >= 0 && size_t(column.tableIdx) < q.tables.size() &&
            (type == INT32_TYPE || type == INT64_TYPE);
    };
    for (const auto &e : q.predicates) {
        if (e->kind != Expression::EQ || !e->left || !e->right ||
            e->left->kind != Expression::COLUMN || e->right->kind != Expression::COLUMN ||
            !e->left->column || !e->right->column) continue;
        const auto &a = *e->left->column, &b = *e->right->column;
        if (!integer(a) || !integer(b) || a.tableIdx == b.tableIdx) continue;
        edges.push_back({&a, &b}); edges.push_back({&b, &a});
    }
    std::sort(edges.begin(), edges.end(), [&](const Edge &a, const Edge &b) {
        return rows[a.build->tableIdx].size() < rows[b.build->tableIdx].size();
    });
    for (const auto &edge : edges) {
        const auto &build = rows[edge.build->tableIdx]; auto &probe = rows[edge.probe->tableIdx];
        if (build.size() > 64 || probe.size() / 4 <= build.size()) continue;
        if (build.empty()) { probe.clear(); continue; }
        std::array<int64_t, 64> keys{}; size_t count = 0; bool supported = true;
        for (uint64_t id : build) {
            const auto &rg = q.tables[edge.build->tableIdx]->GetRowGroup(id >> 32);
            if (rg.columns[edge.build->columnIdx]->type != ColumnDataBase::RAW_COLUMN_DATA) { supported = false; break; }
            keys[count++] = int64_t(Read(rg, *edge.build, uint32_t(id)).number);
        }
        if (!supported) continue;
        std::sort(keys.begin(), keys.begin() + count);
        count = std::unique(keys.begin(), keys.begin() + count) - keys.begin();
        int64_t first = keys[0], last = keys[count - 1];
        Wide width = Wide(last) - first + 1; uint64_t bits = 0;
        if (width <= 64) for (size_t i = 0; i < count; ++i) bits |= uint64_t(1) << uint64_t(Wide(keys[i]) - first);
        // Avoid rescanning a probe whose complete physical range is already
        // covered, such as an unfiltered nation-to-supplier equality.
        bool covered = width == Wide(count);
        const auto &table = *q.tables[edge.probe->tableIdx];
        for (int g = 0; g < table.RowGroupCount(); ++g) {
            const auto &rg = table.GetRowGroup(g); if (!rg.size) continue;
            auto data = rg.columns[edge.probe->columnIdx].get(); int64_t low, high;
            if (data->type != ColumnDataBase::RAW_COLUMN_DATA) { supported = false; break; }
            if (edge.probe->Type()->type_num() == INT32_TYPE) {
                auto raw = static_cast<const RawColumnData<Int32Type> *>(data); low = raw->minValue; high = raw->maxValue;
            } else {
                auto raw = static_cast<const RawColumnData<Int64Type> *>(data); low = raw->minValue; high = raw->maxValue;
            }
            covered &= low >= first && high <= last;
        }
        if (!supported || covered) continue;
        size_t keep = 0, begin = 0;
        while (begin < probe.size()) {
            uint64_t group = probe[begin] >> 32; size_t end = begin + 1;
            while (end < probe.size() && (probe[end] >> 32) == group) ++end;
            const auto &rg = table.GetRowGroup(group);
            auto raw = static_cast<const RawColumnDataBase *>(rg.columns[edge.probe->columnIdx].get());
            auto apply = [&]<class T, bool Bitmap>() {
                auto values = reinterpret_cast<const T *>(raw->values);
                for (size_t i = begin; i < end; ++i) {
                    uint64_t id = probe[i]; int64_t key = values[uint32_t(id)]; bool found;
                    if constexpr (Bitmap) {
                        uint64_t delta = uint64_t(key) - uint64_t(first);
                        found = delta < uint64_t(width) && (bits >> delta & 1);
                    } else found = std::binary_search(keys.begin(), keys.begin() + count, key);
                    if (found) probe[keep++] = id;
                }
            };
            auto decode = [&]<class T>() {
                if (width <= 64) apply.template operator()<T, true>();
                else apply.template operator()<T, false>();
            };
            switch (raw->bytesPerValue) {
            case 1: decode.template operator()<int8_t>(); break;
            case 2: decode.template operator()<int16_t>(); break;
            case 4: decode.template operator()<int32_t>(); break;
            case 8: decode.template operator()<int64_t>(); break;
            default: throw std::runtime_error("Invalid integer join storage width");
            }
            begin = end;
        }
        probe.resize(keep);
    }
}
}
}
