#include "executor_result.hpp"
#include "executor_buffer.hpp"
#include "executor_group_key.hpp"
#include "executor_flat_map.hpp"
#include <array>

namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteRadixGroupCount(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.grouped || !q.groupBy.empty() || q.groupExpressions.empty() ||
        (q.limit && q.orderBy.empty())) return {};
    for (const auto &a : q.aggregateClauses)
        if (a.type != AggregateClause::AGGREGATE_PROJECT &&
            (a.type != AggregateClause::AGGREGATE_COUNT ||
             (a.expression && a.expression->kind != Expression::COLUMN))) return {};
    Budget budget(q.memoryBudgetBytes);
    GroupKeys keys(q, &budget);
    if (!keys.packed) return {};
    constexpr size_t partitions = 1024;
    const auto &table = *q.tables[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    struct Input {
        std::pmr::vector<GroupKey> keys;
        std::array<size_t, partitions> offsets{};
        explicit Input(Budget &memory) : keys(&memory) {}
    };
    Scratch metadata(budget, (workers * 2 + 1) * sizeof(Input));
    auto partition = [&](const GroupKey &key) { return GroupHash{&keys}(key) >> 54; };
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    const uint64_t inputBudget = (q.memoryBudgetBytes - budget.used.load()) / 2;
    auto inputs = RunTasks(workers, [&](int worker) {
        ScopedValueResource valuesMemory(budget);
        Input input(budget);
        {
            size_t rows = 0;
            for (int g = worker; g < table.RowGroupCount(); g += workers) rows += table.GetRowGroup(g).size;
            // Reserve at most half the remaining budget for filtered input.
            // Capacity does not construct records or touch unselected pages.
            size_t capacity = filter
                ? std::min<uint64_t>(rows, inputBudget / workers / sizeof(GroupKey)) : rows;
            input.keys.reserve(capacity);
        }
        for (int g = worker; g < table.RowGroupCount(); g += workers) {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count) continue;
            keys.prepare(g, count == rg.size ? nullptr : bitmap);
            Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
                auto key = keys.make(g, row);
                input.keys.push_back(key); ++input.offsets[partition(key)];
            });
        }
        return input;
    });
    std::array<size_t, partitions + 1> offsets{};
    for (size_t p = 0; p < partitions; ++p) {
        offsets[p + 1] = offsets[p];
        for (auto &input : inputs) {
            size_t count = input.offsets[p];
            input.offsets[p] = offsets[p + 1];
            offsets[p + 1] += count;
        }
    }
    OverwriteBuffer<GroupKey> ordered(offsets.back(), budget);
    RunTasks(workers, [&](int worker) {
        auto &input = inputs[worker];
        for (auto key : input.keys) ordered[input.offsets[partition(key)]++] = key;
        std::pmr::vector<GroupKey> empty(&budget); input.keys.swap(empty);
        return 0;
    });
    auto candidates = RunTasks(workers, [&](int worker) {
        ScopedValueResource valuesMemory(budget);
        QueryDesc local = q; local.offset = 0;
        if (q.limit) local.limit = *q.limit + q.offset;
        QueryResults result(local, budget);
        for (size_t p = worker; p < partitions; p += workers) {
            FlatMap<GroupKey, uint64_t, GroupHash, GroupEqual> counts(budget, GroupHash{&keys}, GroupEqual{&keys});
            for (size_t i = offsets[p]; i < offsets[p + 1]; ++i) {
                auto [position, added] = counts.get(ordered[i], [] { return uint64_t(0); });
                ++counts.at(position).second;
            }
            counts.forEach([&](const GroupKey &key, uint64_t count) {
                result.addLazy([&](size_t output) {
                    const auto &a = q.aggregateClauses[output];
                    if (a.type == AggregateClause::AGGREGATE_COUNT) return FinalCell{Cell{Wide(count)}};
                    return FinalCell{keys.value(key, a.expression), a.expression->type.get()};
                });
            });
        }
        return result.takeCandidates();
    });
    QueryResults result(q, budget);
    for (auto &worker : candidates) for (auto &row : worker) result.add(std::move(row));
    return result.finish();
}
} // namespace pgaccel
