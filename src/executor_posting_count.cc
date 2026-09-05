#include "executor_result.hpp"
#include "executor_group_key.hpp"
#include "column_index.hpp"

namespace pgaccel {
std::optional<Result<QueryOutput>> ExecutePostingGroupedCount(const QueryDesc &q, bool parallel)
{
    if (!q.grouped || !q.filterClauses.empty()) return {};
    for (const auto &a : q.aggregateClauses)
        if (a.type != AggregateClause::AGGREGATE_PROJECT &&
            (a.type != AggregateClause::AGGREGATE_COUNT ||
             (a.expression && a.expression->kind != Expression::COLUMN))) return {};
    Budget budget(q.memoryBudgetBytes);
    GroupKeys keys(q, &budget);
    if (keys.identity.size() != 1 || keys.identity[0]->kind != Expression::COLUMN) return {};
    const auto &table = *q.tables[0];
    auto index = table.Dictionary(keys.identity[0]->column->columnIdx);
    if (!index || !index->HasDensePostings()) return {};
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    std::pmr::vector<uint64_t> starts(&budget);
    for (int g = 0; g <= table.RowGroupCount(); ++g) starts.push_back(index->RowOffset(g));
    auto offsets = index->PostingOffsets();
    auto partials = RunTasks(workers, [&](int worker) {
        QueryDesc local = q; local.offset = 0;
        if (q.limit) local.limit = *q.limit + q.offset;
        QueryResults result(local, budget);
        size_t begin = index->size() * worker / workers, end = index->size() * (worker + 1) / workers;
        for (size_t id = begin; id < end; ++id) {
            uint64_t count = offsets[id + 1] - offsets[id];
            result.addLazy([&](size_t output) {
                const auto &a = q.aggregateClauses[output];
                if (a.type == AggregateClause::AGGREGATE_COUNT) return FinalCell{Cell{Wide(count)}};
                uint32_t position = index->PostingRows(id).front();
                int group = std::upper_bound(starts.begin(), starts.end(), position) - starts.begin() - 1;
                auto value = Evaluate(*a.expression, table.GetRowGroup(group), position - starts[group]);
                return FinalCell{value, a.expression->type.get()};
            });
        }
        return result.takeCandidates();
    });
    QueryResults result(q, budget);
    for (auto &worker : partials) for (auto &row : worker) result.add(std::move(row));
    return result.finish();
}
} // namespace pgaccel
