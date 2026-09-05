#include "executor_result.hpp"

namespace pgaccel
{
std::optional<Result<QueryOutput>> ExecuteProjection(const QueryDesc &q, bool avx, bool parallel)
{
    if (q.grouped || !q.groupBy.empty() || !q.having.empty())
        return {};
    for (const auto &a : q.aggregateClauses)
        if (a.type != AggregateClause::AGGREGATE_PROJECT)
            return {};
    // Retain the existing input-prefix behavior for unordered LIMIT queries.
    if (q.limit && q.orderBy.empty())
        return {};
    Budget budget(q.memoryBudgetBytes);
    auto &table = *q.tables[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    if (filter) filter->Prepare(table, budget, workers);
    auto task = [&](int worker)
    {
        ScopedValueResource valueMemory(budget);
        QueryDesc local = q;
        local.offset = 0;
        if (q.limit)
            local.limit = *q.limit + q.offset;
        QueryResults result(local, budget);
        for (int group = worker; group < table.RowGroupCount(); group += workers)
        {
            const auto &rg = table.GetRowGroup(group);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count)
                continue;
            Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row)
            {
                result.addLazy([&](size_t column)
                {
                    const auto &expression = *q.aggregateClauses[column].expression;
                    return FinalCell{Evaluate(expression, rg, row), expression.type.get()};
                });
            });
        }
        return result.takeCandidates();
    };
    auto candidates = RunTasks(workers, task);
    QueryResults result(q, budget);
    for (auto &worker : candidates)
        for (auto &row : worker)
            result.add(std::move(row));
    return Result<QueryOutput>(result.finish());
}
} // namespace pgaccel
