#include "executor_result.hpp"

namespace pgaccel {
Result<QueryOutput> ExecuteFilteredCount(const QueryDesc &q, bool avx, bool parallel)
{
    Budget budget(q.memoryBudgetBytes);
    const auto &table = *q.tables[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    auto filter = CreateFilterNode(q.filterClauses, avx);
    filter->Prepare(table, budget, workers);
    auto partials = RunTasks(workers, [&](int worker) {
        uint64_t count = 0;
        for (int g = worker; g < table.RowGroupCount(); g += workers)
            count += filter->ExecuteCount(table.GetRowGroup(g));
        return count;
    });
    uint64_t count = std::accumulate(partials.begin(), partials.end(), uint64_t(0));
    QueryResults result(q, budget);
    result.add({{Cell{Wide(count)}}});
    return result.finish();
}
} // namespace pgaccel
