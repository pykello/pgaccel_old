#include "executor_result.hpp"
namespace pgaccel
{
std::optional<Result<QueryOutput>> ExecuteDistinct(const QueryDesc &q, bool avx, bool parallel)
{
    if (q.grouped || q.aggregateClauses.size() != 1 ||
        q.aggregateClauses[0].type != AggregateClause::AGGREGATE_COUNT_DISTINCT ||
        !q.aggregateClauses[0].columnRef)
        return {};
    Budget budget(q.memoryBudgetBytes);
    auto &table = *q.tables[0];
    auto column = *q.aggregateClauses[0].columnRef;
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    using Partitions = std::vector<Set>;
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    auto task = [&](int worker)
    {
        Partitions sets;
        for (int i = 0; i < workers; ++i)
            sets.emplace_back(&budget);
        auto insert = [&](Cell value) { sets[Hash{}(value) % workers].insert(value); };
        for (int g = worker; g < table.RowGroupCount(); g += workers)
        {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count)
                continue;
            auto selection = count == rg.size ? nullptr : bitmap;
            auto data = rg.columns[column.columnIdx].get();
            if (data->type == ColumnDataBase::DICT_COLUMN_DATA)
            {
                auto dict = static_cast<const DictColumnDataBase *>(data);
                Scratch scratch(budget, dict->dictSize());
                std::vector<uint8_t> seen(dict->dictSize(), 0);
                Selected(rg, selection, [&](int row) { seen[Code(dict, row)] = 1; });
                for (int code = 0; code < dict->dictSize(); ++code)
                    if (seen[code])
                        insert(Label(dict, code, column.Type()->type_num()));
            }
            else
                Selected(rg, selection, [&](int row) { insert(Read(rg, column, row)); });
        }
        return sets;
    };
    auto launch = workers == 1 ? std::launch::deferred : std::launch::async;
    std::vector<std::future<Partitions>> scans;
    for (int w = 0; w < workers; ++w)
        scans.push_back(std::async(launch, task, w));
    std::vector<Partitions> partials;
    for (auto &scan : scans)
        partials.push_back(scan.get());
    auto merge = [&](int owner)
    {
        Set result(&budget);
        for (auto &worker : partials)
            result.merge(worker[owner]);
        return uint64_t(result.size());
    };
    std::vector<std::future<uint64_t>> merges;
    for (int owner = 0; owner < workers; ++owner)
        merges.push_back(std::async(launch, merge, owner));
    uint64_t total = 0;
    for (auto &merge : merges)
        total += merge.get();
    QueryResults result(q, budget);
    result.add({{Cell{Wide(total)}}});
    return Result<QueryOutput>(result.finish());
}
} // namespace pgaccel
