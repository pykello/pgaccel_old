#include "executor_result.hpp"
namespace pgaccel
{
std::optional<Result<QueryOutput>> ExecuteGroupedCount(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.grouped || q.groupBy.size() != 1)
        return {};
    for (const auto &a : q.aggregateClauses)
        if (a.type != AggregateClause::AGGREGATE_PROJECT &&
            (a.type != AggregateClause::AGGREGATE_COUNT ||
             (a.expression && a.expression->kind != Expression::COLUMN)))
            return {};
    Budget budget(q.memoryBudgetBytes);
    using Counts = std::pmr::unordered_map<Cell, uint64_t, Hash>;
    using Partitions = std::vector<Counts>;
    auto &table = *q.tables[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    auto scan = [&](int worker)
    {
        Partitions maps;
        for (int i = 0; i < workers; ++i)
            maps.emplace_back(&budget);
        auto get = [&](Cell key) -> uint64_t &
        {
            auto &map = maps[Hash{}(key) % workers];
            return map.try_emplace(key, 0).first->second;
        };
        for (int g = worker; g < table.RowGroupCount(); g += workers)
        {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int selected = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!selected)
                continue;
            auto selection = selected == rg.size ? nullptr : bitmap;
            auto data = rg.columns[q.groupBy[0].columnIdx].get();
            if (data->type == ColumnDataBase::DICT_COLUMN_DATA)
            {
                auto dict = static_cast<const DictColumnDataBase *>(data);
                Scratch scratch(budget, dict->dictSize() * sizeof(uint64_t *));
                std::vector<uint64_t *> cache(dict->dictSize(), nullptr);
                Selected(rg, selection,
                         [&](int row)
                         {
                             int code = Code(dict, row);
                             if (!cache[code])
                                 cache[code] =
                                     &get(Label(dict, code, q.groupBy[0].Type()->type_num()));
                             ++*cache[code];
                         });
            }
            else
                Selected(rg, selection, [&](int row) { ++get(Read(rg, q.groupBy[0], row)); });
        }
        return maps;
    };
    auto partials=RunTasks(workers,scan);
    auto finalize = [&](Counts &counts)
    {
        QueryDesc local=q; local.offset=0;
        if(q.limit)local.limit=*q.limit+q.offset;
        QueryResults result(local,budget);
        for (const auto &[key, count] : counts)
        {
            FinalRow row;
            row.reserve(q.aggregateClauses.size());
            for (const auto &a : q.aggregateClauses)
            {
                if (a.type == AggregateClause::AGGREGATE_COUNT)
                    row.push_back({Cell{Wide(count)}});
                else
                    row.push_back({a.columnRef ? key : Evaluate(*a.expression, RowGroup{}, 0),
                                   a.expression->type.get()});
            }
            result.add(std::move(row));
        }
        return result.takeCandidates();
    };
    auto merge = [&](int owner)
    {
        Counts result(&budget);
        for (auto &worker : partials)
        {
            auto &source = worker[owner];
            for (auto it = source.begin(); it != source.end();)
            {
                auto existing = result.find(it->first);
                if (existing == result.end())
                    result.insert(source.extract(it++));
                else
                {
                    existing->second += it->second;
                    it = source.erase(it);
                }
            }
        }
        return finalize(result);
    };
    auto merges=RunTasks(workers,merge);
    QueryResults result(q, budget);
    for (auto &merge : merges)
    {
        for(auto &row:merge) result.add(std::move(row));
    }
    return Result<QueryOutput>(result.finish());
}
} // namespace pgaccel
