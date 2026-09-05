#include "executor_dense.hpp"
#include "executor_result.hpp"
namespace pgaccel
{
Result<QueryOutput> ExecuteModern(const QueryDesc &q, bool avx, bool parallel)
{
    try
    {
        Budget budget(q.memoryBudgetBytes);
        auto filter =
            q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
        auto &table = *q.tables[0];
        size_t n = q.aggregateClauses.size();
        bool aggregate = q.grouped;
        bool distinct = false;
        for (auto &a : q.aggregateClauses)
        {
            aggregate |= a.type != AggregateClause::AGGREGATE_PROJECT;
            distinct |= a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT;
        }
        QueryDesc aggregates = q;
        aggregates.aggregateClauses.clear();
        std::vector<int> stateIndex(n, -1);
        for (size_t i = 0; i < n; ++i)
            if (q.aggregateClauses[i].type != AggregateClause::AGGREGATE_PROJECT)
            {
                stateIndex[i] = aggregates.aggregateClauses.size();
                aggregates.aggregateClauses.push_back(q.aggregateClauses[i]);
            }
        const size_t stateCount = aggregates.aggregateClauses.size();
        QueryResults results(q, budget);
        if (!aggregate)
        {
            for (int g = 0; g < table.RowGroupCount(); ++g)
            {
                const auto &rg = table.GetRowGroup(g);
                uint8_t bitmap[RowGroupSize / 8];
                if (filter && !filter->ExecuteSet(rg, bitmap))
                    continue;
                Selected(rg, filter ? bitmap : nullptr,
                         [&](int r)
                         {
                             if (q.limit && q.orderBy.empty() && results.size() >= *q.limit)
                                 return;
                             FinalRow row;
                             row.reserve(n);
                             for (auto &a : q.aggregateClauses)
                                 row.push_back(
                                     {Evaluate(*a.expression, rg, r), a.expression->type.get()});
                             results.add(std::move(row));
                         });
                if (q.limit && q.orderBy.empty() && results.size() >= *q.limit)
                    break;
            }
        }
        else
        {
            int threads = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
            using Partitions = std::vector<Groups>;
            auto task = [&](int thread)
            {
                Groups groups(&budget);
                if (q.groupBy.empty() && !q.grouped)
                    Get(groups, {}, stateCount, budget);
                for (int g = thread; g < table.RowGroupCount(); g += threads)
                {
                    const auto &rg = table.GetRowGroup(g);
                    uint8_t bitmap[RowGroupSize / 8];
                    int selected = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
                    if (!selected)
                        continue;
                    const uint8_t *selection = filter && selected != rg.size ? bitmap : nullptr;
                    if (q.groupBy.empty() && stateCount == 1 &&
                        aggregates.aggregateClauses[0].type ==
                            AggregateClause::AGGREGATE_COUNT_DISTINCT &&
                        aggregates.aggregateClauses[0].columnRef)
                    {
                        const auto &column = *aggregates.aggregateClauses[0].columnRef;
                        auto data = rg.columns[column.columnIdx].get();
                        if (data->type == ColumnDataBase::DICT_COLUMN_DATA)
                        {
                            auto dictionary = static_cast<const DictColumnDataBase *>(data);
                            Scratch scratch(budget, dictionary->dictSize());
                            std::vector<uint8_t> seen(dictionary->dictSize(), 0);
                            Selected(rg, selection,
                                     [&](int row) { seen[Code(dictionary, row)] = 1; });
                            auto &state = Get(groups, {}, stateCount, budget)[0];
                            for (int code = 0; code < dictionary->dictSize(); ++code)
                                if (seen[code])
                                    Insert(state,
                                           Label(dictionary, code, column.Type()->type_num()),
                                           budget);
                            continue;
                        }
                    }
                    const DictColumnDataBase *dict = nullptr;
                    if (!q.groupBy.empty() && rg.columns[q.groupBy[0].columnIdx]->type ==
                                                  ColumnDataBase::DICT_COLUMN_DATA)
                        dict = static_cast<const DictColumnDataBase *>(
                            rg.columns[q.groupBy[0].columnIdx].get());
                    if (dict && DenseAggregate(aggregates, rg, selection, dict, groups, budget))
                        continue;
                    if (dict && !distinct)
                    {
                        // Dense local codes, contiguous accumulators, only selected rows and
                        // visited labels.
                        Scratch scratch(budget,
                                        uint64_t(dict->dictSize()) *
                                            (sizeof(States) + stateCount * sizeof(State) + 8));
                        std::vector<States> local(dict->dictSize());
                        std::vector<int> visited;
                        Selected(rg, selection,
                                 [&](int r)
                                 {
                                     int code = Code(dict, r);
                                     auto &state = local[code];
                                     if (state.empty())
                                     {
                                         state.resize(stateCount);
                                         visited.push_back(code);
                                     }
                                     for (size_t i = 0; i < stateCount; ++i)
                                         Update(state[i], aggregates.aggregateClauses[i], rg, r,
                                                budget);
                                 });
                        for (int code : visited)
                        {
                            auto &state =
                                Get(groups, Label(dict, code, q.groupBy[0].Type()->type_num()),
                                    stateCount, budget);
                            Merge(state, local[code], aggregates, budget);
                        }
                    }
                    else
                    {
                        Scratch scratch(budget,
                                        dict ? uint64_t(dict->dictSize()) * sizeof(States *) : 0);
                        std::vector<States *> cache(dict ? dict->dictSize() : 0, nullptr);
                        States *single =
                            q.groupBy.empty() ? &Get(groups, {}, stateCount, budget) : nullptr;
                        Selected(rg, selection,
                                 [&](int r)
                                 {
                                     States *state = single;
                                     if (dict)
                                     {
                                         int code = Code(dict, r);
                                         state = cache[code];
                                         if (!state)
                                             state = cache[code] = &Get(
                                                 groups,
                                                 Label(dict, code, q.groupBy[0].Type()->type_num()),
                                                 stateCount, budget);
                                     }
                                     else if (!state)
                                         state = &Get(groups, Read(rg, q.groupBy[0], r), stateCount,
                                                      budget);
                                     for (size_t i = 0; i < stateCount; ++i)
                                         Update((*state)[i], aggregates.aggregateClauses[i], rg, r,
                                                budget);
                                 });
                    }
                }
                Partitions partitions;
                for (int owner = 0; owner < threads; ++owner)
                    partitions.emplace_back(&budget);
                for (auto it = groups.begin(); it != groups.end();)
                {
                    int owner = Hash{}(it->first) % threads;
                    partitions[owner].insert(groups.extract(it++));
                }
                return partitions;
            };
            auto launch = threads == 1 ? std::launch::deferred : std::launch::async;
            std::vector<std::future<Partitions>> scans;
            for (int t = 0; t < threads; ++t)
                scans.push_back(std::async(launch, task, t));
            std::vector<Partitions> partials;
            for (auto &scan : scans)
                partials.push_back(scan.get());
            auto combine = [&](int owner)
            {
                Groups groups(&budget);
                for (auto &worker : partials)
                {
                    auto &source = worker[owner];
                    for (auto it = source.begin(); it != source.end();)
                    {
                        auto found = groups.find(it->first);
                        if (found == groups.end())
                            groups.insert(source.extract(it++));
                        else
                        {
                            Merge(found->second, it->second, aggregates, budget);
                            it = source.erase(it);
                        }
                    }
                }
                return groups;
            };
            std::vector<std::future<Groups>> merged;
            for (int owner = 0; owner < threads; ++owner)
                merged.push_back(std::async(launch, combine, owner));
            for (auto &future : merged)
            {
                auto groups = future.get();
                for (auto &entry : groups)
                {
                    FinalRow row;
                    row.reserve(n);
                    for (size_t i = 0; i < n; ++i)
                    {
                        auto &a = q.aggregateClauses[i];
                        State projected;
                        auto &s = stateIndex[i] < 0 ? projected : entry.second[stateIndex[i]];
                        FinalCell v;
                        v.type = a.expression ? a.expression->type.get() : nullptr;
                        switch (a.type)
                        {
                        case AggregateClause::AGGREGATE_PROJECT:
                            v.value = a.expression->kind == Expression::CONSTANT
                                          ? Evaluate(*a.expression, RowGroup{}, 0)
                                          : entry.first;
                            break;
                        case AggregateClause::AGGREGATE_COUNT:
                            v.type = nullptr;
                            v.value.number = s.count;
                            break;
                        case AggregateClause::AGGREGATE_COUNT_DISTINCT:
                            v.type = nullptr;
                            v.value.number = s.distinct ? s.distinct->size() : 0;
                            break;
                        case AggregateClause::AGGREGATE_SUM:
                            v.value.number = s.sum;
                            v.null = !s.count;
                            break;
                        case AggregateClause::AGGREGATE_AVG:
                            v.average = true;
                            v.null = !s.count;
                            if (s.count)
                                v.mean = (long double)s.sum / s.count /
                                         (long double)Power(Scale(v.type));
                            break;
                        default:
                            v.value = s.extreme;
                            v.null = !s.count;
                            break;
                        }
                        row.push_back(v);
                    }
                    results.add(std::move(row));
                }
            }
        }
        return results.finish();
    }
    catch (const std::exception &e)
    {
        return Status::Invalid(e.what());
    }
}
} // namespace pgaccel
