#include "executor_result.hpp"
#include "executor_flat_map.hpp"
#include "executor_integer_set.hpp"
#include <bit>
namespace pgaccel
{
namespace {
template <class Key, class KeyHash>
Result<QueryOutput> GroupedCount(const QueryDesc &q, bool avx, bool parallel)
{
    Budget budget(q.memoryBudgetBytes);
    using Counts = FlatMap<Key, uint64_t, KeyHash, std::equal_to<Key>>;
    auto encode = [](Cell value) -> Key {
        if constexpr (std::is_same_v<Key, Cell>) return value;
        else return uint64_t(value.number);
    };
    using Partitions = std::vector<Counts>;
    auto &table = *q.tables[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    auto scan = [&](int worker)
    {
        Partitions maps;
        for (int i = 0; i < workers; ++i)
            maps.emplace_back(budget);
        // Cache stable indexes, not references into a growing payload vector.
        auto get = [&](Cell value) -> uint64_t
        {
            Key key = encode(value);
            uint32_t owner = (KeyHash{}(key) >> 32) % workers;
            auto [index, added] = maps[owner].get(key, [] { return uint64_t(0); });
            return (uint64_t(owner) << 32) | index;
        };
        auto increment = [&](uint64_t location) {
            ++maps[location >> 32].at(uint32_t(location)).second;
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
                Scratch scratch(budget, dict->dictSize() * sizeof(uint64_t));
                std::vector<uint64_t> cache(dict->dictSize(), UINT64_MAX);
                Selected(rg, selection,
                         [&](int row)
                         {
                             int code = Code(dict, row);
                             if (cache[code] == UINT64_MAX)
                                 cache[code] =
                                     get(Label(dict, code, q.groupBy[0].Type()->type_num()));
                             increment(cache[code]);
                         });
            }
            else
                Selected(rg, selection, [&](int row) { increment(get(Read(rg, q.groupBy[0], row))); });
        }
        return maps;
    };
    auto partials=RunTasks(workers,scan);
    auto finalize = [&](Counts &counts)
    {
        QueryDesc local=q; local.offset=0;
        if(q.limit)local.limit=*q.limit+q.offset;
        QueryResults result(local,budget);
        for (uint32_t index = 0; index < counts.size(); ++index)
        {
            const auto &[stored, count] = counts.at(index);
            Cell key;
            if constexpr (std::is_same_v<Key, Cell>) key = stored;
            else key.number = std::bit_cast<int64_t>(stored);
            result.addLazy([&](size_t i) {
                const auto &a=q.aggregateClauses[i];
                if(a.type==AggregateClause::AGGREGATE_COUNT)return FinalCell{Cell{Wide(count)}};
                return FinalCell{a.columnRef?key:Evaluate(*a.expression,RowGroup{},0),a.expression->type.get()};
            });
        }
        return result.takeCandidates();
    };
    auto merge = [&](int owner)
    {
        Counts result(budget);
        for (auto &worker : partials)
        {
            auto &source = worker[owner];
            source.forEach([&](const Key &key, uint64_t count) {
                auto [index, added] = result.get(key, [=] { return count; });
                if (!added) result.at(index).second += count;
            });
            source.release();
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
} // namespace
std::optional<Result<QueryOutput>> ExecuteGroupedCount(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.grouped || q.groupBy.size() != 1)
        return {};
    for (const auto &a : q.aggregateClauses)
        if (a.type != AggregateClause::AGGREGATE_PROJECT &&
            (a.type != AggregateClause::AGGREGATE_COUNT ||
             (a.expression && a.expression->kind != Expression::COLUMN)))
            return {};
    if (q.groupBy[0].Type()->type_num() == STRING_TYPE)
        return GroupedCount<Cell, Hash>(q, avx, parallel);
    return GroupedCount<uint64_t, IntegerHash>(q, avx, parallel);
}
} // namespace pgaccel
