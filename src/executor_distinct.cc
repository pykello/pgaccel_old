#include "executor_result.hpp"
#include "executor_integer_set.hpp"
namespace pgaccel
{
namespace
{
Result<QueryOutput> IntegerDistinct(const QueryDesc &q, bool avx, bool parallel)
{
    Budget budget(q.memoryBudgetBytes);
    auto &table = *q.tables[0];
    const auto &column = *q.aggregateClauses[0].columnRef;
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    using Integers = IntegerSet;
    using Partitions = std::vector<Integers>;
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    auto task = [&](int worker)
    {
        Partitions sets;
        for (int i = 0; i < workers; ++i)
            sets.emplace_back(budget);
        auto insert = [&](int64_t value)
        {
            uint64_t bits = uint64_t(value);
            uint64_t hash = IntegerHash{}(bits);
            sets[(hash >> 32) % workers].insert(bits, hash);
        };
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
                auto dict = static_cast<const DictColumnData<DateType> *>(data);
                Scratch scratch(budget, dict->dictSize());
                std::vector<uint8_t> seen(dict->dictSize(), 0);
                Selected(rg, selection, [&](int row) { seen[Code(dict, row)] = 1; });
                for (int code = 0; code < dict->dictSize(); ++code)
                    if (seen[code])
                        insert(dict->dict[code]);
            }
            else
            {
                auto raw = static_cast<const RawColumnDataBase *>(data);
                auto scan = [&]<class T>()
                {
                    auto values = reinterpret_cast<const T *>(raw->values);
                    Selected(rg, selection, [&](int row) { insert(values[row]); });
                };
                switch (raw->bytesPerValue)
                {
                case 1: scan.template operator()<int8_t>(); break;
                case 2: scan.template operator()<int16_t>(); break;
                case 4: scan.template operator()<int32_t>(); break;
                case 8: scan.template operator()<int64_t>(); break;
                }
            }
        }
        return sets;
    };
    auto partials = RunTasks(workers, task);
    auto merge = [&](int owner)
    {
        Integers result(budget);
        for (auto &worker : partials)
        {
            auto &source = worker[owner];
            if (result.empty())
                result.swap(source);
            else
            {
                source.forEach([&](uint64_t value) { result.insert(value); });
                source.release();
            }
        }
        return uint64_t(result.size());
    };
    auto counts = RunTasks(workers, merge);
    uint64_t total = std::accumulate(counts.begin(), counts.end(), uint64_t(0));
    QueryResults result(q, budget);
    result.add({{Cell{Wide(total)}}});
    return result.finish();
}
} // namespace

std::optional<Result<QueryOutput>> ExecuteDistinct(const QueryDesc &q, bool avx, bool parallel)
{
    if (q.grouped || q.aggregateClauses.size() != 1 ||
        q.aggregateClauses[0].type != AggregateClause::AGGREGATE_COUNT_DISTINCT ||
        !q.aggregateClauses[0].columnRef)
        return {};
    if (q.aggregateClauses[0].columnRef->Type()->type_num() != STRING_TYPE)
        return IntegerDistinct(q, avx, parallel);
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
    auto partials=RunTasks(workers,task);
    auto merge = [&](int owner)
    {
        Set result(&budget);
        for (auto &worker : partials)
            result.merge(worker[owner]);
        return uint64_t(result.size());
    };
    auto merges=RunTasks(workers,merge);
    uint64_t total = 0;
    for (auto &merge : merges)
        total += merge;
    QueryResults result(q, budget);
    result.add({{Cell{Wide(total)}}});
    return Result<QueryOutput>(result.finish());
}
} // namespace pgaccel
