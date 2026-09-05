#include "executor_result.hpp"
#include "executor_integer_set.hpp"
#include "column_index.hpp"
#include <bit>
namespace pgaccel
{
namespace
{
Result<QueryOutput> IndexedDistinct(const QueryDesc &q, const GlobalDictionary &index,
                                    bool avx, bool parallel)
{
    Budget budget(q.memoryBudgetBytes);
    uint64_t total = index.size();
    if (!q.filterClauses.empty()) {
        const auto &table = *q.tables[0];
        const auto &column = *q.aggregateClauses[0].columnRef;
        auto filter = CreateFilterNode(q.filterClauses, avx);
        int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
        size_t words = (index.size() + 63) / 64;
        Scratch metadata(budget, workers * sizeof(std::pmr::vector<uint64_t>));
        auto partials = RunTasks(workers, [&](int worker) {
            std::pmr::vector<uint64_t> seen(words, 0, &budget);
            for (int g = worker; g < table.RowGroupCount(); g += workers) {
                const auto &rg = table.GetRowGroup(g);
                uint8_t bitmap[RowGroupSize / 8];
                int count = filter->ExecuteSet(rg, bitmap);
                if (!count) continue;
                auto data = rg.columns[column.columnIdx].get();
                auto codes = index.Codes(g);
                auto dictionary = data->type == ColumnDataBase::DICT_COLUMN_DATA
                    ? static_cast<const DictColumnDataBase *>(data) : nullptr;
                Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
                    uint32_t id = codes[dictionary ? Code(dictionary, row) : row];
                    seen[id / 64] |= uint64_t(1) << (id % 64);
                });
            }
            return seen;
        });
        auto counts = RunTasks(workers, [&](int worker) {
            uint64_t count = 0;
            for (size_t word = worker; word < words; word += workers) {
                uint64_t bits = 0;
                for (const auto &seen : partials) bits |= seen[word];
                count += std::popcount(bits);
            }
            return count;
        });
        total = std::accumulate(counts.begin(), counts.end(), uint64_t(0));
    }
    QueryResults result(q, budget);
    result.add({{Cell{Wide(total)}}});
    return result.finish();
}

// Partition raw values before building exact sets so each final set fits a
// smaller cache footprint and no worker-local set union is necessary.
Result<QueryOutput> RadixIntegerDistinct(const QueryDesc &q, bool avx, bool parallel)
{
    constexpr size_t partitions = 1024;
    Budget budget(q.memoryBudgetBytes);
    auto &table = *q.tables[0];
    const auto &column = *q.aggregateClauses[0].columnRef;
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    struct Input
    {
        std::pmr::vector<uint64_t> values;
        std::array<size_t, partitions> offsets{};
        explicit Input(Budget &budget) : values(&budget) {}
    };
    Scratch metadata(budget, (2 * workers + 1) * sizeof(Input));
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    auto partition = [](uint64_t value) { return IntegerHash{}(value) >> 54; };
    auto scan = [&](int worker)
    {
        Input input(budget);
        if (!filter)
        {
            size_t rows = 0;
            for (int group = worker; group < table.RowGroupCount(); group += workers)
                rows += table.GetRowGroup(group).size;
            input.values.reserve(rows);
        }
        for (int group = worker; group < table.RowGroupCount(); group += workers)
        {
            const auto &rg = table.GetRowGroup(group);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count)
                continue;
            auto selection = count == rg.size ? nullptr : bitmap;
            auto raw = static_cast<const RawColumnDataBase *>(rg.columns[column.columnIdx].get());
            auto read = [&]<class T>()
            {
                auto values = reinterpret_cast<const T *>(raw->values);
                Selected(rg, selection, [&](int row) {
                    uint64_t value = uint64_t(int64_t(values[row]));
                    input.values.push_back(value);
                    ++input.offsets[partition(value)];
                });
            };
            switch (raw->bytesPerValue)
            {
            case 1: read.template operator()<int8_t>(); break;
            case 2: read.template operator()<int16_t>(); break;
            case 4: read.template operator()<int32_t>(); break;
            case 8: read.template operator()<int64_t>(); break;
            }
        }
        return input;
    };
    auto inputs = RunTasks(workers, scan);
    std::array<size_t, partitions + 1> starts{};
    size_t total = 0;
    for (size_t part = 0; part < partitions; ++part)
    {
        starts[part] = total;
        for (auto &input : inputs)
        {
            size_t count = input.offsets[part];
            input.offsets[part] = total;
            total += count;
        }
    }
    starts[partitions] = total;
    std::pmr::vector<uint64_t> ordered(total, 0, &budget);
    RunTasks(workers, [&](int worker) {
        auto &input = inputs[worker];
        for (uint64_t value : input.values)
            ordered[input.offsets[partition(value)]++] = value;
        std::pmr::vector<uint64_t> empty(&budget);
        input.values.swap(empty);
        return 0;
    });
    auto counts = RunTasks(workers, [&](int worker) {
        uint64_t count = 0;
        for (size_t part = worker; part < partitions; part += workers)
        {
            IntegerSet values(budget);
            for (size_t index = starts[part]; index < starts[part + 1]; ++index)
                values.insert(ordered[index]);
            count += values.size();
        }
        return count;
    });
    QueryResults result(q, budget);
    result.add({{Cell{Wide(std::accumulate(counts.begin(), counts.end(), uint64_t(0)))}}});
    return result.finish();
}

Result<QueryOutput> IntegerDistinct(const QueryDesc &q, bool avx, bool parallel)
{
    auto &table = *q.tables[0];
    const auto &column = *q.aggregateClauses[0].columnRef;
    uint64_t rows = 0;
    bool raw = true;
    for (int group = 0; group < table.RowGroupCount(); ++group)
    {
        const auto &rg = table.GetRowGroup(group);
        rows += rg.size;
        raw &= rg.columns[column.columnIdx]->type == ColumnDataBase::RAW_COLUMN_DATA;
    }
    if (raw && rows >= (1 << 20))
        return RadixIntegerDistinct(q, avx, parallel);
    Budget budget(q.memoryBudgetBytes);
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
    if (auto index = q.tables[0]->Dictionary(q.aggregateClauses[0].columnRef->columnIdx))
        return IndexedDistinct(q, *index, avx, parallel);
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
