#include "executor_result.hpp"
#include "executor_flat_map.hpp"
#include "executor_integer_set.hpp"
#include <bit>
#include "column_index.hpp"
#include <array>
namespace pgaccel
{
namespace {
template <class Count>
Result<QueryOutput> IndexedGroupedCount(const QueryDesc &q, const GlobalDictionary &index,
                                       bool avx, bool parallel)
{
    constexpr size_t partitions = 1024;
    Budget budget(q.memoryBudgetBytes);
    const auto &table = *q.tables[0];
    const auto &column = q.groupBy[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    unsigned bits = std::bit_width(index.size() ? index.size() - 1 : size_t(0));
    unsigned shift = bits > 10 ? bits - 10 : 0;
    size_t width = size_t(1) << shift;
    struct Input {
        std::pmr::vector<uint32_t> values;
        std::array<size_t, partitions> offsets{};
        explicit Input(Budget &budget) : values(&budget) {}
    };
    Scratch metadata(budget, (2 * workers + 1) * sizeof(Input));
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    auto inputs = RunTasks(workers, [&](int worker) {
        Input input(budget);
        if (!filter) {
            size_t rows = 0;
            for (int g = worker; g < table.RowGroupCount(); g += workers) rows += table.GetRowGroup(g).size;
            input.values.reserve(rows);
        }
        for (int g = worker; g < table.RowGroupCount(); g += workers) {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count) continue;
            auto data = rg.columns[column.columnIdx].get();
            auto dictionary = data->type == ColumnDataBase::DICT_COLUMN_DATA
                ? static_cast<const DictColumnDataBase *>(data) : nullptr;
            auto codes = index.Codes(g);
            Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
                uint32_t id = codes[dictionary ? Code(dictionary, row) : row];
                input.values.push_back(id);
                ++input.offsets[id >> shift];
            });
        }
        return input;
    });
    std::array<size_t, partitions + 1> offsets{};
    for (size_t part = 0; part < partitions; ++part) {
        offsets[part + 1] = offsets[part];
        for (auto &input : inputs) {
            size_t count = input.offsets[part];
            input.offsets[part] = offsets[part + 1];
            offsets[part + 1] += count;
        }
    }
    std::pmr::vector<uint32_t> ordered(offsets.back(), 0, &budget);
    RunTasks(workers, [&](int worker) {
        auto &input = inputs[worker];
        for (uint32_t id : input.values) ordered[input.offsets[id >> shift]++] = id;
        std::pmr::vector<uint32_t> empty(input.values.get_allocator());
        input.values.swap(empty);
        return 0;
    });
    auto candidates = RunTasks(workers, [&](int worker) {
        QueryDesc local = q;
        local.offset = 0;
        if (q.limit) local.limit = *q.limit + q.offset;
        QueryResults results(local, budget);
        for (size_t part = worker; part < partitions; part += workers) {
            if (offsets[part] == offsets[part + 1]) continue;
            size_t base = part * width;
            auto admit = [&](size_t id, Count count) {
                results.addLazy([&](size_t output) {
                    const auto &a = q.aggregateClauses[output];
                    if (a.type == AggregateClause::AGGREGATE_COUNT)
                        return FinalCell{Cell{Wide(count)}};
                    Cell key;
                    if (a.columnRef) {
                        if (column.Type()->type_num() == STRING_TYPE)
                            key = {0, index.Strings()[id], true};
                        else key.number = index.Numbers()[id];
                    } else key = Evaluate(*a.expression, RowGroup{}, 0);
                    return FinalCell{key, a.expression->type.get()};
                });
            };
            size_t range = std::min(width, index.size() - base);
            if (offsets[part + 1] - offsets[part] < range / 8) {
                // Selective filters should not clear and scan the entire domain.
                FlatMap<uint32_t, Count, IntegerHash, std::equal_to<uint32_t>> sparse(budget);
                for (size_t position = offsets[part]; position < offsets[part + 1]; ++position) {
                    auto [entry, added] = sparse.get(ordered[position], [] { return Count(0); });
                    ++sparse.at(entry).second;
                }
                sparse.forEach(admit);
            } else {
                std::pmr::vector<Count> counts(range, 0, &budget);
                for (size_t position = offsets[part]; position < offsets[part + 1]; ++position)
                    ++counts[ordered[position] - base];
                for (size_t i = 0; i < counts.size(); ++i)
                    if (counts[i]) admit(base + i, counts[i]);
            }
        }
        return results.takeCandidates();
    });
    QueryResults results(q, budget);
    for (auto &worker : candidates) for (auto &row : worker) results.add(std::move(row));
    return results.finish();
}

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
    if (auto index = q.tables[0]->Dictionary(q.groupBy[0].columnIdx)) {
        uint64_t rows = 0;
        for (int g = 0; g < q.tables[0]->RowGroupCount(); ++g) rows += q.tables[0]->GetRowGroup(g).size;
        if (rows <= UINT32_MAX) return IndexedGroupedCount<uint32_t>(q, *index, avx, parallel);
        return IndexedGroupedCount<uint64_t>(q, *index, avx, parallel);
    }
    if (q.groupBy[0].Type()->type_num() == STRING_TYPE)
        return GroupedCount<Cell, Hash>(q, avx, parallel);
    return GroupedCount<uint64_t, IntegerHash>(q, avx, parallel);
}
} // namespace pgaccel
