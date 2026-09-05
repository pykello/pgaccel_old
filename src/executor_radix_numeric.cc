#include "executor_result.hpp"
#include "executor_buffer.hpp"
#include "executor_group_key.hpp"
#include "executor_flat_map.hpp"
#include <array>

namespace pgaccel {
namespace {
struct NumericInput {
    const uint8_t *values;
    const int32_t *labels = nullptr;
    int width;
    explicit NumericInput(const ColumnDataBase *data) {
        if (data->type == ColumnDataBase::DICT_COLUMN_DATA) {
            auto dictionary = static_cast<const DictColumnData<DateType> *>(data);
            values = dictionary->values; labels = dictionary->dict.data();
            width = dictionary->bytesPerValue();
        } else {
            auto raw = static_cast<const RawColumnDataBase *>(data);
            values = raw->values; width = raw->bytesPerValue;
        }
    }
    int64_t read(int row) const {
        if (labels) return labels[width == 1 ? values[row] : reinterpret_cast<const uint16_t *>(values)[row]];
        switch (width) {
        case 1: return reinterpret_cast<const int8_t *>(values)[row];
        case 2: return reinterpret_cast<const int16_t *>(values)[row];
        case 4: return reinterpret_cast<const int32_t *>(values)[row];
        case 8: return reinterpret_cast<const int64_t *>(values)[row];
        }
        throw std::runtime_error("Invalid numeric input width");
    }
};
// A two-word representation avoids 16-byte alignment padding in each
// hash payload while retaining the complete checked 128-bit sum.
struct NumericWideSum {
    std::array<uint64_t, 2> words{};
    Wide value() const { return std::bit_cast<Wide>(words); }
    void add(int64_t number) { words = std::bit_cast<std::array<uint64_t, 2>>(Add(value(), number)); }
};
static_assert(sizeof(NumericWideSum) == 16 && alignof(NumericWideSum) == 8);
template<size_t N>
Result<QueryOutput> RadixNumeric(const QueryDesc &q, GroupKeys &keys, Budget &budget,
                                const std::vector<const AggregateClause *> &numeric, bool avx, bool parallel)
{
    constexpr size_t partitions = 1024;
    struct Record { GroupKey key; std::array<int64_t, N> values; };
    struct State { uint64_t count = 0; std::array<NumericWideSum, N> sums{}; };
    static_assert(sizeof(State) == 8 + 16 * N);
    struct Input {
        std::pmr::vector<Record> records;
        std::array<size_t, partitions> offsets{};
        explicit Input(Budget &memory) : records(&memory) {}
    };
    const auto &table = *q.tables[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    Scratch metadata(budget, (workers * 2 + 1) * sizeof(Input));
    auto partition = [&](const GroupKey &key) { return GroupHash{&keys}(key) >> 54; };
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    const uint64_t inputBudget = (q.memoryBudgetBytes - budget.used.load()) / 2;
    auto inputs = RunTasks(workers, [&](int worker) {
        ScopedValueResource valuesMemory(budget);
        Input input(budget);
        {
            size_t rows = 0;
            for (int g = worker; g < table.RowGroupCount(); g += workers) rows += table.GetRowGroup(g).size;
            // Reserve at most half the remaining budget for filtered input.
            // Capacity does not construct records or touch unselected pages.
            size_t capacity = filter
                ? std::min<uint64_t>(rows, inputBudget / workers / sizeof(Record)) : rows;
            input.records.reserve(capacity);
        }
        for (int g = worker; g < table.RowGroupCount(); g += workers) {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count) continue;
            std::pmr::vector<NumericInput> readers(&budget);
            readers.reserve(N);
            for (auto a : numeric) readers.emplace_back(rg.columns[a->expression->column->columnIdx].get());
            keys.prepare(g, count == rg.size ? nullptr : bitmap);
            Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
                Record record{keys.make(g, row), {}};
                for (size_t i = 0; i < N; ++i) record.values[i] = readers[i].read(row);
                ++input.offsets[partition(record.key)]; input.records.push_back(record);
            });
        }
        return input;
    });
    std::array<size_t, partitions + 1> offsets{};
    for (size_t p = 0; p < partitions; ++p) {
        offsets[p + 1] = offsets[p];
        for (auto &input : inputs) {
            size_t count = input.offsets[p]; input.offsets[p] = offsets[p + 1];
            offsets[p + 1] += count;
        }
    }
    OverwriteBuffer<Record> ordered(offsets.back(), budget);
    RunTasks(workers, [&](int worker) {
        auto &input = inputs[worker];
        for (const auto &record : input.records) ordered[input.offsets[partition(record.key)]++] = record;
        std::pmr::vector<Record> empty(&budget); input.records.swap(empty);
        return 0;
    });
    auto candidates = RunTasks(workers, [&](int worker) {
        ScopedValueResource valuesMemory(budget);
        QueryDesc local = q; local.offset = 0;
        if (q.limit) local.limit = *q.limit + q.offset;
        QueryResults result(local, budget);
        for (size_t p = worker; p < partitions; p += workers) {
            FlatMap<GroupKey, State, GroupHash, GroupEqual> groups(budget, GroupHash{&keys}, GroupEqual{&keys});
            for (size_t i = offsets[p]; i < offsets[p + 1]; ++i) {
                const auto &record = ordered[i];
                auto [position, added] = groups.get(record.key, [] { return State{}; });
                auto &state = groups.at(position).second; ++state.count;
                for (size_t j = 0; j < N; ++j) state.sums[j].add(record.values[j]);
            }
            groups.forEach([&](const GroupKey &key, const State &state) {
                result.addLazy([&](size_t output) {
                    const auto &a = q.aggregateClauses[output];
                    if (a.type == AggregateClause::AGGREGATE_PROJECT)
                        return FinalCell{keys.value(key, a.expression), a.expression->type.get()};
                    if (a.type == AggregateClause::AGGREGATE_COUNT) return FinalCell{Cell{Wide(state.count)}};
                    size_t slot = std::find(numeric.begin(), numeric.end(), &a) - numeric.begin();
                    FinalCell cell{Cell{state.sums[slot].value()}, a.expression->type.get()};
                    if (a.type == AggregateClause::AGGREGATE_AVG) {
                        cell.average = true;
                        cell.mean = (long double)state.sums[slot].value() / state.count / (long double)Power(Scale(cell.type));
                    }
                    return cell;
                });
            });
        }
        return result.takeCandidates();
    });
    QueryResults result(q, budget);
    for (auto &worker : candidates) for (auto &row : worker) result.add(std::move(row));
    return result.finish();
}
} // namespace
std::optional<Result<QueryOutput>> ExecuteRadixNumeric(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.grouped || !q.groupBy.empty() || q.groupExpressions.empty() ||
        (q.limit && q.orderBy.empty())) return {};
    std::vector<const AggregateClause *> numeric;
    for (const auto &a : q.aggregateClauses) {
        if (a.type == AggregateClause::AGGREGATE_PROJECT) continue;
        if (a.type == AggregateClause::AGGREGATE_COUNT &&
            (!a.expression || a.expression->kind == Expression::COLUMN)) continue;
        if ((a.type != AggregateClause::AGGREGATE_SUM && a.type != AggregateClause::AGGREGATE_AVG) ||
            !a.expression || a.expression->kind != Expression::COLUMN ||
            a.expression->type->type_num() == STRING_TYPE) return {};
        numeric.push_back(&a);
    }
    if (numeric.empty() || numeric.size() > 3) return {};
    // Nearly unique keys already have a cheaper exact singleton path.
    for (const auto &expression : q.groupExpressions)
        if (expression->kind == Expression::COLUMN)
            if (auto index = q.tables[0]->Dictionary(expression->column->columnIdx);
                index && index->HasSparsePostings()) return {};
    Budget budget(q.memoryBudgetBytes);
    GroupKeys keys(q, &budget);
    if (!keys.packed) return {};
    switch (numeric.size()) {
    case 1: return RadixNumeric<1>(q, keys, budget, numeric, avx, parallel);
    case 2: return RadixNumeric<2>(q, keys, budget, numeric, avx, parallel);
    case 3: return RadixNumeric<3>(q, keys, budget, numeric, avx, parallel);
    }
    return {};
}
} // namespace pgaccel
