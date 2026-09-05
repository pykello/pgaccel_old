#include "executor_result.hpp"
#include "executor_buffer.hpp"
#include "executor_flat_map.hpp"
#include "executor_dictionary_expression.hpp"
#include "column_index.hpp"
#include <array>
#include <bit>

namespace pgaccel {
namespace {
// Group selected source values before evaluating a deterministic expression.
// Weights are query-local row counts, never persistent aggregate answers.
Result<QueryOutput> DictionaryWeighted(const QueryDesc &q, const ColumnRef &source,
    const GlobalDictionary &index, bool needMinimum, bool needMaximum,
    bool avx, bool parallel)
{
    constexpr size_t partitions = 1024;
    struct State {
        Wide sum = 0;
        uint64_t count = 0;
        uint32_t representative = 0, minimum = 0, maximum = 0;
    };
    using Map = FlatMap<Cell, State, Hash, std::equal_to<Cell>>;
    struct Input {
        std::pmr::vector<uint32_t> values;
        std::array<size_t, partitions> offsets{};
        explicit Input(Budget &budget) : values(&budget) {}
    };
    Budget budget(q.memoryBudgetBytes);
    ScopedValueResource valuesMemory(budget);
    const auto &table = *q.tables[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    unsigned bits = std::bit_width(index.size() ? index.size() - 1 : size_t(0));
    unsigned shift = bits > 10 ? bits - 10 : 0;
    size_t width = size_t(1) << shift;
    Scratch metadata(budget, (2 * workers + 1) * sizeof(Input));
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    uint64_t inputBudget = (q.memoryBudgetBytes - budget.used.load()) / 2;
    auto inputs = RunTasks(workers, [&](int worker) {
        ScopedValueResource localMemory(budget);
        Input input(budget);
        size_t rows = 0;
        for (int g = worker; g < table.RowGroupCount(); g += workers) rows += table.GetRowGroup(g).size;
        input.values.reserve(filter ? std::min<uint64_t>(rows, inputBudget / workers / sizeof(uint32_t)) : rows);
        for (int g = worker; g < table.RowGroupCount(); g += workers) {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count) continue;
            auto dictionary = static_cast<const DictColumnDataBase *>(rg.columns[source.columnIdx].get());
            auto codes = index.Codes(g);
            Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
                uint32_t id = codes[Code(dictionary, row)];
                input.values.push_back(id); ++input.offsets[id >> shift];
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
    OverwriteBuffer<uint32_t> ordered(offsets.back(), budget);
    RunTasks(workers, [&](int worker) {
        auto &input = inputs[worker];
        for (uint32_t id : input.values) ordered[input.offsets[id >> shift]++] = id;
        std::pmr::vector<uint32_t> empty(&budget); input.values.swap(empty);
        return 0;
    });
    auto strings = index.Strings();
    auto partials = RunTasks(workers, [&](int worker) {
        ScopedValueResource localMemory(budget);
        std::vector<Map> maps;
        maps.reserve(workers);
        for (int i = 0; i < workers; ++i) maps.emplace_back(budget);
        for (size_t p = worker; p < partitions; p += workers) {
            if (offsets[p] == offsets[p + 1]) continue;
            size_t base = p * width, range = std::min(width, index.size() - base);
            std::pmr::vector<uint64_t> counts(range, 0, &budget);
            for (size_t i = offsets[p]; i < offsets[p + 1]; ++i) ++counts[ordered[i] - base];
            for (size_t i = 0; i < range; ++i) {
                uint64_t count = counts[i];
                if (!count) continue;
                uint32_t id = base + i;
                Cell input{0, strings[id], true};
                // Only values actually selected by WHERE reach the evaluator.
                Cell key = EvaluateWith(*q.groupExpressions[0], [&](const ColumnRef &) { return input; });
                size_t owner = (Hash{}(key) >> 32) % workers;
                auto [position, added] = maps[owner].get(key, [&] { return State{0, 0, id, id, id}; });
                auto &state = maps[owner].at(position).second;
                state.count += count;
                state.sum = Add(state.sum, Multiply(Wide(input.text.size()), Wide(count)));
                if (needMinimum && strings[id] < strings[state.minimum]) state.minimum = id;
                if (needMaximum && strings[state.maximum] < strings[id]) state.maximum = id;
            }
        }
        return maps;
    });
    auto candidates = RunTasks(workers, [&](int owner) {
        ScopedValueResource localMemory(budget);
        Map merged(budget);
        for (auto &worker : partials) {
            auto &map = worker[owner];
            map.forEach([&](const Cell &key, const State &state) {
                auto [position, added] = merged.get(key, [&] { return state; });
                if (added) return;
                auto &into = merged.at(position).second;
                into.count += state.count; into.sum = Add(into.sum, state.sum);
                if (needMinimum && strings[state.minimum] < strings[into.minimum]) into.minimum = state.minimum;
                if (needMaximum && strings[into.maximum] < strings[state.maximum]) into.maximum = state.maximum;
            });
            map.release();
        }
        QueryDesc local = q; local.offset = 0;
        if (q.limit) local.limit = *q.limit + q.offset;
        QueryResults result(local, budget);
        merged.forEach([&](const Cell &key, const State &state) {
            result.addLazy([&](size_t output) {
                const auto &a = q.aggregateClauses[output];
                Cell input{0, strings[state.representative], true};
                if (a.type == AggregateClause::AGGREGATE_PROJECT) {
                    Cell value = a.expression.get() == q.groupExpressions[0].get() ? key
                        : EvaluateWith(*a.expression, [&](const ColumnRef &) { return input; });
                    return FinalCell{value, a.expression->type.get()};
                }
                if (a.type == AggregateClause::AGGREGATE_COUNT) return FinalCell{Cell{Wide(state.count)}};
                if (a.type == AggregateClause::AGGREGATE_MIN || a.type == AggregateClause::AGGREGATE_MAX)
                    return FinalCell{Cell{0, strings[a.type == AggregateClause::AGGREGATE_MIN ? state.minimum : state.maximum], true}, a.expression->type.get()};
                FinalCell cell{Cell{state.sum}, a.expression->type.get()};
                if (a.type == AggregateClause::AGGREGATE_AVG) {
                    cell.average = true; cell.mean = (long double)state.sum / state.count;
                }
                return cell;
            });
        });
        return result.takeCandidates();
    });
    QueryResults result(q, budget);
    for (auto &worker : candidates) for (auto &row : worker) result.add(std::move(row));
    return result.finish();
}
} // namespace
std::optional<Result<QueryOutput>> ExecuteDictionaryWeighted(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.grouped || q.groupExpressions.size() != 1) return {};
    auto source = DictionaryExpressionSource(*q.groupExpressions[0]);
    if (!source || source->Type()->type_num() != STRING_TYPE) return {};
    auto index = q.tables[0]->Dictionary(source->columnIdx);
    if (!index) return {};
    auto sameSource = [&](const Expression &e) {
        auto visit = [&](auto &&self, const Expression &node) -> bool {
            if (node.kind == Expression::COLUMN && *node.column != *source) return false;
            for (const auto &child : {node.left, node.right, node.condition})
                if (child && !self(self, *child)) return false;
            return true;
        };
        return visit(visit, e);
    };
    bool minimum = false, maximum = false;
    for (const auto &a : q.aggregateClauses) {
        if (a.expression && !sameSource(*a.expression)) return {};
        if (a.type == AggregateClause::AGGREGATE_PROJECT) continue;
        if (a.type == AggregateClause::AGGREGATE_COUNT &&
            (!a.expression || a.expression->kind == Expression::COLUMN)) continue;
        if ((a.type == AggregateClause::AGGREGATE_MIN || a.type == AggregateClause::AGGREGATE_MAX) &&
            a.expression && a.expression->kind == Expression::COLUMN) {
            minimum |= a.type == AggregateClause::AGGREGATE_MIN;
            maximum |= a.type == AggregateClause::AGGREGATE_MAX; continue;
        }
        if ((a.type != AggregateClause::AGGREGATE_SUM && a.type != AggregateClause::AGGREGATE_AVG) ||
            !a.expression || a.expression->kind != Expression::LENGTH ||
            a.expression->left->kind != Expression::COLUMN) return {};
    }
    for (int g = 0; g < q.tables[0]->RowGroupCount(); ++g)
        if (q.tables[0]->GetRowGroup(g).columns[source->columnIdx]->type != ColumnDataBase::DICT_COLUMN_DATA)
            return {};
    return DictionaryWeighted(q, *source, *index, minimum, maximum, avx, parallel);
}
} // namespace pgaccel
