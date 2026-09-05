#include "executor_result.hpp"
#include "column_index.hpp"
#include <array>

namespace pgaccel {
namespace {
template<size_t N>
Result<QueryOutput> DenseLength(const QueryDesc &q, Budget &budget,
    const GlobalDictionary &index, int keyColumn,
    const std::vector<const AggregateClause *> &numeric, bool avx, bool parallel)
{
    struct State {
        uint64_t count = 0;
        std::array<Wide, N> sums{};
        uint32_t group = 0, row = 0;
    };
    const auto &table = *q.tables[0];
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    auto partials = RunTasks(workers, [&](int worker) {
        ScopedValueResource valuesMemory(budget);
        std::pmr::vector<State> states(index.size(), &budget);
        for (int g = worker; g < table.RowGroupCount(); g += workers) {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count) continue;
            std::array<const DictColumnData<StringType> *, N> strings;
            for (size_t i = 0; i < N; ++i)
                strings[i] = static_cast<const DictColumnData<StringType> *>(
                    rg.columns[numeric[i]->expression->left->column->columnIdx].get());
            auto codes = index.Codes(g);
            auto keyData = rg.columns[keyColumn].get();
            auto dictionary = keyData->type == ColumnDataBase::DICT_COLUMN_DATA
                ? static_cast<const DictColumnDataBase *>(keyData) : nullptr;
            Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
                uint32_t id = codes[dictionary ? Code(dictionary, row) : row];
                auto &state = states[id];
                if (!state.count) { state.group = g; state.row = row; }
                ++state.count;
                for (size_t i = 0; i < N; ++i) {
                    auto d = strings[i];
                    state.sums[i] = Add(state.sums[i], Wide(d->dict[Code(d, row)].size()));
                }
            });
        }
        return states;
    });
    QueryResults result(q, budget);
    for (size_t id = 0; id < index.size(); ++id) {
        State merged;
        for (const auto &states : partials) {
            const auto &state = states[id];
            if (!state.count) continue;
            merged.group = state.group; merged.row = state.row;
            merged.count += state.count;
            for (size_t i = 0; i < N; ++i) merged.sums[i] = Add(merged.sums[i], state.sums[i]);
        }
        if (!merged.count) continue;
        result.addLazy([&](size_t output) {
            const auto &a = q.aggregateClauses[output];
            if (a.type == AggregateClause::AGGREGATE_PROJECT)
                return FinalCell{Evaluate(*a.expression, table.GetRowGroup(merged.group), merged.row), a.expression->type.get()};
            if (a.type == AggregateClause::AGGREGATE_COUNT) return FinalCell{Cell{Wide(merged.count)}};
            size_t slot = std::find(numeric.begin(), numeric.end(), &a) - numeric.begin();
            FinalCell cell{Cell{merged.sums[slot]}, a.expression->type.get()};
            if (a.type == AggregateClause::AGGREGATE_AVG) {
                cell.average = true; cell.mean = (long double)merged.sums[slot] / merged.count;
            }
            return cell;
        });
    }
    return result.finish();
}
} // namespace
std::optional<Result<QueryOutput>> ExecuteDenseLength(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.grouped || q.groupExpressions.size() != 1 ||
        q.groupExpressions[0]->kind != Expression::COLUMN) return {};
    int column = q.groupExpressions[0]->column->columnIdx;
    auto index = q.tables[0]->Dictionary(column);
    if (!index || index->size() > 65536) return {};
    std::vector<const AggregateClause *> numeric;
    for (const auto &a : q.aggregateClauses) {
        if (a.type == AggregateClause::AGGREGATE_PROJECT) continue;
        if (a.type == AggregateClause::AGGREGATE_COUNT &&
            (!a.expression || a.expression->kind == Expression::COLUMN)) continue;
        if ((a.type != AggregateClause::AGGREGATE_SUM && a.type != AggregateClause::AGGREGATE_AVG) ||
            !a.expression || a.expression->kind != Expression::LENGTH ||
            a.expression->left->kind != Expression::COLUMN ||
            a.expression->left->type->type_num() != STRING_TYPE) return {};
        numeric.push_back(&a);
    }
    if (numeric.empty() || numeric.size() > 3) return {};
    for (int g = 0; g < q.tables[0]->RowGroupCount(); ++g)
        for (auto a : numeric)
            if (q.tables[0]->GetRowGroup(g).columns[a->expression->left->column->columnIdx]->type !=
                ColumnDataBase::DICT_COLUMN_DATA) return {};
    Budget budget(q.memoryBudgetBytes);
    switch (numeric.size()) {
    case 1: return DenseLength<1>(q, budget, *index, column, numeric, avx, parallel);
    case 2: return DenseLength<2>(q, budget, *index, column, numeric, avx, parallel);
    case 3: return DenseLength<3>(q, budget, *index, column, numeric, avx, parallel);
    }
    return {};
}
} // namespace pgaccel
