#include "executor_bitmap_probe.hpp"
#include "executor_boolean_bitmap.hpp"
#include "executor_rank_join.hpp"
#include "executor_result.hpp"
#include "executor_tasks.hpp"
#include <cmath>

namespace pgaccel {
namespace {
bool AvgOnTable(const Expression &e, int table, int depth = 0) {
    if (depth > 64 || (e.column && e.column->tableIdx != table)) return false;
    for (const auto &p : {e.left, e.right, e.condition})
        if (p && !AvgOnTable(*p, table, depth + 1)) return false;
    return true;
}
bool AvgFormula(const Expression &e, int slot, int depth = 0) {
    if (depth > 64) return false;
    if (e.kind == Expression::CONSTANT) return e.type->type_num() != STRING_TYPE;
    if (e.kind == Expression::COLUMN) return e.column && e.column->tableIdx == -1 && e.column->columnIdx == slot;
    return (e.kind == Expression::ADD || e.kind == Expression::SUBTRACT ||
            e.kind == Expression::MULTIPLY || e.kind == Expression::DIVIDE) &&
        e.left && e.right && AvgFormula(*e.left, slot, depth + 1) && AvgFormula(*e.right, slot, depth + 1);
}
struct AvgState { Wide sum = 0; uint64_t count = 0; };
struct AvgRow { uint32_t stripe, row, slot; };
struct AvgPartial {
    std::pmr::vector<AvgState> states;
    std::pmr::vector<AvgRow> rows;
    AvgPartial(size_t size, Budget &budget) : states(size, &budget), rows(&budget) {}
};
}
std::optional<Result<QueryOutput>> ExecuteCorrelatedAvg(const QueryDesc &q, bool avx, bool parallel) {
    if (q.tables.size() != 2 || q.scalarQueries.size() != 1 || q.grouped ||
        !q.groupExpressions.empty() || !q.having.empty() || !q.membershipQueries.empty() ||
        !q.existenceJoins.empty() || !q.leftJoins.empty() || q.aggregateClauses.size() > 32) return {};
    const auto &scalar = q.scalarQueries[0];
    const auto &child = *scalar.query;
    if (child.memoryBudgetBytes < q.memoryBudgetBytes) return {};
    if (scalar.outerKeys.size() != 1 || scalar.innerKeys.size() != 1 || scalar.keyOutputs.size() != 1 ||
        child.tables.size() != 1 || !child.grouped || child.groupExpressions.size() != 1 ||
        !child.predicates.empty() || !child.filterClauses.empty() || !child.membershipQueries.empty() ||
        !child.scalarQueries.empty() || !child.existenceJoins.empty() || !child.leftJoins.empty() ||
        !child.materializedSources.empty() || !child.having.empty() || !child.orderBy.empty() || child.limit || child.offset ||
        child.aggregateClauses.empty() || child.aggregateClauses[0].type != AggregateClause::AGGREGATE_FORMULA) return {};
    auto build = scalar.outerKeys[0];
    if (build.tableIdx < 0 || build.tableIdx > 1 ||
        (build.Type()->type_num() != INT32_TYPE && build.Type()->type_num() != INT64_TYPE)) return {};
    int probeTable = 1 - build.tableIdx;
    if (child.tables[0] != q.tables[probeTable]) return {};
    int average = -1;
    for (size_t i = 1; i < child.aggregateClauses.size(); ++i) {
        const auto &a = child.aggregateClauses[i];
        if (a.type == AggregateClause::AGGREGATE_AVG && a.columnRef && average < 0) average = int(i);
        else if (a.type != AggregateClause::AGGREGATE_PROJECT || i != scalar.keyOutputs[0]) return {};
    }
    if (average < 0 || !AvgFormula(*child.aggregateClauses[0].expression, average)) return {};
    auto quantity = *child.aggregateClauses[average].columnRef; quantity.tableIdx = probeTable;
    auto probeKey = scalar.innerKeys[0]; probeKey.tableIdx = probeTable;
    if (Scale(build.Type().get()) != Scale(probeKey.Type().get()) ||
        !child.groupExpressions[0]->column || *child.groupExpressions[0]->column != scalar.innerKeys[0]) return {};
    const Expression *comparison = nullptr;
    bool edge = false;
    std::vector<std::shared_ptr<Expression>> buildPredicates;
    for (const auto &p : q.predicates) {
        if (p->kind == Expression::EQ && p->left->column && p->right->column &&
            ((*p->left->column == build && *p->right->column == probeKey) ||
             (*p->right->column == build && *p->left->column == probeKey))) { edge = true; continue; }
        if (AvgOnTable(*p, build.tableIdx)) { buildPredicates.push_back(p); continue; }
        if (comparison || p->kind < Expression::EQ || p->kind > Expression::GTE ||
            !p->left->column || !p->right->column) return {};
        auto a = *p->left->column, b = *p->right->column;
        if (a.tableIdx == -2) std::swap(a, b);
        if (a.tableIdx != probeTable || b.tableIdx != -2 || b.columnIdx != 0 ||
            (a.Type()->type_num() != INT32_TYPE && a.Type()->type_num() != INT64_TYPE && a.Type()->type_num() != DECIMAL_TYPE)) return {};
        comparison = p.get();
    }
    if (!edge || !comparison) return {};
    for (const auto &f : q.filterClauses) if (f.columnRef.tableIdx != build.tableIdx) return {};
    for (const auto &a : q.aggregateClauses) {
        if (a.type == AggregateClause::AGGREGATE_SUM) {
            if (!a.expression || !AvgOnTable(*a.expression, probeTable)) return {};
        } else if (a.type == AggregateClause::AGGREGATE_FORMULA) {
            if (!a.expression || !AvgOnTable(*a.expression, -1)) return {};
        } else if (a.type != AggregateClause::AGGREGATE_COUNT || a.expression) return {};
    }
    try {
        Budget budget(q.memoryBudgetBytes); ScopedValueResource memory(budget);
        auto &dimension = *q.tables[build.tableIdx];
        int64_t lo = INT64_MAX, hi = INT64_MIN; uint64_t dimensionRows = 0;
        for (int g = 0; g < dimension.RowGroupCount(); ++g) {
            const auto &rg = dimension.GetRowGroup(g); dimensionRows += rg.size;
            if (!rg.size) continue;
            auto data = rg.columns[build.columnIdx].get();
            if (data->type != ColumnDataBase::RAW_COLUMN_DATA) throw NotDense{};
            if (build.Type()->type_num() == INT32_TYPE) {
                auto raw = static_cast<const RawColumnData<Int32Type> *>(data); lo = std::min(lo, int64_t(raw->minValue)); hi = std::max(hi, int64_t(raw->maxValue));
            } else {
                auto raw = static_cast<const RawColumnData<Int64Type> *>(data); lo = std::min(lo, raw->minValue); hi = std::max(hi, raw->maxValue);
            }
        }
        if (!dimensionRows || Wide(hi) - lo + 1 > (Wide(1) << 29) || Wide(hi) - lo + 1 > Wide(dimensionRows) * 8) throw NotDense{};
        RankJoinIndex index(lo, hi, budget);
        BooleanBitmapProgram filters(budget, avx); int root = filters.truth();
        for (const auto &f : q.filterClauses) root = filters.combine(true, root, filters.leaf(f));
        for (const auto &p : buildPredicates) root = filters.combine(true, root, filters.compile(*p, build.tableIdx));
        std::pmr::vector<uint64_t> bitmap((filters.bytes() + 7) / 8, &budget);
        std::array<int, 64> counts{};
        uint64_t selectedBuild = 0;
        for (int g = 0; g < dimension.RowGroupCount(); ++g) {
            const auto &rg = dimension.GetRowGroup(g);
            filters.evaluate(rg, reinterpret_cast<uint8_t *>(bitmap.data()), counts);
            selectedBuild += counts[root];
            Selected(rg, filters.bitmap(reinterpret_cast<const uint8_t *>(bitmap.data()), root), [&](int row) {
                if (!index.insert(int64_t(Read(rg, build, row).number))) throw NotDense{};
            });
        }
        if (selectedBuild > 1000000 || selectedBuild > dimensionRows / 8) throw NotDense{};
        index.finalize();
        BitmapDomain domain{lo, hi, index.words()};
        std::pmr::vector<uint32_t> nonempty(&budget);
        Wide width=Wide(hi)-lo+1;
        // Large sparse domains benefit from a cache-sized exact summary:
        // one bit says whether a 32-key membership word contains any key.
        if(avx && lo>=0 && hi<=INT32_MAX && width>(1<<20) && Wide(index.size())*128<width) {
            size_t words=size_t((width+31)/32);
            nonempty.resize((words+31)/32,0);
            auto bits=reinterpret_cast<const uint32_t *>(index.words());
            for(size_t word=0;word<words;++word) if(bits[word]) nonempty[word/32]|=uint32_t(1)<<(word%32);
            domain.nonemptyWords=nonempty.data();
        }
        const auto &facts = *q.tables[probeTable];
        int workers = parallel ? std::min(8, std::max(1, facts.RowGroupCount())) : 1;
        auto partials = RunTasks(workers, [&](int worker) {
            ScopedValueResource memory(budget); Scratch scratch(budget, 4096);
            AvgPartial partial(index.size(), budget); uint32_t selected[Block];
            for (int g = worker; g < facts.RowGroupCount(); g += workers) {
                const auto &rg = facts.GetRowGroup(g);
                auto data = rg.columns[probeKey.columnIdx].get();
                if (data->type != ColumnDataBase::RAW_COLUMN_DATA) throw NotDense{};
                auto raw = static_cast<const RawColumnDataBase *>(data);
                auto consume = [&](int n) {
                    for (int i = 0; i < n; ++i) {
                        int row = selected[i]; auto slot = index.rank(int64_t(Read(rg, probeKey, row).number));
                        if (!slot) throw std::logic_error("Selected correlation key is absent");
                        auto &state = partial.states[*slot];
                        state.sum = Add(state.sum, Read(rg, quantity, row).number); ++state.count;
                        partial.rows.push_back({uint32_t(g), uint32_t(row), *slot});
                    }
                };
                if (avx && raw->bytesPerValue == 4 && lo >= 0 && hi <= INT32_MAX) {
                    for (int begin = 0; begin < rg.size; begin += Block)
                        consume(domain.nonemptyWords
                            ? BitmapSelectRange32<true>(domain, reinterpret_cast<const int32_t *>(raw->values), begin, std::min(Block, rg.size - begin), nullptr, selected)
                            : BitmapSelectRange32(domain, reinterpret_cast<const int32_t *>(raw->values), begin, std::min(Block, rg.size - begin), nullptr, selected));
                } else {
                    int n = 0;
                    for (int row = 0; row < rg.size; ++row) if (domain.contains(int64_t(Read(rg, probeKey, row).number))) {
                        selected[n++] = row; if (n == Block) { consume(n); n = 0; }
                    }
                    if (n) consume(n);
                }
            }
            return partial;
        });
        std::pmr::vector<Wide> thresholds(index.size(), &budget);
        for (size_t slot = 0; slot < index.size(); ++slot) {
            AvgState state;
            for (const auto &p : partials) { state.sum = Add(state.sum, p.states[slot].sum); state.count += p.states[slot].count; }
            if (!state.count) continue;
            long double mean = (long double)state.sum / state.count / (long double)Power(Scale(quantity.Type().get()));
            thresholds[slot] = EvaluateWith(*child.aggregateClauses[0].expression, [&](const ColumnRef &column) {
                long double scaled = mean * (long double)Power(Scale(column.Type().get()));
                if (!std::isfinite(scaled) || scaled <= -std::ldexp(1.0L, 127) || scaled >= std::ldexp(1.0L, 127)) throw NotDense{};
                return Cell{Wide(std::round(scaled))};
            }).number;
        }
        struct Total { std::array<Wide, 32> sums{}; uint64_t count = 0; };
        auto totals = RunTasks(workers, [&](int worker) {
            ScopedValueResource memory(budget); Total total;
            for (const auto &selected : partials[worker].rows) {
                const auto &rg = facts.GetRowGroup(selected.stripe);
                if (!EvaluateWith(*comparison, [&](const ColumnRef &column) {
                    return column.tableIdx == -2 ? Cell{thresholds[selected.slot]} : Read(rg, column, selected.row);
                }).number) continue;
                ++total.count;
                for (size_t i = 0; i < q.aggregateClauses.size(); ++i) if (q.aggregateClauses[i].type == AggregateClause::AGGREGATE_SUM)
                    total.sums[i] = Add(total.sums[i], Evaluate(*q.aggregateClauses[i].expression, rg, selected.row).number);
            }
            return total;
        });
        Total total;
        for (const auto &p : totals) { total.count += p.count; for (size_t i = 0; i < q.aggregateClauses.size(); ++i) total.sums[i] = Add(total.sums[i], p.sums[i]); }
        struct Null {};
        std::function<FinalCell(size_t)> finish = [&](size_t i) {
            const auto &a = q.aggregateClauses[i]; FinalCell cell; cell.type = a.expression ? a.expression->type.get() : nullptr;
            if (a.type == AggregateClause::AGGREGATE_COUNT) { cell.type = nullptr; cell.value.number = total.count; }
            else if (a.type == AggregateClause::AGGREGATE_SUM) { cell.null = !total.count; cell.value.number = total.sums[i]; }
            else try { cell.value = EvaluateWith(*a.expression, [&](const ColumnRef &column) { auto value = finish(column.columnIdx); if (value.null) throw Null{}; return value.value; }); }
                catch (const Null &) { cell.null = true; }
            return cell;
        };
        QueryResults results(q, budget); results.addLazy(finish); return results.finish();
    } catch (const NotDense &) { return {}; }
    catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
