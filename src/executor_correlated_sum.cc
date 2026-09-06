#include "executor_boolean_bitmap.hpp"
#include "executor_flat_map.hpp"
#include "executor_membership.hpp"
#include "executor_result.hpp"
#include "executor_tasks.hpp"
#include <bit>

namespace pgaccel {
namespace {
struct SumKey {
    int64_t a = 0, b = 0;
    bool operator==(const SumKey &) const = default;
};
struct SumKeyHash {
    size_t operator()(const SumKey &key) const {
        return Hash{}(Cell{key.a}) ^ std::rotl(uint64_t(Hash{}(Cell{key.b})), 29);
    }
};
bool SumScalarFormula(const Expression &e, int slot, int depth = 0) {
    if (depth > 64) return false;
    if (e.kind == Expression::CONSTANT) return e.type->type_num() != STRING_TYPE;
    if (e.kind == Expression::COLUMN) return e.column && e.column->tableIdx == -1 && e.column->columnIdx == slot;
    return (e.kind == Expression::ADD || e.kind == Expression::SUBTRACT ||
            e.kind == Expression::MULTIPLY || e.kind == Expression::DIVIDE) &&
        e.left && e.right && SumScalarFormula(*e.left, slot, depth + 1) && SumScalarFormula(*e.right, slot, depth + 1);
}
struct SumState { Wide sum = 0; uint64_t count = 0; };
struct NarrowSumState { int64_t sum = 0; uint64_t count = 0; };
// Bound positive and negative contributions separately: every subset, not
// just the final total, must fit before worker accumulators can be narrowed.
bool SumFits64(const ColumnarTable &table, const ColumnRef &column) {
    auto type = column.Type()->type_num();
    if (type != INT32_TYPE && type != INT64_TYPE && type != DECIMAL_TYPE) return false;
    Wide low = 0, high = 0;
    for (int g = 0; g < table.RowGroupCount(); ++g) {
        const auto &rg = table.GetRowGroup(g); if (!rg.size) continue;
        auto data = rg.columns[column.columnIdx].get(); Wide lo, hi;
        if (data->type != ColumnDataBase::RAW_COLUMN_DATA) return false;
        if (type == INT32_TYPE) {
            auto raw = static_cast<const RawColumnData<Int32Type> *>(data); lo = raw->minValue; hi = raw->maxValue;
        } else if (type == DECIMAL_TYPE) {
            auto raw = static_cast<const RawColumnData<DecimalType> *>(data); lo = raw->minValue; hi = raw->maxValue;
        } else {
            auto raw = static_cast<const RawColumnData<Int64Type> *>(data); lo = raw->minValue; hi = raw->maxValue;
        }
        low += std::min<Wide>(0, lo) * rg.size; high += std::max<Wide>(0, hi) * rg.size;
        if (low < INT64_MIN || high > INT64_MAX) return false;
    }
    return true;
}
struct SumOuterRow { uint32_t stripe, row, slot; };
}
std::optional<Result<QueryOutput>> ExecuteCorrelatedSum(const QueryDesc &q, bool avx, bool parallel) {
    if (q.tables.size() != 1 || q.scalarQueries.size() != 1 || q.membershipQueries.size() != 1 ||
        q.grouped || !q.groupExpressions.empty() || !q.having.empty() ||
        !q.existenceJoins.empty() || !q.leftJoins.empty() || q.predicates.size() != 1) return {};
    const auto &scalar = q.scalarQueries[0]; const auto &child = *scalar.query;
    if (child.memoryBudgetBytes < q.memoryBudgetBytes) return {};
    size_t keys = scalar.outerKeys.size();
    if (!keys || keys > 2 || scalar.innerKeys.size() != keys || scalar.keyOutputs.size() != keys ||
        child.tables.size() != 1 || !child.grouped || child.groupExpressions.size() != keys ||
        !child.predicates.empty() || !child.membershipQueries.empty() || !child.scalarQueries.empty() ||
        !child.existenceJoins.empty() || !child.leftJoins.empty() || !child.materializedSources.empty() ||
        !child.having.empty() || !child.orderBy.empty() || child.limit || child.offset || child.aggregateClauses.empty()) return {};
    for (size_t i = 0; i < keys; ++i) {
        if (scalar.outerKeys[i].tableIdx != 0 || scalar.innerKeys[i].tableIdx != 0 ||
            !child.groupExpressions[i]->column || *child.groupExpressions[i]->column != scalar.innerKeys[i]) return {};
        for (const auto &column : {scalar.outerKeys[i], scalar.innerKeys[i]})
            if (column.Type()->type_num() != INT32_TYPE && column.Type()->type_num() != INT64_TYPE) return {};
    }
    int sum = -1;
    for (size_t i = 0; i < child.aggregateClauses.size(); ++i) {
        const auto &a = child.aggregateClauses[i];
        if (a.type == AggregateClause::AGGREGATE_SUM && a.columnRef && sum < 0) sum = int(i);
        else if (i == 0 && a.type == AggregateClause::AGGREGATE_FORMULA) continue;
        else if (a.type != AggregateClause::AGGREGATE_PROJECT ||
            std::find(scalar.keyOutputs.begin(), scalar.keyOutputs.end(), i) == scalar.keyOutputs.end()) return {};
    }
    if (sum < 0 || (sum != 0 && !SumScalarFormula(*child.aggregateClauses[0].expression, sum))) return {};
    for (const auto &a : q.aggregateClauses)
        if (a.type != AggregateClause::AGGREGATE_PROJECT || !a.columnRef || a.columnRef->tableIdx != 0) return {};
    const auto &comparison = *q.predicates[0];
    if (comparison.kind < Expression::EQ || comparison.kind > Expression::GTE ||
        !comparison.left->column || !comparison.right->column) return {};
    auto a = *comparison.left->column, b = *comparison.right->column;
    if (a.tableIdx == -2) std::swap(a, b);
    if (a.tableIdx != 0 || b.tableIdx != -2 || b.columnIdx != 0) return {};
    const auto &membership = q.membershipQueries[0]; const auto &source = *membership.query;
    if (source.memoryBudgetBytes < q.memoryBudgetBytes) return {};
    auto memberKey = std::find(scalar.outerKeys.begin(), scalar.outerKeys.end(), membership.column);
    if (membership.anti || memberKey == scalar.outerKeys.end() || source.tables.size() != 1 || source.grouped ||
        !source.groupExpressions.empty() || !source.predicates.empty() || !source.membershipQueries.empty() ||
        !source.scalarQueries.empty() || !source.existenceJoins.empty() || !source.leftJoins.empty() ||
        !source.materializedSources.empty() || !source.having.empty() || source.limit || source.offset ||
        source.aggregateClauses.size() != 1 || source.aggregateClauses[0].type != AggregateClause::AGGREGATE_PROJECT ||
        !source.aggregateClauses[0].columnRef) return {};
    auto sourceKey = *source.aggregateClauses[0].columnRef;
    if (sourceKey.tableIdx != 0 || (sourceKey.Type()->type_num() != INT32_TYPE && sourceKey.Type()->type_num() != INT64_TYPE)) return {};
    auto innerMember = scalar.innerKeys[memberKey - scalar.outerKeys.begin()];
    try {
        Budget budget(q.memoryBudgetBytes); ScopedValueResource memory(budget);
        MembershipSet members(budget, 0);
        {
            auto sourceFilter = source.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(source.filterClauses, avx);
            int groups = source.tables[0]->RowGroupCount();
            int sourceWorkers = parallel ? std::min(8, std::max(1, groups)) : 1;
            auto selected = RunTasks(sourceWorkers, [&](int worker) {
                ScopedValueResource memory(budget); Scratch scratch(budget, BITMAP_SIZE);
                std::pmr::vector<Wide> keys(&budget);
                for (int g = worker; g < groups; g += sourceWorkers) {
                    const auto &rg = source.tables[0]->GetRowGroup(g); uint8_t bitmap[BITMAP_SIZE];
                    if (sourceFilter && !sourceFilter->ExecuteSet(rg, bitmap)) continue;
                    Selected(rg, sourceFilter ? bitmap : nullptr,
                        [&](int row) { keys.push_back(Read(rg, sourceKey, row).number); });
                }
                return keys;
            });
            size_t count = 0;
            for (const auto &keys : selected) count += keys.size();
            std::pmr::vector<Wide> sourceKeys(&budget); sourceKeys.reserve(count);
            for (const auto &keys : selected) sourceKeys.insert(sourceKeys.end(), keys.begin(), keys.end());
            members.assignNumbers(sourceKeys, 0, budget);
        }
        using SumIndex = FlatMap<SumKey, uint8_t, SumKeyHash, std::equal_to<SumKey>>;
        std::pmr::vector<SumOuterRow> outerRows(&budget);
        auto outerFilter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
        auto keyAt = [&](const RowGroup &rg, const std::vector<ColumnRef> &columns, int row) {
            SumKey key; key.a = int64_t(Read(rg, columns[0], row).number);
            if (keys == 2) key.b = int64_t(Read(rg, columns[1], row).number);
            return key;
        };
        {
            int groups = q.tables[0]->RowGroupCount();
            int outerWorkers = parallel ? std::min(8, std::max(1, groups)) : 1;
            auto selected = RunTasks(outerWorkers, [&](int worker) {
                ScopedValueResource memory(budget); Scratch scratch(budget, BITMAP_SIZE);
                std::pmr::vector<SumOuterRow> rows(&budget);
                for (int g = int64_t(groups) * worker / outerWorkers;
                     g < int64_t(groups) * (worker + 1) / outerWorkers; ++g) {
                    const auto &rg = q.tables[0]->GetRowGroup(g); uint8_t bitmap[BITMAP_SIZE];
                    if (outerFilter) { if (!outerFilter->ExecuteSet(rg, bitmap)) continue; }
                    else memset(bitmap, 0xff, (rg.size + 7) / 8);
                    members.intersect(rg, membership.column, false, avx, bitmap);
                    Selected(rg, bitmap, [&](int row) {
                        rows.push_back({uint32_t(g), uint32_t(row), 0});
                    });
                }
                return rows;
            });
            size_t count = 0;
            for (const auto &rows : selected) count += rows.size();
            outerRows.reserve(count);
            for (const auto &rows : selected) outerRows.insert(outerRows.end(), rows.begin(), rows.end());
        }
        int partitions = parallel && outerRows.size() >= 4096 ? 8 : 1;
        auto partition = [&](const SumKey &key) { return uint64_t(key.a) & (partitions - 1); };
        std::pmr::vector<SumIndex> indexes(&budget); indexes.reserve(partitions);
        for (int p = 0; p < partitions; ++p) indexes.emplace_back(budget);
        std::array<uint32_t, 9> offsets{};
        size_t slotCount = 0;
        {
            std::pmr::vector<SumKey> rowKeys(outerRows.size(), &budget);
            RunTasks(partitions, [&](int worker) {
                ScopedValueResource memory(budget);
                for (size_t i = outerRows.size() * worker / partitions;
                     i < outerRows.size() * (worker + 1) / partitions; ++i) {
                    const auto &selected = outerRows[i];
                    rowKeys[i] = keyAt(q.tables[0]->GetRowGroup(selected.stripe), scalar.outerKeys, selected.row);
                }
                return 0;
            });
            // Each partition owns its directory and every occurrence of its
            // keys. Bounded repeated routing passes avoid shared insertion.
            RunTasks(partitions, [&](int worker) {
                ScopedValueResource memory(budget);
                size_t count = 0;
                for (const auto &key : rowKeys) count += partition(key) == size_t(worker);
                indexes[worker].reserve(count);
                for (size_t i = 0; i < rowKeys.size(); ++i) if (partition(rowKeys[i]) == size_t(worker))
                    outerRows[i].slot = indexes[worker].get(rowKeys[i], [] { return uint8_t(0); }).first;
                return 0;
            });
            for (int p = 0; p < partitions; ++p) {
                slotCount += indexes[p].size();
                if (slotCount > UINT32_MAX) throw std::runtime_error("Correlated sum slot count exceeds 32 bits");
                offsets[p + 1] = uint32_t(slotCount);
            }
            RunTasks(partitions, [&](int worker) {
                for (size_t i = outerRows.size() * worker / partitions;
                     i < outerRows.size() * (worker + 1) / partitions; ++i)
                    outerRows[i].slot += offsets[partition(rowKeys[i])];
                return 0;
            });
        }
        auto lookup = [&](const SumKey &key) {
            auto p = partition(key); auto slot = indexes[p].find(key);
            return slot == SumIndex::missing ? slot : offsets[p] + slot;
        };
        QueryResults results(q, budget);
        if (outerRows.empty()) return results.finish();
        auto filter = child.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(child.filterClauses, avx);
        auto &facts = *child.tables[0];
        int workers = parallel ? std::min(8, std::max(1, facts.RowGroupCount())) : 1;
        auto scan = [&]<class State>() { return RunTasks(workers, [&](int worker) {
            ScopedValueResource memory(budget); Scratch scratch(budget, BITMAP_SIZE);
            std::pmr::vector<State> states(slotCount, &budget); uint8_t bitmap[BITMAP_SIZE];
            for (int g = worker; g < facts.RowGroupCount(); g += workers) {
                const auto &rg = facts.GetRowGroup(g);
                if (filter) { if (!filter->ExecuteSet(rg, bitmap)) continue; }
                else memset(bitmap, 0xff, (rg.size + 7) / 8);
                members.intersect(rg, innerMember, false, avx, bitmap);
                Selected(rg, bitmap, [&](int row) {
                    auto slot = lookup(keyAt(rg, scalar.innerKeys, row));
                    if (slot == SumIndex::missing) return;
                    auto &state = states[slot];
                    Wide value = Read(rg, *child.aggregateClauses[sum].columnRef, row).number;
                    if constexpr (std::is_same_v<State, NarrowSumState>) state.sum += int64_t(value);
                    else state.sum = Add(state.sum, value);
                    ++state.count;
                });
            }
            return states;
        }); };
        std::pmr::vector<SumState> states(slotCount, &budget);
        auto merge = [&](const auto &partials) { RunTasks(workers, [&](int worker) {
            ScopedValueResource memory(budget);
            for (size_t slot = slotCount * worker / workers;
                 slot < slotCount * (worker + 1) / workers; ++slot) {
                for (const auto &p : partials) {
                    states[slot].sum = Add(states[slot].sum, p[slot].sum);
                    states[slot].count += p[slot].count;
                }
                if (states[slot].count && sum != 0)
                    states[slot].sum = EvaluateWith(*child.aggregateClauses[0].expression,
                        [&](const ColumnRef &) { return Cell{states[slot].sum}; }).number;
            }
            return 0;
        }); };
        if (SumFits64(facts, *child.aggregateClauses[sum].columnRef)) merge(scan.template operator()<NarrowSumState>());
        else merge(scan.template operator()<SumState>());
        bool numeric = q.numericMembershipOutput && q.aggregateClauses.size() == 1 &&
            q.aggregateClauses[0].columnRef->Type()->type_num() != STRING_TYPE &&
            q.orderBy.empty() && !q.limit && !q.offset;
        // Standard storage outlives this child's memory resource. Charge its
        // whole capacity before allocation; the parent accounts retained bytes.
        Scratch numericMemory(budget, numeric ? outerRows.size() * sizeof(Wide) + 64 : 0);
        std::vector<Wide> numericValues;
        auto qualifies = [&](const SumOuterRow &selected) {
            const auto &state = states[selected.slot];
            if (!state.count) return false;
            const auto &rg = q.tables[0]->GetRowGroup(selected.stripe);
            return EvaluateWith(comparison, [&](const ColumnRef &column) {
                return column.tableIdx == -2 ? Cell{state.sum} : Read(rg, column, selected.row);
            }).number != 0;
        };
        if (numeric) {
            numericValues.resize(outerRows.size());
            auto counts = RunTasks(workers, [&](int worker) {
                ScopedValueResource memory(budget);
                size_t begin = outerRows.size() * worker / workers, count = 0;
                size_t end = outerRows.size() * (worker + 1) / workers;
                for (size_t i = begin; i < end; ++i) {
                    const auto &selected = outerRows[i];
                    if (!qualifies(selected)) continue;
                    const auto &rg = q.tables[0]->GetRowGroup(selected.stripe);
                    numericValues[begin + count++] =
                        Read(rg, *q.aggregateClauses[0].columnRef, selected.row).number;
                }
                return count;
            });
            size_t count = 0;
            for (int worker = 0; worker < workers; ++worker) {
                size_t begin = outerRows.size() * worker / workers;
                if (counts[worker] && count != begin)
                    memmove(numericValues.data() + count, numericValues.data() + begin,
                            counts[worker] * sizeof(Wide));
                count += counts[worker];
            }
            numericValues.resize(count);
        } else for (const auto &selected : outerRows) {
            if (!qualifies(selected)) continue;
            const auto &rg = q.tables[0]->GetRowGroup(selected.stripe);
            results.addLazy([&](size_t i) {
                const auto &column = *q.aggregateClauses[i].columnRef;
                return FinalCell{Read(rg, column, selected.row), column.Type().get()};
            });
        }
        auto output = results.finish();
        if (numeric) output.numericMembership = std::move(numericValues);
        return output;
    } catch (const NotDense &) { return {}; }
    catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
