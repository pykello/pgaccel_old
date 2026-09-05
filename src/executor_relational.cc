#include "executor_result.hpp"
#include "executor_result_number.hpp"
#include "nodes.h"
#include <functional>
#include <cmath>

namespace pgaccel {
namespace {
using RowId = uint64_t;
using Key = std::pmr::vector<Cell>;
struct KeyHash {
    size_t operator()(const Key &key) const {
        size_t hash = 0;
        for (const auto &value : key)
            hash ^= Hash{}(value) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        return hash;
    }
};
struct JoinedGroup {
    States states;
    std::pmr::vector<RowId> representative;
    JoinedGroup(size_t n, const std::pmr::vector<RowId> &row, Budget &budget)
        : states(n, &budget), representative(row, &budget) {}
};
uint64_t References(const Expression &e) {
    uint64_t mask = e.column && e.column->tableIdx >= 0 ? uint64_t(1) << e.column->tableIdx : 0;
    for (const auto &child : {e.left, e.right, e.condition})
        if (child) mask |= References(*child);
    return mask;
}
std::shared_ptr<Expression> LocalPredicate(const std::shared_ptr<Expression> &e, uint64_t table) {
    if ((References(*e) & ~table) == 0) return e;
    if (e->kind != Expression::AND && e->kind != Expression::OR) return {};
    auto left = LocalPredicate(e->left, table), right = LocalPredicate(e->right, table);
    if (e->kind == Expression::OR && (!left || !right)) return {};
    if (!left) return right;
    if (!right) return left;
    auto local = std::make_shared<Expression>(*e); local->left = left; local->right = right; return local;
}
}

// Hash inner joins stream row identities into typed aggregation. The largest
// filtered relation is probed; other relations retain every duplicate key.
// Strings remain borrowed from immutable source stripes throughout execution.
Result<QueryOutput> ExecuteRelational(const QueryDesc &q, bool avx) {
    try {
        if (q.tables.empty() || q.tables.size() > 63)
            return Status::Invalid("Relational execution requires 1 to 63 tables");
        Budget budget(q.memoryBudgetBytes);
        ScopedValueResource valuesMemory(budget);
        const size_t tables = q.tables.size(), outputs = q.aggregateClauses.size();
        uint64_t subquerySpillBytes = 0;
        auto childPlan = [&](const QueryDesc &source) {
            if (subquerySpillBytes > q.spillByteBudget) throw std::runtime_error("Subquery spill budget exceeded");
            auto child = source;
            child.memoryBudgetBytes = std::min(child.memoryBudgetBytes, budget.limit - budget.used.load());
            child.allowSpill = q.allowSpill && child.allowSpill;
            child.spillByteBudget = std::min(child.spillByteBudget, q.spillByteBudget - subquerySpillBytes);
            return child;
        };
        std::pmr::vector<std::optional<Cell>> scalarValues(&budget);
        for (const auto &scalar : q.scalarQueries) {
            auto sub = childPlan(*scalar.query);
            auto result = ExecuteQuery(sub, avx, false);
            if (!result.ok()) return Status::Invalid("Scalar subquery: " + result.status().Message());
            subquerySpillBytes += result->spillBytes;
            Scratch childMemory(budget, result->peakMemoryBytes);
            if (result->values.size() > 1) return Status::Invalid("Scalar subquery returned multiple rows");
            if (result->values.empty() || result->values[0][0] == "NULL") scalarValues.push_back(std::nullopt);
            else scalarValues.push_back(Cell{ResultNumber(result->values[0][0], Scale(scalar.resultType.get()))});
        }
        struct Membership {
            std::pmr::unordered_set<Cell, Hash> values;
            int scale;
            explicit Membership(Budget &budget, int s) : values(&budget), scale(s) {}
        };
        std::vector<Membership> memberships; memberships.reserve(q.membershipQueries.size());
        for (const auto &membership : q.membershipQueries) {
            int sourceScale = Scale(membership.resultType.get());
            int scale = std::max(sourceScale, Scale(membership.column.Type().get()));
            memberships.emplace_back(budget, scale);
            auto sub = childPlan(*membership.query);
            auto result = ExecuteQuery(sub, avx, false);
            if (!result.ok()) return Status::Invalid("Membership subquery: " + result.status().Message());
            subquerySpillBytes += result->spillBytes;
            // Conservatively retain the child's reported peak while its output
            // is converted, accounting for both child output and the new set.
            Scratch childMemory(budget, result->peakMemoryBytes);
            for (const auto &row : result->values) {
                Cell key;
                auto type = membership.resultType->type_num();
                if (type == STRING_TYPE) {
                    auto owner = std::allocate_shared<std::pmr::string>(std::pmr::polymorphic_allocator<std::pmr::string>(&budget), row[0]);
                    key = {0, *owner, true, owner};
                } else {
                    int64_t value = type == DATE_TYPE ? ParseDate(row[0]) :
                        type == TIMESTAMP_TYPE ? ParseTimestamp(row[0]) : ParseDecimal(sourceScale, row[0]);
                    key.number = Multiply(value, Power(scale - sourceScale));
                }
                memberships.back().values.insert(std::move(key));
            }
        }
        std::pmr::vector<RowId> bound(tables, 0, &budget);
        struct MissingScalar {};
        auto read = [&](const ColumnRef &column) {
            if (column.tableIdx == -2) {
                if (!scalarValues.at(column.columnIdx)) throw MissingScalar{};
                return *scalarValues[column.columnIdx];
            }
            if (column.tableIdx < 0) throw std::invalid_argument("Invalid relational column binding");
            RowId id = bound[column.tableIdx];
            return Read(q.tables[column.tableIdx]->GetRowGroup(id >> 32), column, uint32_t(id));
        };
        auto keep = [&](const Expression &predicate) {
            try { return bool(EvaluateWith(predicate, read).number); }
            catch (const MissingScalar &) { return false; }
        };
        std::vector<const QueryDesc::ExistenceJoin *> existence(tables, nullptr);
        for (const auto &join : q.existenceJoins) existence[join.table] = &join;
        std::vector<std::shared_ptr<Expression>> edges = q.predicates;
        for (const auto &join : q.existenceJoins)
            for (const auto &e : join.predicates)
                if (e->kind == Expression::EQ && e->left->column && e->right->column &&
                    (e->left->column->tableIdx == join.table || e->right->column->tableIdx == join.table)) edges.push_back(e);
        std::vector<std::pmr::vector<RowId>> rows;
        std::vector<uint64_t> masks;
        for (const auto &predicate : q.predicates) masks.push_back(References(*predicate));
        for (size_t table = 0; table < tables; ++table) {
            rows.emplace_back(&budget);
            std::vector<FilterClause> clauses;
            for (const auto &filter : q.filterClauses)
                if (filter.columnRef.tableIdx == int(table)) clauses.push_back(filter);
            std::vector<std::shared_ptr<Expression>> local;
            for (const auto &predicate : q.predicates)
                if (auto projected = LocalPredicate(predicate, uint64_t(1) << table)) local.push_back(projected);
            if (existence[table])
                for (const auto &predicate : existence[table]->predicates)
                    if (auto projected = LocalPredicate(predicate, uint64_t(1) << table)) local.push_back(projected);
            auto filter = clauses.empty() ? FilterNodeP{} : CreateFilterNode(clauses, avx);
            for (int g = 0; g < q.tables[table]->RowGroupCount(); ++g) {
                const auto &rg = q.tables[table]->GetRowGroup(g);
                uint8_t bitmap[BITMAP_SIZE];
                if (filter && !filter->ExecuteSet(rg, bitmap)) continue;
                Selected(rg, filter ? bitmap : nullptr, [&](int row) {
                    bound[table] = (uint64_t(g) << 32) | uint32_t(row);
                    for (const auto &predicate : local)
                        if (!keep(*predicate)) return;
                    for (size_t i = 0; i < q.membershipQueries.size(); ++i) {
                        const auto &query = q.membershipQueries[i];
                        if (query.column.tableIdx != int(table)) continue;
                        auto key = read(query.column);
                        if (!key.string) key.number = Multiply(key.number, Power(memberships[i].scale - Scale(query.column.Type().get())));
                        if (memberships[i].values.contains(key) == query.anti) return;
                    }
                    rows.back().push_back(bound[table]);
                });
            }
        }
        size_t probe = 0;
        for (size_t t = 1; t < tables; ++t)
            if (!existence[t] && (existence[probe] || rows[t].size() > rows[probe].size())) probe = t;
        struct Join {
            size_t table;
            ColumnRef build, probe;
            std::pmr::unordered_multimap<Cell, RowId, Hash> index;
            Join(size_t t, ColumnRef b, ColumnRef p, Budget &budget)
                : table(t), build(b), probe(p), index(&budget) {}
        };
        std::vector<Join> joins;
        joins.reserve(tables - 1);
        // Sample filtered build keys to avoid low-cardinality edges that would
        // multiply rows before a selective primary-key edge becomes available.
        std::map<ColumnRef, double> fanout;
        auto cost = [&](const ColumnRef &column) {
            if (auto it = fanout.find(column); it != fanout.end()) return it->second;
            const auto &input = rows[column.tableIdx];
            size_t samples = std::min<size_t>(4096, input.size());
            std::pmr::unordered_set<Cell, Hash> distinct(&budget);
            for (size_t i = 0; i < samples; ++i) {
                bound[column.tableIdx] = input[i * input.size() / samples];
                distinct.insert(read(column));
            }
            double estimate = samples && distinct.size() < samples ?
                double(input.size()) / distinct.size() : 1.0;
            uint64_t total = 0;
            for (int g = 0; g < q.tables[column.tableIdx]->RowGroupCount(); ++g)
                total += q.tables[column.tableIdx]->GetRowGroup(g).size;
            estimate *= total ? double(input.size()) / total : 0;
            return fanout.emplace(column, estimate).first->second;
        };
        uint64_t joined = uint64_t(1) << probe;
        while (joins.size() + 1 < tables) {
            std::optional<std::pair<ColumnRef, ColumnRef>> edge;
            bool ordinaryRemaining = false;
            for (size_t t = 0; t < tables; ++t)
                ordinaryRemaining |= !existence[t] && !(joined & (uint64_t(1) << t));
            for (const auto &predicate : edges) {
                if (predicate->kind != Expression::EQ || !predicate->left->column || !predicate->right->column)
                    continue;
                auto a = *predicate->left->column, b = *predicate->right->column;
                if (a.tableIdx < 0 || b.tableIdx < 0) continue;
                if (Scale(a.Type().get()) != Scale(b.Type().get())) continue;
                if (joined & (uint64_t(1) << a.tableIdx)) std::swap(a, b);
                if (ordinaryRemaining && existence[a.tableIdx]) continue;
                if (!(joined & (uint64_t(1) << a.tableIdx)) && (joined & (uint64_t(1) << b.tableIdx)) &&
                    (!edge || cost(a) < cost(edge->first) ||
                     (cost(a) == cost(edge->first) && rows[a.tableIdx].size() < rows[edge->first.tableIdx].size())))
                    edge = {a, b};
            }
            if (!edge) return Status::Invalid("Inner joins require a connected equality graph");
            auto [build, other] = *edge;
            joins.emplace_back(build.tableIdx, build, other, budget);
            auto &join = joins.back();
            join.index.reserve(rows[join.table].size());
            for (RowId id : rows[join.table]) {
                bound[join.table] = id;
                join.index.emplace(read(build), id);
            }
            // The hash table owns row identities now; release selection storage.
            std::pmr::vector<RowId> empty(&budget); rows[join.table].swap(empty);
            joined |= uint64_t(1) << join.table;
        }
        bool aggregate = q.grouped;
        for (const auto &a : q.aggregateClauses) aggregate |= a.type != AggregateClause::AGGREGATE_PROJECT;
        std::pmr::unordered_map<Key, JoinedGroup, KeyHash> groups(&budget);
        if (aggregate && !q.grouped)
            groups.try_emplace(Key(&budget), outputs, bound, budget);
        QueryDesc effective = q;
        for (auto &having : effective.having) if (having.bound) {
            try {
                having.value = EvaluateWith(*having.bound, read).number;
                having.scale = Scale(having.bound->type.get());
            } catch (const MissingScalar &) { having.nullBound = true; }
        }
        QueryResults results(effective, budget);
        auto emit = [&] {
            if (!aggregate) {
                results.addLazy([&](size_t i) {
                    const auto &a = q.aggregateClauses[i];
                    return FinalCell{EvaluateWith(*a.expression, read), a.expression->type.get()};
                });
                return;
            }
            Key key(&budget);
            for (const auto &e : q.groupExpressions) key.push_back(EvaluateWith(*e, read));
            auto found = groups.find(key);
            if (found == groups.end())
                found = groups.try_emplace(std::move(key), outputs, bound, budget).first;
            auto &states = found->second.states;
            for (size_t i = 0; i < outputs; ++i) {
                auto &state = states[i]; const auto &a = q.aggregateClauses[i];
                if (a.type == AggregateClause::AGGREGATE_PROJECT || a.type == AggregateClause::AGGREGATE_FORMULA) continue;
                if (a.type == AggregateClause::AGGREGATE_COUNT) { ++state.count; continue; }
                Cell value = EvaluateWith(*a.expression, read);
                if (a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT) Insert(state, value, budget);
                else if (a.type == AggregateClause::AGGREGATE_SUM || a.type == AggregateClause::AGGREGATE_AVG)
                    state.sum = Add(state.sum, value.number);
                else if (!state.count || (a.type == AggregateClause::AGGREGATE_MIN ?
                         Less(value, state.extreme) : Less(state.extreme, value))) state.extreme = value;
                ++state.count;
            }
        };
        std::function<void(size_t, uint64_t)> visit = [&](size_t stage, uint64_t active) {
            if (stage == joins.size()) { emit(); return; }
            auto &join = joins[stage];
            auto range = join.index.equal_range(read(join.probe));
            uint64_t next = active | (uint64_t(1) << join.table);
            if (const auto *exists = existence[join.table]) {
                bool matched = false;
                for (auto it = range.first; it != range.second && !matched; ++it) {
                    bound[join.table] = it->second; matched = true;
                    for (const auto &predicate : exists->predicates)
                        if (!keep(*predicate)) { matched = false; break; }
                }
                if (matched != exists->anti) visit(stage + 1, next);
                return;
            }
            for (auto it = range.first; it != range.second; ++it) {
                bound[join.table] = it->second;
                bool selected = true;
                for (size_t p = 0; p < masks.size() && selected; ++p)
                    if ((masks[p] & ~next) == 0 && (masks[p] & ~active) != 0)
                        selected = keep(*q.predicates[p]);
                if (selected) visit(stage + 1, next);
            }
        };
        for (RowId id : rows[probe]) {
            bound[probe] = id;
            visit(0, uint64_t(1) << probe);
        }
        if (aggregate) for (const auto &[key, group] : groups) {
            bound = group.representative;
            struct NullArithmetic {};
            std::function<FinalCell(size_t)> finalize = [&](size_t i) {
                const auto &a = q.aggregateClauses[i]; const auto &state = group.states[i];
                FinalCell value;
                value.type = a.expression ? a.expression->type.get() : nullptr;
                switch (a.type) {
                case AggregateClause::AGGREGATE_FORMULA:
                    try {
                        value.value = EvaluateWith(*a.expression, [&](const ColumnRef &column) {
                            if (column.tableIdx != -1) return read(column);
                            auto input = finalize(column.columnIdx);
                            if (input.null) throw NullArithmetic{};
                            if (input.average) {
                                long double scaled = input.mean * (long double)Power(Scale(column.Type().get()));
                                if (!std::isfinite(scaled) || scaled < (long double)std::numeric_limits<Wide>::min() ||
                                    scaled >= -(long double)std::numeric_limits<Wide>::min())
                                    throw std::overflow_error("Average arithmetic overflow");
                                return Cell{Wide(scaled)};
                            }
                            return input.value;
                        });
                    } catch (const NullArithmetic &) { value.null = true; }
                    break;
                case AggregateClause::AGGREGATE_PROJECT:
                    value.value = EvaluateWith(*a.expression, read); break;
                case AggregateClause::AGGREGATE_COUNT:
                    value.type = nullptr; value.value.number = state.count; break;
                case AggregateClause::AGGREGATE_COUNT_DISTINCT:
                    value.type = nullptr; value.value.number = state.distinct ? state.distinct->size() : 0; break;
                case AggregateClause::AGGREGATE_SUM:
                    value.value.number = state.sum; value.null = !state.count; break;
                case AggregateClause::AGGREGATE_AVG:
                    value.average = true; value.null = !state.count;
                    if (state.count) value.mean = (long double)state.sum / state.count / (long double)Power(Scale(value.type));
                    break;
                default: value.value = state.extreme; value.null = !state.count; break;
                }
                return value;
            };
            results.addLazy(finalize);
        }
        auto output = results.finish(); output.spillBytes += subquerySpillBytes;
        return output;
    } catch (const std::exception &e) {
        // Do not send a multi-table query to the single-table spill executor.
        return Status::Invalid(e.what());
    }
}
}
