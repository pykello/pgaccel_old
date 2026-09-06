#include "executor_result.hpp"
#include "executor_result_number.hpp"
#include "executor_sequence_join.hpp"
#include "executor_membership.hpp"
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
Cell ResultKey(const std::string &text, const AccelType *type, int scale, Budget &budget) {
    if (type->type_num() == STRING_TYPE) {
        auto owner = std::allocate_shared<std::pmr::string>(std::pmr::polymorphic_allocator<std::pmr::string>(&budget), text);
        return {0, *owner, true, owner};
    }
    Wide value = type->type_num() == DATE_TYPE ? ParseDate(text) :
        type->type_num() == TIMESTAMP_TYPE ? ParseTimestamp(text) : ResultNumber(text, Scale(type));
    return {Multiply(value, Power(scale - Scale(type)))};
}
uint64_t OutputFootprint(const QueryOutput &output) {
    // Include owned container capacities and conservative allocation overhead.
    uint64_t bytes = sizeof(QueryOutput) + 64;
    auto strings = [&](const Row &row) {
        bytes += row.capacity() * sizeof(std::string) + 64;
        for (const auto &value : row) bytes += value.capacity() + 17;
    };
    strings(output.fieldNames);
    bytes += output.values.capacity() * sizeof(Row) + 64;
    for (const auto &row : output.values) strings(row);
    return bytes;
}
// Physical join columns are signed 64-bit values or borrowed strings.
// Keep the general expression Cell out of every retained hash entry.
struct JoinIndex {
    struct IntegerHash {
        size_t operator()(int64_t value) const noexcept { return Hash{}(Cell{Wide(value)}); }
    };
    bool strings;
    SequenceJoinIndex sequence;
    std::pmr::unordered_multimap<int64_t, RowId, IntegerHash> numbers;
    std::pmr::unordered_multimap<std::string_view, RowId> texts;
    JoinIndex(bool text, Budget &budget) : strings(text), sequence(&budget), numbers(&budget), texts(&budget) {}
    static int64_t number(const Cell &key) {
        if (key.string || key.number < INT64_MIN || key.number > INT64_MAX)
            throw std::invalid_argument("Invalid physical numeric join key");
        return int64_t(key.number);
    }
    void reserve(size_t n) { if (strings) texts.reserve(n); else numbers.reserve(n); }
    void insert(const Cell &key, RowId row) {
        if (strings) {
            if (!key.string) throw std::invalid_argument("Invalid physical string join key");
            texts.emplace(key.text, row);
        } else numbers.emplace(number(key), row);
    }
    template<class F> void each(const Cell &key, F &&visit) const {
        if (strings) {
            if (!key.string) throw std::invalid_argument("Invalid physical string join key");
            auto range = texts.equal_range(key.text);
            for (auto it = range.first; it != range.second; ++it) if (!visit(it->second)) break;
        } else {
            if(sequence.active()) {
                if(auto row=sequence.lookup(number(key))) visit(*row);
                return;
            }
            auto range = numbers.equal_range(number(key));
            for (auto it = range.first; it != range.second; ++it) if (!visit(it->second)) break;
        }
    }
};
struct JoinedGroup {
    States states;
    std::pmr::vector<RowId> representative;
    JoinedGroup(size_t n, const std::pmr::vector<RowId> &row, Budget &budget)
        : states(n, &budget), representative(row, &budget) {}
};
uint64_t References(const Expression &e) {
    uint64_t mask = e.column ? (e.column->tableIdx >= 0 ? uint64_t(1) << e.column->tableIdx : e.column->dependencies) : 0;
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
        auto childOutputBytes = [&](const QueryOutput &output) {
            // Child hash states are gone before result conversion starts.
            // Preserve that earlier peak, then charge only retained output.
            { Scratch executed(budget, output.peakMemoryBytes); }
            return OutputFootprint(output);
        };
        struct ScalarCache {
            std::pmr::unordered_map<Key, Cell, KeyHash> values;
            explicit ScalarCache(Budget &budget) : values(&budget) {}
        };
        std::vector<ScalarCache> scalarValues; scalarValues.reserve(q.scalarQueries.size());
        for (const auto &scalar : q.scalarQueries) {
            scalarValues.emplace_back(budget);
            auto sub = childPlan(*scalar.query);
            // A required outer IN domain also bounds the matching correlation
            // key. Apply it before grouping so unreachable child groups never
            // consume aggregate state. Keep the original membership query:
            // its normal executor preserves types, duplicates and budgets.
            for (size_t key = 0; key < scalar.outerKeys.size(); ++key) {
                for (const auto &membership : q.membershipQueries) {
                    if (membership.anti || membership.column != scalar.outerKeys[key]) continue;
                    auto restricted = membership;
                    restricted.column = scalar.innerKeys[key];
                    sub.membershipQueries.push_back(std::move(restricted));
                }
            }
            auto result = ExecuteQuery(sub, avx, false);
            if (!result.ok()) return Status::Invalid("Scalar subquery: " + result.status().Message());
            subquerySpillBytes += result->spillBytes;
            Scratch childMemory(budget, childOutputBytes(*result));
            if (scalar.keyOutputs.empty() && result->values.size() > 1)
                return Status::Invalid("Scalar subquery returned multiple rows");
            for (const auto &row : result->values) {
                if (row[0] == "NULL") continue;
                Key key(&budget);
                for (size_t i = 0; i < scalar.keyOutputs.size(); ++i) {
                    int scale = std::max(Scale(scalar.outerKeys[i].Type().get()), Scale(scalar.innerKeys[i].Type().get()));
                    key.push_back(ResultKey(row[scalar.keyOutputs[i]], scalar.innerKeys[i].Type().get(), scale, budget));
                }
                Cell value{ResultNumber(row[0], Scale(scalar.resultType.get()))};
                if (!scalarValues.back().values.emplace(std::move(key), value).second)
                    return Status::Invalid("Scalar correlation produced duplicate result keys");
            }
        }
        std::vector<MembershipSet> memberships; memberships.reserve(q.membershipQueries.size());
        for (const auto &membership : q.membershipQueries) {
            int sourceScale = Scale(membership.resultType.get());
            int scale = std::max(sourceScale, Scale(membership.column.Type().get()));
            memberships.emplace_back(budget, scale);
            auto sub = childPlan(*membership.query);
            auto result = ExecuteQuery(sub, avx, false);
            if (!result.ok()) return Status::Invalid("Membership subquery: " + result.status().Message());
            subquerySpillBytes += result->spillBytes;
            Scratch childMemory(budget, childOutputBytes(*result));
            for (const auto &row : result->values) {
                Cell key = ResultKey(row[0], membership.resultType.get(), scale, budget);
                memberships.back().values.insert(std::move(key));
            }
            memberships.back().compact(budget);
        }
        std::pmr::vector<RowId> bound(tables, 0, &budget);
        struct MissingScalar {};
        auto baseRead = [&](const ColumnRef &column) {
            if (column.tableIdx < 0) throw std::invalid_argument("Invalid relational column binding");
            RowId id = bound[column.tableIdx];
            if (id == UINT64_MAX) throw std::invalid_argument("Unsupported nullable column read");
            return Read(q.tables[column.tableIdx]->GetRowGroup(id >> 32), column, uint32_t(id));
        };
        auto read = [&](const ColumnRef &column) {
            if (column.tableIdx != -2) return baseRead(column);
            const auto &scalar = q.scalarQueries.at(column.columnIdx);
            Key key(&budget);
            for (size_t i = 0; i < scalar.outerKeys.size(); ++i) {
                auto value = baseRead(scalar.outerKeys[i]);
                int scale = std::max(Scale(scalar.outerKeys[i].Type().get()), Scale(scalar.innerKeys[i].Type().get()));
                if (!value.string) value.number = Multiply(value.number, Power(scale - Scale(scalar.outerKeys[i].Type().get())));
                key.push_back(value);
            }
            auto &cache = scalarValues.at(column.columnIdx).values;
            auto found = cache.find(key);
            if (found == cache.end()) throw MissingScalar{};
            return found->second;
        };
        auto keep = [&](const Expression &predicate) {
            try { return bool(EvaluateWith(predicate, read).number); }
            catch (const MissingScalar &) { return false; }
        };
        std::vector<const QueryDesc::ExistenceJoin *> existence(tables, nullptr);
        for (const auto &join : q.existenceJoins) existence[join.table] = &join;
        std::vector<const QueryDesc::LeftJoin *> left(tables, nullptr);
        for (const auto &join : q.leftJoins) left[join.table] = &join;
        std::vector<std::shared_ptr<Expression>> edges = q.predicates;
        for (const auto &join : q.leftJoins)
            for (const auto &e : join.predicates)
                if (e->kind == Expression::EQ && e->left->column && e->right->column &&
                    (e->left->column->tableIdx == join.table || e->right->column->tableIdx == join.table)) edges.push_back(e);
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
            if (left[table])
                for (const auto &predicate : left[table]->predicates)
                    if (auto projected = LocalPredicate(predicate, uint64_t(1) << table)) local.push_back(projected);
            auto filter = clauses.empty() ? FilterNodeP{} : CreateFilterNode(clauses, avx);
            for (int g = 0; g < q.tables[table]->RowGroupCount(); ++g) {
                const auto &rg = q.tables[table]->GetRowGroup(g);
                uint8_t bitmap[BITMAP_SIZE];
                if (filter && !filter->ExecuteSet(rg, bitmap)) continue;
                bool selected = bool(filter);
                for (size_t i = 0; i < q.membershipQueries.size(); ++i) {
                    const auto &query = q.membershipQueries[i];
                    if (query.column.tableIdx != int(table)) continue;
                    if (!selected) memset(bitmap, 0xff, (rg.size + 7) / 8);
                    memberships[i].intersect(rg, query.column, query.anti, avx, bitmap);
                    selected = true;
                }
                Selected(rg, selected ? bitmap : nullptr, [&](int row) {
                    bound[table] = (uint64_t(g) << 32) | uint32_t(row);
                    for (const auto &predicate : local)
                        if (!keep(*predicate)) return;
                    rows.back().push_back(bound[table]);
                });
            }
        }
        size_t probe = 0;
        for (size_t t = 1; t < tables; ++t)
            if (!existence[t] && !left[t] && (existence[probe] || left[probe] || rows[t].size() > rows[probe].size())) probe = t;
        // A verified positional build has no hash-directory construction.
        // For two ordinary inputs, probe the smaller filtered side when the
        // larger side can supply this layout. Include the proof in query time.
        std::optional<std::pair<ColumnRef,SequenceJoinIndex>> positional;
        if(tables==2 && q.existenceJoins.empty() && q.leftJoins.empty()) {
            for(const auto &predicate:edges) {
                if(predicate->kind!=Expression::EQ || !predicate->left->column || !predicate->right->column) continue;
                auto a=*predicate->left->column,b=*predicate->right->column;
                if(a.tableIdx<0 || b.tableIdx<0 || a.tableIdx==b.tableIdx ||
                    Scale(a.Type().get())!=Scale(b.Type().get())) continue;
                if(size_t(a.tableIdx)!=probe) std::swap(a,b);
                if(rows[b.tableIdx].size()>=rows[a.tableIdx].size()) continue;
                SequenceJoinIndex candidate(&budget);
                if(candidate.prepare(*q.tables[a.tableIdx],a,rows[a.tableIdx].size(),avx)) {
                    positional.emplace(a,std::move(candidate)); probe=b.tableIdx; break;
                }
            }
        }
        struct Join {
            size_t table;
            ColumnRef build, probe;
            JoinIndex index;
            Join(size_t t, ColumnRef b, ColumnRef p, Budget &budget)
                : table(t), build(b), probe(p), index(b.Type()->type_num() == STRING_TYPE, budget) {}
        };
        std::vector<Join> joins;
        joins.reserve(tables - 1);
        // Sample filtered build keys to avoid low-cardinality edges that would
        // multiply rows before a selective primary-key edge becomes available.
        std::map<ColumnRef, double> fanout;
        auto cost = [&](const ColumnRef &column) {
            if(positional && positional->first==column) return 0.0;
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
                ordinaryRemaining |= !existence[t] && !left[t] && !(joined & (uint64_t(1) << t));
            for (const auto &predicate : edges) {
                if (predicate->kind != Expression::EQ || !predicate->left->column || !predicate->right->column)
                    continue;
                auto a = *predicate->left->column, b = *predicate->right->column;
                if (a.tableIdx < 0 || b.tableIdx < 0) continue;
                if (Scale(a.Type().get()) != Scale(b.Type().get())) continue;
                if (joined & (uint64_t(1) << a.tableIdx)) std::swap(a, b);
                if (ordinaryRemaining && (existence[a.tableIdx] || left[a.tableIdx])) continue;
                if (!(joined & (uint64_t(1) << a.tableIdx)) && (joined & (uint64_t(1) << b.tableIdx)) &&
                    (!edge || cost(a) < cost(edge->first) ||
                     (cost(a) == cost(edge->first) && rows[a.tableIdx].size() < rows[edge->first.tableIdx].size())))
                    edge = {a, b};
            }
            if (!edge) return Status::Invalid("Inner joins require a connected equality graph");
            auto [build, other] = *edge;
            joins.emplace_back(build.tableIdx, build, other, budget);
            auto &join = joins.back();
            bool sequence=false;
            if(positional && positional->first==build) {
                join.index.sequence=positional->second; sequence=true;
            } else sequence=join.index.sequence.prepare(*q.tables[join.table],build,rows[join.table].size(),avx);
            if(!sequence) {
                join.index.reserve(rows[join.table].size());
                for (RowId id : rows[join.table]) {
                    bound[join.table] = id;
                    join.index.insert(read(build), id);
                }
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
                if ((a.type == AggregateClause::AGGREGATE_COUNT || a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT) &&
                    a.expression && a.expression->column && a.expression->column->tableIdx >= 0 &&
                    bound[a.expression->column->tableIdx] == UINT64_MAX) continue;
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
            auto key = read(join.probe);
            uint64_t next = active | (uint64_t(1) << join.table);
            if (const auto *exists = existence[join.table]) {
                bool matched = false;
                join.index.each(key, [&](RowId row) {
                    bound[join.table] = row; matched = true;
                    for (const auto &predicate : exists->predicates)
                        if (!keep(*predicate)) { matched = false; break; }
                    return !matched;
                });
                if (matched != exists->anti) visit(stage + 1, next);
                return;
            }
            if (const auto *outer = left[join.table]) {
                bool matched = false;
                join.index.each(key, [&](RowId row) {
                    bound[join.table] = row;
                    bool selected = true;
                    for (const auto &predicate : outer->predicates)
                        if (!keep(*predicate)) { selected = false; break; }
                    if (selected) { matched = true; visit(stage + 1, next); }
                    return true;
                });
                if (!matched) { bound[join.table] = UINT64_MAX; visit(stage + 1, next); }
                return;
            }
            join.index.each(key, [&](RowId row) {
                bound[join.table] = row;
                bool selected = true;
                for (size_t p = 0; p < masks.size() && selected; ++p)
                    if ((masks[p] & ~next) == 0 && (masks[p] & ~active) != 0)
                        selected = keep(*q.predicates[p]);
                if (selected) visit(stage + 1, next);
                return true;
            });
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
                                return Cell{Wide(std::round(scaled))};
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
