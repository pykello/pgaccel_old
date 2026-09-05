#include "executor_state.hpp"
namespace pgaccel {
Result<QueryOutput> ExecuteModern(const QueryDesc &q, bool avx, bool parallel) {
    try {
        Budget budget{{0},q.memoryBudgetBytes};
        auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses,avx);
        auto &table = *q.tables[0]; size_t n = q.aggregateClauses.size();
        bool aggregate = q.grouped;
        bool distinct = false;
        for (auto &a : q.aggregateClauses) { aggregate |= a.type != AggregateClause::AGGREGATE_PROJECT; distinct |= a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT; }
        QueryOutput output; for (auto &a : q.aggregateClauses) output.fieldNames.push_back(a.ToString());
        std::vector<FinalRow> rows;
        if (!aggregate) {
            for (int g = 0; g < table.RowGroupCount(); ++g) {
                const auto &rg = table.GetRowGroup(g); uint8_t bitmap[RowGroupSize/8];
                if (filter && !filter->ExecuteSet(rg,bitmap)) continue;
                Selected(rg,filter ? bitmap : nullptr,[&](int r) {
                    if (q.limit && q.orderBy.empty() && rows.size() >= *q.limit) return;
                    budget.charge(64+n*sizeof(FinalCell)); FinalRow row;
                    for (auto &a : q.aggregateClauses) row.push_back({Evaluate(*a.expression,rg,r),a.expression->type.get()});
                    rows.push_back(std::move(row));
                });
                if (q.limit && q.orderBy.empty() && rows.size() >= *q.limit) break;
            }
        } else {
            int threads = parallel ? std::min(8,std::max(1,table.RowGroupCount())) : 1;
            auto task = [&](int thread) {
                Groups groups;
                if (q.groupBy.empty() && !q.grouped) Get(groups,{},n,budget);
                for (int g = thread; g < table.RowGroupCount(); g += threads) {
                    const auto &rg = table.GetRowGroup(g); uint8_t bitmap[RowGroupSize/8];
                    int selected = filter ? filter->ExecuteSet(rg,bitmap) : rg.size;
                    if (!selected) continue;
                    const uint8_t *selection = filter && selected != rg.size ? bitmap : nullptr;
                    const DictColumnDataBase *dict = nullptr;
                    if (!q.groupBy.empty() && rg.columns[q.groupBy[0].columnIdx]->type == ColumnDataBase::DICT_COLUMN_DATA)
                        dict = static_cast<const DictColumnDataBase*>(rg.columns[q.groupBy[0].columnIdx].get());
                    if (dict && !distinct) {
                        // Dense local codes, contiguous accumulators, only selected rows and visited labels.
                        std::vector<States> local(dict->dictSize()); std::vector<int> visited;
                        Selected(rg,selection,[&](int r) {
                            int code = Code(dict,r); auto &state = local[code];
                            if (state.empty()) { state.resize(n); visited.push_back(code); }
                            for (size_t i = 0; i < n; ++i) Update(state[i],q.aggregateClauses[i],rg,r,budget);
                        });
                        for (int code : visited) {
                            auto &state = Get(groups,Label(dict,code,q.groupBy[0].Type()->type_num()),n,budget);
                            Merge(state,local[code],q,budget);
                        }
                    } else {
                        std::vector<States*> cache(dict ? dict->dictSize() : 0,nullptr);
                        States *single = q.groupBy.empty() ? &Get(groups,{},n,budget) : nullptr;
                        Selected(rg,selection,[&](int r) {
                            States *state = single;
                            if (dict) {
                                int code = Code(dict,r); state = cache[code];
                                if (!state) state = cache[code] = &Get(groups,Label(dict,code,q.groupBy[0].Type()->type_num()),n,budget);
                            } else if (!state) state = &Get(groups,Read(rg,q.groupBy[0],r),n,budget);
                            for (size_t i = 0; i < n; ++i) Update((*state)[i],q.aggregateClauses[i],rg,r,budget);
                        });
                    }
                }
                return groups;
            };
            Groups groups;
            if (threads == 1) groups = task(0);
            else {
                std::vector<std::future<Groups>> futures;
                for (int t = 0; t < threads; ++t) futures.push_back(std::async(std::launch::async,task,t));
                for (auto &future : futures) {
                    auto local = future.get();
                    for (auto &entry : local) {
                        auto found = groups.find(entry.first);
                        if (found == groups.end()) groups.emplace(entry.first,std::move(entry.second));
                        else Merge(found->second,entry.second,q,budget);
                    }
                }
            }
            for (auto &entry : groups) {
                budget.charge(64+n*sizeof(FinalCell)); FinalRow row;
                for (size_t i = 0; i < n; ++i) {
                    auto &a = q.aggregateClauses[i]; auto &s = entry.second[i]; FinalCell v;
                    v.type = a.expression ? a.expression->type.get() : nullptr;
                    switch (a.type) {
                    case AggregateClause::AGGREGATE_PROJECT:
                        v.value = a.expression->kind == Expression::CONSTANT ? Evaluate(*a.expression,RowGroup{},0) : entry.first; break;
                    case AggregateClause::AGGREGATE_COUNT: v.type = nullptr; v.value.number = s.count; break;
                    case AggregateClause::AGGREGATE_COUNT_DISTINCT: v.type = nullptr; v.value.number = s.distinct ? s.distinct->size() : 0; break;
                    case AggregateClause::AGGREGATE_SUM: v.value.number = s.sum; v.null = !s.count; break;
                    case AggregateClause::AGGREGATE_AVG: v.average = true; v.null = !s.count; if (s.count) v.mean = (long double)s.sum/s.count/(long double)Power(Scale(v.type)); break;
                    default: v.value = s.extreme; v.null = !s.count; break;
                    }
                    row.push_back(v);
                }
                rows.push_back(std::move(row));
            }
        }
        auto compare = [&](const FinalRow &a, const FinalRow &b) {
            for (const auto &o : q.orderBy) {
                const auto &l = a[o.output], &r = b[o.output];
                if (l.null != r.null) return l.null; // SQL default NULLS FIRST ascending.
                if (l.null) continue;
                bool eq = l.average ? l.mean == r.mean : l.value == r.value;
                if (eq) continue;
                bool less = l.average ? l.mean < r.mean : Less(l.value,r.value);
                return o.descending ? !less : less;
            }
            // Stable deterministic tie-breaks; ties remain SQL-equivalent.
            for (size_t i = 0; i < a.size(); ++i) if (!(a[i].value == b[i].value)) return Less(a[i].value,b[i].value);
            return false;
        };
        size_t keep = q.limit ? std::min<uint64_t>(*q.limit,rows.size()) : rows.size();
        if (!q.orderBy.empty() || q.grouped) {
            if (keep < rows.size()) std::partial_sort(rows.begin(),rows.begin()+keep,rows.end(),compare);
            else std::sort(rows.begin(),rows.end(),compare);
        }
        rows.resize(keep);
        for (const auto &r : rows) { Row row; for (const auto &v : r) row.push_back(Format(v)); output.values.push_back(std::move(row)); }
        return output;
    } catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
