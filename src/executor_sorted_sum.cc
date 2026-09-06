#include "executor_sorted_sum.hpp"
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteSortedSum(const QueryDesc &q,bool avx,bool parallel) {
    if (q.tables.size()!=1 || !q.grouped || q.groupExpressions.size()!=1 ||
        !q.groupExpressions[0]->column || q.aggregateClauses.size()!=2 ||
        !q.predicates.empty() || !q.scalarQueries.empty() || !q.membershipQueries.empty() ||
        !q.leftJoins.empty() || !q.existenceJoins.empty() || !q.materializedSources.empty()) return {};
    int key=-1,sum=-1;
    for (size_t i=0;i<2;++i) {
        const auto &a=q.aggregateClauses[i];
        if (a.type==AggregateClause::AGGREGATE_PROJECT && a.columnRef &&
            *a.columnRef==*q.groupExpressions[0]->column) key=i;
        else if (a.type==AggregateClause::AGGREGATE_SUM && a.expression) sum=i;
        else return {};
    }
    if (key<0 || sum<0) return {};
    for (const auto &h:q.having) if (h.bound) return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);
        auto filter=q.filterClauses.empty()?FilterNodeP{}:CreateFilterNode(q.filterClauses,avx);
        auto scan=ScanSortedSums(*q.tables[0],*q.groupExpressions[0]->column,*q.aggregateClauses[sum].expression,
            [&](const RowGroup &rg,uint8_t *bitmap) {
                if (filter) return bool(filter->ExecuteSet(rg,bitmap));
                memset(bitmap,0xff,(rg.size+7)/8);return true;
            },budget,avx,parallel);
        return FinishSortedSums(q,key,sum,scan,budget,parallel);
    } catch (const NotDense &) { return {}; }
    catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
