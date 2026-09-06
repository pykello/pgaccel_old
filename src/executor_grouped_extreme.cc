#include "executor_bounded_sum.hpp"
#include "executor_literal_filter.hpp"
#include "executor_sequence_join.hpp"
#include "executor_result.hpp"
#include "util.h"
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteGroupedExtreme(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=2 || q.materializedSources.size()!=1 || q.scalarQueries.size()!=1 ||
        q.predicates.size()<2 || q.grouped || !q.groupExpressions.empty() || !q.groupBy.empty() ||
        !q.having.empty() || !q.membershipQueries.empty() || !q.leftJoins.empty() || !q.existenceJoins.empty())return {};
    const auto &source=q.materializedSources[0];const auto &group=*source.query;
    const auto &scalar=q.scalarQueries[0];const auto &extreme=*scalar.query;
    if(source.table>=2 || !scalar.outerKeys.empty() || !scalar.innerKeys.empty() || !scalar.keyOutputs.empty() ||
        extreme.tables.size()!=1 || extreme.materializedSources.size()!=1 ||
        extreme.materializedSources[0].table!=0 || extreme.materializedSources[0].query!=source.query ||
        extreme.aggregateClauses.size()!=1 || extreme.grouped || !extreme.groupExpressions.empty() || !extreme.groupBy.empty() ||
        !extreme.filterClauses.empty() || !extreme.predicates.empty() || !extreme.having.empty() ||
        !extreme.scalarQueries.empty() || !extreme.membershipQueries.empty() || !extreme.leftJoins.empty() ||
        !extreme.existenceJoins.empty() || extreme.limit || extreme.offset ||
        extreme.memoryBudgetBytes<q.memoryBudgetBytes || group.memoryBudgetBytes<q.memoryBudgetBytes)return {};
    const auto &aggregate=extreme.aggregateClauses[0];
    if((aggregate.type!=AggregateClause::AGGREGATE_MAX && aggregate.type!=AggregateClause::AGGREGATE_MIN) ||
        !aggregate.columnRef || aggregate.columnRef->tableIdx!=0)return {};
    if(group.tables.size()!=1 || !group.grouped || group.groupExpressions.size()!=1 || !group.groupExpressions[0]->column ||
        group.aggregateClauses.size()!=2 || group.outputColumnCount.value_or(2)!=2 ||
        !group.predicates.empty() || !group.having.empty() || !group.scalarQueries.empty() || !group.membershipQueries.empty() ||
        !group.leftJoins.empty() || !group.existenceJoins.empty() || !group.materializedSources.empty() || group.limit || group.offset)return {};
    int key=-1,sum=-1;
    for(int i=0;i<2;++i) {
        const auto &a=group.aggregateClauses[i];
        if(a.type==AggregateClause::AGGREGATE_PROJECT && a.columnRef && *a.columnRef==*group.groupExpressions[0]->column)key=i;
        else if(a.type==AggregateClause::AGGREGATE_SUM && a.expression)sum=i;
        else return {};
    }
    if(key<0 || sum<0 || aggregate.columnRef->columnIdx!=sum)return {};
    int dimension=1-source.table;ColumnRef dimensionKey;bool joined=false,compared=false;
    auto dimensionClauses=q.filterClauses;
    for(const auto &f:dimensionClauses)if(f.columnRef.tableIdx!=dimension)return {};
    for(const auto &e:q.predicates) {
        if(auto filter=ExactLiteralFilter(*e)) {
            if(filter->columnRef.tableIdx!=dimension)return {};
            dimensionClauses.push_back(std::move(*filter));continue;
        }
        if(e->kind!=Expression::EQ || !e->left->column || !e->right->column)return {};
        auto a=*e->left->column,b=*e->right->column;
        if(a.tableIdx!=int(source.table))std::swap(a,b);
        if(a.tableIdx!=int(source.table))return {};
        if(b.tableIdx==-2 && b.columnIdx==0 && a.columnIdx==sum && !compared)compared=true;
        else if(b.tableIdx==dimension && a.columnIdx==key && !joined){dimensionKey=b;joined=true;}
        else return {};
        if(Scale(a.Type().get())!=Scale(b.Type().get()))return {};
    }
    if(!joined || !compared || (dimensionKey.Type()->type_num()!=INT32_TYPE && dimensionKey.Type()->type_num()!=INT64_TYPE))return {};
    for(const auto &a:q.aggregateClauses)if(a.type!=AggregateClause::AGGREGATE_PROJECT || !a.columnRef ||
        (a.columnRef->tableIdx!=dimension && (a.columnRef->tableIdx!=int(source.table) ||
            (a.columnRef->columnIdx!=key && a.columnRef->columnIdx!=sum))))return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);
        const auto &table=*q.tables[dimension];uint64_t rows=0;
        for(int g=0;g<table.RowGroupCount();++g)rows+=table.GetRowGroup(g).size;
        SequenceJoinIndex sequence(&budget);if(!sequence.prepare(table,dimensionKey,rows,avx))return {};
        auto filter=group.filterClauses.empty()?FilterNodeP{}:CreateFilterNode(group.filterClauses,avx);
        auto sums=ScanBoundedSums(*group.tables[0],*group.aggregateClauses[key].columnRef,
            *group.aggregateClauses[sum].expression,filter.get(),budget,avx,parallel);
        bool found=false;Wide best=0;
        for(size_t i=0;i<sums.sums.size();++i)if(sums.contains(i)) {
            Wide value=sums.sums[i];
            if(!found || (aggregate.type==AggregateClause::AGGREGATE_MAX?value>best:value<best)){found=true;best=value;}
        }
        QueryResults results(q,budget);if(!found)return results.finish();
        auto dimensionFilter=dimensionClauses.empty()?FilterNodeP{}:CreateFilterNode(dimensionClauses,avx);
        int cachedStripe=-1;uint8_t bitmap[RowGroupSize/8];
        for(size_t i=0;i<sums.sums.size();++i)if(sums.contains(i) && sums.sums[i]==best) {
            int64_t groupKey=int64_t(Wide(sums.first)+i);auto row=sequence.lookup(groupKey);if(!row)continue;
            const auto &rg=table.GetRowGroup(*row>>32);
            if(dimensionFilter) {
                if(cachedStripe!=int(*row>>32)){dimensionFilter->ExecuteSet(rg,bitmap);cachedStripe=*row>>32;}
                if(!IsBitSet(bitmap,uint32_t(*row)))continue;
            }
            results.addLazy([&](size_t output) {
                const auto &column=*q.aggregateClauses[output].columnRef;FinalCell cell;cell.type=column.Type().get();
                if(column.tableIdx==dimension)cell.value=Read(rg,column,uint32_t(*row));
                else cell.value.number=column.columnIdx==key?Wide(groupKey):best;
                return cell;
            });
        }
        return results.finish();
    } catch(const NotDense &){return {};}
    catch(const std::exception &e){return Status::Invalid(e.what());}
}
}
