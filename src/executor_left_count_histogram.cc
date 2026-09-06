#include "executor_arithmetic_program.hpp"
#include "executor_literal_filter.hpp"
#include "executor_sequence_join.hpp"
#include "executor_result.hpp"
#include "util.h"
#include <bit>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteLeftCountHistogram(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=1 || q.materializedSources.size()!=1 || q.materializedSources[0].table!=0 ||
        !q.grouped || q.groupExpressions.size()!=1 || !q.groupExpressions[0]->column || q.aggregateClauses.size()!=2 ||
        !q.filterClauses.empty() || !q.predicates.empty() || !q.having.empty() || !q.scalarQueries.empty() ||
        !q.membershipQueries.empty() || !q.leftJoins.empty() || !q.existenceJoins.empty())return {};
    const auto &child=*q.materializedSources[0].query;
    if(child.tables.size()!=2 || child.leftJoins.size()!=1 || !child.grouped || child.groupExpressions.size()!=1 ||
        !child.groupExpressions[0]->column || child.aggregateClauses.size()!=2 || child.outputColumnCount.value_or(2)!=2 ||
        !child.filterClauses.empty() || !child.predicates.empty() || !child.having.empty() || !child.scalarQueries.empty() ||
        !child.membershipQueries.empty() || !child.existenceJoins.empty() || !child.materializedSources.empty() ||
        child.limit || child.offset || child.memoryBudgetBytes<q.memoryBudgetBytes)return {};
    int right=child.leftJoins[0].table;if(right<0 || right>1)return {};int left=1-right;
    int childCount=-1,childKey=-1;ColumnRef primary=*child.groupExpressions[0]->column,foreign;
    if(primary.tableIdx!=left)return {};
    for(int i=0;i<2;++i) {
        const auto &a=child.aggregateClauses[i];
        if(a.type==AggregateClause::AGGREGATE_PROJECT && a.columnRef && *a.columnRef==primary)childKey=i;
        else if(a.type==AggregateClause::AGGREGATE_COUNT && a.columnRef && a.expression &&
            a.expression->kind==Expression::COLUMN && a.columnRef->tableIdx==right)childCount=i;
        else return {};
    }
    if(childKey<0 || childCount<0)return {};
    int countOutput=-1,keyOutput=-1;
    const auto &groupColumn=*q.groupExpressions[0]->column;
    if(groupColumn.tableIdx!=0 || groupColumn.columnIdx!=childCount)return {};
    for(int i=0;i<2;++i) {
        const auto &a=q.aggregateClauses[i];
        if(a.type==AggregateClause::AGGREGATE_PROJECT && a.columnRef && *a.columnRef==groupColumn)keyOutput=i;
        else if(a.type==AggregateClause::AGGREGATE_COUNT && !a.expression)countOutput=i;
        else return {};
    }
    if(keyOutput<0 || countOutput<0)return {};
    bool joined=false;std::vector<FilterClause> clauses;
    for(const auto &e:child.leftJoins[0].predicates) {
        if(e->kind==Expression::EQ && e->left->column && e->right->column) {
            auto a=*e->left->column,b=*e->right->column;if(a.tableIdx!=left)std::swap(a,b);
            if(joined || a!=primary || b.tableIdx!=right)return {};foreign=b;joined=true;
        } else {
            auto f=ExactLiteralFilter(*e);
            if(!f && e->kind==Expression::NOT && e->left && e->left->kind==Expression::LIKE) {
                f=ExactLiteralFilter(*e->left);if(f)f->op=FilterClause::FILTER_NOT_LIKE;
            }
            if(!f || f->columnRef.tableIdx!=right)return {};clauses.push_back(std::move(*f));
        }
    }
    if(!joined)return {};
    for(const auto &column:{primary,foreign,*child.aggregateClauses[childCount].columnRef})
        if(column.Type()->type_num()!=INT32_TYPE && column.Type()->type_num()!=INT64_TYPE)return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);
        const auto &dimension=*child.tables[left],&fact=*child.tables[right];uint64_t leftRows=0,rightRows=0;
        for(int g=0;g<dimension.RowGroupCount();++g)leftRows+=dimension.GetRowGroup(g).size;
        for(int g=0;g<fact.RowGroupCount();++g)rightRows+=fact.GetRowGroup(g).size;
        if(rightRows>UINT32_MAX)throw NotDense{};
        SequenceJoinIndex sequence(&budget);if(!sequence.prepare(dimension,primary,leftRows,avx))return {};
        auto filter=clauses.empty()?FilterNodeP{}:CreateFilterNode(clauses,avx);
        int workers=parallel?std::min(8,std::max(1,fact.RowGroupCount())):1;
        auto counts=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);std::pmr::vector<uint32_t> values(sequence.size(),0,&budget);
            Scratch scratch(budget,Block*sizeof(int64_t)+RowGroupSize/8+4096);
            alignas(64)int64_t keys[Block];uint8_t bitmap[RowGroupSize/8];
            for(int g=worker;g<fact.RowGroupCount();g+=workers) {
                const auto &rg=fact.GetRowGroup(g);int count=filter?filter->ExecuteSet(rg,bitmap):rg.size;if(!count)continue;
                auto data=rg.columns[foreign.columnIdx].get();
                auto counted=rg.columns[child.aggregateClauses[childCount].columnRef->columnIdx].get();
                if(data->type!=ColumnDataBase::RAW_COLUMN_DATA || counted->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
                auto raw=static_cast<const RawColumnDataBase *>(data);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int n=std::min(Block,rg.size-begin);
                    switch(raw->bytesPerValue) {
                    case 1:DecodeNumbers<int8_t,false>(raw,begin,n,keys,avx);break;
                    case 2:DecodeNumbers<int16_t,false>(raw,begin,n,keys,avx);break;
                    case 4:DecodeNumbers<int32_t,false>(raw,begin,n,keys,avx);break;
                    case 8:DecodeNumbers<int64_t,false>(raw,begin,n,keys,avx);break;
                    default:throw NotDense{};
                    }
                    for(int i=0;i<n;++i)if(count==rg.size || IsBitSet(bitmap,begin+i)) {
                        uint64_t slot=uint64_t(keys[i])-uint64_t(sequence.minimum());
                        if(slot<sequence.size())++values[slot];
                    }
                }
            }
            return values;
        });
        auto histograms=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);std::pmr::vector<uint64_t> bins(64,0,&budget);
            for(uint64_t slot=leftRows*worker/workers;slot<leftRows*(worker+1)/workers;++slot) {
                uint64_t count=0;for(const auto &part:counts)count+=part[slot];
                if(count>=(1<<20))throw NotDense{};
                if(count>=bins.size())bins.resize(std::bit_ceil(count+1),0);
                ++bins[count];
            }
            return bins;
        });
        std::pmr::vector<uint64_t> bins(&budget);
        for(const auto &part:histograms){if(bins.size()<part.size())bins.resize(part.size(),0);for(size_t i=0;i<part.size();++i)bins[i]+=part[i];}
        QueryResults result(q,budget);
        for(size_t i=0;i<bins.size();++i)if(bins[i])result.addLazy([&](size_t output) {
            return FinalCell{Cell{output==size_t(keyOutput)?Wide(i):Wide(bins[i])},
                output==size_t(keyOutput)?q.aggregateClauses[output].expression->type.get():nullptr};
        });
        return result.finish();
    } catch(const NotDense &){return {};}
    catch(const std::exception &e){return Status::Invalid(e.what());}
}
}
