#include "executor_bitmap_probe.hpp"
#include "executor_date_order.hpp"
#include "executor_result.hpp"
#include "executor_buffer.hpp"
#include "util.h"

namespace pgaccel {
namespace {
std::pair<int64_t,int64_t> ExistsKeyBounds(const RowGroup &rg,const ColumnRef &key) {
    auto data=rg.columns[key.columnIdx].get();if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
    if(key.Type()->type_num()==INT64_TYPE) {auto d=static_cast<const RawColumnData<Int64Type> *>(data);return {d->minValue,d->maxValue};}
    if(key.Type()->type_num()==INT32_TYPE) {auto d=static_cast<const RawColumnData<Int32Type> *>(data);return {d->minValue,d->maxValue};}
    throw NotDense{};
}
}
std::optional<Result<QueryOutput>> ExecuteExistsCount(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=2 || q.existenceJoins.size()!=1 || !q.leftJoins.empty() ||
        !q.membershipQueries.empty() || !q.scalarQueries.empty() || !q.predicates.empty() ||
        !q.grouped || q.groupExpressions.size()!=1 || q.aggregateClauses.size()>8)return {};
    const auto &join=q.existenceJoins.front();int inner=join.table,outer=1-inner;
    if(inner<0 || inner>1 || q.groupExpressions[0]->kind!=Expression::COLUMN)return {};
    auto group=*q.groupExpressions[0]->column;
    if(group.tableIdx!=outer || (group.Type()->type_num()!=STRING_TYPE && group.Type()->type_num()!=DATE_TYPE))return {};
    for(const auto &a:q.aggregateClauses) {
        if(a.type==AggregateClause::AGGREGATE_COUNT && !a.expression)continue;
        if(a.type==AggregateClause::AGGREGATE_PROJECT && a.expression && a.expression->column && *a.expression->column==group)continue;
        return {};
    }
    std::optional<ColumnRef> outerKey,innerKey;std::vector<std::shared_ptr<Expression>> comparisons;
    for(const auto &p:join.predicates) {
        if(p->kind==Expression::EQ && p->left->column && p->right->column &&
            p->left->column->tableIdx!=p->right->column->tableIdx) {
            if(outerKey)return {};
            auto a=*p->left->column,b=*p->right->column;if(a.tableIdx!=outer)std::swap(a,b);
            if(a.tableIdx!=outer || b.tableIdx!=inner || Scale(a.Type().get())!=Scale(b.Type().get()))return {};
            outerKey=a;innerKey=b;
        } else if(DateOrderPredicate::supports(*p,inner))comparisons.push_back(p);
        else return {};
    }
    if(!outerKey || comparisons.size()>8)return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);QueryResults results(q,budget);
        auto &outside=*q.tables[outer],&inside=*q.tables[inner];
        int64_t low=INT64_MAX,high=INT64_MIN;uint64_t outerRows=0;
        for(int g=0;g<outside.RowGroupCount();++g) {
            const auto &rg=outside.GetRowGroup(g);if(!rg.size)continue;
            auto bounds=ExistsKeyBounds(rg,*outerKey);low=std::min(low,bounds.first);high=std::max(high,bounds.second);outerRows+=rg.size;
        }
        if(!outerRows)return results.finish();
        Wide width=Wide(high)-low+1;
        if(width<=0 || width>Wide(outerRows)*8 || width>(Wide(1)<<34))return {};
        std::vector<FilterClause> clauses[2];for(const auto &f:q.filterClauses)clauses[f.columnRef.tableIdx].push_back(f);
        FilterNodeP filters[2];for(int i=0;i<2;++i)if(!clauses[i].empty())filters[i]=CreateFilterNode(clauses[i],avx);
        OverwriteBuffer<uint64_t> candidates(size_t((width+63)/64),budget),hits(candidates.size(),budget);
        int workers=parallel?std::min(8,std::max(1,outside.RowGroupCount())):1;
        RunTasks(workers,[&](int worker) {
            size_t begin=candidates.size()*worker/workers,end=candidates.size()*(worker+1)/workers;
            std::fill(candidates.data()+begin,candidates.data()+end,0);
            std::fill(hits.data()+begin,hits.data()+end,0);return true;
        });
        auto selected=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,16384);uint8_t bitmap[RowGroupSize/8];uint64_t total=0;
            for(int g=worker;g<outside.RowGroupCount();g+=workers) {
                const auto &rg=outside.GetRowGroup(g);int count=filters[outer]?filters[outer]->ExecuteSet(rg,bitmap):rg.size;total+=count;
                if(!count)continue;
                Selected(rg,count==rg.size?nullptr:bitmap,[&](int row) {
                    Wide delta=Read(rg,*outerKey,row).number-low;if(delta<0 || delta>=width)throw NotDense{};
                    std::atomic_ref<uint64_t>(candidates[size_t(delta)/64]).fetch_or(uint64_t(1)<<(size_t(delta)%64),std::memory_order_relaxed);
                });
            }
            return total;
        });
        if(std::accumulate(selected.begin(),selected.end(),uint64_t(0))==0)return results.finish();
        BitmapDomain domain{low,high,candidates.data()};
        workers=parallel?std::min(8,std::max(1,inside.RowGroupCount())):1;
        RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,16384);uint8_t bitmap[RowGroupSize/8];uint32_t matched[Block];
            for(int g=worker;g<inside.RowGroupCount();g+=workers) {
                const auto &rg=inside.GetRowGroup(g);int count=filters[inner]?filters[inner]->ExecuteSet(rg,bitmap):rg.size;if(!count)continue;
                auto data=rg.columns[innerKey->columnIdx].get();if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
                auto raw=static_cast<const RawColumnDataBase *>(data);
                std::vector<DateOrderPredicate> predicates;for(const auto &p:comparisons)predicates.emplace_back(*p,rg,budget);
                auto consume=[&](int n) {
                    for(const auto &p:predicates)n=p.select(matched,n,matched,avx);
                    for(int i=0;i<n;++i) {
                        auto delta=uint64_t(Read(rg,*innerKey,matched[i]).number-low);
                        std::atomic_ref<uint64_t>(hits[delta/64]).fetch_or(uint64_t(1)<<(delta%64),std::memory_order_relaxed);
                    }
                };
                if(avx && raw->bytesPerValue==4 && low>=0 && high<=INT32_MAX) {
                    for(int begin=0;begin<rg.size;begin+=Block)consume(BitmapSelectRange32(domain,
                        reinterpret_cast<const int32_t *>(raw->values),begin,std::min(Block,rg.size-begin),count==rg.size?nullptr:bitmap,matched));
                } else {
                    int n=0;Selected(rg,count==rg.size?nullptr:bitmap,[&](int row) {
                        auto key=Read(rg,*innerKey,row).number;if(key<low || key>high || !domain.contains(int64_t(key)))return;
                        matched[n++]=row;if(n==Block){consume(n);n=0;}
                    });if(n)consume(n);
                }
            }
            return true;
        });
        // EXISTS keeps each outer row once, including duplicate outer keys.
        workers=parallel?std::min(8,std::max(1,outside.RowGroupCount())):1;
        auto partials=RunTasks(workers,[&](int worker) {
        ScopedValueResource memory(budget);Scratch scratch(budget,16384);
        std::pmr::unordered_map<Cell,uint64_t,Hash> totals(&budget);uint8_t bitmap[RowGroupSize/8];
        for(int g=worker;g<outside.RowGroupCount();g+=workers) {
            const auto &rg=outside.GetRowGroup(g);int count=filters[outer]?filters[outer]->ExecuteSet(rg,bitmap):rg.size;if(!count)continue;
            auto data=rg.columns[group.columnIdx].get();if(data->type!=ColumnDataBase::DICT_COLUMN_DATA)throw NotDense{};
            auto dict=static_cast<const DictColumnDataBase *>(data);if(dict->dictSize()>256)throw NotDense{};
            std::array<uint64_t,256> counts{};int bytes=dict->bytesPerValue();
            Selected(rg,count==rg.size?nullptr:bitmap,[&](int row) {
                uint64_t delta=uint64_t(Read(rg,*outerKey,row).number-low);
                bool exists=hits[delta/64]>>(delta%64)&1;if(exists==join.anti)return;
                int code=bytes==1?dict->values[row]:reinterpret_cast<const uint16_t *>(dict->values)[row];++counts[code];
            });
            for(size_t code=0;code<dict->dictSize();++code)if(counts[code])totals[Label(dict,code,group.Type()->type_num())]+=counts[code];
        }
        return totals;
        });
        std::pmr::unordered_map<Cell,uint64_t,Hash> totals(&budget);
        for(const auto &part:partials)for(const auto &[label,count]:part)totals[label]+=count;
        for(const auto &[label,count]:totals)results.addLazy([&](size_t i) {
            FinalCell cell;const auto &a=q.aggregateClauses[i];
            if(a.type==AggregateClause::AGGREGATE_COUNT)cell.value.number=count;
            else {cell.type=group.Type().get();cell.value=label;}return cell;
        });
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
