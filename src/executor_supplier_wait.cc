#include "executor_identity_payload.hpp"
#include "executor_wait_summary.hpp"
#include "executor_date_order.hpp"
#include "executor_literal_filter.hpp"
#include "executor_result.hpp"

namespace pgaccel {
namespace {
bool WaitCorrelation(const Expression &e,Expression::Kind kind,const ColumnRef &outer,int inner) {
    if(e.kind!=kind || !e.left || !e.right || e.left->kind!=Expression::COLUMN || e.right->kind!=Expression::COLUMN ||
       !e.left->column || !e.right->column)return false;
    auto a=*e.left->column,b=*e.right->column;if(!(a==outer))std::swap(a,b);
    return a==outer && b.tableIdx==inner && b.columnIdx==outer.columnIdx;
}
bool WaitSameDate(const Expression &a,const Expression &b,int inner) {
    if(!DateOrderPredicate::supports(b,inner))return false;
    auto signature=[](const Expression &e) {
        int left=e.left->column->columnIdx,right=e.right->column->columnIdx;
        bool strict=e.kind==Expression::LT || e.kind==Expression::GT;
        if(e.kind==Expression::GT || e.kind==Expression::GTE)std::swap(left,right);
        return std::tuple{left,right,strict};
    };
    return signature(a)==signature(b);
}

}
std::optional<Result<QueryOutput>> ExecuteSupplierWait(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=6 || q.existenceJoins.size()!=2 || !q.grouped || q.groupExpressions.size()!=1 ||
       !q.having.empty() || !q.leftJoins.empty() || !q.membershipQueries.empty() ||
       !q.scalarQueries.empty() || !q.materializedSources.empty() || q.aggregateClauses.size()>4)return {};
    const auto &group=q.groupExpressions[0];
    if(group->kind!=Expression::COLUMN || !group->column || group->type->type_num()!=STRING_TYPE)return {};
    int supplier=group->column->tableIdx;
    for(const auto &a:q.aggregateClauses) {
        if(a.type==AggregateClause::AGGREGATE_COUNT && !a.expression)continue;
        if(a.type==AggregateClause::AGGREGATE_PROJECT && a.expression && a.expression->kind==Expression::COLUMN && a.expression->column==group->column)continue;
        return {};
    }
    QueryDesc joins=q;joins.predicates.clear();std::shared_ptr<Expression> late;int fact=-1;
    for(const auto &p:q.predicates) {
        if(p->left && p->left->column && DateOrderPredicate::supports(*p,p->left->column->tableIdx)) {
            if(late)return {};late=p;fact=p->left->column->tableIdx;
        } else if(auto f=ExactLiteralFilter(*p))joins.filterClauses.push_back(std::move(*f));
        else joins.predicates.push_back(p);
    }
    if(!late || fact<0 || supplier<0 || supplier==fact)return {};
    int positive=-1,negative=-1;
    for(const auto &join:q.existenceJoins) {
        if(join.table<0 || join.table>=int(q.tables.size()) || join.table==fact || join.table==supplier ||
           q.tables[join.table]!=q.tables[fact])return {};
        int &slot=join.anti?negative:positive;if(slot>=0)return {};slot=join.table;
    }
    if(positive<0 || negative<0 || positive==negative)return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);Scratch planning(budget,16384);
        UniqueKeyJoinGraph plan;if(!plan.prepare(joins,fact,budget,avx,parallel) || plan.outgoing[fact].size()!=2 ||
           !plan.keys[supplier] || !plan.keys[supplier]->sequence.active() || plan.keys[supplier]->sequence.size()>UINT32_MAX ||
           plan.outgoing[supplier].size()!=1)return {};
        int nation=plan.outgoing[supplier].front().target,orders=-1;std::optional<ColumnRef> supplierKey,orderKey;
        for(const auto &edge:plan.outgoing[fact]) {
            if(edge.target==supplier)supplierKey=edge.source;
            else {orders=edge.target;orderKey=edge.source;}
        }
        if(!supplierKey || !orderKey || !plan.keys[orders] || !plan.keys[nation] ||
           !plan.outgoing[orders].empty() || !plan.outgoing[nation].empty() ||
           std::set<int>{fact,supplier,nation,orders,positive,negative}.size()!=6)return {};
        for(int inner:{positive,negative})if(plan.keys[inner] || !plan.outgoing[inner].empty())return {};
        for(const auto &join:q.existenceJoins) {
            bool equality=false,inequality=false,date=false;
            for(const auto &p:join.predicates) {
                if(!equality && WaitCorrelation(*p,Expression::EQ,*orderKey,join.table))equality=true;
                else if(!inequality && WaitCorrelation(*p,Expression::NE,*supplierKey,join.table))inequality=true;
                else if(join.anti && !date && WaitSameDate(*late,*p,join.table))date=true;
                else return {};
            }
            if(!equality || !inequality || date!=join.anti)return {};
        }
        int tables=q.tables.size();std::vector<std::vector<FilterClause>> clauses(tables);
        for(const auto &f:joins.filterClauses) {
            int t=f.columnRef.tableIdx;if(t!=orders && t!=supplier && t!=nation)return {};
            clauses[t].push_back(f);
        }
        std::vector<FilterNodeP> filters(tables);
        for(int t:{orders,supplier,nation})if(!clauses[t].empty())filters[t]=CreateFilterNode(clauses[t],avx);
        auto payloads=BuildIdentityPayloads(q,plan,fact,{nation,supplier},std::vector<int>(tables,supplier),
            std::vector<const uint8_t *>(tables,nullptr),filters,budget,avx,parallel);
        const auto &supplierPayload=*payloads[supplier];auto supplierDomain=supplierPayload.domain();
        const auto &orderTable=*q.tables[orders];const auto primary=plan.keys[orders]->column;
        int64_t low=INT64_MAX,high=INT64_MIN;uint64_t orderRows=0;
        for(int g=0;g<orderTable.RowGroupCount();++g) {
            const auto &rg=orderTable.GetRowGroup(g);if(!rg.size)continue;orderRows+=rg.size;
            const auto &raw=IdentityRaw(rg,primary);low=std::min(low,IdentityRead(raw,0));high=std::max(high,IdentityRead(raw,rg.size-1));
        }
        Wide width=Wide(high)-low+1;if(width<=0 || width>Wide(orderRows)*8 || width>(Wide(1)<<34))throw NotDense{};
        std::pmr::vector<uint64_t> eligible(size_t((width+63)/64),0,&budget),candidates(eligible.size(),0,&budget);
        BitmapDomain orderDomain{low,high,eligible.data()},candidateDomain{low,high,candidates.data()};
        int orderWorkers=parallel?std::min(8,std::max(1,orderTable.RowGroupCount())):1;
        RunTasks(orderWorkers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,RowGroupSize/8+128);uint8_t bitmap[RowGroupSize/8];
            for(int g=worker;g<orderTable.RowGroupCount();g+=orderWorkers) {
                const auto &rg=orderTable.GetRowGroup(g);int n=filters[orders]?filters[orders]->ExecuteSet(rg,bitmap):rg.size;if(!n)continue;
                const auto &keys=IdentityRaw(rg,primary);
                Selected(rg,n==rg.size?nullptr:bitmap,[&](int row) {
                    uint64_t offset=uint64_t(Wide(IdentityRead(keys,row))-low);
                    std::atomic_ref<uint64_t>(eligible[offset/64]).fetch_or(uint64_t(1)<<(offset%64),std::memory_order_relaxed);
                });
            }
            return true;
        });
        const auto &table=*q.tables[fact];int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        auto selected=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,Block*4+4096);uint32_t rows[Block];uint64_t count=0;
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);const auto &suppliers=IdentityRaw(rg,*supplierKey),&keys=IdentityRaw(rg,*orderKey);
                DateOrderPredicate date(*late,rg,budget);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int size=std::min(Block,rg.size-begin),n=0;
                    if(avx && suppliers.bytesPerValue==4 && supplierDomain.first>=0 && supplierDomain.last<=INT32_MAX)
                        n=BitmapSelectRange32(supplierDomain,reinterpret_cast<const int32_t *>(suppliers.values),begin,size,nullptr,rows);
                    else for(int row=begin;row<begin+size;++row)if(supplierDomain.contains(IdentityRead(suppliers,row)))rows[n++]=row;
                    n=date.select(rows,n,rows,avx);
                    for(int i=0;i<n;++i) {
                        int64_t key=IdentityRead(keys,rows[i]);if(!orderDomain.contains(key))continue;
                        uint64_t offset=uint64_t(Wide(key)-low);
                        std::atomic_ref<uint64_t>(candidates[offset/64]).fetch_or(uint64_t(1)<<(offset%64),std::memory_order_relaxed);++count;
                    }
                }
            }
            return count;
        });
        QueryResults results(q,budget);if(std::accumulate(selected.begin(),selected.end(),uint64_t(0))==0)return results.finish();
        struct Frame {uint32_t stripe;int64_t low,high;};std::pmr::vector<Frame> frames(&budget);
        for(int g=0;g<table.RowGroupCount();++g) {
            const auto &rg=table.GetRowGroup(g);if(!rg.size)continue;const auto &raw=IdentityRaw(rg,*orderKey);int64_t lo,hi;
            if(orderKey->Type()->type_num()==INT64_TYPE){auto &r=static_cast<const RawColumnData<Int64Type> &>(raw);lo=r.minValue;hi=r.maxValue;}
            else if(orderKey->Type()->type_num()==INT32_TYPE){auto &r=static_cast<const RawColumnData<Int32Type> &>(raw);lo=r.minValue;hi=r.maxValue;}
            else throw NotDense{};
            frames.push_back({uint32_t(g),lo,hi});
        }
        std::sort(frames.begin(),frames.end(),[](const auto &a,const auto &b){return a.low<b.low || (a.low==b.low && a.high<b.high);});
        for(size_t f=1;f<frames.size();++f)if(frames[f-1].high>frames[f].low)throw NotDense{};
        struct Stripe {std::pmr::vector<WaitSummary> summaries;Stripe(Budget &b):summaries(&b) {}};
        std::pmr::vector<Stripe> stripes(&budget);stripes.reserve(frames.size());for(size_t f=0;f<frames.size();++f)stripes.emplace_back(budget);
        RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,Block*8+4096);uint32_t rows[Block],lateRows[Block];
            for(size_t f=worker;f<frames.size();f+=workers) {
                const auto &rg=table.GetRowGroup(frames[f].stripe);const auto &keys=IdentityRaw(rg,*orderKey),&suppliers=IdentityRaw(rg,*supplierKey);
                DateOrderPredicate date(*late,rg,budget);auto &summaries=stripes[f].summaries;
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int size=std::min(Block,rg.size-begin),n=0;
                    if(avx && keys.bytesPerValue==4 && low>=0 && high<=INT32_MAX)
                        n=BitmapSelectRange32(candidateDomain,reinterpret_cast<const int32_t *>(keys.values),begin,size,nullptr,rows);
                    else for(int row=begin;row<begin+size;++row)if(candidateDomain.contains(IdentityRead(keys,row)))rows[n++]=row;
                    int lateCount=date.select(rows,n,lateRows,avx),nextLate=0;
                    for(int i=0;i<n;++i) {
                        int row=rows[i];int64_t key=IdentityRead(keys,row),s=IdentityRead(suppliers,row);
                        if(summaries.empty() || summaries.back().key!=key) {
                            if(!summaries.empty() && summaries.back().key>key)throw NotDense{};
                            summaries.emplace_back(key,s);
                        }
                        bool isLate=nextLate<lateCount && lateRows[nextLate]==uint32_t(row);nextLate+=isLate;
                        summaries.back().observe(s,isLate);
                    }
                }
            }
            return true;
        });
        std::pmr::vector<uint64_t> counts(plan.keys[supplier]->sequence.size(),0,&budget);
        auto emit=[&](const WaitSummary &summary) {
            if(!summary.different || summary.lateDifferent || !summary.lateCount)return;
            uint32_t id=supplierPayload.lookup(summary.firstLate);if(id)counts[id-1]+=summary.lateCount;
        };
        std::optional<WaitSummary> pending;
        for(const auto &stripe:stripes)for(const auto &summary:stripe.summaries) {
            if(pending && pending->key==summary.key)pending->merge(summary);
            else {if(pending)emit(*pending);pending=summary;}
        }
        if(pending)emit(*pending);
        std::pmr::unordered_map<Cell,uint64_t,Hash> grouped(&budget);
        for(uint64_t id=0;id<counts.size();++id)if(counts[id]) {
            auto row=plan.keys[supplier]->sequence.lookup(plan.keys[supplier]->sequence.minimum()+int64_t(id));if(!row)throw NotDense{};
            grouped[Read(q.tables[supplier]->GetRowGroup(*row>>32),*group->column,uint32_t(*row))]+=counts[id];
        }
        for(const auto &[label,count]:grouped)results.addLazy([&](size_t i) {
            FinalCell cell;const auto &a=q.aggregateClauses[i];
            if(a.type==AggregateClause::AGGREGATE_COUNT)cell.value.number=count;
            else {cell.type=a.expression->type.get();cell.value=label;}
            return cell;
        });
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
