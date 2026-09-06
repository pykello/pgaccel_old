#include "executor_convergent_plan.hpp"
#include "executor_identity_payload.hpp"
#include "executor_bitmap_probe.hpp"
#include "executor_small_group_key.hpp"
#include "executor_result.hpp"
#include "executor_compact_unsigned.hpp"

namespace pgaccel {

std::optional<Result<QueryOutput>> ExecuteConvergentSum(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()<3 || q.tables.size()>8 || !q.grouped || q.groupExpressions.size()!=1 ||
       q.groupExpressions[0]->kind!=Expression::COLUMN || !q.having.empty() ||
       !q.leftJoins.empty() || !q.existenceJoins.empty() || !q.membershipQueries.empty() ||
       !q.scalarQueries.empty() || !q.materializedSources.empty() || q.aggregateClauses.size()>4) return {};
    const auto groupColumn=*q.groupExpressions[0]->column;
    int center=groupColumn.tableIdx,fact=-1,sumIndex=-1;
    if(center<0 || center>=int(q.tables.size())) return {};
    for(size_t i=0;i<q.aggregateClauses.size();++i) {
        const auto &a=q.aggregateClauses[i];
        if(a.type==AggregateClause::AGGREGATE_SUM && a.expression && sumIndex<0) {
            sumIndex=i;
            auto visit=[&](auto &&self,const Expression &e)->bool {
                if(e.column) {
                    if(fact<0)fact=e.column->tableIdx;
                    if(e.column->tableIdx!=fact)return false;
                }
                for(const auto &child:{e.left,e.right,e.condition})if(child && !self(self,*child))return false;
                return true;
            };
            if(!visit(visit,*a.expression))return {};
        } else if(a.type!=AggregateClause::AGGREGATE_PROJECT || !a.expression ||
                  a.expression->kind!=Expression::COLUMN || *a.expression->column!=groupColumn) return {};
    }
    if(fact<0 || fact>=int(q.tables.size()) || sumIndex<0 || fact==center)return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);
        Scratch planning(budget,8192);
        ConvergentJoinPlan plan;
        if(!plan.prepare(q,fact,center,budget,avx,parallel))return {};
        const int tables=q.tables.size();
        std::vector<std::vector<FilterClause>> clauses(tables);
        for(const auto &f:q.filterClauses) {
            if(f.columnRef.tableIdx<0 || f.columnRef.tableIdx>=tables)return {};
            clauses[f.columnRef.tableIdx].push_back(f);
        }
        std::vector<FilterNodeP> filters(tables);
        for(int t=0;t<tables;++t)if(!clauses[t].empty())filters[t]=CreateFilterNode(clauses[t],avx);
        std::vector<int> centerOf(tables,center);
        std::vector<const uint8_t *> accepted(tables,nullptr);
        auto payloads=BuildIdentityPayloads(q,plan,fact,plan.order,centerOf,accepted,filters,budget,avx,parallel);
        auto arms=OrderIdentityArms(plan.outgoing[fact],plan,payloads);
        const auto &first=*payloads[arms[0].target],&second=*payloads[arms[1].target];
        const auto domain=first.domain();const auto &table=*q.tables[fact];
        uint32_t centers=plan.keys[center]->sequence.size();
        int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        struct Partial {
            std::pmr::vector<Wide> sums;
            std::pmr::vector<uint8_t> seen;
            Partial(uint32_t count,Budget &budget):sums(count,0,&budget),seen(count,0,&budget) {}
        };
        auto parts=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,32*Block*8+RowGroupSize/8+4096);
            Partial partial(centers,budget);uint8_t bitmap[RowGroupSize/8];
            alignas(64)int64_t buffers[32*Block];uint32_t selected[Block],groups[Block];
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);
                int count=filters[fact]?filters[fact]->ExecuteSet(rg,bitmap):rg.size;
                if(!count)continue;
                const auto &a=IdentityRaw(rg,arms[0].source),&b=IdentityRaw(rg,arms[1].source);
                Program program;int root=program.compile(*q.aggregateClauses[sumIndex].expression,rg);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int size=std::min(Block,rg.size-begin),n=0;
                    if(avx && a.bytesPerValue==4 && domain.first>=0 && domain.last<=INT32_MAX)
                        n=BitmapSelectRange32(domain,reinterpret_cast<const int32_t *>(a.values),begin,size,
                            count==rg.size?nullptr:bitmap,selected);
                    else for(int row=begin;row<begin+size;++row)
                        if((count==rg.size || (bitmap[row/8]>>(row%8)&1)) && domain.contains(IdentityRead(a,row)))selected[n++]=row;
                    int accepted=0;
                    for(int i=0;i<n;++i) {
                        int row=selected[i];uint32_t code=first.lookup(IdentityRead(a,row));
                        if(code && code==second.lookup(IdentityRead(b,row))) {
                            selected[accepted]=row;groups[accepted++]=code-1;
                        }
                    }
                    if(!accepted)continue;
                    program.evaluate<true>(0,accepted,buffers,avx,selected);
                    for(int i=0;i<accepted;++i) {
                        partial.sums[groups[i]]+=buffers[root*Block+i];partial.seen[groups[i]]=1;
                    }
                }
            }
            return partial;
        });
        SmallGroupKey groupKeys(*q.tables[center],{groupColumn},budget);
        std::pmr::vector<Wide> sums(groupKeys.slots(),0,&budget);
        std::pmr::vector<uint64_t> representatives(groupKeys.slots(),UINT64_MAX,&budget);
        plan.keys[center]->sequence.forEachStripe([&](uint32_t g,uint64_t offset) {
            const auto &rg=q.tables[center]->GetRowGroup(g);
            for(int row=0;row<rg.size;++row) {
                Wide sum=0;bool seen=false;
                for(const auto &part:parts){sum=Add(sum,part.sums[offset+row]);seen|=part.seen[offset+row];}
                if(!seen)continue;
                uint32_t group=groupKeys.word(g,row);sums[group]=Add(sums[group],sum);
                representatives[group]=(uint64_t(g)<<32)|uint32_t(row);
            }
        });
        QueryResults results(q,budget);
        for(uint32_t group=0;group<groupKeys.slots();++group)if(representatives[group]!=UINT64_MAX) {
            uint64_t row=representatives[group];
            results.addLazy([&](size_t i) {
                FinalCell cell;cell.type=q.aggregateClauses[i].expression->type.get();
                if(int(i)==sumIndex)cell.value.number=sums[group];
                else cell.value=Read(q.tables[center]->GetRowGroup(row>>32),groupColumn,uint32_t(row));
                return cell;
            });
        }
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
