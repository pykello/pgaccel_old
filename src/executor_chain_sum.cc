#include "executor_independent_plan.hpp"
#include "executor_identity_payload.hpp"
#include "executor_literal_filter.hpp"
#include "executor_result.hpp"

namespace pgaccel {
namespace {
void ChainTables(const Expression &e,std::set<int> &tables) {
    if(e.column)tables.insert(e.column->tableIdx);
    for(const auto &child:{e.left,e.right,e.condition})if(child)ChainTables(*child,tables);
}
}
std::optional<Result<QueryOutput>> ExecuteChainSum(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()<3 || q.tables.size()>8 || !q.grouped || q.groupExpressions.empty() ||
       !q.having.empty() || !q.leftJoins.empty() || !q.existenceJoins.empty() ||
       !q.membershipQueries.empty() || !q.scalarQueries.empty() || !q.materializedSources.empty() ||
       q.aggregateClauses.size()>16)return {};
    std::vector<ColumnRef> groups;
    for(const auto &e:q.groupExpressions) {
        if(e->kind!=Expression::COLUMN || !e->column)return {};
        groups.push_back(*e->column);
    }
    int sum=-1;
    for(size_t i=0;i<q.aggregateClauses.size();++i) {
        const auto &a=q.aggregateClauses[i];if(!a.expression)return {};
        if(a.type==AggregateClause::AGGREGATE_SUM && sum<0)sum=i;
        else if(a.type!=AggregateClause::AGGREGATE_PROJECT || a.expression->kind!=Expression::COLUMN ||
                !a.expression->column || std::find(groups.begin(),groups.end(),*a.expression->column)==groups.end())return {};
    }
    if(sum<0)return {};
    std::set<int> facts;ChainTables(*q.aggregateClauses[sum].expression,facts);if(facts.size()!=1 || *facts.begin()<0)return {};
    int fact=*facts.begin();QueryDesc joins=q;joins.predicates.clear();
    for(const auto &p:q.predicates) {
        if(auto filter=ExactLiteralFilter(*p))joins.filterClauses.push_back(std::move(*filter));
        else joins.predicates.push_back(p);
    }
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);Scratch planning(budget,16384);
        IndependentJoinPlan plan;if(!plan.prepare(joins,fact,budget,avx,parallel) || plan.outgoing[fact].size()!=1)return {};
        int tables=q.tables.size(),center=-1;
        for(const auto &column:groups) {
            int t=column.tableIdx;if(t<0 || t>=tables || !plan.keys[t] || !(plan.keys[t]->column==column) ||
               !plan.keys[t]->sequence.active() || plan.keys[t]->sequence.size()>UINT32_MAX)continue;
            std::vector<bool> reachable(tables,false);int next=t;
            while(true) {reachable[next]=true;if(plan.outgoing[next].empty())break;next=plan.outgoing[next].front().target;}
            if(std::all_of(groups.begin(),groups.end(),[&](const auto &c){return c.tableIdx>=0 && c.tableIdx<tables && reachable[c.tableIdx];})) {center=t;break;}
        }
        if(center<0)return {};
        uint64_t countGroups=plan.keys[center]->sequence.size();
        std::vector<std::vector<FilterClause>> clauses(tables);
        for(const auto &f:joins.filterClauses) {
            if(f.columnRef.tableIdx<0 || f.columnRef.tableIdx>=tables)return {};
            clauses[f.columnRef.tableIdx].push_back(f);
        }
        std::vector<FilterNodeP> filters(tables);
        for(int t=0;t<tables;++t)if(!clauses[t].empty())filters[t]=CreateFilterNode(clauses[t],avx);
        std::vector<int> centerOf(tables,center);
        auto payloads=BuildIdentityPayloads(q,plan,fact,plan.order,centerOf,
            std::vector<const uint8_t *>(tables,nullptr),filters,budget,avx,parallel);
        const auto &edge=plan.outgoing[fact].front();const auto &payload=*payloads[edge.target];auto domain=payload.domain();
        const auto &table=*q.tables[fact];int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        struct Partial {std::pmr::vector<Wide> sums;std::pmr::vector<uint8_t> seen;
            Partial(size_t n,Budget &b):sums(n,0,&b),seen(n,0,&b) {}};
        auto parts=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,32*Block*8+RowGroupSize/8+4096);
            Partial part(countGroups,budget);uint8_t bitmap[RowGroupSize/8];alignas(64)int64_t buffers[32*Block];
            uint32_t selected[Block],ids[Block];
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);int count=filters[fact]?filters[fact]->ExecuteSet(rg,bitmap):rg.size;
                if(!count)continue;const auto &foreign=IdentityRaw(rg,edge.source);
                Program program;int root=program.compile(*q.aggregateClauses[sum].expression,rg);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int size=std::min(Block,rg.size-begin),n=0;
                    if(avx && foreign.bytesPerValue==4 && domain.first>=0 && domain.last<=INT32_MAX)
                        n=BitmapSelectRange32(domain,reinterpret_cast<const int32_t *>(foreign.values),begin,size,count==rg.size?nullptr:bitmap,selected);
                    else for(int row=begin;row<begin+size;++row)
                        if((count==rg.size || (bitmap[row/8]>>(row%8)&1)) && domain.contains(IdentityRead(foreign,row)))selected[n++]=row;
                    if(!n)continue;
                    for(int i=0;i<n;++i) {
                        auto id=payload.lookup(IdentityRead(foreign,selected[i]));
                        if(!id || id>countGroups)throw NotDense{};ids[i]=id-1;
                    }
                    program.evaluate<true>(0,n,buffers,avx,selected);
                    for(int i=0;i<n;++i){part.sums[ids[i]]+=buffers[root*Block+i];part.seen[ids[i]]=1;}
                }
            }
            return part;
        });
        QueryResults results(q,budget);
        for(uint32_t id=0;id<countGroups;++id) {
            Wide sumValue=0;bool seen=false;for(const auto &part:parts){sumValue=Add(sumValue,part.sums[id]);seen|=part.seen[id];}
            if(!seen)continue;
            // Grouping includes the proved unique center key. Every other
            // grouping field is functionally determined along its key chain.
            auto read=[&](const ColumnRef &column)->Cell {
                int t=center;auto row=plan.keys[center]->sequence.lookup(plan.keys[center]->sequence.minimum()+int64_t(id));
                if(!row)throw NotDense{};
                while(t!=column.tableIdx) {
                    const auto &join=plan.outgoing[t].front();const auto &rg=q.tables[t]->GetRowGroup(*row>>32);
                    int64_t key=IdentityRead(IdentityRaw(rg,join.source),uint32_t(*row));t=join.target;
                    row=plan.keys[t]->sequence.active()?plan.keys[t]->sequence.lookup(key):plan.keys[t]->sorted.lookup(key);
                    if(!row)throw NotDense{};
                }
                return Read(q.tables[t]->GetRowGroup(*row>>32),column,uint32_t(*row));
            };
            results.addLazy([&](size_t i) {
                const auto &a=q.aggregateClauses[i];FinalCell cell;cell.type=a.expression->type.get();
                if(int(i)==sum)cell.value.number=sumValue;else cell.value=read(*a.expression->column);
                return cell;
            });
        }
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
