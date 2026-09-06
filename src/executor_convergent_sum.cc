#include "executor_convergent_plan.hpp"
#include "executor_rank_join.hpp"
#include "executor_bitmap_probe.hpp"
#include "executor_small_group_key.hpp"
#include "executor_result.hpp"

namespace pgaccel {
namespace {
int64_t ConvergentRead(const RawColumnDataBase &data,int row) {
    switch(data.bytesPerValue) {
    case 1:return reinterpret_cast<const int8_t *>(data.values)[row];
    case 2:return reinterpret_cast<const int16_t *>(data.values)[row];
    case 4:return reinterpret_cast<const int32_t *>(data.values)[row];
    case 8:return reinterpret_cast<const int64_t *>(data.values)[row];
    default:throw NotDense{};
    }
}
const RawColumnDataBase &ConvergentRaw(const RowGroup &rg,const ColumnRef &column) {
    auto data=rg.columns[column.columnIdx].get();
    if(data->type!=ColumnDataBase::RAW_COLUMN_DATA || data->size!=rg.size) throw NotDense{};
    return *static_cast<const RawColumnDataBase *>(data);
}
struct ConvergentPayload {
    int64_t first=0,last=0;
    uint64_t selected=0;
    std::pmr::vector<uint32_t> dense;
    std::pmr::vector<uint64_t> bits;
    std::unique_ptr<RankJoinIndex> rank;
    explicit ConvergentPayload(Budget &budget):dense(&budget),bits(&budget) {}
    uint32_t lookup(int64_t key) const {
        if(key<first || key>last) return 0;
        if(rank) {auto id=rank->rank(key);return id?uint32_t(rank->row(*id)):0;}
        return dense[uint64_t(Wide(key)-first)];
    }
    BitmapDomain domain() const {return {first,last,rank?rank->words():bits.data()};}
};
}

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
        std::vector<std::unique_ptr<ConvergentPayload>> payloads(tables);
        for(int t:plan.order) {
            const auto &table=*q.tables[t];auto &key=*plan.keys[t];
            auto payload=std::make_unique<ConvergentPayload>(budget);
            std::pmr::vector<uint64_t> offsets(table.RowGroupCount(),0,&budget);
            if(key.sequence.active()) {
                payload->first=key.sequence.minimum();payload->last=key.sequence.maximum();
                payload->dense.resize(key.sequence.size(),0);
                key.sequence.forEachStripe([&](uint32_t g,uint64_t offset){offsets[g]=offset;});
            } else {
                payload->first=INT64_MAX;payload->last=INT64_MIN;uint64_t size=0;
                for(int g=0;g<table.RowGroupCount();++g) {
                    const auto &rg=table.GetRowGroup(g);if(!rg.size)continue;
                    const auto &raw=ConvergentRaw(rg,key.column);size+=rg.size;
                    payload->first=std::min(payload->first,ConvergentRead(raw,0));
                    payload->last=std::max(payload->last,ConvergentRead(raw,rg.size-1));
                }
                Wide width=Wide(payload->last)-payload->first+1;
                if(width<=0 || width>Wide(size)*8 || width>(Wide(1)<<32))throw NotDense{};
                payload->rank=std::make_unique<RankJoinIndex>(payload->first,payload->last,budget);
            }
            struct Pair {int64_t key;uint32_t value;};
            int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
            auto parts=RunTasks(workers,[&](int worker) {
                ScopedValueResource memory(budget);Scratch scratch(budget,RowGroupSize/8+256);
                std::pmr::vector<Pair> pairs(&budget);uint8_t bitmap[RowGroupSize/8];
                const auto *edge=plan.outgoing[t].empty()?nullptr:&plan.outgoing[t].front();
                for(int g=worker;g<table.RowGroupCount();g+=workers) {
                    const auto &rg=table.GetRowGroup(g);
                    int count=filters[t]?filters[t]->ExecuteSet(rg,bitmap):rg.size;
                    if(!count)continue;
                    const auto *foreign=edge?&ConvergentRaw(rg,edge->source):nullptr;
                    const auto *primary=payload->rank?&ConvergentRaw(rg,key.column):nullptr;
                    Selected(rg,count==rg.size?nullptr:bitmap,[&](int row) {
                        uint32_t value=edge?payloads[edge->target]->lookup(ConvergentRead(*foreign,row)):1;
                        if(!value)return;
                        if(t==center)value=uint32_t(offsets[g]+row+1);
                        if(primary) {
                            int64_t k=ConvergentRead(*primary,row);
                            payload->rank->insert<true>(k);pairs.push_back({k,value});
                        } else payload->dense[offsets[g]+row]=value;
                    });
                }
                return pairs;
            });
            if(payload->rank) {
                payload->rank->finalize();payload->selected=payload->rank->size();
                RunTasks(workers,[&](int worker) {
                    for(const auto &pair:parts[worker])payload->rank->setRow(pair.key,pair.value);
                    return true;
                });
            } else {
                payload->bits.resize((payload->dense.size()+63)/64,0);
                for(size_t i=0;i<payload->dense.size();++i)if(payload->dense[i]) {
                    payload->bits[i/64]|=uint64_t(1)<<(i%64);++payload->selected;
                }
            }
            payloads[t]=std::move(payload);
        }
        auto arms=plan.outgoing[fact];
        auto fraction=[&](const auto &edge) {
            const auto &p=*payloads[edge.target];return double(p.selected)/double(Wide(p.last)-p.first+1);
        };
        if(fraction(arms[1])<fraction(arms[0]))std::swap(arms[0],arms[1]);
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
                const auto &a=ConvergentRaw(rg,arms[0].source),&b=ConvergentRaw(rg,arms[1].source);
                Program program;int root=program.compile(*q.aggregateClauses[sumIndex].expression,rg);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int size=std::min(Block,rg.size-begin),n=0;
                    if(avx && a.bytesPerValue==4 && domain.first>=0 && domain.last<=INT32_MAX)
                        n=BitmapSelectRange32(domain,reinterpret_cast<const int32_t *>(a.values),begin,size,
                            count==rg.size?nullptr:bitmap,selected);
                    else for(int row=begin;row<begin+size;++row)
                        if((count==rg.size || (bitmap[row/8]>>(row%8)&1)) && domain.contains(ConvergentRead(a,row)))selected[n++]=row;
                    int accepted=0;
                    for(int i=0;i<n;++i) {
                        int row=selected[i];uint32_t code=first.lookup(ConvergentRead(a,row));
                        if(code && code==second.lookup(ConvergentRead(b,row))) {
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
