#include "executor_independent_plan.hpp"
#include "executor_identity_payload.hpp"
#include "executor_repeated_key.hpp"
#include "executor_small_group_key.hpp"
#include "executor_literal_filter.hpp"
#include "executor_result.hpp"

namespace pgaccel {
namespace {
void ProfitTables(const Expression &e,std::set<int> &tables) {
    if(e.column)tables.insert(e.column->tableIdx);
    for(const auto &child:{e.left,e.right,e.condition})if(child)ProfitTables(*child,tables);
}
}
std::optional<Result<QueryOutput>> ExecutePairedProfit(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()<5 || q.tables.size()>8 || !q.grouped || q.groupExpressions.size()!=2 ||
       !q.having.empty() || !q.leftJoins.empty() || !q.existenceJoins.empty() ||
       !q.membershipQueries.empty() || !q.scalarQueries.empty() || !q.materializedSources.empty() ||
       q.aggregateClauses.size()>5)return {};
    std::optional<ColumnRef> group;std::shared_ptr<Expression> year;int sum=-1;
    for(const auto &e:q.groupExpressions) {
        if(!group && e->kind==Expression::COLUMN && e->column)group=e->column;
        else if(!year && e->kind==Expression::EXTRACT && e->literal=="year" && e->left &&
                e->left->kind==Expression::COLUMN && e->left->column && e->left->type->type_num()==DATE_TYPE)year=e;
        else return {};
    }
    if(!group || !year)return {};
    for(size_t i=0;i<q.aggregateClauses.size();++i) {
        const auto &a=q.aggregateClauses[i];if(!a.expression)return {};
        if(a.type==AggregateClause::AGGREGATE_SUM && sum<0)sum=i;
        else if(a.type==AggregateClause::AGGREGATE_PROJECT) {
            const auto &e=*a.expression;
            if(e.kind==Expression::COLUMN && e.column==group)continue;
            if(e.kind==Expression::EXTRACT && e.literal=="year" && e.left &&
               e.left->kind==Expression::COLUMN && e.left->column==year->left->column)continue;
            return {};
        } else return {};
    }
    if(sum<0)return {};
    const auto &amount=*q.aggregateClauses[sum].expression;
    if(amount.kind!=Expression::SUBTRACT || !amount.left || !amount.right ||
       amount.right->kind!=Expression::MULTIPLY || !amount.right->left || !amount.right->right ||
       amount.right->left->kind!=Expression::COLUMN || amount.right->right->kind!=Expression::COLUMN)return {};
    std::set<int> facts;ProfitTables(*amount.left,facts);if(facts.size()!=1)return {};
    int fact=*facts.begin();auto quantity=amount.right->left->column,cost=amount.right->right->column;
    if(!quantity || !cost)return {};if(quantity->tableIdx!=fact)std::swap(quantity,cost);
    int pairTable=cost->tableIdx,center=group->tableIdx,yearTable=year->left->column->tableIdx;
    if(fact<0 || pairTable<0 || center<0 || yearTable<0 || quantity->tableIdx!=fact || pairTable==fact ||
       center==fact || yearTable==fact || pairTable==center || pairTable==yearTable || center==yearTable)return {};
    auto numeric=[](const ColumnRef &c){auto t=c.Type()->type_num();return t==INT32_TYPE || t==INT64_TYPE || t==DECIMAL_TYPE;};
    if(!numeric(*cost) || !numeric(*quantity))return {};
    QueryDesc joins=q;joins.predicates.clear();
    for(const auto &p:q.predicates) {
        if(auto filter=ExactLiteralFilter(*p))joins.filterClauses.push_back(std::move(*filter));
        else joins.predicates.push_back(p);
    }
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);Scratch planning(budget,16384);
        IndependentJoinPlan plan;if(!plan.prepare(joins,fact,budget,avx,parallel,pairTable))return {};
        if(plan.keys[pairTable] || plan.outgoing[pairTable].size()!=2 || plan.outgoing[fact].size()!=3 ||
           !plan.outgoing[center].empty() || !plan.keys[center]->sequence.active() ||
           plan.keys[center]->sequence.size()>65536)return {};
        RepeatedKeyIndex repeated(budget);int run=-1;
        for(int i=0;i<2;++i) {
            const auto &edge=plan.outgoing[pairTable][i];const auto &key=*plan.keys[edge.target];
            if(plan.outgoing[edge.target].empty() && key.sequence.active() &&
               repeated.prepare(*q.tables[pairTable],edge.source,key.sequence.minimum(),key.sequence.size(),avx,parallel)) {run=i;break;}
        }
        if(run<0)return {};
        const auto &runEdge=plan.outgoing[pairTable][run],&matchEdge=plan.outgoing[pairTable][1-run];
        int runTarget=runEdge.target,matchTarget=matchEdge.target;
        if(plan.armOf[runTarget]<0 || plan.armOf[matchTarget]<0 ||
           plan.armOf[matchTarget]!=plan.armOf[center] || plan.armOf[runTarget]==plan.armOf[yearTable] ||
           plan.armOf[yearTable]==plan.armOf[center])return {};
        int runArm=-1,matchArm=-1,yearArm=-1;
        for(int a=0;a<3;++a) {
            int t=plan.outgoing[fact][a].target;
            if(t==runTarget)runArm=a;
            if(t==matchTarget)matchArm=a;
            if(plan.armOf[t]==plan.armOf[yearTable])yearArm=a;
        }
        if(runArm<0 || matchArm<0 || yearArm<0)return {};
        int tables=q.tables.size();std::vector<std::vector<FilterClause>> clauses(tables);
        for(const auto &f:joins.filterClauses) {
            if(f.columnRef.tableIdx<0 || f.columnRef.tableIdx>=tables)return {};
            clauses[f.columnRef.tableIdx].push_back(f);
        }
        std::vector<FilterNodeP> filters(tables);
        for(int t=0;t<tables;++t)if(!clauses[t].empty())filters[t]=CreateFilterNode(clauses[t],avx);
        const auto date=*year->left->column;YearCodeDomain years(*q.tables[yearTable],date);
        SmallGroupKey groups(*q.tables[center],{*group},budget);
        uint64_t slots=uint64_t(groups.slots())*years.size();if(slots>65536)return {};
        std::pmr::vector<uint32_t> labels(plan.keys[center]->sequence.size(),0,&budget);
        std::pmr::vector<uint64_t> reps(groups.slots(),UINT64_MAX,&budget);
        plan.keys[center]->sequence.forEachStripe([&](uint32_t g,uint64_t offset) {
            const auto &rg=q.tables[center]->GetRowGroup(g);
            for(int row=0;row<rg.size;++row) {auto label=groups.word(g,row);labels[offset+row]=label;reps[label]=(uint64_t(g)<<32)|uint32_t(row);}
        });
        std::vector<int> centerOf(tables,-1);std::vector<IdentityProjection> projections(tables);
        for(int t:plan.order) {
            auto &p=projections[t];
            if(t==center){centerOf[t]=center;p.maximum=labels.size();}
            else if(t==yearTable){p.kind=IdentityProjection::YEAR;p.maximum=years.size();p.column=date;p.years=&years;}
            else if(plan.outgoing[t].empty())p.kind=IdentityProjection::ONE;
            else {int child=plan.outgoing[t].front().target;p.maximum=projections[child].maximum;centerOf[t]=centerOf[child];}
        }
        auto payloads=BuildIdentityPayloads(q,plan,fact,plan.order,centerOf,
            std::vector<const uint8_t *>(tables,nullptr),filters,budget,avx,parallel,&projections);
        const auto &pairs=*q.tables[pairTable];
        struct PairStripe {const RawColumnDataBase *match,*cost;std::pmr::vector<uint8_t> bitmap;
            PairStripe(const RawColumnDataBase *m,const RawColumnDataBase *c,Budget &b):match(m),cost(c),bitmap(&b) {}};
        std::pmr::vector<PairStripe> pairStripes(&budget);pairStripes.reserve(pairs.RowGroupCount());
        for(int g=0;g<pairs.RowGroupCount();++g) {
            const auto &rg=pairs.GetRowGroup(g);
            pairStripes.emplace_back(&IdentityRaw(rg,matchEdge.source),&IdentityRaw(rg,*cost),budget);
            if(filters[pairTable]) {
                auto &bitmap=pairStripes.back().bitmap;bitmap.resize(RowGroupSize/8,0);
                filters[pairTable]->ExecuteSet(rg,bitmap.data());
            }
        }
        const auto &arms=plan.outgoing[fact];auto domain=payloads[runTarget]->domain();
        const auto &table=*q.tables[fact];int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        struct Partial {std::pmr::vector<Wide> sums;std::pmr::vector<uint8_t> seen;
            Partial(size_t n,Budget &b):sums(n,0,&b),seen(n,0,&b) {}};
        Wide revenueScale=Power(Scale(amount.type.get())-Scale(amount.left->type.get()));
        Wide costScale=Power(Scale(amount.type.get())-Scale(amount.right->type.get()));
        auto parts=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,32*Block*8+RowGroupSize/8+4096);
            Partial part(slots,budget);uint8_t bitmap[RowGroupSize/8];alignas(64)int64_t buffers[32*Block];
            uint32_t selected[Block];
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);int count=filters[fact]?filters[fact]->ExecuteSet(rg,bitmap):rg.size;
                if(!count)continue;
                const auto &runKey=IdentityRaw(rg,arms[runArm].source),&matchKey=IdentityRaw(rg,arms[matchArm].source);
                const auto &yearKey=IdentityRaw(rg,arms[yearArm].source),&quantities=IdentityRaw(rg,*quantity);
                Program program;int root=program.compile(*amount.left,rg);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int size=std::min(Block,rg.size-begin),n=0;
                    if(avx && runKey.bytesPerValue==4 && domain.first>=0 && domain.last<=INT32_MAX)
                        n=BitmapSelectRange32(domain,reinterpret_cast<const int32_t *>(runKey.values),begin,size,count==rg.size?nullptr:bitmap,selected);
                    else for(int row=begin;row<begin+size;++row)
                        if((count==rg.size || (bitmap[row/8]>>(row%8)&1)) && domain.contains(IdentityRead(runKey,row)))selected[n++]=row;
                    if(!n)continue;program.evaluate<true>(0,n,buffers,avx,selected);
                    for(int i=0;i<n;++i) {
                        int row=selected[i];auto key=IdentityRead(matchKey,row);
                        uint32_t label=payloads[matchTarget]->lookup(key),y=payloads[arms[yearArm].target]->lookup(IdentityRead(yearKey,row));
                        if(!label || !y)continue;
                        auto pair=repeated.lookup(IdentityRead(runKey,row));if(!pair)continue;
                        auto &frame=pairStripes[*pair>>32];uint32_t start=uint32_t(*pair);
                        if(label>labels.size() || y>years.size())throw NotDense{};
                        uint32_t id=labels[label-1]*years.size()+y-1;
                        for(uint32_t r=start;r<start+repeated.fanout();++r)
                            if((frame.bitmap.empty() || (frame.bitmap[r/8]>>(r%8)&1)) && IdentityRead(*frame.match,r)==key) {
                                Wide revenue=Multiply(buffers[root*Block+i],revenueScale);
                                Wide expense=Multiply(Multiply(IdentityRead(*frame.cost,r),IdentityRead(quantities,row)),costScale);
                                Wide value;if(__builtin_sub_overflow(revenue,expense,&value))throw std::overflow_error("Profit overflow");
                                part.sums[id]=Add(part.sums[id],value);part.seen[id]=1;
                            }
                    }
                }
            }
            return part;
        });
        QueryResults results(q,budget);
        for(uint32_t id=0;id<slots;++id) {
            Wide value=0;bool seen=false;for(const auto &part:parts){value=Add(value,part.sums[id]);seen|=part.seen[id];}
            if(!seen)continue;uint64_t representative=reps[id/years.size()];
            results.addLazy([&](size_t i) {
                const auto &a=q.aggregateClauses[i];FinalCell cell;cell.type=a.expression->type.get();
                if(int(i)==sum)cell.value.number=value;
                else if(a.expression->kind==Expression::COLUMN)cell.value=Read(q.tables[center]->GetRowGroup(representative>>32),*group,uint32_t(representative));
                else cell.value.number=years.first+id%years.size();
                return cell;
            });
        }
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
