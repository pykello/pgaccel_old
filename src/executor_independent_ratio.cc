#include "executor_independent_plan.hpp"
#include "executor_identity_payload.hpp"
#include "executor_result.hpp"
#include "executor_literal_filter.hpp"

namespace pgaccel {
namespace {
bool RatioOnTable(const Expression &e,int table) {
    if(e.column && e.column->tableIdx!=table)return false;
    if(e.kind==Expression::EXISTS || e.kind==Expression::MEMBERSHIP || e.kind==Expression::AGGREGATE)return false;
    for(const auto &child:{e.left,e.right,e.condition})if(child && !RatioOnTable(*child,table))return false;
    return true;
}
void RatioTables(const Expression &e,std::set<int> &tables) {
    if(e.column)tables.insert(e.column->tableIdx);
    for(const auto &child:{e.left,e.right,e.condition})if(child)RatioTables(*child,tables);
}
}
std::optional<Result<QueryOutput>> ExecuteIndependentRatio(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()<3 || q.tables.size()>8 || !q.grouped || q.groupExpressions.size()!=1 ||
       !q.having.empty() || !q.leftJoins.empty() || !q.existenceJoins.empty() ||
       !q.membershipQueries.empty() || !q.scalarQueries.empty() || !q.materializedSources.empty() ||
       q.aggregateClauses.size()>6)return {};
    const auto &year=q.groupExpressions[0];
    if(year->kind!=Expression::EXTRACT || year->literal!="year" || !year->left ||
       year->left->kind!=Expression::COLUMN || !year->left->column || year->left->type->type_num()!=DATE_TYPE)return {};
    int sum=-1,conditional=-1;
    for(size_t i=0;i<q.aggregateClauses.size();++i) {
        const auto &a=q.aggregateClauses[i];if(!a.expression)return {};
        if(a.type==AggregateClause::AGGREGATE_SUM) {
            int &slot=a.expression->kind==Expression::CASE?conditional:sum;
            if(slot>=0)return {};slot=i;
        } else if(a.type==AggregateClause::AGGREGATE_PROJECT) {
            const auto &e=*a.expression;
            if(e.kind!=Expression::EXTRACT || e.literal!="year" || !e.left ||
               e.left->kind!=Expression::COLUMN || e.left->column!=year->left->column)return {};
        } else if(a.type!=AggregateClause::AGGREGATE_FORMULA)return {};
    }
    if(sum<0 || conditional<0)return {};
    const auto &plain=*q.aggregateClauses[sum].expression,&choice=*q.aggregateClauses[conditional].expression;
    if(!choice.left || !choice.right || !choice.condition || choice.right->kind!=Expression::CONSTANT ||
       choice.right->type->type_num()==STRING_TYPE)return {};
    std::set<int> factTables,conditionTables;RatioTables(plain,factTables);RatioTables(*choice.condition,conditionTables);
    if(factTables.size()!=1 || conditionTables.size()!=1)return {};
    int fact=*factTables.begin(),center=*conditionTables.begin(),yearTable=year->left->column->tableIdx;
    if(fact<0 || center<0 || yearTable<0 || fact==center || fact==yearTable || center==yearTable ||
       !RatioOnTable(plain,fact) || !RatioOnTable(*choice.left,fact) || !RatioOnTable(*choice.condition,center))return {};
    for(const auto &a:q.aggregateClauses)if(a.type==AggregateClause::AGGREGATE_FORMULA) {
        std::set<int> refs;RatioTables(*a.expression,refs);if(refs!=std::set<int>{-1})return {};
        std::function<bool(const Expression &)> valid=[&](const Expression &e) {
            if(e.column && e.column->columnIdx!=sum && e.column->columnIdx!=conditional)return false;
            if(e.kind==Expression::CASE || e.kind==Expression::EXISTS || e.kind==Expression::MEMBERSHIP || e.kind==Expression::AGGREGATE)return false;
            for(const auto &child:{e.left,e.right,e.condition})if(child && !valid(*child))return false;
            return true;
        };
        if(!valid(*a.expression))return {};
    }
    QueryDesc joins=q;joins.predicates.clear();
    for(const auto &p:q.predicates) {
        if(auto filter=ExactLiteralFilter(*p))joins.filterClauses.push_back(std::move(*filter));
        else joins.predicates.push_back(p);
    }
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);Scratch planning(budget,16384);
        if(EvaluateWith(*choice.right,[](const ColumnRef &)->Cell{throw NotDense{};}).number!=0)return {};
        IndependentJoinPlan plan;if(!plan.prepare(joins,fact,budget,avx,parallel))return {};
        if(plan.armOf[center]==plan.armOf[yearTable] || !plan.outgoing[center].empty() ||
           !plan.keys[center]->sequence.active() || plan.keys[center]->sequence.size()>65536)return {};
        int tables=q.tables.size();const auto date=*year->left->column;
        YearCodeDomain years(*q.tables[yearTable],date);
        std::vector<std::vector<FilterClause>> clauses(tables);
        for(const auto &f:joins.filterClauses) {
            if(f.columnRef.tableIdx<0 || f.columnRef.tableIdx>=tables)return {};
            clauses[f.columnRef.tableIdx].push_back(f);
        }
        std::vector<FilterNodeP> filters(tables);
        for(int t=0;t<tables;++t)if(!clauses[t].empty())filters[t]=CreateFilterNode(clauses[t],avx);
        std::pmr::vector<uint8_t> matches(plan.keys[center]->sequence.size(),0,&budget);
        plan.keys[center]->sequence.forEachStripe([&](uint32_t g,uint64_t offset) {
            const auto &rg=q.tables[center]->GetRowGroup(g);
            for(int row=0;row<rg.size;++row)matches[offset+row]=bool(Evaluate(*choice.condition,rg,row).number);
        });
        std::vector<int> centerOf(tables,-1);std::vector<IdentityProjection> projections(tables);
        for(int t:plan.order) {
            auto &p=projections[t];
            if(t==center){centerOf[t]=center;p.maximum=matches.size();}
            else if(t==yearTable){p.kind=IdentityProjection::YEAR;p.maximum=years.size();p.column=date;p.years=&years;}
            else if(plan.outgoing[t].empty())p.kind=IdentityProjection::ONE;
            else {int child=plan.outgoing[t].front().target;p.maximum=projections[child].maximum;centerOf[t]=centerOf[child];}
        }
        auto payloads=BuildIdentityPayloads(q,plan,fact,plan.order,centerOf,
            std::vector<const uint8_t *>(tables,nullptr),filters,budget,avx,parallel,&projections);
        auto arms=plan.outgoing[fact];
        std::stable_sort(arms.begin(),arms.end(),[&](const auto &a,const auto &b) {
            const auto &x=*payloads[a.target],&y=*payloads[b.target];
            return double(x.selected)/double(Wide(x.last)-x.first+1)<double(y.selected)/double(Wide(y.last)-y.first+1);
        });
        const auto &table=*q.tables[fact];auto domain=payloads[arms[0].target]->domain();
        int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        struct Partial {std::array<Wide,64> total{},chosen{};std::array<bool,64> seen{};};
        const Wide conditionalScale=Power(Scale(choice.type.get())-Scale(choice.left->type.get()));
        auto parts=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,64*Block*8+RowGroupSize/8+sizeof(Partial)+4096);
            Partial part;uint8_t bitmap[RowGroupSize/8];alignas(64)int64_t plainBuffer[32*Block],chosenBuffer[32*Block];
            uint32_t selected[Block],groupIds[Block];bool chosen[Block];
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);int count=filters[fact]?filters[fact]->ExecuteSet(rg,bitmap):rg.size;
                if(!count)continue;
                std::array<const RawColumnDataBase *,4> foreign{};
                for(size_t a=0;a<arms.size();++a)foreign[a]=&IdentityRaw(rg,arms[a].source);
                Program totalProgram,chosenProgram;int totalRoot=totalProgram.compile(plain,rg),chosenRoot=chosenProgram.compile(*choice.left,rg);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int size=std::min(Block,rg.size-begin),n=0;const auto &first=*foreign[0];
                    if(avx && first.bytesPerValue==4 && domain.first>=0 && domain.last<=INT32_MAX)
                        n=BitmapSelectRange32(domain,reinterpret_cast<const int32_t *>(first.values),begin,size,count==rg.size?nullptr:bitmap,selected);
                    else for(int row=begin;row<begin+size;++row)
                        if((count==rg.size || (bitmap[row/8]>>(row%8)&1)) && domain.contains(IdentityRead(first,row)))selected[n++]=row;
                    int kept=0;
                    for(int i=0;i<n;++i) {
                        int row=selected[i];uint32_t y=0,c=0;bool keep=true;
                        for(size_t a=0;a<arms.size();++a) {
                            int target=arms[a].target;uint32_t code=payloads[target]->lookup(IdentityRead(*foreign[a],row));
                            if(!code){keep=false;break;}
                            if(plan.armOf[target]==plan.armOf[yearTable])y=code;
                            if(plan.armOf[target]==plan.armOf[center])c=code;
                        }
                        if(!keep)continue;
                        if(!y || y>years.size() || !c || c>matches.size())throw NotDense{};
                        selected[kept]=row;groupIds[kept]=y-1;chosen[kept++]=matches[c-1];
                    }
                    if(!kept)continue;
                    totalProgram.evaluate<true>(0,kept,plainBuffer,avx,selected);
                    chosenProgram.evaluate<true>(0,kept,chosenBuffer,avx,selected);
                    for(int i=0;i<kept;++i) {
                        uint32_t y=groupIds[i];part.total[y]+=plainBuffer[totalRoot*Block+i];part.seen[y]=true;
                        if(chosen[i])part.chosen[y]+=Multiply(chosenBuffer[chosenRoot*Block+i],conditionalScale);
                    }
                }
            }
            return part;
        });
        QueryResults results(q,budget);
        for(uint32_t y=0;y<years.size();++y) {
            Wide total=0,chosen=0;bool seen=false;
            for(const auto &part:parts){total=Add(total,part.total[y]);chosen=Add(chosen,part.chosen[y]);seen|=part.seen[y];}
            if(!seen)continue;
            results.addLazy([&](size_t i) {
                const auto &a=q.aggregateClauses[i];FinalCell cell;cell.type=a.expression->type.get();
                if(int(i)==sum)cell.value.number=total;
                else if(int(i)==conditional)cell.value.number=chosen;
                else if(a.type==AggregateClause::AGGREGATE_PROJECT)cell.value.number=years.first+y;
                else cell.value=EvaluateWith(*a.expression,[&](const ColumnRef &column)->Cell {
                    if(column.tableIdx!=-1)throw NotDense{};
                    if(column.columnIdx==sum)return Cell{total};
                    if(column.columnIdx==conditional)return Cell{chosen};
                    throw NotDense{};
                });
                return cell;
            });
        }
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
