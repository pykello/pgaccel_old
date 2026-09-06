#include "executor_separate_plan.hpp"
#include "executor_identity_payload.hpp"
#include "executor_small_group_key.hpp"
#include "executor_result.hpp"
#include "executor_literal_filter.hpp"

namespace pgaccel {
namespace {
int SeparateYear(int64_t day) {
    if(day < -719528 || day > 2932896)throw NotDense{};
    return int(std::chrono::year_month_day{std::chrono::sys_days{std::chrono::days{day}}}.year());
}
bool SeparateOnTables(const Expression &e,int a,int b) {
    if(e.column && e.column->tableIdx!=a && e.column->tableIdx!=b)return false;
    if(e.kind==Expression::EXISTS || e.kind==Expression::MEMBERSHIP || e.kind==Expression::AGGREGATE)return false;
    for(const auto &child:{e.left,e.right,e.condition})if(child && !SeparateOnTables(*child,a,b))return false;
    return true;
}
}
std::optional<Result<QueryOutput>> ExecuteSeparateSum(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()<3 || q.tables.size()>8 || !q.grouped || q.groupExpressions.size()!=3 ||
       !q.having.empty() || !q.leftJoins.empty() || !q.existenceJoins.empty() ||
       !q.membershipQueries.empty() || !q.scalarQueries.empty() || !q.materializedSources.empty() ||
       q.aggregateClauses.size()>6)return {};
    std::vector<ColumnRef> groups;std::shared_ptr<Expression> year;
    for(const auto &e:q.groupExpressions) {
        if(e->kind==Expression::COLUMN && e->column)groups.push_back(*e->column);
        else if(!year && e->kind==Expression::EXTRACT && e->literal=="year" && e->left &&
                e->left->kind==Expression::COLUMN && e->left->column && e->left->type->type_num()==DATE_TYPE)year=e;
        else return {};
    }
    if(groups.size()!=2 || !year)return {};
    int fact=year->left->column->tableIdx,sumIndex=-1;
    if(fact<0 || fact>=int(q.tables.size()))return {};
    std::array<int,2> centers{groups[0].tableIdx,groups[1].tableIdx};
    std::vector<int> projection(q.aggregateClauses.size(),-1);
    for(size_t i=0;i<q.aggregateClauses.size();++i) {
        const auto &a=q.aggregateClauses[i];if(!a.expression)return {};
        if(a.type==AggregateClause::AGGREGATE_SUM && sumIndex<0 && SeparateOnTables(*a.expression,fact,fact))sumIndex=i;
        else if(a.type==AggregateClause::AGGREGATE_PROJECT) {
            if(a.expression->kind==Expression::COLUMN && *a.expression->column==groups[0])projection[i]=0;
            else if(a.expression->kind==Expression::COLUMN && *a.expression->column==groups[1])projection[i]=1;
            else if(a.expression->kind==Expression::EXTRACT && a.expression->literal=="year" &&
                    a.expression->left && a.expression->left->kind==Expression::COLUMN &&
                    a.expression->left->column==year->left->column)projection[i]=2;
            else return {};
        } else return {};
    }
    if(sumIndex<0)return {};
    QueryDesc joins=q;joins.predicates.clear();
    std::vector<std::shared_ptr<Expression>> coupled;
    for(const auto &p:q.predicates) {
        if(auto filter=ExactLiteralFilter(*p)) {
            joins.filterClauses.push_back(std::move(*filter));continue;
        }
        auto integer=[](const ColumnRef &c){auto t=c.Type()->type_num();return t==INT32_TYPE || t==INT64_TYPE;};
        if(p->kind==Expression::EQ && p->left && p->right && p->left->column && p->right->column &&
           p->left->column->tableIdx!=p->right->column->tableIdx && integer(*p->left->column) && integer(*p->right->column))joins.predicates.push_back(p);
        else if(SeparateOnTables(*p,centers[0],centers[1]))coupled.push_back(p);
        else return {};
    }
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);Scratch planning(budget,16384);
        SeparateJoinPlan plan;if(!plan.prepare(joins,fact,centers,budget,avx,parallel))return {};
        int tables=q.tables.size();
        std::vector<std::vector<FilterClause>> clauses(tables);
        for(const auto &f:joins.filterClauses) {
            if(f.columnRef.tableIdx<0 || f.columnRef.tableIdx>=tables)return {};
            clauses[f.columnRef.tableIdx].push_back(f);
        }
        std::vector<FilterNodeP> filters(tables);
        for(int t=0;t<tables;++t)if(!clauses[t].empty())filters[t]=CreateFilterNode(clauses[t],avx);
        const auto &table=*q.tables[fact];const auto date=*year->left->column;
        int firstYear=INT_MAX,lastYear=INT_MIN;
        for(int g=0;g<table.RowGroupCount();++g) {
            const auto &rg=table.GetRowGroup(g);if(!rg.size)continue;
            auto data=rg.columns[date.columnIdx].get();int64_t low,high;
            if(data->type==ColumnDataBase::RAW_COLUMN_DATA) {
                auto raw=static_cast<const RawColumnData<DateType> *>(data);low=raw->minValue;high=raw->maxValue;
            } else if(data->type==ColumnDataBase::DICT_COLUMN_DATA) {
                const auto &values=static_cast<const DictColumnData<DateType> *>(data)->dict;
                if(values.empty())throw NotDense{};low=values.front();high=values.back();
            } else throw NotDense{};
            firstYear=std::min(firstYear,SeparateYear(low));lastYear=std::max(lastYear,SeparateYear(high));
        }
        if(firstYear>lastYear || lastYear-firstYear>=64)throw NotDense{};
        uint32_t years=lastYear-firstYear+1;
        SmallGroupKey key0(*q.tables[centers[0]],{groups[0]},budget),key1(*q.tables[centers[1]],{groups[1]},budget);
        uint64_t slots=uint64_t(key0.slots())*key1.slots()*years;
        if(slots>65536)throw NotDense{};
        uint32_t counts[2]={uint32_t(plan.keys[centers[0]]->sequence.size()),uint32_t(plan.keys[centers[1]]->sequence.size())};
        std::pmr::vector<uint64_t> rows0(counts[0],0,&budget),rows1(counts[1],0,&budget);
        std::pmr::vector<uint8_t> valid0(counts[0],0,&budget),valid1(counts[1],0,&budget);
        std::pmr::vector<uint8_t> live0(counts[0],0,&budget),live1(counts[1],0,&budget);
        std::pmr::vector<uint64_t> reps0(key0.slots(),UINT64_MAX,&budget),reps1(key1.slots(),UINT64_MAX,&budget);
        for(int arm=0;arm<2;++arm) {
            int center=centers[arm];auto &rows=arm?rows1:rows0;auto &valid=arm?valid1:valid0;
            auto &reps=arm?reps1:reps0;auto &keys=arm?key1:key0;
            plan.keys[center]->sequence.forEachStripe([&](uint32_t g,uint64_t offset) {
                const auto &rg=q.tables[center]->GetRowGroup(g);uint8_t bitmap[RowGroupSize/8];
                int count=filters[center]?filters[center]->ExecuteSet(rg,bitmap):rg.size;
                for(int row=0;row<rg.size;++row) {
                    rows[offset+row]=(uint64_t(g)<<32)|uint32_t(row);
                    reps[keys.word(g,row)]=rows[offset+row];
                }
                if(count)Selected(rg,count==rg.size?nullptr:bitmap,[&](int row){valid[offset+row]=1;});
            });
        }
        std::pmr::vector<uint32_t> pairs(size_t(counts[0])*counts[1],UINT32_MAX,&budget);
        for(uint32_t a=0;a<counts[0];++a)if(valid0[a])for(uint32_t b=0;b<counts[1];++b)if(valid1[b]) {
            auto read=[&](const ColumnRef &c) {
                uint64_t row=c.tableIdx==centers[0]?rows0[a]:rows1[b];
                return Read(q.tables[c.tableIdx]->GetRowGroup(row>>32),c,uint32_t(row));
            };
            bool keep=true;for(const auto &p:coupled)if(!EvaluateWith(*p,read).number){keep=false;break;}
            if(!keep)continue;
            live0[a]=1;live1[b]=1;
            pairs[a*counts[1]+b]=(key0.word(rows0[a]>>32,uint32_t(rows0[a]))*key1.slots()+
                key1.word(rows1[b]>>32,uint32_t(rows1[b])))*years;
        }
        std::vector<const uint8_t *> accepted(tables,nullptr);accepted[centers[0]]=live0.data();accepted[centers[1]]=live1.data();
        auto payloads=BuildIdentityPayloads(q,plan,fact,plan.order,plan.centerOf,accepted,filters,budget,avx,parallel);
        auto arms=plan.outgoing[fact];
        auto fraction=[&](const auto &edge) {const auto &p=*payloads[edge.target];return double(p.selected)/double(Wide(p.last)-p.first+1);};
        if(fraction(arms[1])<fraction(arms[0]))std::swap(arms[0],arms[1]);
        bool reversed=plan.centerOf[arms[0].target]!=centers[0];
        const auto &first=*payloads[arms[0].target],&second=*payloads[arms[1].target];auto domain=first.domain();
        int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        struct Partial {
            std::pmr::vector<Wide> sums;std::pmr::vector<uint8_t> seen;
            Partial(size_t n,Budget &budget):sums(n,0,&budget),seen(n,0,&budget) {}
        };
        auto parts=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,32*Block*8+RowGroupSize/8+4096);
            Partial part(slots,budget);uint8_t bitmap[RowGroupSize/8];
            alignas(64)int64_t buffers[32*Block];uint32_t selected[Block],groupIds[Block];
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);int count=filters[fact]?filters[fact]->ExecuteSet(rg,bitmap):rg.size;
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
                    int kept=0;
                    for(int i=0;i<n;++i) {
                        int row=selected[i];uint32_t ca=first.lookup(IdentityRead(a,row)),cb=second.lookup(IdentityRead(b,row));
                        if(!ca || !cb)continue;
                        uint32_t pair=reversed?(cb-1)*counts[1]+ca-1:(ca-1)*counts[1]+cb-1;
                        if(pairs[pair]==UINT32_MAX)continue;
                        int y=int(Evaluate(*year,rg,row).number)-firstYear;
                        if(y<0 || uint32_t(y)>=years)throw NotDense{};
                        selected[kept]=row;groupIds[kept++]=pairs[pair]+y;
                    }
                    if(!kept)continue;
                    program.evaluate<true>(0,kept,buffers,avx,selected);
                    for(int i=0;i<kept;++i){part.sums[groupIds[i]]+=buffers[root*Block+i];part.seen[groupIds[i]]=1;}
                }
            }
            return part;
        });
        QueryResults results(q,budget);
        for(uint32_t group=0;group<slots;++group) {
            Wide sum=0;bool seen=false;for(const auto &part:parts){sum=Add(sum,part.sums[group]);seen|=part.seen[group];}
            if(!seen)continue;
            uint32_t pair=group/years;uint64_t rows[2]={reps0[pair/key1.slots()],reps1[pair%key1.slots()]};
            results.addLazy([&](size_t i) {
                FinalCell cell;cell.type=q.aggregateClauses[i].expression->type.get();
                if(int(i)==sumIndex)cell.value.number=sum;
                else if(projection[i]==2)cell.value.number=firstYear+group%years;
                else {int arm=projection[i];uint64_t row=rows[arm];cell.value=Read(q.tables[centers[arm]]->GetRowGroup(row>>32),groups[arm],uint32_t(row));}
                return cell;
            });
        }
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
