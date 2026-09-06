#include "executor_identity_payload.hpp"
#include "executor_boolean_bitmap.hpp"
#include "executor_scaled_bound.hpp"
#include "executor_result_number.hpp"
#include "executor_result.hpp"

namespace pgaccel {
namespace {
bool PrefixSlice(const Expression &e,const ColumnRef &phone) {
    return e.kind==Expression::SUBSTRING && e.sliceStart==1 && e.sliceLength==2 && e.left &&
        e.left->kind==Expression::COLUMN && e.left->column && *e.left->column==phone;
}
bool PrefixLiterals(const Expression &e,const ColumnRef &phone,std::set<std::string> &values,int depth=0) {
    if(depth>64)return false;
    if(e.kind==Expression::OR)return e.left && e.right && PrefixLiterals(*e.left,phone,values,depth+1) && PrefixLiterals(*e.right,phone,values,depth+1);
    if(e.kind!=Expression::EQ || !e.left || !e.right)return false;
    auto slice=e.left,value=e.right;if(!PrefixSlice(*slice,phone))std::swap(slice,value);
    if(!PrefixSlice(*slice,phone) || value->kind!=Expression::CONSTANT || value->type->type_num()!=STRING_TYPE ||
       value->literal.size()!=2 || !IsAscii(value->literal))return false;
    values.insert(value->literal);return values.size()<=255;
}
struct PrefixFrame {
    const DictColumnData<StringType> *phone;std::pmr::vector<uint8_t> labels;
    PrefixFrame(const RowGroup &rg,const ColumnRef &column,Budget &budget):labels(&budget) {
        auto data=rg.columns[column.columnIdx].get();
        if(data->type!=ColumnDataBase::DICT_COLUMN_DATA || data->size!=rg.size)throw NotDense{};
        phone=static_cast<const DictColumnData<StringType> *>(data);
    }
    uint8_t label(int row) const {
        auto id=phone->bytesPerValue()==1?phone->values[row]:reinterpret_cast<const uint16_t *>(phone->values)[row];
        return labels[id];
    }
};
}
std::optional<Result<QueryOutput>> ExecutePrefixAntiJoin(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=2 || q.existenceJoins.size()!=1 || !q.existenceJoins[0].anti || q.scalarQueries.size()!=1 ||
       !q.grouped || q.groupExpressions.size()!=1 || !q.having.empty() || !q.leftJoins.empty() ||
       !q.membershipQueries.empty() || !q.materializedSources.empty() || q.aggregateClauses.size()>8)return {};
    const auto &slice=q.groupExpressions[0];
    if(slice->kind!=Expression::SUBSTRING || !slice->left || !slice->left->column || slice->left->type->type_num()!=STRING_TYPE)return {};
    auto phone=*slice->left->column;if(!PrefixSlice(*slice,phone))return {};
    int customer=phone.tableIdx,orders=q.existenceJoins[0].table;
    if(customer<0 || customer>1 || orders<0 || orders>1 || customer==orders)return {};
    std::optional<ColumnRef> balance;
    for(const auto &a:q.aggregateClauses) {
        if(a.type==AggregateClause::AGGREGATE_COUNT && !a.expression)continue;
        if(a.type==AggregateClause::AGGREGATE_PROJECT && a.expression && PrefixSlice(*a.expression,phone))continue;
        if(a.type!=AggregateClause::AGGREGATE_SUM || !a.expression || a.expression->kind!=Expression::COLUMN || !a.expression->column ||
           a.expression->column->tableIdx!=customer)return {};
        if(balance && balance!=a.expression->column)return {};balance=a.expression->column;
    }
    if(!balance)return {};
    auto numeric=balance->Type()->type_num();if(numeric!=INT32_TYPE && numeric!=INT64_TYPE && numeric!=DECIMAL_TYPE)return {};
    const auto &scalar=q.scalarQueries[0];const auto &child=*scalar.query;
    if(!scalar.outerKeys.empty() || !scalar.innerKeys.empty() || !scalar.keyOutputs.empty() || child.tables.size()!=1 ||
       child.tables[0]!=q.tables[customer] || child.grouped || !child.groupExpressions.empty() || !child.having.empty() ||
       !child.leftJoins.empty() || !child.existenceJoins.empty() || !child.scalarQueries.empty() || !child.membershipQueries.empty() ||
       !child.materializedSources.empty() || !child.orderBy.empty() || child.limit || child.offset ||
       child.memoryBudgetBytes<q.memoryBudgetBytes || child.aggregateClauses.size()!=1)return {};
    const auto &avg=child.aggregateClauses[0];
    if(avg.type!=AggregateClause::AGGREGATE_AVG || !avg.expression || avg.expression->kind!=Expression::COLUMN ||
       !avg.expression->column || avg.expression->column->tableIdx!=0 || avg.expression->column->columnIdx!=balance->columnIdx)return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);Scratch planning(budget,131072);
        BooleanBitmapProgram outerFilters(budget,avx),innerFilters(budget,avx),orderFilters(budget,avx);
        int outerRoot=outerFilters.truth(),innerRoot=innerFilters.truth(),orderRoot=orderFilters.truth();
        auto append=[](BooleanBitmapProgram &program,int &root,int leaf){root=program.combine(true,root,leaf);};
        for(const auto &f:q.filterClauses) {
            if(f.columnRef.tableIdx==customer)append(outerFilters,outerRoot,outerFilters.leaf(f));
            else if(f.columnRef.tableIdx==orders)append(orderFilters,orderRoot,orderFilters.leaf(f));
            else return {};
        }
        std::set<std::string> prefixes;const Expression *comparison=nullptr;FilterClause::Op comparisonOp=FilterClause::FILTER_EQ;
        for(const auto &p:q.predicates) {
            std::set<std::string> literals;
            if(PrefixLiterals(*p,phone,literals)) {if(!prefixes.empty())return {};prefixes=std::move(literals);continue;}
            if(p->kind>=Expression::EQ && p->kind<=Expression::GTE && p->left && p->right && p->left->column && p->right->column) {
                auto a=*p->left->column,b=*p->right->column;bool reverse=a.tableIdx==-2;if(reverse)std::swap(a,b);
                if(a==*balance && b.tableIdx==-2 && b.columnIdx==0) {
                    if(comparison)return {};comparison=p.get();
                    switch(p->kind) {
                    case Expression::EQ:comparisonOp=FilterClause::FILTER_EQ;break;case Expression::NE:comparisonOp=FilterClause::FILTER_NE;break;
                    case Expression::GT:comparisonOp=reverse?FilterClause::FILTER_LT:FilterClause::FILTER_GT;break;
                    case Expression::GTE:comparisonOp=reverse?FilterClause::FILTER_LTE:FilterClause::FILTER_GTE;break;
                    case Expression::LT:comparisonOp=reverse?FilterClause::FILTER_GT:FilterClause::FILTER_LT;break;
                    case Expression::LTE:comparisonOp=reverse?FilterClause::FILTER_GTE:FilterClause::FILTER_LTE;break;
                    default:throw NotDense{};
                    }
                    continue;
                }
            }
            append(outerFilters,outerRoot,outerFilters.compile(*p,customer));
        }
        if(prefixes.empty() || !comparison)return {};
        auto childPhone=phone;childPhone.tableIdx=0;bool childPrefix=false;
        for(const auto &f:child.filterClauses) {if(f.columnRef.tableIdx!=0)return {};append(innerFilters,innerRoot,innerFilters.leaf(f));}
        for(const auto &p:child.predicates) {
            std::set<std::string> literals;
            if(PrefixLiterals(*p,childPhone,literals)) {if(childPrefix || literals!=prefixes)return {};childPrefix=true;}
            else append(innerFilters,innerRoot,innerFilters.compile(*p,0));
        }
        if(!childPrefix)return {};
        std::optional<ColumnRef> primary,foreign;
        for(const auto &p:q.existenceJoins[0].predicates) {
            if(p->kind==Expression::EQ && p->left && p->right && p->left->column && p->right->column) {
                auto a=*p->left->column,b=*p->right->column;if(a.tableIdx!=customer)std::swap(a,b);
                auto integer=[](const ColumnRef &c){auto t=c.Type()->type_num();return t==INT32_TYPE || t==INT64_TYPE;};
                if(a.tableIdx==customer && b.tableIdx==orders && integer(a) && integer(b)) {
                    if(primary)return {};primary=a;foreign=b;continue;
                }
            }
            append(orderFilters,orderRoot,orderFilters.compile(*p,orders));
        }
        if(!primary)return {};
        const auto &table=*q.tables[customer];uint64_t rows=0;
        for(int g=0;g<table.RowGroupCount();++g)rows+=table.GetRowGroup(g).size;
        SequenceJoinIndex sequence(&budget);if(!sequence.prepare(table,*primary,rows,avx))return {};
        std::pmr::vector<uint64_t> offsets(table.RowGroupCount(),0,&budget);
        sequence.forEachStripe([&](uint32_t g,uint64_t offset){offsets[g]=offset;});
        std::vector<std::string> names(prefixes.begin(),prefixes.end());std::array<uint8_t,65536> lookup{};
        for(size_t i=0;i<names.size();++i)lookup[uint8_t(names[i][0])|(uint32_t(uint8_t(names[i][1]))<<8)]=i+1;
        std::pmr::vector<PrefixFrame> frames(&budget);frames.reserve(table.RowGroupCount());
        for(int g=0;g<table.RowGroupCount();++g)frames.emplace_back(table.GetRowGroup(g),phone,budget);
        int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        struct Average {Wide sum=0;uint64_t count=0;};
        auto averages=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);Average result;
            std::pmr::vector<uint64_t> scratch(innerFilters.bytes()/8,0,&budget);std::array<int,64> counts;
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);auto &frame=frames[g];frame.labels.reserve(frame.phone->dict.size());
                for(const auto &word:frame.phone->dict) {
                    if(!IsAscii(word) && !IsValidUtf8(word))throw NotDense{};
                    auto prefix=EvaluateWith(*slice,[&](const ColumnRef &)->Cell{return {0,word,true};});
                    uint8_t code=0;if(prefix.text.size()==2 && IsAscii(prefix.text))code=lookup[uint8_t(prefix.text[0])|(uint32_t(uint8_t(prefix.text[1]))<<8)];
                    frame.labels.push_back(code);
                }
                auto bytes=reinterpret_cast<uint8_t *>(scratch.data());innerFilters.evaluate(rg,bytes,counts);
                if(!counts[innerRoot])continue;auto bitmap=innerFilters.bitmap(bytes,innerRoot);const auto &values=IdentityRaw(rg,*balance);
                Selected(rg,counts[innerRoot]==rg.size?nullptr:bitmap,[&](int row){if(frame.label(row)){result.sum+=IdentityRead(values,row);++result.count;}});
            }
            return result;
        });
        Wide sumValue=0;uint64_t countValue=0;for(const auto &part:averages){sumValue=Add(sumValue,part.sum);countValue+=part.count;}
        QueryResults results(q,budget);if(!countValue)return results.finish();
        FinalCell average;average.type=balance->Type().get();average.average=true;
        average.mean=(long double)sumValue/countValue/(long double)Power(Scale(average.type));
        Wide threshold=ResultNumber(Format(average),Scale(scalar.resultType.get()));
        ScaledBound bound(comparisonOp,threshold,Scale(scalar.resultType.get()),Scale(balance->Type().get()));
        std::pmr::vector<uint64_t> candidates((rows+63)/64,0,&budget);
        auto selected=RunTasks(workers,[&](int worker) {
            ScopedValueResource memory(budget);uint64_t count=0;
            std::pmr::vector<uint64_t> scratch(outerFilters.bytes()/8,0,&budget);std::array<int,64> counts;
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);auto bytes=reinterpret_cast<uint8_t *>(scratch.data());outerFilters.evaluate(rg,bytes,counts);
                if(!counts[outerRoot])continue;const auto &values=IdentityRaw(rg,*balance);auto bitmap=outerFilters.bitmap(bytes,outerRoot);
                Selected(rg,counts[outerRoot]==rg.size?nullptr:bitmap,[&](int row) {
                    if(!frames[g].label(row) || !bound.matches(IdentityRead(values,row)))return;
                    uint64_t offset=offsets[g]+row;
                    std::atomic_ref<uint64_t>(candidates[offset/64]).fetch_or(uint64_t(1)<<(offset%64),std::memory_order_relaxed);++count;
                });
            }
            return count;
        });
        if(std::accumulate(selected.begin(),selected.end(),uint64_t(0))==0)return results.finish();
        BitmapDomain domain{sequence.minimum(),sequence.maximum(),candidates.data()};
        const auto &orderTable=*q.tables[orders];int orderWorkers=parallel?std::min(8,std::max(1,orderTable.RowGroupCount())):1;
        auto hits=RunTasks(orderWorkers,[&](int worker) {
            ScopedValueResource memory(budget);Scratch stack(budget,Block*4+256);uint32_t selected[Block];
            std::pmr::vector<uint64_t> hit(candidates.size(),0,&budget),scratch(orderFilters.bytes()/8,0,&budget);std::array<int,64> counts;
            for(int g=worker;g<orderTable.RowGroupCount();g+=orderWorkers) {
                const auto &rg=orderTable.GetRowGroup(g);auto bytes=reinterpret_cast<uint8_t *>(scratch.data());orderFilters.evaluate(rg,bytes,counts);
                if(!counts[orderRoot])continue;auto bitmap=orderFilters.bitmap(bytes,orderRoot);const auto &keys=IdentityRaw(rg,*foreign);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int size=std::min(Block,rg.size-begin),n=0;
                    if(avx && keys.bytesPerValue==4 && domain.first>=0 && domain.last<=INT32_MAX)
                        n=BitmapSelectRange32(domain,reinterpret_cast<const int32_t *>(keys.values),begin,size,counts[orderRoot]==rg.size?nullptr:bitmap,selected);
                    else for(int row=begin;row<begin+size;++row)
                        if((counts[orderRoot]==rg.size || (bitmap[row/8]>>(row%8)&1)) && domain.contains(IdentityRead(keys,row)))selected[n++]=row;
                    for(int i=0;i<n;++i){uint64_t offset=uint64_t(Wide(IdentityRead(keys,selected[i]))-domain.first);hit[offset/64]|=uint64_t(1)<<(offset%64);}
                }
            }
            return hit;
        });
        for(const auto &hit:hits)for(size_t w=0;w<candidates.size();++w)candidates[w]&=~hit[w];
        std::array<uint64_t,256> counts{};std::array<Wide,256> sums{};
        for(size_t w=0;w<candidates.size();++w) {
            uint64_t bits=candidates[w];while(bits) {
                uint64_t offset=w*64+std::countr_zero(bits);bits&=bits-1;
                auto row=sequence.lookup(sequence.minimum()+int64_t(offset));if(!row)throw NotDense{};
                uint32_t stripe=*row>>32,r=uint32_t(*row);uint8_t label=frames[stripe].label(r);
                ++counts[label];sums[label]=Add(sums[label],IdentityRead(IdentityRaw(table.GetRowGroup(stripe),*balance),r));
            }
        }
        for(size_t label=1;label<=names.size();++label)if(counts[label])results.addLazy([&](size_t i) {
            FinalCell cell;const auto &a=q.aggregateClauses[i];
            if(a.type==AggregateClause::AGGREGATE_COUNT)cell.value.number=counts[label];
            else if(a.type==AggregateClause::AGGREGATE_SUM){cell.type=a.expression->type.get();cell.value.number=sums[label];}
            else {cell.type=a.expression->type.get();cell.value={0,names[label-1],true};}
            return cell;
        });
        return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
