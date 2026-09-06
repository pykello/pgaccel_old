#include "executor_arithmetic_program.hpp"
#include "executor_sequence_join.hpp"
#include "executor_result.hpp"
#include "util.h"
#include <functional>

namespace pgaccel {
namespace {
bool OnTable(const Expression &e,int table) {
    if(e.column && e.column->tableIdx!=table) return false;
    for(const auto &child:{e.left,e.right,e.condition}) if(child && !OnTable(*child,table)) return false;
    return true;
}
struct SumPlan {std::shared_ptr<Expression> value,condition;int bitmap=-1;};
}
std::optional<Result<QueryOutput>> ExecuteJoinSum(const QueryDesc &q,bool avx,bool parallel) {
    if(q.tables.size()!=2 || q.grouped || !q.groupExpressions.empty() || !q.having.empty() ||
        !q.existenceJoins.empty() || !q.leftJoins.empty() || !q.membershipQueries.empty() ||
        !q.scalarQueries.empty() || q.predicates.size()!=1 || q.aggregateClauses.size()>32) return {};
    const auto &edge=*q.predicates[0];
    if(edge.kind!=Expression::EQ || !edge.left->column || !edge.right->column) return {};
    auto left=*edge.left->column,right=*edge.right->column;
    if(left.tableIdx<0 || right.tableIdx<0 || left.tableIdx==right.tableIdx ||
        Scale(left.Type().get())!=Scale(right.Type().get())) return {};
    try {
        Budget budget(q.memoryBudgetBytes);ScopedValueResource memory(budget);
        SequenceJoinIndex index(&budget);
        ColumnRef build=left,probe=right;
        std::vector<SumPlan> plans;
        bool supported=false;
        for(int orientation=0;orientation<2 && !supported;++orientation) {
            if(orientation) std::swap(build,probe);
            if(std::any_of(q.filterClauses.begin(),q.filterClauses.end(),[&](const auto &f){return f.columnRef.tableIdx!=probe.tableIdx;})) continue;
            plans.clear();bool valid=true;
            for(const auto &a:q.aggregateClauses) {
                SumPlan plan;
                if(a.type==AggregateClause::AGGREGATE_FORMULA) {
                    valid &= a.expression && OnTable(*a.expression,-1);
                } else if(a.type==AggregateClause::AGGREGATE_COUNT) {
                    valid &= !a.expression;
                } else if(a.type==AggregateClause::AGGREGATE_SUM) {
                    plan.value=a.expression;
                    if(plan.value && plan.value->kind==Expression::CASE) {
                        const auto &e=*plan.value;
                        if(!e.right || e.right->kind!=Expression::CONSTANT ||
                            e.right->type->type_num()==STRING_TYPE || Evaluate(*e.right,RowGroup{},0).number!=0 ||
                            !e.condition || e.condition->kind!=Expression::LIKE || !e.condition->regexp ||
                            !e.condition->left || e.condition->left->kind!=Expression::COLUMN ||
                            e.condition->left->column->tableIdx!=build.tableIdx ||
                            e.condition->left->type->type_num()!=STRING_TYPE ||
                            Scale(e.type.get())!=Scale(e.left->type.get())) {valid=false;break;}
                        plan.condition=e.condition;plan.value=e.left;
                    }
                    valid &= plan.value && OnTable(*plan.value,probe.tableIdx);
                } else valid=false;
                plans.push_back(plan);
            }
            if(!valid) continue;
            uint64_t rows=0;
            for(int g=0;g<q.tables[build.tableIdx]->RowGroupCount();++g) rows+=q.tables[build.tableIdx]->GetRowGroup(g).size;
            supported=index.prepare(*q.tables[build.tableIdx],build,rows,avx);
        }
        if(!supported) return {};
        std::pmr::vector<std::pmr::vector<uint64_t>> bitmaps(&budget);
        for(auto &plan:plans) if(plan.condition) {
            plan.bitmap=bitmaps.size();bitmaps.emplace_back();
            auto &bits=bitmaps.back();bits.resize((index.size()+63)/64,0);
            index.forEachStripe([&](uint32_t group,uint64_t offset) {
                const auto &rg=q.tables[build.tableIdx]->GetRowGroup(group);
                auto data=rg.columns[plan.condition->left->column->columnIdx].get();
                if(data->type!=ColumnDataBase::DICT_COLUMN_DATA) throw NotDense{};
                auto dict=static_cast<const DictColumnData<StringType> *>(data);
                std::pmr::vector<uint8_t> matches(dict->dictSize(),0,&budget);
                for(size_t code=0;code<matches.size();++code)
                    matches[code]=re2::RE2::FullMatch(dict->dict[code],*plan.condition->regexp);
                const int width=dict->bytesPerValue();
                for(int row=0;row<rg.size;++row) {
                    int code=width==1?dict->values[row]:reinterpret_cast<const uint16_t *>(dict->values)[row];
                    if(matches[code]) bits[(offset+row)/64]|=uint64_t(1)<<((offset+row)%64);
                }
            });
        }
        auto &table=*q.tables[probe.tableIdx];
        auto filter=q.filterClauses.empty()?FilterNodeP{}:CreateFilterNode(q.filterClauses,avx);
        int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        struct Partial {uint64_t count=0;std::array<Wide,32> sums{};};
        auto scan=[&](int worker) {
            ScopedValueResource memory(budget);Scratch scratch(budget,32*Block*8+65536);
            alignas(64) int64_t buffers[32*Block],keys[Block];
            uint32_t selected[Block];uint8_t filterBits[RowGroupSize/8];
            Partial result;
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);
                int count=filter?filter->ExecuteSet(rg,filterBits):rg.size;
                if(!count) continue;
                auto keyData=rg.columns[probe.columnIdx].get();
                if(keyData->type!=ColumnDataBase::RAW_COLUMN_DATA) throw NotDense{};
                auto raw=static_cast<const RawColumnDataBase *>(keyData);
                Program program;std::array<int,32> roots;roots.fill(-1);
                for(size_t i=0;i<plans.size();++i) if(plans[i].value) {
                    roots[i]=program.compile(*plans[i].value,rg);
                    const auto &p=program.ops[roots[i]];
                    if(p.low*count<INT64_MIN || p.high*count>INT64_MAX) throw NotDense{};
                }
                std::array<int64_t,32> sums{};
                int n=0;
                auto consume=[&] {
                    switch(raw->bytesPerValue) {
                    case 1:DecodeNumbers<int8_t,true>(raw,0,n,keys,avx,selected);break;
                    case 2:DecodeNumbers<int16_t,true>(raw,0,n,keys,avx,selected);break;
                    case 4:DecodeNumbers<int32_t,true>(raw,0,n,keys,avx,selected);break;
                    case 8:DecodeNumbers<int64_t,true>(raw,0,n,keys,avx,selected);break;
                    default:throw NotDense{};
                    }
                    program.evaluate<true>(0,n,buffers,avx,selected);
                    int row=0;
                    if(avx) for(;row+8<=n;row+=8) {
                        auto key=_mm512_load_si512(keys+row);
                        auto delta=_mm512_sub_epi64(key,_mm512_set1_epi64(index.minimum()));
                        __mmask8 valid=_mm512_cmpge_epi64_mask(key,_mm512_set1_epi64(index.minimum())) &
                            _mm512_cmple_epi64_mask(key,_mm512_set1_epi64(index.maximum()));
                        result.count+=__builtin_popcount(valid);
                        for(size_t i=0;i<plans.size();++i) if(roots[i]>=0) {
                            auto mask=valid;
                            if(plans[i].bitmap>=0) {
                                auto words=_mm512_mask_i64gather_epi64(_mm512_setzero_si512(),valid,
                                    _mm512_srli_epi64(delta,6),bitmaps[plans[i].bitmap].data(),8);
                                auto shifted=_mm512_srlv_epi64(words,_mm512_and_si512(delta,_mm512_set1_epi64(63)));
                                mask &= _mm512_test_epi64_mask(shifted,_mm512_set1_epi64(1));
                            }
                            sums[i]+=_mm512_reduce_add_epi64(_mm512_maskz_loadu_epi64(mask,buffers+roots[i]*Block+row));
                        }
                    }
                    for(;row<n;++row) {
                        Wide offset=Wide(keys[row])-index.minimum();
                        if(offset<0 || offset>=index.size()) continue;
                        ++result.count;
                        for(size_t i=0;i<plans.size();++i) if(roots[i]>=0) {
                            int bitmap=plans[i].bitmap;
                            if(bitmap<0 || (bitmaps[bitmap][uint64_t(offset)/64]>>(uint64_t(offset)%64)&1))
                                sums[i]+=buffers[roots[i]*Block+row];
                        }
                    }
                    n=0;
                };
                Selected(rg,count==rg.size?nullptr:filterBits,[&](int row){selected[n++]=row;if(n==Block)consume();});
                if(n) consume();
                for(size_t i=0;i<plans.size();++i) result.sums[i]=Add(result.sums[i],sums[i]);
            }
            return result;
        };
        auto partials=RunTasks(workers,scan);Partial total;
        for(const auto &p:partials) {total.count+=p.count;for(size_t i=0;i<plans.size();++i)total.sums[i]=Add(total.sums[i],p.sums[i]);}
        struct Null {};
        std::function<FinalCell(size_t)> finish=[&](size_t i) {
            const auto &a=q.aggregateClauses[i];FinalCell cell;
            cell.type=a.expression?a.expression->type.get():nullptr;
            if(a.type==AggregateClause::AGGREGATE_COUNT) {cell.type=nullptr;cell.value.number=total.count;}
            else if(a.type==AggregateClause::AGGREGATE_SUM) {cell.null=!total.count;cell.value.number=total.sums[i];}
            else try {
                cell.value=EvaluateWith(*a.expression,[&](const ColumnRef &column) {
                    auto input=finish(column.columnIdx);if(input.null)throw Null{};return input.value;
                });
            } catch(const Null &) {cell.null=true;}
            return cell;
        };
        QueryResults results(q,budget);results.addLazy(finish);return results.finish();
    } catch(const NotDense &) {return {};}
    catch(const std::exception &e) {return Status::Invalid(e.what());}
}
}
