#include "executor_group_key.hpp"
#include "executor_flat_map.hpp"
#include "executor_result.hpp"
#include "executor_arithmetic_program.hpp"
#include "util.h"

namespace pgaccel {
namespace {
struct DenseState {
    uint64_t count=0;
    std::pmr::vector<Wide> sums;
    DenseState(size_t n, Budget &b):sums(n, &b) {}
};
}
std::optional<Result<QueryOutput>> ExecuteDenseArithmetic(const QueryDesc &q, bool avx, bool parallel) {
    if(q.groupExpressions.empty() || q.groupExpressions.size()>2) return {};
    for(const auto &e:q.groupExpressions) if(e->kind!=Expression::COLUMN) return {};
    for(const auto &a:q.aggregateClauses) {
        if(a.type==AggregateClause::AGGREGATE_PROJECT) {
            if(!a.expression || a.expression->kind!=Expression::COLUMN) return {};
        } else if(a.type!=AggregateClause::AGGREGATE_SUM && a.type!=AggregateClause::AGGREGATE_AVG &&
                  a.type!=AggregateClause::AGGREGATE_COUNT) return {};
    }
    try {
        Budget budget(q.memoryBudgetBytes);
        ScopedValueResource memory(budget);
        GroupKeys keys{q,&budget};
        using Map=FlatMap<GroupKey,DenseState,GroupHash,GroupEqual>;
        auto newMap=[&]{return Map(budget,GroupHash{&keys},GroupEqual{&keys});};
        auto &table=*q.tables[0];
        const size_t outputs=q.aggregateClauses.size();
        auto filter=q.filterClauses.empty()?FilterNodeP{}:CreateFilterNode(q.filterClauses,avx);
        int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        auto scan=[&](int worker) {
            ScopedValueResource memory(budget);
            auto groups=newMap();
            Scratch scratch(budget,32*Block*sizeof(int64_t)+65536);
            alignas(64) int64_t buffers[32*Block], codes[Block];
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);
                if(!rg.size) continue;
                const DictColumnDataBase *dicts[2]{};
                int product=1;
                for(size_t k=0;k<q.groupExpressions.size();++k) {
                    auto data=rg.columns[q.groupExpressions[k]->column->columnIdx].get();
                    if(data->type!=ColumnDataBase::DICT_COLUMN_DATA) throw NotDense{};
                    dicts[k]=static_cast<const DictColumnDataBase *>(data);
                    if(dicts[k]->dictSize()>16/product) throw NotDense{};
                    product*=dicts[k]->dictSize();
                }
                const int width0=dicts[0]->bytesPerValue();
                const int width1=dicts[1]?dicts[1]->bytesPerValue():0;
                const int radix=dicts[1]?dicts[1]->dictSize():1;
                const bool trackPairs=dicts[1] && product>2;
                auto codeAt=[](const DictColumnDataBase *dict,int width,int row) {
                    return width==1?dict->values[row]:reinterpret_cast<const uint16_t *>(dict->values)[row];
                };
                Program program;
                std::vector<int> roots(outputs,-1);
                for(size_t i=0;i<outputs;++i) {
                    const auto &a=q.aggregateClauses[i];
                    if(a.type==AggregateClause::AGGREGATE_SUM || a.type==AggregateClause::AGGREGATE_AVG) {
                        roots[i]=program.compile(*a.expression,rg);
                        const auto &p=program.ops[roots[i]];
                        if(p.low*rg.size<INT64_MIN || p.high*rg.size>INT64_MAX) throw NotDense{};
                    }
                }
                uint8_t bitmap[RowGroupSize/8];
                int count=filter?filter->ExecuteSet(rg,bitmap):rg.size;
                if(!count) continue;
                uint64_t counts[16]{};
                int representatives[16]{};
                std::vector<int64_t> sums(product*program.ops.size(),0);
                for(int begin=0;begin<rg.size;begin+=Block) {
                    int n=std::min(Block,rg.size-begin);
                    uint32_t active=trackPairs?0:(uint32_t(1)<<product)-1;
                    int r=0;
                    if(avx) {
                        auto observed=_mm512_setzero_si512();
                        auto decode=[](const DictColumnDataBase *dict,int width,int row) {
                            return width==1?_mm512_cvtepu8_epi64(_mm_loadl_epi64(
                                reinterpret_cast<const __m128i *>(dict->values+row))):
                                _mm512_cvtepu16_epi64(_mm_loadu_si128(
                                reinterpret_cast<const __m128i *>(dict->values+2*row)));
                        };
                        for(;r+8<=n;r+=8) {
                            int row=begin+r;
                            auto code=decode(dicts[0],width0,row);
                            if(dicts[1]) code=_mm512_add_epi64(
                                _mm512_mul_epu32(code,_mm512_set1_epi64(radix)),decode(dicts[1],width1,row));
                            if(count!=rg.size) code=_mm512_mask_mov_epi64(
                                _mm512_set1_epi64(-1),bitmap[row/8],code);
                            if(trackPairs)observed=_mm512_or_si512(observed,
                                _mm512_sllv_epi64(_mm512_set1_epi64(1),code));
                            _mm512_store_si512(codes+r,code);
                        }
                        if(trackPairs)active=uint32_t(_mm512_reduce_or_epi64(observed));
                    }
                    for(;r<n;++r) {
                        int row=begin+r;
                        int code=codeAt(dicts[0],width0,row);
                        if(dicts[1]) code=code*radix+codeAt(dicts[1],width1,row);
                        codes[r]=count==rg.size || IsBitSet(bitmap,row)?code:-1;
                        if(trackPairs && codes[r]>=0)active|=uint32_t(1)<<code;
                    }
                    if(!active)continue;
                    program.evaluate(begin,n,buffers,avx);
                    for(;active;active&=active-1) {
                        int code=__builtin_ctz(active);
                        __mmask8 masks[Block/8];
                        const int vectorEnd=avx?n/8*8:0;
                        int matches=0, representative=-1;
                        for(int j=0;j<vectorEnd;j+=8) {
                            auto mask=_mm512_cmpeq_epi64_mask(_mm512_load_si512(codes+j),_mm512_set1_epi64(code));
                            masks[j/8]=mask; matches+=__builtin_popcount(mask);
                            if(mask) representative=j+__builtin_ctz(mask);
                        }
                        for(int j=vectorEnd;j<n;++j) if(codes[j]==code) {++matches;representative=j;}
                        if(!matches) continue;
                        counts[code]+=matches; representatives[code]=begin+representative;
                        bool seen[32]{};
                        for(int root:roots) if(root>=0 && !seen[root]) {
                            seen[root]=true;
                            auto values=buffers+root*Block;
                            int j=0; int64_t sum=0;
                            if(avx) {
                                __m512i total=_mm512_setzero_si512();
                                __m512i t1=total,t2=total,t3=total;
                                for(;j+32<=vectorEnd;j+=32) {
                                    total=_mm512_add_epi64(total,_mm512_maskz_loadu_epi64(masks[j/8],values+j));
                                    t1=_mm512_add_epi64(t1,_mm512_maskz_loadu_epi64(masks[j/8+1],values+j+8));
                                    t2=_mm512_add_epi64(t2,_mm512_maskz_loadu_epi64(masks[j/8+2],values+j+16));
                                    t3=_mm512_add_epi64(t3,_mm512_maskz_loadu_epi64(masks[j/8+3],values+j+24));
                                }
                                total=_mm512_add_epi64(_mm512_add_epi64(total,t1),_mm512_add_epi64(t2,t3));
                                for(;j<vectorEnd;j+=8)
                                    total=_mm512_add_epi64(total,_mm512_maskz_loadu_epi64(masks[j/8],values+j));
                                sum=_mm512_reduce_add_epi64(total);
                            }
                            for(;j<n;++j) if(codes[j]==code) sum+=values[j];
                            sums[code*program.ops.size()+root]+=sum;
                        }
                    }
                }
                keys.prepare(g,nullptr);
                for(int code=0;code<product;++code) if(counts[code]) {
                    auto [index,added]=groups.get(keys.make(g,representatives[code]),[&]{return DenseState(outputs,budget);});
                    auto &state=groups.at(index).second; state.count+=counts[code];
                    for(size_t i=0;i<outputs;++i) if(roots[i]>=0)
                        state.sums[i]=Add(state.sums[i],sums[code*program.ops.size()+roots[i]]);
                }
            }
            return groups;
        };
        auto partials=RunTasks(workers,scan);
        auto merged=newMap();
        for(auto &part:partials) for(uint32_t i=0;i<part.size();++i) {
            auto &[key,state]=part.at(i);
            auto [index,added]=merged.get(key,[&]{return DenseState(outputs,budget);});
            auto &target=merged.at(index).second; target.count+=state.count;
            for(size_t j=0;j<outputs;++j) target.sums[j]=Add(target.sums[j],state.sums[j]);
        }
        QueryResults results(q,budget);
        for(uint32_t index=0;index<merged.size();++index) {
            auto &[key,state]=merged.at(index);
            results.addLazy([&](size_t i) {
                const auto &a=q.aggregateClauses[i]; FinalCell cell;
                cell.type=a.expression?a.expression->type.get():nullptr;
                if(a.type==AggregateClause::AGGREGATE_PROJECT)
                    cell.value=Evaluate(*a.expression,table.GetRowGroup(key.group),key.row);
                else if(a.type==AggregateClause::AGGREGATE_COUNT) {cell.type=nullptr;cell.value.number=state.count;}
                else if(a.type==AggregateClause::AGGREGATE_SUM) cell.value.number=state.sums[i];
                else {cell.average=true;cell.mean=(long double)state.sums[i]/state.count/(long double)Power(Scale(cell.type));}
                return cell;
            });
        }
        return results.finish();
    } catch(const NotDense &) {return {};}
}
}
