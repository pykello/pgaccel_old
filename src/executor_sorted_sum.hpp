#pragma once
#include "executor_arithmetic_program.hpp"
#include "executor_result.hpp"
#include "executor_scaled_bound.hpp"
#include "executor_tasks.hpp"

namespace pgaccel {
namespace {
struct SortedSumGroup { int64_t key,sum; };
struct SortedSumFrame { uint32_t stripe; int64_t low,high; };
struct SortedSumStripe {
    std::pmr::vector<SortedSumGroup> groups;
    int64_t sum=0;uint64_t count=0;
    explicit SortedSumStripe(Budget &budget):groups(&budget) {}
};
struct SortedSumScan {
    std::pmr::vector<SortedSumFrame> frames;
    std::pmr::vector<SortedSumStripe> stripes;
    Wide sum=0;uint64_t count=0;
    explicit SortedSumScan(Budget &budget):frames(&budget),stripes(&budget) {}
};
using SortedSumBounds=std::pmr::vector<std::pair<size_t,ScaledBound>>;
SortedSumBounds PrepareSortedSumBounds(const QueryDesc &q,size_t keyOutput,size_t sumOutput,Budget &budget) {
    SortedSumBounds bounds(&budget);
    for (const auto &h:q.having) {
        if (h.bound || (h.output!=keyOutput && h.output!=sumOutput)) throw NotDense{};
        int scale=Scale(q.aggregateClauses[h.output].expression->type.get());
        bounds.emplace_back(h.output,ScaledBound(h.op,h.value,h.scale,scale,h.nullBound));
    }
    return bounds;
}
bool SortedSumMatches(const SortedSumBounds &bounds,size_t keyOutput,int64_t key,Wide sum) {
    for (const auto &[output,bound]:bounds)
        if (!bound.matches(output==keyOutput?Wide(key):sum)) return false;
    return true;
}
struct KeepSortedSums { bool operator()(int64_t,int64_t) const {return true;} };
// Ordered ranges may touch at a boundary. Only each stripe's first and last
// selected group can then require a cross-stripe merge.
template<class Filter,class Keep=KeepSortedSums>
SortedSumScan ScanSortedSums(const ColumnarTable &table,const ColumnRef &key,const Expression &value,
    Filter filter,Budget &budget,bool avx,bool parallel,Keep keep={},bool allRows=false) {
    SortedSumScan result(budget);
    auto type=key.Type()->type_num();if (type!=INT32_TYPE && type!=INT64_TYPE) throw NotDense{};
    for (int g=0;g<table.RowGroupCount();++g) {
        const auto &rg=table.GetRowGroup(g);if (!rg.size) continue;
        auto data=rg.columns[key.columnIdx].get();
        if (data->type!=ColumnDataBase::RAW_COLUMN_DATA) throw NotDense{};
        int64_t low,high;
        if (type==INT32_TYPE) {
            auto raw=static_cast<const RawColumnData<Int32Type> *>(data);low=raw->minValue;high=raw->maxValue;
        } else {
            auto raw=static_cast<const RawColumnData<Int64Type> *>(data);low=raw->minValue;high=raw->maxValue;
        }
        result.frames.push_back({uint32_t(g),low,high});
    }
    std::sort(result.frames.begin(),result.frames.end(),[](const auto &a,const auto &b) {
        return a.low<b.low || (a.low==b.low && a.high<b.high);
    });
    for (size_t i=1;i<result.frames.size();++i)
        if (result.frames[i-1].high>result.frames[i].low) throw NotDense{};
    result.stripes.reserve(result.frames.size());
    for (size_t i=0;i<result.frames.size();++i) result.stripes.emplace_back(budget);
    int workers=parallel?std::min<size_t>(8,std::max<size_t>(1,result.frames.size())):1;
    RunTasks(workers,[&](int worker) {
        ScopedValueResource memory(budget);
        Scratch scratch(budget,33*Block*sizeof(int64_t)+Block*sizeof(uint32_t)+RowGroupSize/8+32*sizeof(Operation)+128);
        alignas(64) int64_t buffers[32*Block],keys[Block];uint32_t selected[Block];uint8_t bitmap[RowGroupSize/8];
        for (size_t f=worker;f<result.frames.size();f+=workers) {
            const auto &rg=table.GetRowGroup(result.frames[f].stripe);
            if (!filter(rg,bitmap)) continue;
            Program program;int root=program.compile(value,rg);const auto &op=program.ops[root];
            if (op.low*rg.size<INT64_MIN || op.high*rg.size>INT64_MAX) throw NotDense{};
            const auto raw=static_cast<const RawColumnDataBase *>(rg.columns[key.columnIdx].get());
            auto &stripe=result.stripes[f];int n=0;
            auto stripeKeep=[&] {
                if constexpr (requires { keep.begin(); }) return keep.begin();
                else return keep;
            }();
            // Avoid repeated tiny allocations when retaining complete groups.
            // Keep the combined initial reservation below 1/8 of the query
            // allowance; selective HAVING paths keep their existing growth.
            if constexpr (std::is_same_v<Keep,KeepSortedSums>) {
                constexpr size_t initial=4096;
                if(result.frames.size()<=budget.limit/(8*initial*sizeof(SortedSumGroup))) {
                    size_t selectedCount=0;
                    for(int offset=0;offset<(rg.size+7)/8;offset+=8) {
                        uint64_t bits=0;memcpy(&bits,bitmap+offset,std::min(8,(rg.size+7)/8-offset));
                        selectedCount+=__builtin_popcountll(bits);
                    }
                    auto domain=Wide(result.frames[f].high)-result.frames[f].low+1;
                    stripe.groups.reserve(size_t(std::min<Wide>(std::min(initial,selectedCount),domain)));
                }
            }
            auto consume=[&]<bool Gather>(int begin) {
                switch (raw->bytesPerValue) {
                case 1:DecodeNumbers<int8_t,Gather,true>(raw,begin,n,keys,avx,selected);break;
                case 2:DecodeNumbers<int16_t,Gather,true>(raw,begin,n,keys,avx,selected);break;
                case 4:DecodeNumbers<int32_t,Gather,true>(raw,begin,n,keys,avx,selected);break;
                case 8:DecodeNumbers<int64_t,Gather,true>(raw,begin,n,keys,avx,selected);break;
                default:throw NotDense{};
                }
                program.evaluate<Gather,true>(begin,n,buffers,avx,selected);const auto values=buffers+root*Block;
                auto append=[&](int64_t key,int64_t sum) {
                    if (stripe.groups.empty() || stripe.groups.back().key!=key) {
                        if (!stripe.groups.empty() && stripe.groups.back().key>key) throw NotDense{};
                        // A completed interior group cannot cross a stripe
                        // boundary. Keep the first and current last group
                        // until the wide boundary merge is complete.
                        if constexpr (!std::is_same_v<Keep,KeepSortedSums>)
                            if (stripe.groups.size()>1 && !stripeKeep(stripe.groups.back().key,stripe.groups.back().sum))
                                stripe.groups.pop_back();
                        stripe.groups.push_back({key,sum});
                    } else stripe.groups.back().sum+=sum;
                    stripe.sum+=sum;
                };
                if constexpr (Gather) {
                    for(int i=0;i<n;++i) append(keys[i],values[i]);
                } else {
                    // Contiguous scans often have runs of equal keys. Reduce
                    // each run before updating retained group storage. The
                    // existing stripe bounds prove these partial sums fit.
                    for(int i=0;i<n;) {
                        int64_t key=keys[i],sum=values[i++];
                        while(i<n && keys[i]==key) sum+=values[i++];
                        append(key,sum);
                    }
                }
                stripe.count+=n;n=0;
            };
            if(allRows) {
                for(int begin=0;begin<rg.size;begin+=Block) {
                    n=std::min(Block,rg.size-begin);consume.template operator()<false>(begin);
                }
            } else {
                Selected(rg,bitmap,[&](int row){selected[n++]=row;if(n==Block)consume.template operator()<true>(0);});
                if(n)consume.template operator()<true>(0);
            }
            if constexpr (requires { keep.complete(stripe.sum); }) keep.complete(stripe.sum);
        }
        return 0;
    });
    for (const auto &stripe:result.stripes) { result.sum=Add(result.sum,stripe.sum);result.count+=stripe.count; }
    return result;
}
QueryOutput FinishSortedSums(const QueryDesc &q,size_t keyOutput,size_t sumOutput,
    const SortedSumScan &scan,Budget &budget,bool parallel) {
    QueryResults result(q,budget);if (!scan.count) return result.finish();
    auto bounds=PrepareSortedSumBounds(q,keyOutput,sumOutput,budget);
    auto emit=[&](QueryResults &target,int64_t key,Wide sum) {
        if (!SortedSumMatches(bounds,keyOutput,key,sum)) return;
        target.addLazy([&](size_t output) {
            return FinalCell{Cell{output==keyOutput?Wide(key):sum},q.aggregateClauses[output].expression->type.get()};
        });
    };
    int workers=parallel?std::min<size_t>(8,std::max<size_t>(1,scan.stripes.size())):1;
    auto partials=RunTasks(workers,[&](int worker) {
        ScopedValueResource memory(budget);QueryDesc local=q;local.offset=0;
        if (q.limit) local.limit=*q.limit+q.offset;
        QueryResults rows(local,budget);
        for (size_t f=worker;f<scan.stripes.size();f+=workers) {
            const auto &groups=scan.stripes[f].groups;
            for (size_t i=1;i+1<groups.size();++i) emit(rows,groups[i].key,groups[i].sum);
        }
        return rows.takeCandidates();
    });
    for (auto &rows:partials) for (auto &row:rows) result.add(std::move(row));
    bool pending=false;int64_t key=0;Wide sum=0;
    auto boundary=[&](const SortedSumGroup &group) {
        if (pending && key!=group.key) { emit(result,key,sum);pending=false; }
        if (!pending) { key=group.key;sum=group.sum;pending=true; }
        else sum=Add(sum,group.sum);
    };
    for (const auto &stripe:scan.stripes) if (!stripe.groups.empty()) {
        boundary(stripe.groups.front());
        if (stripe.groups.size()>1) boundary(stripe.groups.back());
    }
    if (pending) emit(result,key,sum);
    return result.finish();
}
}
}
