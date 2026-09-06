#pragma once
#include "executor_arithmetic_program.hpp"
#include "executor_state.hpp"
#include "executor_tasks.hpp"
namespace pgaccel {
namespace {
struct BoundedSums {
    int64_t first;
    std::pmr::vector<Wide> sums;
    std::pmr::vector<uint64_t> seen;
    BoundedSums(int64_t low,size_t size,Budget &budget):first(low),sums(size,0,&budget),seen((size+63)/64,0,&budget) {}
    bool contains(size_t slot) const {return (seen[slot/64]>>(slot%64))&1;}
};
// Dense integer domains use independent worker arrays. Every partial sum is
// bounded before scanning; the final merge retains checked wide arithmetic.
BoundedSums ScanBoundedSums(const ColumnarTable &table,const ColumnRef &key,const Expression &value,
    const FilterNodeImpl *filter,Budget &budget,bool avx,bool parallel) {
    auto type=key.Type()->type_num();if(type!=INT32_TYPE && type!=INT64_TYPE)throw NotDense{};
    int64_t low=INT64_MAX,high=INT64_MIN;uint64_t rows=0;
    for(int g=0;g<table.RowGroupCount();++g) {
        const auto &rg=table.GetRowGroup(g);if(!rg.size)continue;
        auto data=rg.columns[key.columnIdx].get();if(data->type!=ColumnDataBase::RAW_COLUMN_DATA)throw NotDense{};
        int64_t a,b;
        if(type==INT32_TYPE){auto raw=static_cast<const RawColumnData<Int32Type> *>(data);a=raw->minValue;b=raw->maxValue;}
        else {auto raw=static_cast<const RawColumnData<Int64Type> *>(data);a=raw->minValue;b=raw->maxValue;}
        low=std::min(low,a);high=std::max(high,b);rows+=rg.size;
    }
    if(!rows)return BoundedSums(0,0,budget);
    Wide width=Wide(high)-low+1;if(width<=0 || width>(1<<24) || width>Wide(rows)*8)throw NotDense{};
    const size_t size=size_t(width);
    struct Partial {
        std::pmr::vector<int64_t> sums;
        std::pmr::vector<uint64_t> seen;
        Partial(size_t n,Budget &budget):sums(n,0,&budget),seen((n+63)/64,0,&budget) {}
    };
    int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
    auto partials=RunTasks(workers,[&](int worker) {
        ScopedValueResource memory(budget);Partial part(size,budget);
        Scratch scratch(budget,33*Block*sizeof(int64_t)+Block*sizeof(uint32_t)+RowGroupSize/8+4096);
        alignas(64)int64_t buffers[32*Block],keys[Block];uint32_t selected[Block];uint8_t bitmap[RowGroupSize/8];
        for(int g=worker;g<table.RowGroupCount();g+=workers) {
            const auto &rg=table.GetRowGroup(g);
            int count=filter?filter->ExecuteSet(rg,bitmap):rg.size;if(!count)continue;
            Program program;int root=program.compile(value,rg);const auto &op=program.ops[root];
            if(op.low*rows<INT64_MIN || op.high*rows>INT64_MAX)throw NotDense{};
            auto raw=static_cast<const RawColumnDataBase *>(rg.columns[key.columnIdx].get());
            auto consume=[&]<bool Gather>(int begin,int n) {
                switch(raw->bytesPerValue) {
                case 1:DecodeNumbers<int8_t,Gather>(raw,begin,n,keys,avx,selected);break;
                case 2:DecodeNumbers<int16_t,Gather>(raw,begin,n,keys,avx,selected);break;
                case 4:DecodeNumbers<int32_t,Gather>(raw,begin,n,keys,avx,selected);break;
                case 8:DecodeNumbers<int64_t,Gather>(raw,begin,n,keys,avx,selected);break;
                default:throw NotDense{};
                }
                program.evaluate<Gather>(begin,n,buffers,avx,selected);
                for(int i=0;i<n;++i) {
                    uint64_t slot=uint64_t(keys[i])-uint64_t(low);if(slot>=size)throw NotDense{};
                    part.sums[slot]+=buffers[root*Block+i];part.seen[slot/64]|=uint64_t(1)<<(slot%64);
                }
            };
            if(count==rg.size)for(int begin=0;begin<rg.size;begin+=Block)
                consume.template operator()<false>(begin,std::min(Block,rg.size-begin));
            else {
                int n=0;Selected(rg,bitmap,[&](int row){selected[n++]=row;if(n==Block){consume.template operator()<true>(0,n);n=0;}});
                if(n)consume.template operator()<true>(0,n);
            }
        }
        return part;
    });
    BoundedSums result(low,size,budget);
    RunTasks(workers,[&](int worker) {
        for(size_t word=result.seen.size()*worker/workers;word<result.seen.size()*(worker+1)/workers;++word) {
            for(const auto &part:partials)result.seen[word]|=part.seen[word];
            for(uint64_t bits=result.seen[word];bits;bits&=bits-1) {
                size_t slot=word*64+__builtin_ctzll(bits);
                for(const auto &part:partials)result.sums[slot]=Add(result.sums[slot],part.sums[slot]);
            }
        }
        return true;
    });
    return result;
}
}
}
