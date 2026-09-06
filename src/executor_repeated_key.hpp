#pragma once
#include "executor_arithmetic_program.hpp"
#include "executor_state.hpp"
#include "executor_tasks.hpp"
#include <bit>
namespace pgaccel {
namespace {
// A query-local directory for a proved consecutive key sequence in which
// every key occupies the same power-of-two number of physical rows.
class RepeatedKeyIndex {
    Budget &budget;
    std::pmr::vector<uint32_t> directory;
    int64_t first=0;
    uint64_t count=0;
    uint32_t repeat=0,stride=0;
    template<class T> static bool verify(const RawColumnDataBase &data,unsigned shift,int64_t &start,bool avx) {
        auto values=reinterpret_cast<const T *>(data.values);start=values[0];
        if(Wide(start)+((data.size-1)>>shift)>INT64_MAX)return false;
        int row=0;
        if(avx) {
            auto positions=_mm512_setr_epi64(0,1,2,3,4,5,6,7),shifts=_mm512_set1_epi64(shift);
            auto origin=_mm512_set1_epi64(start);
            for(;row+8<=data.size;row+=8) {
                __m512i actual;
                if constexpr(sizeof(T)==1)actual=_mm512_cvtepi8_epi64(_mm_loadl_epi64(reinterpret_cast<const __m128i *>(values+row)));
                else if constexpr(sizeof(T)==2)actual=_mm512_cvtepi16_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(values+row)));
                else if constexpr(sizeof(T)==4)actual=_mm512_cvtepi32_epi64(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(values+row)));
                else actual=_mm512_loadu_si512(values+row);
                auto expected=_mm512_add_epi64(origin,_mm512_srlv_epi64(positions,shifts));
                if(_mm512_cmpeq_epi64_mask(actual,expected)!=0xff)return false;
                positions=_mm512_add_epi64(positions,_mm512_set1_epi64(8));
            }
        }
        for(;row<data.size;++row)if(Wide(values[row])!=Wide(start)+(row>>shift))return false;
        return true;
    }
public:
    explicit RepeatedKeyIndex(Budget &memory):budget(memory),directory(&memory) {}
    bool prepare(const ColumnarTable &table,const ColumnRef &key,int64_t minimum,uint64_t keys,bool avx,bool parallel) {
        count=0;repeat=0;directory.clear();
        if(!keys || !table.RowGroupCount() || Wide(minimum)+keys-1>INT64_MAX ||
            (key.Type()->type_num()!=INT32_TYPE && key.Type()->type_num()!=INT64_TYPE))return false;
        uint64_t rows=0;for(int g=0;g<table.RowGroupCount();++g)rows+=table.GetRowGroup(g).size;
        if(!rows || rows%keys || rows/keys>16 || !std::has_single_bit(rows/keys))return false;
        uint32_t fanout=rows/keys;unsigned shift=std::countr_zero(fanout);
        struct Frame {int64_t first=0;uint32_t group=0,size=0;};
        std::pmr::vector<Frame> frames(table.RowGroupCount(),Frame{},&budget);
        auto valid=RunTasks(parallel?std::min(8,table.RowGroupCount()):1,[&](int worker) {
            int workers=parallel?std::min(8,table.RowGroupCount()):1;
            for(int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);auto data=rg.columns[key.columnIdx].get();
                if(!rg.size || rg.size%fanout || data->size!=rg.size || data->type!=ColumnDataBase::RAW_COLUMN_DATA)return false;
                auto raw=static_cast<const RawColumnDataBase *>(data);int64_t start=0;bool exact=false;
                switch(raw->bytesPerValue) {
                case 1:exact=verify<int8_t>(*raw,shift,start,avx);break;case 2:exact=verify<int16_t>(*raw,shift,start,avx);break;
                case 4:exact=verify<int32_t>(*raw,shift,start,avx);break;case 8:exact=verify<int64_t>(*raw,shift,start,avx);break;
                }
                if(!exact)return false;frames[g]={start,uint32_t(g),uint32_t(rg.size)};
            }
            return true;
        });
        for(bool exact:valid)if(!exact)return false;
        std::sort(frames.begin(),frames.end(),[](const auto &a,const auto &b){return a.first<b.first;});
        uint64_t offset=0;uint32_t frameStride=frames.front().size;
        for(size_t i=0;i<frames.size();++i) {
            const auto &frame=frames[i];
            if(Wide(frame.first)!=Wide(minimum)+offset || frame.size>frameStride ||
                (i+1<frames.size() && frame.size!=frameStride))return false;
            directory.push_back(frame.group);offset+=frame.size/fanout;
        }
        if(offset!=keys)return false;
        first=minimum;count=keys;repeat=fanout;stride=frameStride;return true;
    }
    uint32_t fanout() const {return repeat;}
    std::optional<uint64_t> lookup(int64_t key) const {
        uint64_t offset=uint64_t(key)-uint64_t(first);if(offset>=count)return {};
        uint64_t row=offset*repeat;
        return (uint64_t(directory[row/stride])<<32)|uint32_t(row%stride);
    }
    template<class F> void forEachStripe(F visit) const {
        if(!count)return;
        for(size_t i=0;i<directory.size();++i)visit(directory[i],uint64_t(i)*stride/repeat);
    }
};
}
}
