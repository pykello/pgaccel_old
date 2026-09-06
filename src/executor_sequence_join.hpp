#pragma once
#include "executor_value.hpp"

namespace pgaccel {
namespace {
// A verified consecutive physical key column is its own row directory. The
// proof runs inside the query; min/max statistics alone are insufficient.
class SequenceJoinIndex {
    int64_t first=0;
    uint64_t count=0;
    uint32_t stride=0;
    std::pmr::vector<uint32_t> directory;
    template<class T>
    static bool verify(const RawColumnDataBase &data,int64_t start,bool avx) {
        auto values=reinterpret_cast<const T *>(data.values);
        int row=0;
        if(avx && data.size>=8) {
            auto expected=_mm512_setr_epi64(start,start+1,start+2,start+3,
                                           start+4,start+5,start+6,start+7);
            for(;row+8<=data.size;row+=8) {
                __m512i actual;
                if constexpr(sizeof(T)==1)
                    actual=_mm512_cvtepi8_epi64(_mm_loadl_epi64(reinterpret_cast<const __m128i *>(values+row)));
                else if constexpr(sizeof(T)==2)
                    actual=_mm512_cvtepi16_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(values+row)));
                else if constexpr(sizeof(T)==4)
                    actual=_mm512_cvtepi32_epi64(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(values+row)));
                else actual=_mm512_loadu_si512(values+row);
                if(_mm512_cmpeq_epi64_mask(actual,expected)!=0xff) return false;
                expected=_mm512_add_epi64(expected,_mm512_set1_epi64(8));
            }
        }
        for(;row<data.size;++row) if(Wide(values[row])!=Wide(start)+row) return false;
        return true;
    }
public:
    explicit SequenceJoinIndex(std::pmr::memory_resource *memory=std::pmr::get_default_resource()):directory(memory) {}
    bool prepare(const ColumnarTable &table,const ColumnRef &column,uint64_t selected,bool avx) {
        count=0; directory.clear();
        if(!selected || !table.RowGroupCount()) return false;
        auto type=column.Type()->type_num();
        if(type!=INT32_TYPE && type!=INT64_TYPE && type!=DECIMAL_TYPE) return false;
        uint64_t total=0;
        for(int g=0;g<table.RowGroupCount();++g) total+=table.GetRowGroup(g).size;
        if(total!=selected) return false;
        struct Frame {int64_t start;uint32_t group,size;};
        std::pmr::vector<Frame> frames(directory.get_allocator().resource());
        frames.reserve(table.RowGroupCount());
        for(int g=0;g<table.RowGroupCount();++g) {
            const auto &rg=table.GetRowGroup(g);
            if(!rg.size || rg.columns[column.columnIdx]->type!=ColumnDataBase::RAW_COLUMN_DATA) return false;
            auto data=static_cast<const RawColumnDataBase *>(rg.columns[column.columnIdx].get());
            int64_t start=int64_t(Read(rg,column,0).number);
            if(Wide(start)+rg.size-1>INT64_MAX) return false;
            bool valid=false;
            switch(data->bytesPerValue) {
            case 1:valid=verify<int8_t>(*data,start,avx);break;
            case 2:valid=verify<int16_t>(*data,start,avx);break;
            case 4:valid=verify<int32_t>(*data,start,avx);break;
            case 8:valid=verify<int64_t>(*data,start,avx);break;
            }
            if(!valid) return false;
            frames.push_back({start,uint32_t(g),uint32_t(rg.size)});
        }
        // Native stripes may be published in completion order. Verify ranges
        // in key order and retain only a small logical-to-physical directory.
        std::sort(frames.begin(),frames.end(),[](const Frame &a,const Frame &b){return a.start<b.start;});
        first=frames.front().start;stride=frames.front().size;
        uint64_t offset=0;
        for(size_t i=0;i<frames.size();++i) {
            const auto &frame=frames[i];
            if(Wide(frame.start)!=Wide(first)+offset || frame.size>stride ||
                (i+1<frames.size() && frame.size!=stride)) return false;
            directory.push_back(frame.group); offset+=frame.size;
        }
        count=total;return true;
    }
    bool active() const {return count!=0;}
    uint64_t size() const {return count;}
    int64_t minimum() const {return first;}
    int64_t maximum() const {return int64_t(Wide(first)+count-1);}
    template<class F> void forEachStripe(F visit) const {
        for(size_t i=0;i<directory.size();++i) visit(directory[i],uint64_t(i)*stride);
    }
    std::optional<uint64_t> lookup(int64_t key) const {
        Wide offset=Wide(key)-first;
        if(offset<0 || offset>=count) return {};
        uint64_t row=uint64_t(offset);
        return (uint64_t(directory[row/stride])<<32)|uint32_t(row%stride);
    }
};
}
}
