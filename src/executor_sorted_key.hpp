#pragma once
#include "executor_state.hpp"
#include "executor_tasks.hpp"
namespace pgaccel {
namespace {
// A query-local directory over verified disjoint, strictly ordered stripes.
// Gaps in the integer key space require no per-key storage.
class SortedKeyIndex {
    struct Frame {
        int64_t low=0,high=0;
        uint32_t stripe=0,size=0;
        const RawColumnDataBase *data=nullptr;
    };
    Budget &budget;
    std::pmr::vector<Frame> frames;
    uint64_t count=0;
    template<class T> static __m512i load(const T *p) {
        if constexpr (sizeof(T)==1) return _mm512_cvtepi8_epi64(_mm_loadl_epi64(reinterpret_cast<const __m128i *>(p)));
        else if constexpr (sizeof(T)==2) return _mm512_cvtepi16_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(p)));
        else if constexpr (sizeof(T)==4) return _mm512_cvtepi32_epi64(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(p)));
        else return _mm512_loadu_si512(p);
    }
    template<class T> static bool verify(Frame &frame,bool avx) {
        const auto values=reinterpret_cast<const T *>(frame.data->values);int row=0;
        if (avx) for (;row+8<int(frame.size);row+=8)
            if (_mm512_cmplt_epi64_mask(load(values+row),load(values+row+1))!=0xff) return false;
        for (;row+1<int(frame.size);++row) if (values[row]>=values[row+1]) return false;
        frame.low=values[0];frame.high=values[frame.size-1];return true;
    }
    template<class T> static std::optional<uint64_t> find(const Frame &frame,int64_t key) {
        const auto begin=reinterpret_cast<const T *>(frame.data->values),end=begin+frame.size;
        auto it=std::lower_bound(begin,end,key);
        if (it==end || int64_t(*it)!=key) return {};
        return (uint64_t(frame.stripe)<<32)|uint32_t(it-begin);
    }
public:
    explicit SortedKeyIndex(Budget &memory):budget(memory),frames(&memory) {}
    bool prepare(const ColumnarTable &table,const ColumnRef &column,bool avx,bool parallel) {
        count=0;frames.clear();
        if (column.Type()->type_num()!=INT32_TYPE && column.Type()->type_num()!=INT64_TYPE) return false;
        frames.resize(table.RowGroupCount());
        int workers=parallel?std::min(8,std::max(1,table.RowGroupCount())):1;
        Scratch scratch(budget,workers*128);
        auto verified=RunTasks(workers,[&](int worker) {
            for (int g=worker;g<table.RowGroupCount();g+=workers) {
                const auto &rg=table.GetRowGroup(g);if (!rg.size) continue;
                auto data=rg.columns[column.columnIdx].get();if (data->type!=ColumnDataBase::RAW_COLUMN_DATA || data->size!=rg.size) return false;
                auto &frame=frames[g];frame.stripe=g;frame.size=rg.size;frame.data=static_cast<const RawColumnDataBase *>(data);
                bool good=false;
                switch (frame.data->bytesPerValue) {
                case 1:good=verify<int8_t>(frame,avx);break;
                case 2:good=verify<int16_t>(frame,avx);break;
                case 4:good=verify<int32_t>(frame,avx);break;
                case 8:good=verify<int64_t>(frame,avx);break;
                }
                if (!good) return false;
            }
            return true;
        });
        for (bool good:verified) if (!good) return false;
        std::erase_if(frames,[](const auto &frame){return !frame.size;});
        std::sort(frames.begin(),frames.end(),[](const auto &a,const auto &b){return a.low<b.low;});
        for (size_t i=1;i<frames.size();++i) if (frames[i-1].high>=frames[i].low) return false;
        for (const auto &frame:frames) count+=frame.size;
        return count!=0;
    }
    uint64_t size() const {return count;}
    std::optional<uint64_t> lookup(int64_t key) const {
        if (!count) return {};
        auto frame=std::lower_bound(frames.begin(),frames.end(),key,[](const auto &frame,int64_t value){return frame.high<value;});
        if (frame==frames.end() || key<frame->low) return {};
        switch (frame->data->bytesPerValue) {
        case 1:return find<int8_t>(*frame,key);
        case 2:return find<int16_t>(*frame,key);
        case 4:return find<int32_t>(*frame,key);
        case 8:return find<int64_t>(*frame,key);
        default:return {};
        }
    }
};
}
}
