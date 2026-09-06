#pragma once
#include "executor_arithmetic_program.hpp"
#include "executor_state.hpp"
#include "executor_buffer.hpp"
#include "executor_tasks.hpp"
#include <bit>
namespace pgaccel {
namespace {
// Packed values contain an exact group in the upper word and an exact
// distinct identity in the lower word. Input buffers are released on scatter.
std::pmr::vector<uint64_t> CountPackedDistinct(std::vector<std::pmr::vector<uint64_t>> &inputs,
    uint32_t slots,Budget &budget,bool parallel) {
    constexpr size_t partitions=1024;
    if(!slots || slots>(1<<24))throw NotDense{};
    int workers=parallel?std::min<size_t>(8,std::max<size_t>(1,inputs.size())):1;
    std::pmr::vector<size_t> cursors(inputs.size()*partitions,0,&budget),offsets(partitions+1,0,&budget);
    RunTasks(workers,[&](int worker) {
        for(size_t i=worker;i<inputs.size();i+=workers)for(uint64_t key:inputs[i]) {
            uint32_t group=key>>32;if(group>=slots)throw NotDense{};
            ++cursors[i*partitions+(group&(partitions-1))];
        }
        return true;
    });
    for(size_t p=0;p<partitions;++p) {
        offsets[p+1]=offsets[p];
        for(size_t i=0;i<inputs.size();++i) {
            auto &cursor=cursors[i*partitions+p];size_t count=cursor;cursor=offsets[p+1];offsets[p+1]+=count;
        }
    }
    std::pmr::vector<uint64_t> counts(slots,0,&budget);
    if(!offsets.back())return counts;
    OverwriteBuffer<uint64_t> ordered(offsets.back(),budget);
    RunTasks(workers,[&](int worker) {
        for(size_t i=worker;i<inputs.size();i+=workers) {
            for(uint64_t key:inputs[i])ordered[cursors[i*partitions+((key>>32)&(partitions-1))]++]=key;
            std::pmr::vector<uint64_t> empty(&budget);inputs[i].swap(empty);
        }
        return true;
    });
    RunTasks(workers,[&](int worker) {
        size_t maximum=0;
        for(size_t p=worker;p<partitions;p+=workers)maximum=std::max(maximum,offsets[p+1]-offsets[p]);
        OverwriteBuffer<uint64_t> temporary(maximum,budget);
        Scratch stack(budget,2048*sizeof(size_t));
        size_t histogram[2048];
        auto bitmapSize=[](size_t n){return std::bit_ceil(std::min<size_t>(1<<14,std::max<size_t>(128,n))*64);};
        OverwriteBuffer<uint64_t> seenAndRepeated(maximum>=128?bitmapSize(maximum)/32:0,budget);
        for(size_t p=worker;p<partitions;p+=workers) {
            auto begin=ordered.data()+offsets[p],end=ordered.data()+offsets[p+1];
            size_t size=end-begin;auto original=begin;
            if(size>=128) {
                size_t bits=bitmapSize(size),words=bits/64;unsigned shift=64-std::countr_zero(bits);
                auto seen=seenAndRepeated.data(),repeated=seen+words;
                std::fill(seen,repeated+words,0);
                // A bucket hit once proves its pair unique. Every duplicate
                // pair hashes to a repeated bucket and receives exact checking.
                for(auto value=begin;value!=end;++value) {
                    uint64_t bucket=(*value*0x9e3779b97f4a7c15ULL)>>shift,bit=uint64_t(1)<<(bucket%64);
                    repeated[bucket/64]|=seen[bucket/64]&bit;seen[bucket/64]|=bit;
                }
                size_t candidates=0;
                for(auto value=begin;value!=end;++value) {
                    uint64_t bucket=(*value*0x9e3779b97f4a7c15ULL)>>shift;
                    if(repeated[bucket/64]&(uint64_t(1)<<(bucket%64)))temporary[candidates++]=*value;
                    else ++counts[*value>>32];
                }
                begin=temporary.data();end=begin+candidates;size=candidates;if(!size)continue;
            }
            if(size<128)std::sort(begin,end);
            else {
                uint64_t differences=0;for(auto value=begin+1;value!=end;++value)differences|=*value^*begin;
                auto source=begin,destination=begin==temporary.data()?original:temporary.data();
                while(differences) {
                    unsigned shift=std::countr_zero(differences),bits=std::min(11u,64-shift);
                    size_t bins=size_t(1)<<bits,mask=bins-1;
                    std::fill(histogram,histogram+bins,0);
                    for(size_t i=0;i<size;++i)++histogram[(source[i]>>shift)&mask];
                    size_t offset=0;
                    for(size_t i=0;i<bins;++i){size_t count=histogram[i];histogram[i]=offset;offset+=count;}
                    for(size_t i=0;i<size;++i)destination[histogram[(source[i]>>shift)&mask]++]=source[i];
                    std::swap(source,destination);differences&=~(uint64_t(mask)<<shift);
                }
                begin=source;end=source+size;
            }
            bool seen=false;uint64_t previous=0;
            for(auto value=begin;value!=end;++value)if(!seen || *value!=previous) {
                ++counts[*value>>32];previous=*value;seen=true;
            }
        }
        return true;
    });
    return counts;
}
}
}
