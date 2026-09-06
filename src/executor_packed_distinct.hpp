#pragma once
#include "executor_arithmetic_program.hpp"
#include "executor_state.hpp"
#include "executor_buffer.hpp"
#include "executor_tasks.hpp"
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
        for(size_t p=worker;p<partitions;p+=workers) {
            auto begin=ordered.data()+offsets[p],end=ordered.data()+offsets[p+1];std::sort(begin,end);
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
