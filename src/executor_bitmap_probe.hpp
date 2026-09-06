#pragma once
#include "executor_arithmetic_program.hpp"

namespace pgaccel {
namespace {
struct BitmapDomain {
    int64_t first,last;const uint64_t *bits;
    bool contains(int64_t key) const {
        if(key<first || key>last)return false;
        uint64_t offset=uint64_t(Wide(key)-first);return bits[offset/64]>>(offset%64)&1;
    }

};
int BitmapSelectRange32(const BitmapDomain &domain,const int32_t *values,int begin,int n,
                      const uint8_t *bitmap,uint32_t *output) {
    auto first=_mm512_set1_epi32(domain.first),last=_mm512_set1_epi32(domain.last);
    const auto lanes=_mm512_setr_epi32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);
    int row=0,count=0;
    for(;row+16<=n;row+=16) {
        uint16_t selected=0xffff;if(bitmap)std::memcpy(&selected,bitmap+(begin+row)/8,2);
        if(!selected)continue;
        auto key=_mm512_loadu_si512(values+begin+row),delta=_mm512_sub_epi32(key,first);
        auto valid=selected & _mm512_cmpge_epi32_mask(key,first) & _mm512_cmple_epi32_mask(key,last);
        if(!valid)continue;
        auto positions=_mm512_srli_epi32(delta,5);
        uint32_t firstWord=uint32_t(_mm_cvtsi128_si32(_mm512_castsi512_si128(positions)));
        __m512i bits;
        if((_mm512_cmpeq_epi32_mask(positions,_mm512_set1_epi32(firstWord)) & valid)==valid)
            bits=_mm512_set1_epi32(reinterpret_cast<const uint32_t *>(domain.bits)[firstWord]);
        else bits=_mm512_mask_i32gather_epi32(_mm512_setzero_si512(),valid,positions,domain.bits,4);
        auto mask=valid & _mm512_test_epi32_mask(_mm512_srlv_epi32(bits,
            _mm512_and_si512(delta,_mm512_set1_epi32(31))),_mm512_set1_epi32(1));
        _mm512_mask_compressstoreu_epi32(output+count,mask,_mm512_add_epi32(lanes,_mm512_set1_epi32(begin+row)));
        count+=__builtin_popcount(mask);
    }
    for(;row<n;++row)if((!bitmap || (bitmap[(begin+row)/8] >> ((begin+row)%8) & 1)) && domain.contains(values[begin+row]))output[count++]=begin+row;
    return count;
}

}
}
