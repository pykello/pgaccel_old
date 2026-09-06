#pragma once
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <immintrin.h>
#include <string_view>
namespace pgaccel {
namespace {
// Original bounded two-byte candidate filter. Masked loads stay within the
// view; every surviving candidate is verified before reporting a match.
size_t FindLiteralBytes(std::string_view text,std::string_view literal,size_t position,bool avx) {
    const size_t length=literal.size();
    if(!avx || length<2 || length>32 || position>text.size() || length>text.size()-position)
        return text.find(literal,position);
    const auto first=_mm512_set1_epi8(literal.front()),last=_mm512_set1_epi8(literal.back());
    const size_t positions=text.size()-length+1;
    for(size_t begin=position;begin<positions;) {
        int n=std::min<size_t>(64,positions-begin);
        uint64_t valid=n==64?UINT64_MAX:(uint64_t(1)<<n)-1;
        auto leading=_mm512_maskz_loadu_epi8(valid,text.data()+begin);
        auto trailing=_mm512_maskz_loadu_epi8(valid,text.data()+begin+length-1);
        uint64_t candidates=valid & _mm512_cmpeq_epi8_mask(leading,first) & _mm512_cmpeq_epi8_mask(trailing,last);
        while(candidates) {
            size_t at=begin+__builtin_ctzll(candidates);
            if(length==2 || std::memcmp(text.data()+at,literal.data(),length)==0)return at;
            candidates&=candidates-1;
        }
        begin+=n;
    }
    return std::string_view::npos;
}
}
}
