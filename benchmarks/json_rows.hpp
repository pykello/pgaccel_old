#pragma once
#include <string>
#include <string_view>
#include <vector>
#include <emmintrin.h>
namespace pgaccel::benchmark {
inline size_t NextJsonEscape(std::string_view value,size_t begin) {
    if(begin>=value.size())return value.size();
    auto escapes=[](__m128i bytes) {
        auto controls=_mm_cmpeq_epi8(_mm_subs_epu8(bytes,_mm_set1_epi8(31)),_mm_setzero_si128());
        return unsigned(_mm_movemask_epi8(_mm_or_si128(controls,_mm_or_si128(
            _mm_cmpeq_epi8(bytes,_mm_set1_epi8('"')),_mm_cmpeq_epi8(bytes,_mm_set1_epi8('\\'))))));
    };
    while(value.size()-begin>=16) {
        unsigned mask=escapes(_mm_loadu_si128(reinterpret_cast<const __m128i *>(value.data()+begin)));
        if(mask)return begin+__builtin_ctz(mask);begin+=16;
    }
    if(value.size()-begin>=8) {
        unsigned mask=escapes(_mm_loadl_epi64(reinterpret_cast<const __m128i *>(value.data()+begin)))&255;
        if(mask)return begin+__builtin_ctz(mask);begin+=8;
    }
    for(;begin<value.size();++begin) {
        unsigned char c=value[begin];if(c<32 || c=='"' || c=='\\')return begin;
    }
    return value.size();
}
inline void AppendJsonString(std::string &output,std::string_view value) {
    constexpr char hex[]="0123456789abcdef";output+='"';size_t begin=0;
    while(begin<value.size()) {
        size_t end=NextJsonEscape(value,begin);output.append(value.substr(begin,end-begin));
        if(end==value.size())break;unsigned char c=value[end];begin=end+1;
        if(c=='"' || c=='\\'){output+='\\';output+=c;}
        else {output+="\\u00";output+=hex[c>>4];output+=hex[c&15];}
    }
    output+='"';
}
inline std::string JsonRows(const std::vector<std::vector<std::string>> &rows) {
    size_t estimate=2+3*rows.size();
    for(const auto &row:rows)for(const auto &value:row)estimate+=value.size()+3;
    std::string output;output.reserve(estimate);output+='[';
    for(size_t r=0;r<rows.size();++r) {
        if(r)output+=',';output+='[';
        for(size_t c=0;c<rows[r].size();++c){if(c)output+=',';AppendJsonString(output,rows[r][c]);}
        output+=']';
    }
    output+=']';return output;
}
}
