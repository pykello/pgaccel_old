#pragma once
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory_resource>
#include <stdexcept>
#include <vector>
#include <immintrin.h>

namespace pgaccel {
// Zero-initialized, budgeted unsigned payloads. memcpy handles wider values
// without imposing alignment or aliasing requirements on the byte storage.
// Callers must prove row bounds; values are checked against the declared domain.
class CompactUnsignedArray {
    uint32_t maximum;
    size_t count;
    unsigned width;
    std::pmr::vector<uint8_t> bytes;
public:
    CompactUnsignedArray(size_t count,uint32_t maximum,std::pmr::memory_resource &memory)
        : maximum(maximum), count(count), width(maximum<=UINT8_MAX?1:maximum<=UINT16_MAX?2:4),bytes(&memory) {
        unsigned padding=4-width;
        if(count>(std::numeric_limits<size_t>::max()-padding)/width)throw std::length_error("Payload array is too large");
        // A gather reads a complete 32-bit word even for an 8/16-bit payload.
        // The final logical element must have enough initialized tail bytes.
        bytes.resize(count?count*width+padding:0,0);
    }
    size_t size() const {return count;}
    unsigned bytesPerValue() const {return width;}
    uint32_t get(size_t row) const {
        if(width==1)return bytes[row];
        if(width==2){uint16_t value;std::memcpy(&value,bytes.data()+row*2,2);return value;}
        uint32_t value;std::memcpy(&value,bytes.data()+row*4,4);return value;
    }
    // Active indices must be nonnegative and below size(). Masked lanes do
    // not access memory, including an empty array with an all-zero mask.
    __m512i gather(__m512i indices,__mmask16 valid) const {
        auto zero=_mm512_setzero_si512();
        if(width==1)return _mm512_and_si512(_mm512_mask_i32gather_epi32(zero,valid,indices,bytes.data(),1),_mm512_set1_epi32(255));
        if(width==2)return _mm512_and_si512(_mm512_mask_i32gather_epi32(zero,valid,indices,bytes.data(),2),_mm512_set1_epi32(65535));
        return _mm512_mask_i32gather_epi32(zero,valid,indices,bytes.data(),4);
    }
    void set(size_t row,uint32_t value) {
        if(value>maximum)throw std::out_of_range("Payload exceeds its proved domain");
        if(width==1)bytes[row]=uint8_t(value);
        else if(width==2){uint16_t narrow=value;std::memcpy(bytes.data()+row*2,&narrow,2);}
        else std::memcpy(bytes.data()+row*4,&value,4);
    }
};
}
