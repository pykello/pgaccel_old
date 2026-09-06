#pragma once
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory_resource>
#include <stdexcept>
#include <vector>

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
        if(count>std::numeric_limits<size_t>::max()/width)throw std::length_error("Payload array is too large");
        bytes.resize(count*width,0);
    }
    size_t size() const {return count;}
    unsigned bytesPerValue() const {return width;}
    uint32_t get(size_t row) const {
        if(width==1)return bytes[row];
        if(width==2){uint16_t value;std::memcpy(&value,bytes.data()+row*2,2);return value;}
        uint32_t value;std::memcpy(&value,bytes.data()+row*4,4);return value;
    }
    void set(size_t row,uint32_t value) {
        if(value>maximum)throw std::out_of_range("Payload exceeds its proved domain");
        if(width==1)bytes[row]=uint8_t(value);
        else if(width==2){uint16_t narrow=value;std::memcpy(bytes.data()+row*2,&narrow,2);}
        else std::memcpy(bytes.data()+row*4,&value,4);
    }
};
}
