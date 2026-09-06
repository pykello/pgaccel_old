#include "executor_compact_unsigned.hpp"
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

namespace {
// Put the allocation immediately before an unreadable page so the test checks
// the declared tail padding rather than incidental allocator spare capacity.
class GuardedResource : public std::pmr::memory_resource {
    void *mapping=nullptr;
    size_t page=sysconf(_SC_PAGESIZE);
    void *do_allocate(size_t bytes,size_t alignment) override {
        if(mapping || bytes>page || alignment!=1)throw std::bad_alloc();
        mapping=mmap(nullptr,page*2,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
        if(mapping==MAP_FAILED){mapping=nullptr;throw std::bad_alloc();}
        if(mprotect(static_cast<char *>(mapping)+page,page,PROT_NONE))throw std::bad_alloc();
        return static_cast<char *>(mapping)+page-bytes;
    }
    void do_deallocate(void *,size_t,size_t) override {}
    bool do_is_equal(const std::pmr::memory_resource &other)const noexcept override{return this==&other;}
public:
    ~GuardedResource(){if(mapping)munmap(mapping,page*2);}
};
}
TEST(CompactGather, MasksInvalidIndicesAndBoundsWordReadsAtTheLastElement) {
    for(uint32_t maximum:{255u,65535u,UINT32_MAX})for(int size:{1,2,17,65,257}) {
        GuardedResource memory;pgaccel::CompactUnsignedArray values(size,maximum,memory);
        for(int i=0;i<size;++i)values.set(i,i%2?maximum:maximum/2);
        alignas(64)int32_t indices[16];alignas(64)uint32_t output[16];uint16_t mask=0;
        for(int i=0;i<16;++i) {
            if(i%3==0)indices[i]=i%2?INT32_MAX:-1;
            else {indices[i]=(i*7)%size;mask|=uint16_t(1)<<i;}
        }
        indices[15]=size-1;mask|=uint16_t(1)<<15;
        _mm512_store_si512(output,values.gather(_mm512_load_si512(indices),mask));
        for(int i=0;i<16;++i)EXPECT_EQ(output[i],mask>>i&1?values.get(indices[i]):0);
    }
    GuardedResource memory;pgaccel::CompactUnsignedArray empty(0,255,memory);
    alignas(64)uint32_t output[16];
    _mm512_store_si512(output,empty.gather(_mm512_set1_epi32(INT32_MAX),0));
    for(auto value:output)EXPECT_EQ(value,0);
}
