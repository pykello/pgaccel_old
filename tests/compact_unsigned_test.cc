#include "executor_compact_unsigned.hpp"
#include "executor_state.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;

TEST(CompactUnsigned, WidthBoundariesZeroInitializationAndExactValues) {
    for(uint32_t maximum:{0u,1u,255u,256u,65535u,65536u,UINT32_MAX}) {
        Budget budget(4096);CompactUnsignedArray values(67,maximum,budget);
        EXPECT_EQ(values.size(),67);EXPECT_EQ(values.bytesPerValue(),maximum<=255?1:maximum<=65535?2:4);
        for(size_t i=0;i<values.size();++i)EXPECT_EQ(values.get(i),0);
        for(size_t i=0;i<values.size();++i)values.set(i,i%2?maximum:maximum/2);
        for(size_t i=0;i<values.size();++i)EXPECT_EQ(values.get(i),i%2?maximum:maximum/2);
        if(maximum<UINT32_MAX)EXPECT_THROW(values.set(0,maximum+1),std::out_of_range);
        EXPECT_EQ(budget.peak.load(),67*values.bytesPerValue()+4-values.bytesPerValue());
    }
}
TEST(CompactUnsigned, EmptyAndBudgetedAllocations) {
    Budget budget(1);CompactUnsignedArray empty(0,UINT32_MAX,budget);EXPECT_EQ(empty.size(),0);
    EXPECT_THROW(CompactUnsignedArray(2,255,budget),std::runtime_error);
    EXPECT_THROW(CompactUnsignedArray(SIZE_MAX,UINT32_MAX,budget),std::length_error);
}
