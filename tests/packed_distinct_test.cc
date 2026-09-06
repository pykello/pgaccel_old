#include "../src/executor_packed_distinct.hpp"
#include <gtest/gtest.h>
#include <set>
using namespace pgaccel;
TEST(PackedDistinct, DuplicatePairsAcrossWorkersAndPartitions) {
    for(bool parallel:{false,true}) {
        Budget budget(1<<22);std::vector<std::pmr::vector<uint64_t>> inputs;
        std::vector<std::set<uint32_t>> expected(2050);
        for(int worker=0;worker<3;++worker) {
            inputs.emplace_back(&budget);
            for(uint32_t group:{0,1,1024,1025,2049})for(uint32_t value:{0u,1u,7u,UINT32_MAX}) {
                if((group+value+worker)%3==0)continue;
                inputs.back().push_back((uint64_t(group)<<32)|value);expected[group].insert(value);
                inputs.back().push_back((uint64_t(group)<<32)|value);
            }
            std::reverse(inputs.back().begin(),inputs.back().end());
        }
        auto counts=CountPackedDistinct(inputs,expected.size(),budget,parallel);
        for(size_t group=0;group<expected.size();++group)EXPECT_EQ(counts[group],expected[group].size());
    }
}
TEST(PackedDistinct, EmptyInputsInvalidGroupAndAllocationLimits) {
    Budget budget(1<<20);std::vector<std::pmr::vector<uint64_t>> inputs;
    auto counts=CountPackedDistinct(inputs,3,budget,true);EXPECT_EQ(counts,(std::pmr::vector<uint64_t>{0,0,0}));
    inputs.emplace_back(&budget);inputs.back().push_back(uint64_t(3)<<32);
    EXPECT_THROW(CountPackedDistinct(inputs,3,budget,true),NotDense);
    inputs.clear();Budget tiny(1);EXPECT_THROW(CountPackedDistinct(inputs,3,tiny,true),std::runtime_error);
}

TEST(PackedDistinct, RadixPassesSkipConstantBitsAndPreserveFullIdentity) {
    for(bool parallel:{false,true}) {
        Budget budget(1<<27);std::vector<std::pmr::vector<uint64_t>> inputs;
        std::map<uint32_t,std::set<uint32_t>> expected;
        uint64_t state=1234567;
        for(int worker=0;worker<4;++worker) {
            inputs.emplace_back(&budget);
            for(int i=0;i<10000;++i) {
                state=state*6364136223846793005ULL+1;
                uint32_t group=((state>>40)%37)*1024+3;
                uint32_t value=i%5?uint32_t(state>>32):UINT32_MAX;
                inputs.back().push_back((uint64_t(group)<<32)|value);
                inputs.back().push_back((uint64_t(group)<<32)|value);
                expected[group].insert(value);
            }
            for(int i=0;i<256;++i)inputs.back().push_back(uint64_t(2)<<32);
        }
        auto counts=CountPackedDistinct(inputs,65536,budget,parallel);
        EXPECT_EQ(counts[2],1);
        for(const auto &[group,values]:expected)EXPECT_EQ(counts[group],values.size());
        for(size_t group=0;group<counts.size();++group)if(group!=2 && !expected.contains(group))EXPECT_EQ(counts[group],0);
    }
}
TEST(PackedDistinct, BitmapCandidatesPreserveHashCollisionsAndUniquePairs) {
    Budget budget(1<<25);std::vector<std::pmr::vector<uint64_t>> inputs;
    inputs.emplace_back(&budget);std::map<uint32_t,std::set<uint32_t>> expected;
    for(uint32_t group=0;group<4;++group)for(uint32_t i=0;i<2048;++i) {
        uint32_t value=group==0?0:group==1?i:(i*7919)%977;
        inputs[0].push_back((uint64_t(group)<<32)|value);expected[group].insert(value);
    }
    // Deliberately collide distinct pairs in the prefilter's 8192 buckets.
    for(uint32_t value=0;expected[4].size()<128;++value) {
        uint64_t pair=(uint64_t(4)<<32)|value;
        if((pair*0x9e3779b97f4a7c15ULL)>>51)continue;
        inputs[0].push_back(pair);expected[4].insert(value);
    }
    auto counts=CountPackedDistinct(inputs,8,budget,true);
    for(const auto &[group,values]:expected)EXPECT_EQ(counts[group],values.size());
}
