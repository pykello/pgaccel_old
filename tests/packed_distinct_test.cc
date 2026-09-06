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
