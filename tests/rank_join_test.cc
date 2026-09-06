#include "executor_rank_join.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(RankJoin, SparseNegativeKeysWordBoundariesAndDuplicates) {
    Budget budget(4096);RankJoinIndex index(-65,130,budget);
    std::vector<int64_t> keys{-65,-64,-2,-1,0,1,62,63,64,65,130};
    for(auto i=keys.rbegin();i!=keys.rend();++i) EXPECT_TRUE(index.insert(*i));
    EXPECT_FALSE(index.insert(0));index.finalize();
    for(size_t i=0;i<keys.size();++i) {EXPECT_EQ(index.rank(keys[i]),std::optional<uint32_t>(i));index.setRow(keys[i],100+i);}
    for(size_t i=0;i<keys.size();++i) EXPECT_EQ(index.row(i),100+i);
    for(int64_t key:{-66,-63,2,66,129,131}) EXPECT_FALSE(index.rank(key));
    EXPECT_THROW(index.insert(2),std::logic_error);
    EXPECT_EQ(index.size(),keys.size());EXPECT_GT(budget.peak.load(),0);
}
TEST(RankJoin, EmptySignedBoundaryAndBudget) {
    Budget budget(4096);RankJoinIndex index(INT64_MIN,INT64_MIN+64,budget);
    index.finalize();EXPECT_FALSE(index.rank(INT64_MIN));
    RankJoinIndex last(INT64_MAX-64,INT64_MAX,budget);
    EXPECT_TRUE(last.insert(INT64_MAX));last.finalize();EXPECT_EQ(last.rank(INT64_MAX),std::optional<uint32_t>(0));
    EXPECT_FALSE(last.rank(INT64_MIN));
    Budget tiny(1);EXPECT_THROW((RankJoinIndex(0,64,tiny)),std::runtime_error);
}
