#include "executor_rank_join.hpp"
#include <gtest/gtest.h>
#include <thread>
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

TEST(RankJoin, ConcurrentBitsDetectDuplicatesAndScatterUniqueRows) {
    Budget budget(1<<20);RankJoinIndex index(0,4095,budget);
    std::atomic<int> accepted{0};std::vector<std::thread> threads;
    for(int worker=0;worker<8;++worker)threads.emplace_back([&,worker]{
        for(int key=worker;key<4096;key+=8)accepted+=index.insert<true>(key);
        accepted+=index.insert<true>(0);
    });
    for(auto &thread:threads)thread.join();threads.clear();index.finalize();
    EXPECT_EQ(accepted.load(),4096);EXPECT_EQ(index.size(),4096);
    for(int worker=0;worker<8;++worker)threads.emplace_back([&,worker]{
        for(int key=worker;key<4096;key+=8)index.setRow(key,uint64_t(key)*7);
    });
    for(auto &thread:threads)thread.join();
    for(int key=0;key<4096;++key)EXPECT_EQ(index.row(*index.rank(key)),uint64_t(key)*7);
}

TEST(RankJoin, DirectoryOnlyOmitsRowStorageButKeepsExactRanks) {
    Budget fullBudget(1<<20),directoryBudget(1<<20);
    RankJoinIndex full(-100,1000,fullBudget),directory(-100,1000,directoryBudget);
    for(int key=-100;key<=1000;key+=3){full.insert(key);directory.insert(key);}
    full.finalize();directory.finalize(false);
    EXPECT_EQ(full.size(),directory.size());
    EXPECT_EQ(fullBudget.used.load()-directoryBudget.used.load(),full.size()*sizeof(uint64_t));
    for(int key=-101;key<=1001;++key)EXPECT_EQ(full.rank(key),directory.rank(key));
    EXPECT_THROW(directory.setRow(-100,1),std::logic_error);
    EXPECT_THROW(directory.row(0),std::logic_error);
    full.finalize(false);EXPECT_THROW(full.row(0),std::logic_error);
    directory.finalize();directory.setRow(-100,77);EXPECT_EQ(directory.row(0),77);
}
