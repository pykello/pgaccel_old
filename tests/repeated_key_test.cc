#include "test_table.hpp"
#include "../src/executor_repeated_key.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(RepeatedKey, ProvedFanoutsSignedEndpointsAndPhysicalStripeOrder) {
    for(int64_t first:{INT64_MIN,INT64_MAX-258})for(int fanout:{1,2,4,8,16}) {
        std::vector<int64_t> a,b;
        for(int i=0;i<259;++i)for(int copy=0;copy<fanout;++copy)(i<160?a:b).push_back(first+i);
        auto registry=test::Table({{test::Raw(b)},{test::Raw(a)}},{{"k",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());auto key=*q->aggregateClauses[0].columnRef;
        for(bool avx:{false,true})for(bool parallel:{false,true}) {
            Budget budget(1<<20);RepeatedKeyIndex index(budget);
            ASSERT_TRUE(index.prepare(*q->tables[0],key,first,259,avx,parallel));EXPECT_EQ(index.fanout(),fanout);
            for(int i=0;i<259;++i) {
                auto row=index.lookup(first+i);ASSERT_TRUE(row);
                EXPECT_EQ(*row,(uint64_t(i<160?1:0)<<32)|uint32_t((i<160?i:i-160)*fanout));
            }
            EXPECT_FALSE(index.lookup(first==INT64_MIN?INT64_MAX:INT64_MIN));
            std::vector<std::pair<uint32_t,uint64_t>> stripes;
            index.forEachStripe([&](uint32_t g,uint64_t offset){stripes.emplace_back(g,offset);});
            EXPECT_EQ(stripes,(std::vector<std::pair<uint32_t,uint64_t>>{{1,0},{0,160}}));
            EXPECT_FALSE(index.prepare(*q->tables[0],key,first,260,avx,parallel));
            EXPECT_FALSE(index.lookup(first));
        }
    }
}
TEST(RepeatedKey, NarrowStorageInvalidRunsAndBudgetLimits) {
    for(int width:{1,2,4,8})for(bool corrupt:{false,true}) {
        std::vector<int64_t> values;for(int key=-8;key<12;++key)for(int i=0;i<4;++i)values.push_back(key);
        if(corrupt)++values[17];
        auto registry=test::Table({{test::Raw(values,width)}},{{"k",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());auto key=*q->aggregateClauses[0].columnRef;
        Budget budget(1<<20);RepeatedKeyIndex index(budget);
        EXPECT_EQ(index.prepare(*q->tables[0],key,-8,20,true,true),!corrupt);
        Budget tiny(1);RepeatedKeyIndex bounded(tiny);EXPECT_THROW(bounded.prepare(*q->tables[0],key,-8,20,true,true),std::runtime_error);
    }
}
