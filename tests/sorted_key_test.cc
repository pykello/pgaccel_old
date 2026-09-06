#include "test_table.hpp"
#include "../src/executor_sorted_key.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(SortedKey, GapsSignedWidthsAndPhysicalStripeOrder) {
    for (int width:{1,2,4,8}) {
        std::vector<int64_t> low,high;
        for (int i=0;i<32;++i) {low.push_back(-120+i*3);high.push_back(i*3);}
        auto registry=test::Table({{test::Raw(high,width)},{test::Raw(low,width)}},{{"k",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());auto column=*q->aggregateClauses[0].columnRef;
        for (bool avx:{false,true}) for (bool parallel:{false,true}) {
            Budget budget(65536);SortedKeyIndex index(budget);ASSERT_TRUE(index.prepare(*q->tables[0],column,avx,parallel));
            EXPECT_EQ(index.size(),64);
            for (int64_t key=-125;key<=100;++key) {
                auto row=index.lookup(key);auto a=std::find(low.begin(),low.end(),key),b=std::find(high.begin(),high.end(),key);
                if (a!=low.end()) {ASSERT_TRUE(row);EXPECT_EQ(*row,(uint64_t(1)<<32)|uint32_t(a-low.begin()));}
                else if (b!=high.end()) {ASSERT_TRUE(row);EXPECT_EQ(*row,uint64_t(b-high.begin()));}
                else EXPECT_FALSE(row);
            }
        }
    }
}
TEST(SortedKey, SignedExtremesAndInvalidUniquenessProofs) {
    for (const auto &keys:std::vector<std::vector<int64_t>>{{INT64_MIN,INT64_MIN+2,INT64_MAX-2,INT64_MAX}}) {
        auto registry=test::Table({{test::Raw(keys)}},{{"k",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());auto column=*q->aggregateClauses[0].columnRef;
        Budget budget(65536);SortedKeyIndex index(budget);ASSERT_TRUE(index.prepare(*q->tables[0],column,true,true));
        for (size_t i=0;i<keys.size();++i) {ASSERT_TRUE(index.lookup(keys[i]));EXPECT_EQ(*index.lookup(keys[i]),i);}
        EXPECT_FALSE(index.lookup(0));EXPECT_FALSE(index.lookup(INT64_MAX-1));
        Budget tiny(1);SortedKeyIndex limited(tiny);EXPECT_THROW(limited.prepare(*q->tables[0],column,true,true),std::runtime_error);
    }
    for (auto groups:std::vector<std::vector<std::vector<ColumnDataP>>>{
        {{test::Raw({1,1,2})}},{{test::Raw({1,3,2})}},
        {{test::Raw({1,3})},{test::Raw({3,4})}},{{test::Raw({1,3})},{test::Raw({2,4})}}}) {
        auto registry=test::Table(std::move(groups),{{"k",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k FROM t",registry);ASSERT_TRUE(q.ok());
        Budget budget(65536);SortedKeyIndex index(budget);
        EXPECT_FALSE(index.prepare(*q->tables[0],*q->aggregateClauses[0].columnRef,true,true));EXPECT_FALSE(index.lookup(1));
    }
}
