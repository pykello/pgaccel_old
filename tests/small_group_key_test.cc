#include "test_table.hpp"
#include "../src/executor_small_group_key.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(SmallGroupKey, ExactCrossStripeDictionaryIdentityAndSignedNumbers) {
    auto registry=test::Table({
        {test::Dict<StringType>({"b","a","b",""}),test::Dict<StringType>({"x","y","x","x"}),test::Raw({-2,0,-2,0},1)},
        {test::Dict<StringType>({"c","b","a",""}),test::Dict<StringType>({"y","x","y","x"}),test::Raw({1,-2,0,1},1)}},
        {{"a",std::make_shared<StringType>()},{"b",std::make_shared<StringType>()},{"n",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT a,b,n FROM t",registry);ASSERT_TRUE(q.ok());std::vector<ColumnRef> columns;
    for(const auto &a:q->aggregateClauses)columns.push_back(*a.columnRef);
    Budget budget(1<<20);SmallGroupKey keys(*q->tables[0],columns,budget);
    std::vector<std::tuple<std::string,std::string,int64_t>> expected{
        {"b","x",-2},{"a","y",0},{"b","x",-2},{"","x",0},
        {"c","y",1},{"b","x",-2},{"a","y",0},{"","x",1}};
    for(int a=0;a<8;++a)for(int b=0;b<8;++b)
        EXPECT_EQ(keys.word(a/4,a%4)==keys.word(b/4,b%4),expected[a]==expected[b]);
    EXPECT_LT(keys.ordinal(keys.word(0,3),0),keys.ordinal(keys.word(0,1),0));
    EXPECT_LT(keys.ordinal(keys.word(0,1),0),keys.ordinal(keys.word(0,0),0));
    EXPECT_LT(keys.ordinal(keys.word(0,0),0),keys.ordinal(keys.word(1,0),0));
    EXPECT_LT(keys.ordinal(keys.word(0,0),2),keys.ordinal(keys.word(1,0),2));
    EXPECT_LE(keys.slots(),64);
    Budget tiny(1);EXPECT_THROW(SmallGroupKey(*q->tables[0],columns,tiny),std::runtime_error);
}
TEST(SmallGroupKey, NumericDictionaryWidthsExtremesAndDomainFallback) {
    for(int64_t first:{INT64_MIN,INT64_MAX-2}) {
        auto registry=test::Table({{test::Raw({first,first+2,first})},
            {test::Raw({first+1,first,first+2})}},{{"n",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT n FROM t",registry);ASSERT_TRUE(q.ok());std::vector<ColumnRef> columns{*q->aggregateClauses[0].columnRef};
        Budget budget(1<<20);SmallGroupKey keys(*q->tables[0],columns,budget);
        EXPECT_EQ(keys.slots(),4);EXPECT_EQ(keys.word(0,0),0);EXPECT_EQ(keys.word(0,1),2);
        EXPECT_EQ(keys.word(1,0),1);EXPECT_EQ(keys.word(1,1),0);
    }
    {
        auto dates=test::Table({{test::Dict<DateType>({-2,0,-2})},{test::Dict<DateType>({-1,-2,0})}},
            {{"d",std::make_shared<DateType>()}});
        auto q=ParseSelect("SELECT d FROM t",dates);ASSERT_TRUE(q.ok());Budget budget(1<<20);
        SmallGroupKey keys(*q->tables[0],{*q->aggregateClauses[0].columnRef},budget);
        EXPECT_EQ(keys.word(0,0),0);EXPECT_EQ(keys.word(0,1),2);EXPECT_EQ(keys.word(1,0),1);
    }
    auto registry=test::Table({{test::Raw({0,1<<24})}},{{"n",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT n FROM t",registry);ASSERT_TRUE(q.ok());Budget budget(1<<20);
    EXPECT_THROW(SmallGroupKey(*q->tables[0],{*q->aggregateClauses[0].columnRef},budget),NotDense);
}
TEST(SmallGroupKey, VerifiedIdenticalDictionariesKeepWideLocalCodesOrdered) {
    std::vector<std::string> labels;for(int i=0;i<259;++i)labels.push_back("label"+std::to_string(i));
    auto reversed=labels;std::reverse(reversed.begin(),reversed.end());
    auto registry=test::Table({{test::Dict<StringType>(labels)},{test::Dict<StringType>(reversed)}},{{"g",std::make_shared<StringType>()}});
    auto q=ParseSelect("SELECT g FROM t",registry);ASSERT_TRUE(q.ok());Budget budget(1<<20);
    SmallGroupKey keys(*q->tables[0],{*q->aggregateClauses[0].columnRef},budget);
    auto sorted=labels;std::sort(sorted.begin(),sorted.end());
    for(size_t i=0;i<labels.size();++i) {
        EXPECT_EQ(keys.word(0,i),keys.word(1,labels.size()-i-1));
        EXPECT_EQ(keys.ordinal(keys.word(0,i),0),std::lower_bound(sorted.begin(),sorted.end(),labels[i])-sorted.begin());
    }
}
