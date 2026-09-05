#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(FilterBounds, FusedNarrowBoundsAndVectorTails) {
    for(int width:{1,2,4,8}) for(int n:{1,2,7,8,15,16,63,64,65,127,128,129}) {
        std::vector<int64_t> values; for(int i=0;i<n;++i) values.push_back(i%2?20:1);
        auto registry=test::Table({{test::Raw(values,width)}},{{"v",std::make_shared<Int64Type>()}});
        for(bool avx:{false,true}) for(auto bounds:std::vector<std::pair<std::string,int>>{
            {"v > 0 AND v < 10",(n+1)/2},{"v > -100000 AND v < 10",(n+1)/2},
            {"v >= 1 AND v <= 20",n},{"v > 20",0},{"v < 1",0},
            {"v != 100000",n},{"v >= 20 AND v < 20",0},
            {"v >= 1 AND v < 100000",n}}) {
            auto q=ParseSelect("SELECT count(*) FROM t WHERE "+bounds.first+";",registry); ASSERT_TRUE(q.ok());
            auto out=ExecuteQuery(*q,avx,false); ASSERT_TRUE(out.ok());
            EXPECT_EQ(out->values,Rows({{std::to_string(bounds.second)}}))<<bounds.first<<" width="<<width<<" n="<<n<<" avx="<<avx;
        }
    }
}
TEST(FilterBounds, MissingDictionaryValuesAndSentinels) {
    auto registry=test::Table({{test::Dict<StringType>({"b","d","f"})}},{{"s",std::make_shared<StringType>()}});
    for(bool avx:{false,true}) for(auto bounds:std::vector<std::pair<std::string,int>>{
        {"s != 'a'",3},{"s != 'e'",3},{"s = 'e'",0},{"s < 'c'",1},{"s <= 'c'",1},
        {"s > 'c'",2},{"s >= 'c'",2},{"s > 'a' AND s < 'e'",2},{"s >= 'z'",0}}) {
        auto q=ParseSelect("SELECT count(*) FROM t WHERE "+bounds.first+";",registry); ASSERT_TRUE(q.ok());
        auto out=ExecuteQuery(*q,avx,false); ASSERT_TRUE(out.ok()); EXPECT_EQ(out->values,Rows({{std::to_string(bounds.second)}}))<<bounds.first;
    }
}
