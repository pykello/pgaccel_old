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
TEST(FilterBounds, RandomizedAgainstIndependentScalarPredicates) {
    uint64_t seed=0x26b55;
    auto next=[&]() { seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;return seed; };
    auto compare=[](int64_t x,int64_t v,int op) { switch(op) {
        case 0:return x==v;case 1:return x!=v;case 2:return x>v;
        case 3:return x>=v;case 4:return x<v;default:return x<=v;
    }};
    const char *ops[]={"=","!=",">",">=","<","<="};
    for(int width:{1,2,4,8}) {
        std::vector<int64_t> values(1031);for(auto &v:values)v=int(next()%201)-100;
        auto registry=test::Table({{test::Raw(values,width)}},{{"v",std::make_shared<Int64Type>()}});
        for(int trial=0;trial<150;++trial) {
            int a=next()%6,b=next()%6;int64_t lo=int(next()%2001)-1000,hi=int(next()%2001)-1000;
            uint64_t expected=0;for(auto v:values)expected+=compare(v,lo,a)&&compare(v,hi,b);
            auto sql="SELECT count(*) FROM t WHERE v "+std::string(ops[a])+" "+std::to_string(lo)+" AND v "+ops[b]+" "+std::to_string(hi);
            auto q=ParseSelect(sql,registry);ASSERT_TRUE(q.ok());
            for(bool avx:{false,true}) {auto r=ExecuteQuery(*q,avx,false);ASSERT_TRUE(r.ok());EXPECT_EQ(r->values,Rows({{std::to_string(expected)}}))<<sql;}
        }
    }
}
