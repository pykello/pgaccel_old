#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(DenseArithmetic, DecimalExpressionsAcrossStripesAndSelections) {
    auto decimal=std::make_shared<DecimalType>(); decimal->scale=2;
    std::vector<std::vector<ColumnDataP>> groups;
    for(int width:{1,2,4,8}) {
        std::vector<std::string> a,b; std::vector<int64_t> x,y;
        for(int i=0;i<269;++i) {a.push_back(i%2?"z":"a");b.push_back(i%3?"q":"p");x.push_back(i%17-8);y.push_back(i%5);}
        groups.push_back({test::Dict<StringType>(a),test::Dict<StringType>(b),test::Raw(x,width),test::Raw(y,width)});
    }
    auto registry=test::Table(std::move(groups),{{"a",std::make_shared<StringType>()},
        {"b",std::make_shared<StringType>()},{"x",decimal},{"y",decimal}});
    for(const std::string where:{"", " WHERE x<0.00", " WHERE x>99.00"}) {
        auto q=ParseSelect("SELECT a,b,sum(x),avg(x),sum(x*(1-y)),sum(x*(1-y)*(1+y)),count(*) FROM t"+where+" GROUP BY a,b ORDER BY a,b",registry);
        ASSERT_TRUE(q.ok())<<q.status().Message();
        // A mathematically neutral division keeps the independent interpreter.
        auto reference=ParseSelect("SELECT a,b,sum(x),avg(x),sum((x*(1-y))/1),sum((x*(1-y)*(1+y))/1),count(*) FROM t"+where+" GROUP BY a,b ORDER BY a,b",registry);
        ASSERT_TRUE(reference.ok())<<reference.status().Message();
        auto expected=ExecuteQuery(*reference,false,false); ASSERT_TRUE(expected.ok())<<expected.status().Message();
        for(bool avx:{false,true}) for(bool parallel:{false,true}) {
            auto result=ExecuteQuery(*q,avx,parallel); ASSERT_TRUE(result.ok())<<result.status().Message();
            ASSERT_EQ(result->values.size(),expected->values.size());
            for(size_t r=0;r<result->values.size();++r) for(size_t c=0;c<7;++c) {
                if(c==4 || c==5) EXPECT_EQ(std::stold(result->values[r][c]),std::stold(expected->values[r][c]));
                else EXPECT_EQ(result->values[r][c],expected->values[r][c]);
            }
        }
    }
}
TEST(DenseArithmetic, WideFallbackAndBudget) {
    auto registry=test::Table({{test::Dict<StringType>({"a","a"}),test::Raw({INT64_MAX,INT64_MAX})}},
        {{"k",std::make_shared<StringType>()},{"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT k,sum(v),sum(v*v),count(*) FROM t GROUP BY k",registry);
    ASSERT_TRUE(q.ok()); auto result=ExecuteQuery(*q,true,true);
    ASSERT_TRUE(result.ok())<<result.status().Message();
    EXPECT_EQ(result->values,(Rows{{"a","18446744073709551614","170141183460469231694793815568465002498","2"}}));
    QueryDesc limited=*q; limited.memoryBudgetBytes=1; limited.allowSpill=false;
    EXPECT_FALSE(ExecuteQuery(limited,true,true).ok());
}
