#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteFilteredSum(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteModern(const QueryDesc &,bool,bool);
}

TEST(FilteredSum, DecimalArithmeticSparseDenseAndTailSelections)
{
    auto decimal=std::make_shared<DecimalType>(); decimal->scale=2;
    std::vector<std::vector<ColumnDataP>> groups;
    for (int width:{1,2,4,8}) {
        std::vector<int64_t> x,y,k;
        for (int i=0;i<269;++i) { x.push_back(i%19-9); y.push_back(i%7-3); k.push_back(i%3); }
        groups.push_back({test::Raw(x,width),test::Raw(y,width),test::Raw(k,width)});
    }
    auto registry=test::Table(std::move(groups),{{"x",decimal},{"y",decimal},{"k",std::make_shared<Int64Type>()}});
    for (const std::string expression:{"x*y","x*(1-y)","x+y","x-y","x"})
        for (const std::string filter:{"k>=0","k=1","k<0"}) {
            auto q=ParseSelect("SELECT sum("+expression+") FROM t WHERE "+filter,registry); ASSERT_TRUE(q.ok());
            auto expected=ExecuteModern(*q,false,false); ASSERT_TRUE(expected.ok());
            for (bool avx:{false,true}) for (bool parallel:{false,true}) {
                auto actual=ExecuteFilteredSum(*q,avx,parallel); ASSERT_TRUE(actual);
                ASSERT_TRUE(actual->ok()) << actual->status().Message();
                EXPECT_EQ((*actual)->values,expected->values);
                auto dispatched=ExecuteQuery(*q,avx,parallel); ASSERT_TRUE(dispatched.ok());
                EXPECT_EQ(dispatched->values,expected->values);
            }
        }
}

TEST(FilteredSum, WideFallbackEmptyOutputAndBudget)
{
    auto registry=test::Table({{test::Raw({INT64_MAX,INT64_MAX,-INT64_MAX}),test::Raw({1,1,1})}},
        {{"v",std::make_shared<Int64Type>()},{"k",std::make_shared<Int64Type>()}});
    for (const std::string expression:{"v","v*2","v/1"}) {
        auto q=ParseSelect("SELECT sum("+expression+") FROM t WHERE k=1",registry); ASSERT_TRUE(q.ok());
        EXPECT_FALSE(ExecuteFilteredSum(*q,true,true));
        auto actual=ExecuteQuery(*q,true,true), expected=ExecuteModern(*q,false,false);
        ASSERT_TRUE(actual.ok()); ASSERT_TRUE(expected.ok()); EXPECT_EQ(actual->values,expected->values);
    }
    auto q=ParseSelect("SELECT sum(k*2) FROM t WHERE k=1 LIMIT 0",registry); ASSERT_TRUE(q.ok());
    auto actual=ExecuteQuery(*q,true,true); ASSERT_TRUE(actual.ok()); EXPECT_TRUE(actual->values.empty());
    auto limited=*q; limited.memoryBudgetBytes=1; limited.allowSpill=false;
    auto failure=ExecuteFilteredSum(limited,true,true); ASSERT_TRUE(failure); EXPECT_FALSE(failure->ok());
}
