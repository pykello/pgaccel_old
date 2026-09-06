#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteSortedSum(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteModern(const QueryDesc &,bool,bool);
}
TEST(SortedSum, ShuffledStripesBoundaryGroupsFiltersAndHaving) {
    for (int width:{1,2,4,8}) {
        auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
        std::vector<int64_t> tail(269);for (int i=0;i<269;++i)tail[i]=i%9-4;
        auto registry=test::Table({{test::Raw({2,3,3,4},width),test::Raw({5,-2,3,7},width)},
            {test::Raw({-1,0,2},width),test::Raw({2,3,4},width)},
            {test::Raw({2,2},width),test::Raw({6,-1},width)},
            {test::Raw(std::vector<int64_t>(269,5),width),test::Raw(tail,width)},
            {test::Raw({5,5,6},width),test::Raw({3,4,9},width)}},
            {{"k",std::make_shared<Int64Type>()},{"v",decimal}});
        for (const std::string filter:{""," WHERE v>=0.00"," WHERE v>99.00"})
            for (const std::string having:{""," HAVING sum(v)>0.07"," HAVING sum(v)<=-0.01"}) {
                std::string sql="SELECT k,sum(v) FROM t"+filter+" GROUP BY k"+having+" ORDER BY sum(v) DESC,k";
                auto q=ParseSelect(sql,registry);ASSERT_TRUE(q.ok())<<q.status().Message();
                auto expected=ExecuteModern(*q,false,false);ASSERT_TRUE(expected.ok());
                for (bool avx:{false,true}) for (bool parallel:{false,true}) {
                    auto fast=ExecuteSortedSum(*q,avx,parallel);ASSERT_TRUE(fast);
                    ASSERT_TRUE(fast->ok())<<fast->status().Message();EXPECT_EQ((*fast)->values,expected->values);
                }
                auto sliced=ParseSelect(sql+" LIMIT 2 OFFSET 1",registry);ASSERT_TRUE(sliced.ok());
                auto actual=ExecuteQuery(*sliced,true,true),reference=ExecuteModern(*sliced,false,false);
                ASSERT_TRUE(actual.ok());ASSERT_TRUE(reference.ok());EXPECT_EQ(actual->values,reference->values);
            }
        auto q=ParseSelect("SELECT k FROM t GROUP BY k HAVING sum(v)>0.07 ORDER BY k",registry);ASSERT_TRUE(q.ok());
        for (auto op:{FilterClause::FILTER_GT,FilterClause::FILTER_LTE}) {
            auto fractional=*q;fractional.having[0].value=op==FilterClause::FILTER_GT?75:-5;
            fractional.having[0].scale=3;fractional.having[0].op=op;
            auto fast=ExecuteSortedSum(fractional,true,true);ASSERT_TRUE(fast);ASSERT_TRUE(fast->ok());
            auto reference=ExecuteModern(fractional,false,false);ASSERT_TRUE(reference.ok());
            EXPECT_EQ((*fast)->values,reference->values);
        }
        auto numeric=*q;numeric.numericMembershipOutput=true;
        auto output=ExecuteQuery(numeric,true,true);ASSERT_TRUE(output.ok());ASSERT_TRUE(output->numericMembership);
        auto expected=ExecuteModern(*q,false,false);ASSERT_TRUE(expected.ok());
        ASSERT_EQ(output->numericMembership->size(),expected->values.size());
        for (size_t i=0;i<expected->values.size();++i) EXPECT_EQ(int64_t((*output->numericMembership)[i]),std::stoll(expected->values[i][0]));
    }
}
TEST(SortedSum, UnsortedRangesAndWideStripeSumsUseGeneralExecution) {
    for (auto groups:std::vector<std::vector<std::vector<ColumnDataP>>>{
        {{test::Raw({1,3,2}),test::Raw({1,2,3})}},
        {{test::Raw({1,3}),test::Raw({1,2})},{test::Raw({2,4}),test::Raw({3,4})}},
        {{test::Raw({1,1}),test::Raw({INT64_MAX,INT64_MAX})}}}) {
        auto registry=test::Table(std::move(groups),{{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k,sum(v) FROM t GROUP BY k ORDER BY k",registry);ASSERT_TRUE(q.ok());
        EXPECT_FALSE(ExecuteSortedSum(*q,true,true));
        auto actual=ExecuteQuery(*q,true,true),expected=ExecuteModern(*q,false,false);
        ASSERT_TRUE(actual.ok());ASSERT_TRUE(expected.ok());EXPECT_EQ(actual->values,expected->values);
    }
}
TEST(SortedSum, BoundaryMergeUsesWideSumsAndHonorsBudget) {
    auto registry=test::Table({{test::Raw({INT64_MIN}),test::Raw({INT64_MAX})},
        {test::Raw({INT64_MIN}),test::Raw({INT64_MAX})}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT k,sum(v) FROM t GROUP BY k ORDER BY k",registry);ASSERT_TRUE(q.ok());
    auto fast=ExecuteSortedSum(*q,true,true);ASSERT_TRUE(fast);ASSERT_TRUE(fast->ok());
    EXPECT_EQ((*fast)->values,Rows({{"-9223372036854775808","18446744073709551614"}}));
    auto tiny=*q;tiny.memoryBudgetBytes=1;tiny.allowSpill=false;
    auto failed=ExecuteSortedSum(tiny,true,true);ASSERT_TRUE(failed);EXPECT_FALSE(failed->ok());
}

TEST(SortedSum, RejectsCompletedInteriorGroupsBeforeRetainingTheirState) {
    std::vector<int64_t> keys,values;
    for (int i=0;i<16000;++i) {keys.push_back(i);values.push_back(1);}
    auto registry=test::Table({{test::Raw(keys,4),test::Raw(values,1)}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT k,sum(v) FROM t GROUP BY k HAVING sum(v)>2 ORDER BY k",registry);ASSERT_TRUE(q.ok());
    auto bounded=*q;bounded.memoryBudgetBytes=128*1024;bounded.allowSpill=false;
    auto result=ExecuteSortedSum(bounded,true,false);ASSERT_TRUE(result);ASSERT_TRUE(result->ok())<<result->status().Message();
    EXPECT_TRUE((*result)->values.empty());EXPECT_LT((*result)->peakMemoryBytes,128*1024);
}
