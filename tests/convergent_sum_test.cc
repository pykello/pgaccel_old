#include "convergent_fixture.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteConvergentSum(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;

TEST(ConvergentSum, ExactIdentityBeforeDuplicateLabelsAndFilteredSums) {
    auto tables=test::ConvergentTables();
    for(const std::string filter:{"", " AND o.keep=1", " AND t.x>=0", " AND c.k<0", " AND n.k=0"})
    for(const std::string suffix:{"", " LIMIT 1 OFFSET 1"}) {
        auto q=ParseSelect(test::ConvergentSQL(filter)+suffix,tables);ASSERT_TRUE(q.ok())<<q.status().Message();
        auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
        for(bool avx:{false,true})for(bool parallel:{false,true}) {
            auto actual=ExecuteConvergentSum(*q,avx,parallel);ASSERT_TRUE(actual.has_value());
            ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
        }
    }
}
TEST(ConvergentSum, DuplicateDimensionKeysRetainMultiplicitiesInFallback) {
    auto tables=test::ConvergentTables(true);auto q=ParseSelect(test::ConvergentSQL(),tables);ASSERT_TRUE(q.ok());
    EXPECT_FALSE(ExecuteConvergentSum(*q,true,true).has_value());
    auto expected=ExecuteRelational(*q,false,false),actual=ExecuteQuery(*q,true,true);
    ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
}
TEST(ConvergentSum, EnforcesTheQueryAllocationLimit) {
    auto tables=test::ConvergentTables();auto parsed=ParseSelect(test::ConvergentSQL(),tables);ASSERT_TRUE(parsed.ok());
    auto q=std::move(parsed).ValueUnsafe();q.memoryBudgetBytes=1024;q.allowSpill=false;
    auto actual=ExecuteConvergentSum(q,true,true);ASSERT_TRUE(actual.has_value());EXPECT_FALSE(actual->ok());
}
