#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;

TEST(CompositeNumeric, InlineAritiesAndDynamicFallbackAcrossWorkers)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int group = 0; group < 8; ++group)
        groups.push_back({test::Raw({1, 1, 2}), test::Raw({3, 3, 4}), test::Raw({5, -2, 9})});
    auto registry = test::Table(std::move(groups),
        {{"a", std::make_shared<Int64Type>()}, {"b", std::make_shared<Int64Type>()},
         {"v", std::make_shared<Int64Type>()}});
    for (int aggregates = 1; aggregates <= 5; ++aggregates)
    {
        std::string sql = "SELECT a,b";
        Row first{"1", "3"}, second{"2", "4"};
        for (int i = 0; i < aggregates; ++i)
        {
            sql += ",sum(v)";
            first.push_back("24");
            second.push_back("72");
        }
        sql += " FROM t GROUP BY a,b ORDER BY a,b";
        auto query = ParseSelect(sql, registry);
        ASSERT_TRUE(query.ok());
        for (bool parallel : {false, true})
        {
            auto result = ExecuteQuery(*query, true, parallel);
            ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, Rows({first, second}));
        }
    }
}

TEST(CompositeNumeric, DependentAffineKeysKeepWideResultsAndUnsafeErrors)
{
    auto registry = test::Table(
        {{test::Raw({INT64_MIN, INT64_MAX, INT64_MIN})},
         {test::Raw({INT64_MAX, 7, 7})}},
        {{"v", std::make_shared<Int64Type>()}});
    auto query = ParseSelect(
        "SELECT v,v+1,v-1,count(*) FROM t GROUP BY v,v+1,v-1 ORDER BY v", registry);
    ASSERT_TRUE(query.ok());
    for (bool parallel : {false, true})
    {
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({
            {"-9223372036854775808", "-9223372036854775807", "-9223372036854775809", "2"},
            {"7", "8", "6", "2"},
            {"9223372036854775807", "9223372036854775808", "9223372036854775806", "2"}}));
    }
    auto unsafe = ParseSelect(
        "SELECT v,count(*) FROM t GROUP BY v,v*9223372036854775807*9223372036854775807", registry);
    ASSERT_TRUE(unsafe.ok());
    auto result = ExecuteQuery(*unsafe, true, true);
    ASSERT_FALSE(result.ok());
    EXPECT_NE(result.status().Message().find("overflow"), std::string::npos);
}
