#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;

TEST(DictionaryExpression, OwnedComputedKeysMergeAcrossLocalCodeSpaces)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g = 0; g < 8; ++g)
        groups.push_back({test::Dict<StringType>(g % 2 ? std::vector<std::string>{"bx", "ax", "bx"}
                                                       : std::vector<std::string>{"ax", "ax", "cx"})});
    auto registry = test::Table(std::move(groups), {{"s", std::make_shared<StringType>()}});
    auto query = ParseSelect(
        "SELECT REGEXP_REPLACE(s,'x','-long-owned-suffix') AS k,count(*) FROM t GROUP BY k ORDER BY k",
        registry);
    ASSERT_TRUE(query.ok());
    for (bool parallel : {false, true}) {
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({{"a-long-owned-suffix", "12"},
                                       {"b-long-owned-suffix", "8"},
                                       {"c-long-owned-suffix", "4"}}));
    }
}

TEST(DictionaryExpression, FilteredLabelsDoNotIntroduceExpressionErrors)
{
    auto registry = test::Table(
        {{test::Dict<StringType>({"", "a", "bb", "a"})},
         {test::Dict<StringType>({"bb", "", "a"})}},
        {{"s", std::make_shared<StringType>()}});
    for (bool parallel : {false, true}) {
        auto query = ParseSelect(
            "SELECT 10/length(s) AS k,count(*) FROM t WHERE s <> '' GROUP BY k ORDER BY k", registry);
        ASSERT_TRUE(query.ok());
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({{"5.000000", "2"}, {"10.000000", "3"}}));
    }
    auto unsafe = ParseSelect("SELECT 10/length(s) AS k,count(*) FROM t GROUP BY k", registry);
    ASSERT_TRUE(unsafe.ok());
    EXPECT_FALSE(ExecuteQuery(*unsafe, true, true).ok());
}
