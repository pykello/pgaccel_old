#include "test_table.hpp"
#include <gtest/gtest.h>
#include <map>

using namespace pgaccel;

TEST(GroupedCount, CachedDictionaryIndexesSurvivePayloadGrowth)
{
    std::vector<std::vector<ColumnDataP>> groups;
    std::map<std::string, uint64_t> expected;
    for (int g = 0; g < 8; ++g) {
        std::vector<std::string> values;
        for (int repeat = 0; repeat < 3; ++repeat)
            for (int i = 0; i < 1500; ++i) {
                auto value = "key-" + std::to_string((i * 37 + g) % 2003);
                values.push_back(value);
                ++expected[value];
            }
        groups.push_back({test::Dict<StringType>(values)});
    }
    auto registry = test::Table(std::move(groups), {{"s", std::make_shared<StringType>()}});
    auto query = ParseSelect("SELECT s,count(*) FROM t GROUP BY s ORDER BY s", registry);
    ASSERT_TRUE(query.ok());
    Rows rows;
    for (const auto &[key, count] : expected) rows.push_back({key, std::to_string(count)});
    for (bool parallel : {false, true}) {
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, rows);
    }
}

TEST(GroupedCount, CompactKeysRetainSignedExtremesAndFilters)
{
    auto registry = test::Table(
        {{test::Raw({INT64_MIN, INT64_MAX, -1, 0})},
         {test::Raw({INT64_MAX, INT64_MIN, 0, 1})}},
        {{"v", std::make_shared<Int64Type>()}});
    for (bool parallel : {false, true}) {
        auto query = ParseSelect("SELECT v,count(*) FROM t WHERE v <> 0 GROUP BY v ORDER BY v", registry);
        ASSERT_TRUE(query.ok());
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({{"-9223372036854775808", "2"}, {"-1", "1"},
                                       {"1", "1"}, {"9223372036854775807", "2"}}));
    }
}
