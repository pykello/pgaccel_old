#include "test_table.hpp"
#include <gtest/gtest.h>
#include <map>
#include <set>

using namespace pgaccel;

TEST(GroupedDistinct, InlinePayloadsAndCachedIndexesPreserveExactUnions)
{
    std::vector<std::vector<ColumnDataP>> groups;
    std::map<std::string, std::set<int64_t>> expected;
    for (int g = 0; g < 8; ++g) {
        std::vector<std::string> strings;
        std::vector<int64_t> values;
        for (int repeat = 0; repeat < 3; ++repeat)
            for (int i = 0; i < 5000; ++i) {
                auto key = "key-" + std::to_string((i * 37 + g) % 5003);
                int64_t value = i % 5 == 0 ? INT64_MIN : i % 5 == 1 ? INT64_MAX : g - repeat;
                strings.push_back(key);
                values.push_back(value);
                expected[key].insert(value);
            }
        groups.push_back({test::Dict<StringType>(strings), test::Raw(values)});
    }
    auto registry = test::Table(std::move(groups),
        {{"s", std::make_shared<StringType>()}, {"v", std::make_shared<Int64Type>()}});
    auto query = ParseSelect("SELECT s,count(DISTINCT v) FROM t GROUP BY s ORDER BY s", registry);
    ASSERT_TRUE(query.ok());
    Rows rows;
    for (const auto &[key, values] : expected) rows.push_back({key, std::to_string(values.size())});
    for (bool parallel : {false, true}) {
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, rows);
    }
}
