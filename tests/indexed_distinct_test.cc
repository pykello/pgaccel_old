#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(IndexedDistinct, FilteredAndUnfilteredValuesMatchIndependentExpectations)
{
    auto registry = test::Table(
        {{test::Raw({INT64_MIN, 0, INT64_MAX}), test::Dict<StringType>({"b", "", "b"}),
          test::Dict<DateType>({-1, 2, -1})},
         {test::Raw({INT64_MAX, 7, INT64_MIN}), test::Dict<StringType>({"a", "b", ""}),
          test::Dict<DateType>({2, 7, -1})}},
        {{"v", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()},
         {"d", std::make_shared<DateType>()}});
    auto built = registry.at("t")->BuildDictionaries(1 << 20);
    ASSERT_TRUE(built.ok());
    for (const auto &[sql, expected] : std::vector<std::pair<std::string, std::string>>{
        {"SELECT count(DISTINCT v) FROM t", "4"},
        {"SELECT count(DISTINCT s) FROM t", "3"},
        {"SELECT count(DISTINCT d) FROM t", "3"},
        {"SELECT count(DISTINCT v) FROM t WHERE s <> ''", "3"},
        {"SELECT count(DISTINCT s) FROM t WHERE v < 0", "2"},
        {"SELECT count(DISTINCT d) FROM t WHERE v < 0", "1"},
        {"SELECT count(DISTINCT v) FROM t WHERE s = 'absent'", "0"}}) {
        auto query = ParseSelect(sql, registry);
        ASSERT_TRUE(query.ok());
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*query, true, parallel);
            ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, Rows({{expected}}));
        }
    }
}
