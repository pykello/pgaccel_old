#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <map>
#include <numeric>

using namespace pgaccel;
TEST(IndexedGroupedCount, PartitionedIdsPreserveCountsFiltersAndPagination)
{
    std::vector<std::vector<ColumnDataP>> groups;
    std::map<int64_t, uint64_t> expected;
    for (int g = 0; g < 8; ++g) {
        std::vector<int64_t> numbers;
        std::vector<std::string> strings;
        for (int repeat = 0; repeat < 3; ++repeat)
            for (int i = 0; i < 5000; ++i) {
                int64_t value = (i * 37 + g) % 5003 - 2501;
                numbers.push_back(value);
                strings.push_back(std::to_string(value));
                ++expected[value];
            }
        groups.push_back({test::Raw(numbers), test::Dict<StringType>(strings)});
    }
    auto registry = test::Table(std::move(groups),
        {{"v", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()}});
    std::vector<std::string> queries{
        "SELECT v,count(*) FROM t GROUP BY v ORDER BY count(*) DESC,v LIMIT 10 OFFSET 7",
        "SELECT s,count(*) FROM t WHERE v < -2400 GROUP BY s ORDER BY count(*) DESC,s LIMIT 10",
        "SELECT s,count(*) FROM t WHERE v = 2501 GROUP BY s ORDER BY s",
        "SELECT v,count(*) FROM t WHERE v > 999999 GROUP BY v ORDER BY v",
        "SELECT v,count(*) FROM t GROUP BY v HAVING count(*) > 22 ORDER BY v LIMIT 10"};
    std::vector<Rows> baseline;
    for (const auto &sql : queries) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok());
        auto result = ExecuteQuery(*q, true, false); ASSERT_TRUE(result.ok());
        baseline.push_back(result->values);
    }
    auto built = registry.at("t")->BuildDictionaries(1 << 24); ASSERT_TRUE(built.ok());
    for (size_t i = 0; i < queries.size(); ++i) {
        auto q = ParseSelect(queries[i], registry); ASSERT_TRUE(q.ok());
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, baseline[i]);
        }
    }
    auto q = ParseSelect("SELECT v,count(*) FROM t GROUP BY v ORDER BY v", registry); ASSERT_TRUE(q.ok());
    auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok());
    Rows rows;
    for (const auto &[value, count] : expected) rows.push_back({std::to_string(value), std::to_string(count)});
    EXPECT_EQ(result->values, rows);
}

TEST(IndexedGroupedCount, SparseSelectionAvoidsUnrelatedIdentities)
{
    std::vector<int64_t> values(40000);
    std::iota(values.begin(), values.end(), 0);
    auto registry = test::Table({{test::Raw(values)}}, {{"v", std::make_shared<Int64Type>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 22).ok());
    auto query = ParseSelect("SELECT v,count(*) FROM t WHERE v = 39999 GROUP BY v", registry);
    ASSERT_TRUE(query.ok());
    auto result = ExecuteQuery(*query, true, true);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->values, Rows({{"39999", "1"}}));
}

TEST(IndexedGroupedCount, DatesRetainLogicalValuesAcrossCodeSpaces)
{
    auto registry = test::Table(
        {{test::Dict<DateType>({-1, 2, -1})}, {test::Dict<DateType>({2, 7, -1})}},
        {{"d", std::make_shared<DateType>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 20).ok());
    auto query = ParseSelect("SELECT d,count(*) FROM t GROUP BY d ORDER BY d", registry);
    ASSERT_TRUE(query.ok());
    for (bool parallel : {false, true}) {
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({{"1969-12-31", "3"}, {"1970-01-03", "2"}, {"1970-01-08", "1"}}));
    }
}
