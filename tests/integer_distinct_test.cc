#include "test_table.hpp"
#include "executor_integer_set.hpp"
#include "executor_state.hpp"
#include <gtest/gtest.h>
#include <set>

using namespace pgaccel;

TEST(IntegerDistinct, ExactSignedKeysAcrossWidthsWorkersAndFilters)
{
    for (int width : {1, 2, 4, 8})
    {
        const int64_t low = width == 8 ? INT64_MIN : -(int64_t(1) << (8 * width - 1));
        const int64_t high = width == 8 ? INT64_MAX : (int64_t(1) << (8 * width - 1)) - 1;
        const std::vector<int64_t> keys{low, -1, 0, 1, high};
        std::vector<std::vector<ColumnDataP>> groups;
        for (int group = 0; group < 8; ++group)
        {
            std::vector<int64_t> values(1031);
            for (size_t row = 0; row < values.size(); ++row)
                values[row] = keys[(row + group) % keys.size()];
            groups.push_back({test::Raw(values, width)});
        }
        auto registry = test::Table(std::move(groups), {{"v", std::make_shared<Int64Type>()}});
        for (const auto &predicate : {std::string(), std::string(" WHERE v >= 0"),
                                      std::string(" WHERE v > " + std::to_string(high))})
        {
            uint64_t expected = predicate.empty() ? 5 : predicate == " WHERE v >= 0" ? 3 : 0;
            auto query = ParseSelect("SELECT count(DISTINCT v) FROM t" + predicate, registry);
            ASSERT_TRUE(query.ok());
            for (bool parallel : {false, true})
            {
                auto result = ExecuteQuery(*query, true, parallel);
                ASSERT_TRUE(result.ok());
                EXPECT_EQ(result->values, Rows({{std::to_string(expected)}}));
            }
        }
    }
}

TEST(IntegerDistinct, ExactDatesAcrossIndependentCodeSpaces)
{
    auto registry = test::Table(
        {{test::Dict<DateType>({0, 2, 2})}, {test::Dict<DateType>({1, 2, 3})}},
        {{"d", std::make_shared<DateType>()}});
    auto query = ParseSelect("SELECT count(DISTINCT d) FROM t WHERE d >= '1970-01-02'", registry);
    ASSERT_TRUE(query.ok());
    for (bool parallel : {false, true})
    {
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({{"3"}}));
    }
}

TEST(IntegerDistinct, GrowthFailurePreservesExactSetAndReleasesBudget)
{
    Budget budget(1024);
    {
        IntegerSet values(budget);
        for (uint64_t value = 1; value <= 48; ++value)
            ASSERT_TRUE(values.insert(value));
        EXPECT_THROW(values.insert(49), std::runtime_error);
        EXPECT_EQ(values.size(), 48);
        std::set<uint64_t> actual;
        values.forEach([&](uint64_t value) { actual.insert(value); });
        EXPECT_EQ(actual.size(), 48);
        EXPECT_EQ(*actual.begin(), 1);
        EXPECT_EQ(*actual.rbegin(), 48);
        EXPECT_TRUE(values.insert(0));
        EXPECT_FALSE(values.insert(0));
        EXPECT_EQ(values.size(), 49);
        EXPECT_EQ(budget.used.load(), 512);
    }
    EXPECT_EQ(budget.used.load(), 0);
}

TEST(IntegerDistinct, RadixPartitionedInputMatchesIndependentSet)
{
    std::vector<std::vector<ColumnDataP>> groups;
    std::set<int64_t> all, positive;
    for (int group = 0; group < 17; ++group)
    {
        std::vector<int64_t> values(RowGroupSize);
        for (int row = 0; row < RowGroupSize; ++row)
        {
            int64_t value = (int64_t(group) * RowGroupSize + row) % 70001 - 35000;
            values[row] = value;
            all.insert(value);
            if (value >= 0) positive.insert(value);
        }
        groups.push_back({test::Raw(values, 4)});
    }
    auto registry = test::Table(std::move(groups), {{"v", std::make_shared<Int64Type>()}});
    for (const auto &[predicate, expected] : std::vector<std::pair<std::string, size_t>>{
             {"", all.size()}, {" WHERE v >= 0", positive.size()}, {" WHERE v > 35000", 0}})
    {
        auto query = ParseSelect("SELECT count(DISTINCT v) FROM t" + predicate, registry);
        ASSERT_TRUE(query.ok());
        for (bool parallel : {false, true})
        {
            auto result = ExecuteQuery(*query, true, parallel);
            ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, Rows({{std::to_string(expected)}}));
        }
    }
}

TEST(IntegerDistinct, InlineSingletonNeedsNoBackingAllocation)
{
    Budget budget(4096);
    InlineIntegerSet values(budget);
    values.insert(UINT64_MAX);
    values.insert(UINT64_MAX);
    EXPECT_EQ(values.size(), 1);
    EXPECT_EQ(budget.used.load(), 0);
    values.insert(0);
    values.insert(uint64_t(INT64_MIN));
    values.insert(0);
    std::set<uint64_t> actual;
    values.forEach([&](uint64_t value) { actual.insert(value); });
    EXPECT_EQ(actual, std::set<uint64_t>({0, UINT64_MAX, uint64_t(INT64_MIN)}));
    EXPECT_EQ(values.size(), actual.size());
}
