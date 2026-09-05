#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;

TEST(ParallelProjection, HiddenOrderingAndOffsetAcrossWorkers)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int group = 0; group < 8; ++group)
        groups.push_back({test::Raw({group, group + 8}),
                          test::Dict<StringType>({"a" + std::to_string(group),
                                                 "b" + std::to_string(group)})});
    auto registry = test::Table(std::move(groups),
        {{"v", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()}});
    auto query = ParseSelect("SELECT s FROM t WHERE v >= 3 ORDER BY v DESC LIMIT 3 OFFSET 2", registry);
    ASSERT_TRUE(query.ok());
    for (bool parallel : {false, true})
    {
        auto result = ExecuteQuery(*query, true, parallel);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({{"b5"}, {"b4"}, {"b3"}}));
        EXPECT_EQ(result->fieldNames.size(), 1);
    }
}

TEST(ParallelProjection, ComputedStringsRemainOwnedAfterWorkerCompletion)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int group = 0; group < 8; ++group)
        groups.push_back({test::Raw({group}),
                          test::Dict<StringType>({"x" + std::to_string(group)})});
    auto registry = test::Table(std::move(groups),
        {{"v", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()}});
    auto query = ParseSelect("SELECT REGEXP_REPLACE(s, 'x', 'long-prefix-') FROM t ORDER BY v LIMIT 2 OFFSET 3", registry);
    ASSERT_TRUE(query.ok());
    auto result = ExecuteQuery(*query, true, true);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->values, Rows({{"long-prefix-3"}, {"long-prefix-4"}}));
}
