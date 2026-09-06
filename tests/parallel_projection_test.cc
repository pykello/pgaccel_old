#include "test_table.hpp"
#include <gtest/gtest.h>
#include <cmath>

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

TEST(ParallelProjection, NumericMembershipPreservesScaleDuplicatesAndSlicing)
{
    auto decimal = std::make_shared<DecimalType>(); decimal->scale = 2;
    auto registry = test::Table({{test::Raw({125,-250,125}),test::Raw({3,1,2})},
                                {test::Raw({900,125}),test::Raw({4,5})}},
        {{"v",decimal},{"k",std::make_shared<Int64Type>()}});
    for (const std::string sql : {
        "SELECT v FROM t ORDER BY k LIMIT 3 OFFSET 1",
        "SELECT v FROM t GROUP BY v ORDER BY v",
        "SELECT v FROM t WHERE k < 0",
        "SELECT v FROM t ORDER BY k LIMIT 0"}) {
        auto query = ParseSelect(sql,registry); ASSERT_TRUE(query.ok());
        auto normal = ExecuteQuery(*query,true,true); ASSERT_TRUE(normal.ok());
        auto numericQuery = *query; numericQuery.numericMembershipOutput = true;
        for (bool parallel : {false,true}) {
            auto typed = ExecuteQuery(numericQuery,true,parallel); ASSERT_TRUE(typed.ok());
            ASSERT_TRUE(typed->numericMembership); EXPECT_TRUE(typed->values.empty());
            ASSERT_EQ(typed->numericMembership->size(),normal->values.size());
            for (size_t i=0;i<normal->values.size();++i)
                EXPECT_EQ(int64_t((*typed->numericMembership)[i]),
                          std::llround(std::stod(normal->values[i][0])*100));
        }
    }
}
