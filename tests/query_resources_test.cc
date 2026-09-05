#include "test_table.hpp"
#include "query_resources.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;

TEST(QueryResources, RaisedStatementBudgetReachesNestedSetsScalarsAndCtes) {
    auto registry = test::Table({{test::Raw({1, 1, 2, 3})}}, {{"k", std::make_shared<Int64Type>()}});
    auto parsed = ParseSelect("WITH counts AS (SELECT k, count(*) AS n FROM t GROUP BY k) "
        "SELECT count(*) FROM t WHERE k IN "
        "(SELECT k FROM counts WHERE n > (SELECT min(n) FROM counts))", registry);
    ASSERT_TRUE(parsed.ok()) << parsed.status().Message();
    auto query = std::move(parsed).ValueUnsafe();
    // Model a statement limit raised while nested plans retain an older cap.
    query.memoryBudgetBytes = 16 << 20; query.allowSpill = false;
    auto &membership = *query.membershipQueries.at(0).query;
    membership.memoryBudgetBytes = 1;
    membership.materializedSources.at(0).query->memoryBudgetBytes = 1;
    membership.scalarQueries.at(0).query->memoryBudgetBytes = 1;
    EXPECT_FALSE(ExecuteQuery(query, true, false).ok());

    SetQueryResourceLimits(query, 16 << 20, 0);
    auto result = ExecuteQuery(query, true, false);
    ASSERT_TRUE(result.ok()) << result.status().Message();
    EXPECT_EQ(result->values, (Rows{{"2"}}));
    EXPECT_EQ(result->spillBytes, 0);
    EXPECT_LE(result->peakMemoryBytes, query.memoryBudgetBytes);

    // A larger configured child cap cannot bypass remaining parent memory.
    query.memoryBudgetBytes = 512;
    EXPECT_FALSE(ExecuteQuery(query, true, false).ok());
}
