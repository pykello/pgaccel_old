#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(AggregateFormula, HiddenStatesRatiosAveragesAndGroupedOrdering) {
    auto registry = test::Table({{test::Raw({1, 1, 2}), test::Raw({10, 20, 60})}},
        {{"k", std::make_shared<Int64Type>()}, {"v", std::make_shared<Int64Type>()}});
    for (const auto &[sql, expected] : std::vector<std::pair<std::string, Rows>>{
        {"SELECT 100.00*sum(CASE WHEN k=1 THEN v ELSE 0 END)/sum(v) FROM t", {{"33.333333"}}},
        {"SELECT k,sum(v)/count(*) AS a FROM t GROUP BY k ORDER BY a DESC", {{"2", "60.000000"}, {"1", "15.000000"}}},
        {"SELECT avg(v)*0.2 FROM t", {{"6.0000000000000"}}},
        {"SELECT count(DISTINCT k)+1 FROM t", {{"3"}}},
        {"SELECT sum(v)+1,count(*)+1 FROM t WHERE k=99", {{"NULL", "1"}}},
        {"SELECT k,sum(v)+k FROM t GROUP BY k ORDER BY k", {{"1", "31"}, {"2", "62"}}}}) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, expected) << sql;
    }
    for (const std::string sql : {
        "SELECT sum(v)+k FROM t", "SELECT sum(avg(v)) FROM t",
        "SELECT count(*) FROM t WHERE sum(v)>0", "SELECT k FROM t GROUP BY sum(k)",
        "SELECT CASE WHEN sum(v)>0 THEN 1 ELSE 0 END FROM t"})
        EXPECT_FALSE(ParseSelect(sql, registry).ok()) << sql;
}
TEST(AggregateFormula, HavingUsesAggregateScaleAndRejectsNullComparisons) {
    auto decimal = std::make_shared<DecimalType>(); decimal->scale = 2;
    auto registry = test::Table({{test::Raw({1, 1, 2}), test::Raw({125, 225, 300})}},
        {{"k", std::make_shared<Int64Type>()}, {"v", decimal}});
    for (const auto &[sql, expected] : std::vector<std::pair<std::string, Rows>>{
        {"SELECT k FROM t GROUP BY k HAVING sum(v)>3.00 ORDER BY k", {{"1"}}},
        {"SELECT k FROM t GROUP BY k HAVING min(v)>=3.00 ORDER BY k", {{"2"}}},
        {"SELECT k FROM t GROUP BY k HAVING count(DISTINCT v)>1 ORDER BY k", {{"1"}}},
        {"SELECT sum(v) FROM t WHERE k=99 HAVING sum(v)<>0", {}}}) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, expected);
    }
}
TEST(AggregateFormula, AverageBindingRoundsAtItsDeclaredDecimalScale) {
    auto decimal = std::make_shared<DecimalType>(); decimal->scale = 2;
    auto registry = test::Table({{test::Raw({3005})}}, {{"v", decimal}});
    auto q = ParseSelect("SELECT avg(v)*0.2 FROM t", registry);
    ASSERT_TRUE(q.ok()); auto result = ExecuteQuery(*q, false, false);
    ASSERT_TRUE(result.ok()); EXPECT_EQ(result->values, (Rows{{"6.0100000000000"}}));
}
