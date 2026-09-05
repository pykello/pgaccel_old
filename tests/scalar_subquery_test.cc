#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(ScalarSubquery, GlobalAggregatesNullsCardinalityAndNestedMembership) {
    auto registry = test::Table({{test::Raw({1, 2, 3}), test::Raw({10, 20, 60})}},
        {{"k", std::make_shared<Int64Type>()}, {"v", std::make_shared<Int64Type>()}});
    for (const auto &[predicate, expected] : std::vector<std::pair<std::string, Rows>>{
        {"v>(SELECT avg(v) FROM t)", {{"60"}}},
        {"k=(SELECT count(*) FROM t)", {{"60"}}},
        {"v>(SELECT sum(v) FROM t WHERE k=99)", {}},
        {"v>(SELECT sum(v)/3 FROM t)", {{"60"}}},
        {"v>(SELECT min(v) FROM t WHERE v IN (SELECT v FROM t WHERE k>=2))", {{"60"}}}}) {
        auto q = ParseSelect("SELECT v FROM t WHERE " + predicate, registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, expected) << predicate;
    }
    auto q = ParseSelect("SELECT v FROM t WHERE v=(SELECT min(v) FROM t GROUP BY k)", registry);
    ASSERT_TRUE(q.ok()); EXPECT_FALSE(ExecuteQuery(*q, false, false).ok());
    EXPECT_FALSE(ParseSelect("SELECT v FROM t WHERE v>(SELECT avg(v) FROM t) OR k=1", registry).ok());
    EXPECT_FALSE(ParseSelect("SELECT (SELECT avg(v) FROM t) FROM t", registry).ok());
}
TEST(ScalarSubquery, HavingUsesWideBoundsAndDifferentDecimalScales) {
    auto decimal = std::make_shared<DecimalType>(); decimal->scale = 2;
    auto registry = test::Table({{test::Raw({1, 1, 2}), test::Raw({125, 225, 300})}},
        {{"k", std::make_shared<Int64Type>()}, {"v", decimal}});
    for (const auto &[sql, expected] : std::vector<std::pair<std::string, Rows>>{
        {"SELECT k FROM t GROUP BY k HAVING sum(v)>(SELECT avg(v) FROM t) ORDER BY k", {{"1"}, {"2"}}},
        {"SELECT sum(v) FROM t HAVING sum(v)>(SELECT sum(v) FROM t WHERE k=99)", {}},
        {"SELECT k FROM t GROUP BY k HAVING sum(v)>(SELECT sum(v)*0.0001000000 FROM t) ORDER BY k", {{"1"}, {"2"}}}}) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, expected);
    }
}
