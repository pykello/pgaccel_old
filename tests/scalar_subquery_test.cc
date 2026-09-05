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
TEST(ScalarSubquery, CorrelatedAggregatesUseExactCompositeKeysAndEmptySemantics) {
    auto registry = test::Table({{test::Raw({1, 1, 2, 3}), test::Raw({1, 2, 1, 1}), test::Raw({5, 8, 10, 99})}},
        {{"k", std::make_shared<Int64Type>()}, {"b", std::make_shared<Int64Type>()}, {"v", std::make_shared<Int64Type>()}});
    auto other = test::Table({{test::Raw({1, 1, 1, 2}), test::Raw({1, 1, 2, 1}), test::Raw({4, 6, 9, 10})}},
        {{"k", std::make_shared<Int64Type>()}, {"b", std::make_shared<Int64Type>()}, {"v", std::make_shared<Int64Type>()}});
    registry.emplace("u", std::move(other.at("t")));
    for (const auto &[predicate, expected] : std::vector<std::pair<std::string, Rows>>{
        {"v>(SELECT min(v) FROM u WHERE u.k=t.k)", {{"1", "1"}, {"1", "2"}}},
        {"v<(SELECT avg(v) FROM u WHERE t.k=u.k AND u.b=t.b)", {{"1", "2"}}},
        {"v<(SELECT sum(v)/2 FROM u WHERE u.k=t.k)", {{"1", "1"}, {"1", "2"}}},
        {"v=(SELECT min(v) FROM t a WHERE a.k=t.k)", {{"1", "1"}, {"2", "1"}, {"3", "1"}}}}) {
        auto q = ParseSelect("SELECT k,b FROM t WHERE " + predicate + " ORDER BY k,b", registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        ASSERT_FALSE(q->scalarQueries[0].keyOutputs.empty());
        auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, expected) << predicate;
    }
    for (const std::string scalar : {"SELECT count(*) FROM u WHERE u.k=t.k",
        "SELECT sum(v) FROM u WHERE u.k<t.k", "SELECT sum(v) FROM u WHERE u.k=t.k AND t.b=1"})
        EXPECT_FALSE(ParseSelect("SELECT k FROM t WHERE v>("+scalar+")", registry).ok());
}
TEST(ScalarSubquery, CorrelationOwnsStringKeysAfterChildOutputIsReleased) {
    auto registry = test::Table({{test::Dict<StringType>({"alpha", "beta", "missing"}), test::Raw({5, 9, 99})}},
        {{"k", std::make_shared<StringType>()}, {"v", std::make_shared<Int64Type>()}});
    auto other = test::Table({{test::Dict<StringType>({"alpha", "alpha", "beta"}), test::Raw({4, 6, 9})}},
        {{"k", std::make_shared<StringType>()}, {"v", std::make_shared<Int64Type>()}});
    registry.emplace("u", std::move(other.at("t")));
    auto q = ParseSelect("SELECT k FROM t WHERE v>(SELECT min(v) FROM u WHERE u.k=t.k) ORDER BY k", registry);
    ASSERT_TRUE(q.ok()) << q.status().Message(); auto result = ExecuteQuery(*q, false, false);
    ASSERT_TRUE(result.ok()) << result.status().Message(); EXPECT_EQ(result->values, (Rows{{"alpha"}}));
}
