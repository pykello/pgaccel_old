#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(BooleanExpression, OrPrecedenceAndShortCircuitInsideCase) {
    auto registry = test::Table({{test::Raw({1, 2, 3})}}, {{"x", std::make_shared<Int64Type>()}});
    for (const auto &[expression, expected] : std::vector<std::pair<std::string, std::string>>{
        {"x=1 OR x=2 AND x=3", "1"}, {"(x=1 OR x=2) AND x<3", "2"},
        {"x>0 OR 1/0=0", "3"}, {"x<0 AND 1/0=0 OR x=2", "1"}}) {
        auto q = ParseSelect("SELECT sum(CASE WHEN " + expression + " THEN 1 ELSE 0 END) FROM t", registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q, true, true);
        ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, (Rows{{expected}}));
    }
}
TEST(BooleanExpression, GeneralWhereRangesMembershipAndLike) {
    auto registry = test::Table({{test::Raw({1, 2, 3, 4}),
        test::Dict<StringType>({"PROMO one", "ordinary", "PROMO two", "a_b"})}},
        {{"x", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()}});
    for (const auto &[predicate, expected] : std::vector<std::pair<std::string, std::string>>{
        {"(x BETWEEN 1 AND 1+1) OR x IN (4)", "7"},
        {"x NOT IN (1,4) AND (s LIKE 'PROMO%' OR x=2)", "5"},
        {"NOT (x BETWEEN 2 AND 4)", "1"},
        {"s LIKE 'a\\_b' OR x=1", "5"},
        {"x NOT BETWEEN 2 AND 3", "5"}}) {
        auto q = ParseSelect("SELECT sum(x) FROM t WHERE " + predicate, registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q, true, true);
        ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, (Rows{{expected}})) << predicate;
    }
    auto q = ParseSelect("SELECT sum(CASE WHEN s LIKE 'PROMO%' THEN x ELSE 0 END) FROM t", registry);
    ASSERT_TRUE(q.ok()); auto result = ExecuteQuery(*q, false, false);
    ASSERT_TRUE(result.ok()); EXPECT_EQ(result->values, (Rows{{"4"}}));
}
