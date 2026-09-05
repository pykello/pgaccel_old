#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace {
TableRegistry Inputs(const std::vector<int64_t> &right = {1, 1, 2, 4}) {
    auto registry = test::Table({{test::Raw({1, 2, 3})}}, {{"k", std::make_shared<Int64Type>()}});
    auto other = test::Table({{test::Raw(right)}}, {{"k", std::make_shared<Int64Type>()}});
    registry.emplace("u", std::move(other.at("t"))); return registry;
}
}
TEST(LeftJoin, PreserveMissingRowsAndCountOnlyMatches) {
    auto registry = Inputs();
    for (bool avx : {false, true}) {
        auto query = ParseSelect("SELECT a.k, count(*), count(b.k), count(DISTINCT b.k) "
            "FROM t a LEFT OUTER JOIN u b ON a.k = b.k AND b.k <> 2 "
            "GROUP BY a.k ORDER BY a.k", registry);
        ASSERT_TRUE(query.ok()) << query.status().Message();
        auto result = ExecuteQuery(*query, avx, false);
        ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, (Rows{{"1", "2", "2", "1"}, {"2", "1", "0", "0"}, {"3", "1", "0", "0"}}));
    }
    // An outer-only ON condition must not remove the preserved row.
    auto query = ParseSelect("SELECT a.k, count(b.k) FROM t a LEFT JOIN u b "
        "ON a.k = b.k AND a.k > 1 WHERE a.k < 3 GROUP BY a.k ORDER BY a.k", registry);
    ASSERT_TRUE(query.ok()) << query.status().Message();
    auto result = ExecuteQuery(*query, true, false);
    ASSERT_TRUE(result.ok()) << result.status().Message();
    EXPECT_EQ(result->values, (Rows{{"1", "0"}, {"2", "1"}}));
}
TEST(LeftJoin, EmptyBuildAndAggregatedDerivedHistogram) {
    for (auto keys : {std::vector<int64_t>{}, std::vector<int64_t>{1, 1, 2}}) {
        auto registry = Inputs(keys);
        auto query = ParseSelect("SELECT c, count(*) AS frequency FROM "
            "(SELECT a.k, count(b.k) FROM t a LEFT JOIN u b ON a.k = b.k GROUP BY a.k) d(k,c) "
            "GROUP BY c ORDER BY c", registry);
        ASSERT_TRUE(query.ok()) << query.status().Message();
        auto result = ExecuteQuery(*query, true, false);
        ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, (keys.empty() ? Rows{{"0", "3"}} : Rows{{"0", "1"}, {"1", "1"}, {"2", "1"}}));
    }
}
TEST(LeftJoin, RejectUnsupportedNullableUses) {
    auto registry = Inputs();
    for (const auto &sql : {
        "SELECT b.k FROM t a LEFT JOIN u b ON a.k = b.k",
        "SELECT sum(b.k) FROM t a LEFT JOIN u b ON a.k = b.k",
        "SELECT count(b.k + 1) FROM t a LEFT JOIN u b ON a.k = b.k",
        "SELECT count(*) FROM t a LEFT JOIN u b ON a.k = b.k WHERE b.k > 1",
        "SELECT count(*) FROM t a LEFT JOIN u b ON a.k = b.k GROUP BY b.k",
        "SELECT count(*) FROM t a LEFT JOIN u b ON a.k = b.k, t c WHERE a.k = c.k"})
        EXPECT_FALSE(ParseSelect(sql, registry).ok()) << sql;
}
