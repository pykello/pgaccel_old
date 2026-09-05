#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(DerivedProjection, SubstituteExpressionsAndPreserveInnerAndOuterFilters) {
    auto registry = test::Table({{test::Raw({1, 2, 3, 4})}}, {{"x", std::make_shared<Int64Type>()}});
    for (const std::string sql : {
        "SELECT k,sum(v) FROM (SELECT x AS k,x*10 AS v FROM t WHERE x>1) AS d WHERE v<40 GROUP BY k ORDER BY k",
        "SELECT k,sum(v) FROM (SELECT k,v FROM (SELECT x AS k,x*10 AS v FROM t WHERE x>1) a) b WHERE v<40 GROUP BY k ORDER BY k"}) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, (Rows{{"2", "20"}, {"3", "30"}}));
    }
    auto q = ParseSelect("SELECT d.* FROM (SELECT x FROM t) d", registry);
    EXPECT_FALSE(q.ok()); // Qualified stars are still rejected explicitly.
    q = ParseSelect("SELECT * FROM (SELECT x*2 AS v,x AS k FROM t) d WHERE d.k=2", registry);
    ASSERT_TRUE(q.ok()) << q.status().Message();
    auto result = ExecuteQuery(*q, false, false); ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->values, (Rows{{"4", "2"}}));
    EXPECT_FALSE(ParseSelect("SELECT x FROM (SELECT x AS y FROM t) d", registry).ok());
    auto limited = ParseSelect("SELECT x FROM (SELECT x FROM t LIMIT 1) d", registry);
    ASSERT_TRUE(limited.ok());
    auto first = ExecuteQuery(*limited, true, false);
    ASSERT_TRUE(first.ok());
    EXPECT_EQ(first->values, (Rows{{"1"}}));
    EXPECT_FALSE(ParseSelect("SELECT x FROM (SELECT x AS x,x AS x FROM t) d", registry).ok());
}
