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
