#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(Substring, CharacterPositionsBorrowedOwnershipAndGrouping) {
    auto registry = test::Table({{test::Dict<StringType>({"abc", "é猫z", "", "a猫z"})}},
        {{"s", std::make_shared<StringType>()}});
    for (const auto &[expression, expected] : std::vector<std::pair<std::string, Rows>>{
        {"substring(s FROM 2 FOR 1)", {{""}, {"b"}, {"猫"}, {"猫"}}},
        {"substring(s,2)", {{""}, {"bc"}, {"猫z"}, {"猫z"}}},
        {"substring(s FROM 99 FOR 1)", {{""}, {""}, {""}, {""}}},
        {"substring(s FROM 1 FOR 0)", {{""}, {""}, {""}, {""}}}}) {
        auto q = ParseSelect("SELECT " + expression + " AS v FROM t ORDER BY v", registry);
        ASSERT_TRUE(q.ok()) << q.status().Message(); auto result = ExecuteQuery(*q, true, true);
        ASSERT_TRUE(result.ok()) << result.status().Message(); EXPECT_EQ(result->values, expected);
    }
    auto q = ParseSelect("SELECT substring(s FROM 2 FOR 1) AS k,count(*) FROM t GROUP BY k ORDER BY k", registry);
    ASSERT_TRUE(q.ok()); auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->values, (Rows{{"", "1"}, {"b", "1"}, {"猫", "2"}}));
    EXPECT_FALSE(ParseSelect("SELECT substring(s FROM 1 FOR 1) FROM t GROUP BY substring(s FROM 2 FOR 1)", registry).ok());
    EXPECT_FALSE(ParseSelect("SELECT substring(s FROM 0) FROM t", registry).ok());
}
