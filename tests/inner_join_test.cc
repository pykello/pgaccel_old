#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace {
TableRegistry JoinTables() {
    auto registry = test::Table({{test::Raw({1, 1, 2, 3}),
        test::Dict<StringType>({"a", "b", "c", "d"})}},
        {{"id", std::make_shared<Int64Type>()}, {"label", std::make_shared<StringType>()}});
    auto other = test::Table({{test::Raw({1, 1, 2, 4}), test::Raw({10, 20, 30, 40})}},
        {{"id", std::make_shared<Int64Type>()}, {"value", std::make_shared<Int64Type>()}});
    registry.emplace("u", std::move(other.at("t")));
    return registry;
}
Rows RunJoin(const std::string &sql, TableRegistry &registry) {
    auto q = ParseSelect(sql, registry);
    if (!q.ok()) { ADD_FAILURE() << q.status().Message(); return {}; }
    auto result = ExecuteQuery(*q, true, true);
    if (!result.ok()) { ADD_FAILURE() << result.status().Message(); return {}; }
    return result->values;
}
}
TEST(InnerJoin, DuplicateKeysFiltersGroupsAndQualifiedAliases) {
    auto registry = JoinTables();
    EXPECT_EQ(RunJoin("SELECT a.label, sum(b.value), count(*) FROM t AS a, u b WHERE a.id=b.id "
        "AND b.value >= 20 GROUP BY a.label ORDER BY a.label", registry),
        (Rows{{"a", "20", "1"}, {"b", "20", "1"}, {"c", "30", "1"}}));
    EXPECT_EQ(RunJoin("SELECT count(*),sum(u.value) FROM t,u WHERE t.id=u.id", registry),
        (Rows{{"5", "90"}}));
    EXPECT_EQ(RunJoin("SELECT t.label,u.value FROM t,u WHERE t.id=u.id ORDER BY u.value DESC,t.label LIMIT 3", registry),
        (Rows{{"c", "30"}, {"a", "20"}, {"b", "20"}}));
    EXPECT_EQ(RunJoin("SELECT count(*),sum(u.value) FROM t,u WHERE t.id=u.id AND t.id=99", registry),
        (Rows{{"0", "NULL"}}));
    EXPECT_FALSE(ParseSelect("SELECT id FROM t,u WHERE t.id=u.id", registry).ok());
    EXPECT_FALSE(ParseSelect("SELECT a.id FROM t a,u a", registry).ok());
    EXPECT_FALSE(ParseSelect("SELECT t.id FROM t a", registry).ok());
    auto budget = ParseSelect("SELECT count(*) FROM t,u WHERE t.id=u.id", registry);
    ASSERT_TRUE(budget.ok()); auto limited = std::move(budget).ValueUnsafe(); limited.memoryBudgetBytes = 1;
    auto failed = ExecuteQuery(limited, true, true);
    ASSERT_FALSE(failed.ok()); EXPECT_NE(failed.status().Message().find("memory budget"), std::string::npos);
    auto cross = ParseSelect("SELECT count(*) FROM t,u", registry);
    ASSERT_TRUE(cross.ok()); EXPECT_FALSE(ExecuteQuery(*cross, false, false).ok());
}
TEST(InnerJoin, SelfJoinAndAdditionalEqualityOrInequality) {
    auto registry = JoinTables();
    EXPECT_EQ(RunJoin("SELECT count(*) FROM t a,t b WHERE a.id=b.id AND a.label < b.label", registry), (Rows{{"1"}}));
    EXPECT_EQ(RunJoin("SELECT count(*) FROM t a,t b WHERE a.id=b.id AND a.label=b.label", registry), (Rows{{"4"}}));
    EXPECT_EQ(RunJoin("SELECT count(*) FROM u WHERE id < value", registry), (Rows{{"4"}}));
}
TEST(InnerJoin, OnlyEqualitiesSharedByEveryAlternativeDriveHashJoins) {
    auto registry = JoinTables();
    EXPECT_EQ(RunJoin("SELECT count(*) FROM t a,u b WHERE "
        "(a.id=b.id AND a.label='a' AND b.value=10) OR "
        "(b.id=a.id AND a.label='c' AND b.value=30)", registry), (Rows{{"2"}}));
    auto q = ParseSelect("SELECT count(*) FROM t a,u b WHERE a.id=b.id OR a.label='a'", registry);
    ASSERT_TRUE(q.ok());
    EXPECT_FALSE(ExecuteQuery(*q, false, false).ok());
}
