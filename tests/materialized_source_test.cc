#include <gtest/gtest.h>
#include "test_table.hpp"
using namespace pgaccel;

TEST(MaterializedSource, NumericBoundariesAndOwnedRows) {
    auto decimal = std::make_shared<DecimalType>(); decimal->scale = 2;
    std::vector<ColumnDesc> schema{{"n", std::make_shared<Int64Type>(), ColumnDataBase::RAW_COLUMN_DATA},
                                   {"d", decimal, ColumnDataBase::RAW_COLUMN_DATA}};
    Rows rows(65537, {"9223372036854775807", "-12.34"});
    auto table = ColumnarTable::NumericIntermediate("t", schema, rows);
    rows.clear();
    ASSERT_EQ(table->RowGroupCount(), 2);
    TableRegistry registry; registry.emplace("t", std::move(table));
    auto query = ParseSelect("SELECT count(*), min(n), max(d) FROM t", registry);
    ASSERT_TRUE(query.ok()) << query.status().Message();
    auto result = ExecuteQuery(*query, true, false);
    ASSERT_TRUE(result.ok()) << result.status().Message();
    EXPECT_EQ(result->values, (Rows{{"65537", "9223372036854775807", "-12.34"}}));
    EXPECT_THROW(ColumnarTable::NumericIntermediate("t", schema, {{"9223372036854775808", "0"}}), std::overflow_error);
    EXPECT_THROW(ColumnarTable::NumericIntermediate("t", schema, {{"NULL", "0"}}), std::invalid_argument);
    EXPECT_THROW(ColumnarTable::NumericIntermediate("t", schema, {{"1"}}), std::invalid_argument);
}

TEST(MaterializedSource, ExecutesInsideParentBudget) {
    auto registry = test::Table({{test::Raw({1, 1, 2})}}, {{"n", std::make_shared<Int64Type>()}});
    auto child = ParseSelect("SELECT n, count(*) AS c FROM t GROUP BY n", registry);
    ASSERT_TRUE(child.ok());
    std::vector<ColumnDesc> schema{{"n", std::make_shared<Int64Type>(), ColumnDataBase::RAW_COLUMN_DATA},
                                  {"c", std::make_shared<Int64Type>(), ColumnDataBase::RAW_COLUMN_DATA}};
    std::shared_ptr<ColumnarTable> placeholder = ColumnarTable::NumericIntermediate("x", schema, {});
    TableRegistry outerRegistry;
    outerRegistry.emplace("x", ColumnarTable::NumericIntermediate("x", schema, {}));
    auto parsed = ParseSelect("SELECT sum(c) FROM x", outerRegistry);
    ASSERT_TRUE(parsed.ok());
    auto query = std::move(parsed).ValueUnsafe(); query.tables[0] = placeholder.get();
    query.materializedSources.push_back({0, std::make_shared<QueryDesc>(std::move(child).ValueUnsafe()), placeholder});
    auto result = ExecuteQuery(query, true, false);
    ASSERT_TRUE(result.ok()) << result.status().Message();
    EXPECT_EQ(result->values, (Rows{{"3"}}));
    EXPECT_GT(result->peakMemoryBytes, 0);
    query.memoryBudgetBytes = 128; query.allowSpill = false;
    EXPECT_FALSE(ExecuteQuery(query, true, false).ok());
}

TEST(MaterializedSource, GroupedDerivedTablesAndColumnAliases) {
    auto registry = test::Table({{test::Raw({1, 1, 2, 3, 3})}}, {{"n", std::make_shared<Int64Type>()}});
    auto query = ParseSelect("SELECT c, count(*) AS frequency FROM "
        "(SELECT n, count(*) FROM t GROUP BY n) AS d(k, c) "
        "GROUP BY c ORDER BY c", registry);
    ASSERT_TRUE(query.ok()) << query.status().Message();
    auto result = ExecuteQuery(*query, true, false);
    ASSERT_TRUE(result.ok()) << result.status().Message();
    EXPECT_EQ(result->values, (Rows{{"1", "1"}, {"2", "2"}}));
    EXPECT_FALSE(ParseSelect("SELECT * FROM (SELECT n FROM t) d(a,b)", registry).ok());
    EXPECT_FALSE(ParseSelect("SELECT * FROM (SELECT sum(n) AS s FROM t) d", registry).ok());
}

TEST(MaterializedSource, CteReusedByScalarAndJoin) {
    auto registry = test::Table({{test::Raw({1, 1, 2, 3, 3})}}, {{"n", std::make_shared<Int64Type>()}});
    auto query = ParseSelect("WITH totals AS (SELECT n AS k, sum(n) AS total FROM t GROUP BY n) "
        "SELECT t.n, total FROM t, totals WHERE t.n = k AND total = "
        "(SELECT max(total) FROM totals) ORDER BY t.n", registry);
    ASSERT_TRUE(query.ok()) << query.status().Message();
    auto result = ExecuteQuery(*query, true, false);
    ASSERT_TRUE(result.ok()) << result.status().Message();
    EXPECT_EQ(result->values, (Rows{{"3", "6"}, {"3", "6"}}));
    // Planning remains reusable and owns schema placeholders after nested parsers die.
    auto second = ExecuteQuery(*query, false, true);
    ASSERT_TRUE(second.ok()) << second.status().Message();
    EXPECT_EQ(second->values, result->values);
    auto chain = ParseSelect("WITH a(k,c) AS (SELECT n, count(*) FROM t GROUP BY n), "
        "b AS (SELECT c, count(*) AS frequency FROM a GROUP BY c) "
        "SELECT sum(frequency) FROM b", registry);
    ASSERT_TRUE(chain.ok()) << chain.status().Message();
    auto chained = ExecuteQuery(*chain, true, false);
    ASSERT_TRUE(chained.ok()) << chained.status().Message();
    EXPECT_EQ(chained->values, (Rows{{"3"}}));
    EXPECT_FALSE(ParseSelect("WITH RECURSIVE a AS (SELECT n FROM t) SELECT k FROM a", registry).ok());
}
