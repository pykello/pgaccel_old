#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;

TEST(TpchDates, TypedLiteralsAndInclusiveBetweenUseNativeFilters)
{
    auto registry = test::Table({{test::Dict<DateType>({ParseDate("1994-01-01"),
        ParseDate("1994-12-31"), ParseDate("1995-01-01")}),
        test::Raw({5, 7, 6})}}, {{"d", std::make_shared<DateType>()},
                               {"x", std::make_shared<Int64Type>()}});
    for (const std::string literal : {"DATE '1994-01-01'", "CAST('1994-01-01' AS date)"}) {
        auto q = ParseSelect("SELECT sum(x) FROM t WHERE d >= " + literal +
            " AND d < DATE '1994-01-01' + INTERVAL '1' YEAR AND x BETWEEN 5 AND 7", registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        ASSERT_EQ(q->filterClauses.size(), 4);
        EXPECT_EQ(q->filterClauses[1].value, "1995-01-01");
        for (bool avx : {false, true}) for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*q, avx, parallel);
            ASSERT_TRUE(result.ok()) << result.status().Message();
            EXPECT_EQ(result->values, (Rows{{"12"}}));
        }
    }
    for (const auto &[literal, expected] : std::vector<std::pair<std::string, std::string>>{
        {"DATE '1998-12-01' - INTERVAL '90' DAY", "1998-09-02"},
        {"DATE '2000-01-31' + INTERVAL '1' MONTH", "2000-02-29"},
        {"DATE '2000-02-29' + INTERVAL '1' YEAR", "2001-02-28"}}) {
        auto q = ParseSelect("SELECT count(*) FROM t WHERE d <= " + literal, registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        EXPECT_EQ(q->filterClauses[0].value, expected);
    }
    EXPECT_FALSE(ParseSelect("SELECT count(*) FROM t WHERE x = DATE '1994-01-01'", registry).ok());
    EXPECT_FALSE(ParseSelect("SELECT count(*) FROM t WHERE d = CAST('1994-01-01' AS integer)", registry).ok());
    EXPECT_FALSE(ParseSelect("SELECT count(*) FROM t WHERE d = DATE '2000-02-30'", registry).ok());
}
TEST(TpchDates, CalendarExtractionGroupsDatesAndFloorsNegativeTimestamps) {
    auto registry = test::Table({{test::Dict<DateType>({ParseDate("1999-12-31"),
        ParseDate("2000-02-29"), ParseDate("2000-12-31")}), test::Raw({-1, 0, 951782400})}},
        {{"d", std::make_shared<DateType>()}, {"ts", std::make_shared<TimestampType>()}});
    auto q = ParseSelect("SELECT extract(year FROM d) AS y,count(*) FROM t GROUP BY y ORDER BY y", registry);
    ASSERT_TRUE(q.ok()); auto result = ExecuteQuery(*q, true, true);
    ASSERT_TRUE(result.ok()) << result.status().Message();
    EXPECT_EQ(result->values, (Rows{{"1999", "1"}, {"2000", "2"}}));
    q = ParseSelect("SELECT extract(year FROM ts),extract(month FROM ts),extract(day FROM ts) FROM t ORDER BY ts", registry);
    ASSERT_TRUE(q.ok()); result = ExecuteQuery(*q, false, false);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->values, (Rows{{"1969", "12", "31"}, {"1970", "1", "1"}, {"2000", "2", "29"}}));
    EXPECT_FALSE(ParseSelect("SELECT extract(hour FROM d) FROM t", registry).ok());
}
TEST(TpchDates, LowerSubqueryDateFiltersWithoutLeakingOuterAntiConditions) {
    auto registry = test::Table({{test::Raw({1, 2}), test::Dict<DateType>({ParseDate("1994-01-01"), ParseDate("1995-01-01")})}},
        {{"id", std::make_shared<Int64Type>()}, {"d", std::make_shared<DateType>()}});
    auto other = test::Table({{test::Raw({1, 2}), test::Dict<DateType>({ParseDate("1994-01-01"), ParseDate("1995-01-01")})}},
        {{"id", std::make_shared<Int64Type>()}, {"d", std::make_shared<DateType>()}});
    registry.emplace("u", std::move(other.at("t")));
    for (const std::string column : {"t.d", "u.d"}) {
        auto q = ParseSelect("SELECT id FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.id=t.id AND " +
            column + ">=DATE '1995-01-01') ORDER BY id", registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        EXPECT_EQ(q->filterClauses.size(), column == "u.d" ? 1 : 0);
        auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok()) << result.status().Message();
        EXPECT_EQ(result->values, (Rows{{"1"}}));
    }
    auto q = ParseSelect("SELECT count(*) FROM t WHERE DATE '1994-01-01' <= d AND id>(SELECT min(id) FROM u)", registry);
    ASSERT_TRUE(q.ok()); ASSERT_EQ(q->filterClauses.size(), 1);
    EXPECT_EQ(q->filterClauses[0].op, FilterClause::FILTER_GTE);
    auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok()); EXPECT_EQ(result->values, (Rows{{"1"}}));
}
