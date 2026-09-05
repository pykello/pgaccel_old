#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(TruncatedGroupKey, NegativeEpochBoundariesKeepExactMinuteGroups)
{
    auto registry = test::Table({{test::Raw({-61,-60,-1,0,59,60})}, {test::Raw({-1,60,119,120})}},
        {{"ts", std::make_shared<TimestampType>()}});
    auto q = ParseSelect("SELECT DATE_TRUNC('minute',ts),count(*) FROM t GROUP BY DATE_TRUNC('minute',ts) ORDER BY DATE_TRUNC('minute',ts)", registry);
    ASSERT_TRUE(q.ok());
    for (bool parallel : {false,true}) {
        auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok());
        EXPECT_EQ(result->values, Rows({{"1969-12-31 23:58:00","1"}, {"1969-12-31 23:59:00","3"},
            {"1970-01-01 00:00:00","2"}, {"1970-01-01 00:01:00","3"}, {"1970-01-01 00:02:00","1"}}));
    }
}
TEST(TruncatedGroupKey, HourAndDayDomainsDoNotWrapAtSignedExtremes)
{
    auto registry = test::Table({{test::Raw({INT64_MIN,INT64_MIN,INT64_MAX,INT64_MAX,-86401,-86400,-1,0,86399,86400})}},
        {{"ts", std::make_shared<TimestampType>()}});
    for (std::string unit : {"minute","hour","day"}) {
        auto q = ParseSelect("SELECT count(*) AS c FROM t GROUP BY DATE_TRUNC('" + unit +
            "',ts) ORDER BY c DESC", registry);
        ASSERT_TRUE(q.ok());
        auto result = ExecuteQuery(*q, true, true); ASSERT_TRUE(result.ok());
        if (unit == "day") EXPECT_EQ(result->values, Rows({{"2"},{"2"},{"2"},{"2"},{"1"},{"1"}}));
        else EXPECT_EQ(result->values, Rows({{"2"},{"2"},{"1"},{"1"},{"1"},{"1"},{"1"},{"1"}}));
    }
}
