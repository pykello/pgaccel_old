#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(NativeValidation, EmptyTableAndMalformedInput) {
    auto t=test::Table({},{{"v",std::make_shared<Int64Type>()}});
    for(auto sql:{"SELECT count(*) FROM t","SELECT count(distinct v) FROM t"}) {
        auto q=ParseSelect(sql,t);ASSERT_TRUE(q.ok());auto r=ExecuteQuery(*q,true,true);ASSERT_TRUE(r.ok());EXPECT_EQ(r->values,Rows({{"0"}}));
    }
    auto q=ParseSelect("SELECT min(v),sum(v),avg(v) FROM t",t);ASSERT_TRUE(q.ok());
    auto r=ExecuteQuery(*q,true,true);ASSERT_TRUE(r.ok());EXPECT_EQ(r->values,Rows({{"NULL","NULL","NULL"}}));
    for(auto metadata:{"", "-1\n", "1\n0 -1 v 2\n", "1\n0 1 v 999\n", "1\n0 1 v 2\n"}) {
        std::stringstream meta(metadata),data;EXPECT_FALSE(ColumnarTable::Load("t",meta,data).ok());
    }
}
TEST(NativeValidation, RejectCorruptDictionaryCodes) {
    auto d=test::Dict<StringType>({"a","b"});
    static_cast<DictColumnDataBase*>(d.get())->values[0]=255;
    EXPECT_THROW(test::Table({{d}},{{"s",std::make_shared<StringType>()}}),std::runtime_error);
}
TEST(DateTypes, TimestampUTCAndNativeRoundTrip) {
    TimestampType timestamp;
    EXPECT_EQ(ParseTimestamp("1969-12-31 23:59:59"),-1);
    EXPECT_EQ(ToString(&timestamp,-1),"1969-12-31 23:59:59");
    EXPECT_THROW(ParseTimestamp("2020-01-01 24:00:00"),std::invalid_argument);
    auto t=test::Table({{test::Raw({0,86400})}},{{"ts",std::make_shared<TimestampType>()}});
    auto q=ParseSelect("SELECT min(ts),max(ts) FROM t WHERE ts >= '1970-01-01 00:00:00'",t);ASSERT_TRUE(q.ok());
    auto r=ExecuteQuery(*q,true,true);ASSERT_TRUE(r.ok());EXPECT_EQ(r->values,Rows({{"1970-01-01 00:00:00","1970-01-02 00:00:00"}}));
}
