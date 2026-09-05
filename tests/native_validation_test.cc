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
TEST(NativeValidation, VersionAndPairIdentity) {
    auto t=test::Table({{test::Raw({7,9})}},{{"v",std::make_shared<Int64Type>()}});
    std::stringstream data1,meta1,data2,meta2;
    ASSERT_TRUE(t.at("t")->Save(meta1,data1).ok());
    ASSERT_TRUE(t.at("t")->Save(meta2,data2).ok());
    EXPECT_EQ(meta1.str().find("PGACCEL_SNAPSHOT 1 "),0);
    data1.seekg(0);meta1.seekg(0);
    EXPECT_TRUE(ColumnarTable::Load("t",meta1,data1).ok());
    data1.clear();data1.seekg(0);meta2.seekg(0);
    EXPECT_FALSE(ColumnarTable::Load("t",meta2,data1).ok());
    auto bad=meta1.str();bad.replace(bad.find(" 1 "),3," 99 ");
    std::stringstream unsupported(bad);data1.clear();data1.seekg(0);
    EXPECT_FALSE(ColumnarTable::Load("t",unsupported,data1).ok());
}
