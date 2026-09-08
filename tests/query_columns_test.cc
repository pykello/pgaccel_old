#include "test_table.hpp"
#include "direct_query.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
using namespace pgaccel;
TEST(QueryColumns, ScopedDependenciesAndDirectRebinding) {
    char directory[]="/tmp/pgaccel-query-columns-XXXXXX";ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup{const char *p;~Cleanup(){std::filesystem::remove_all(p);}}cleanup{directory};
    auto t=test::Table({{test::Raw({1,2,3}),test::Raw({2,4,8}),test::Raw({7,7,7})}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()},{"unused",std::make_shared<Int64Type>()}});
    std::string path=std::string(directory)+"/t.pgaccel";ASSERT_TRUE(t.at("t")->Save(path).ok());
    auto schema=ColumnarTable::LoadSchema("t",path);ASSERT_TRUE(schema.ok());EXPECT_TRUE((*schema)->IsSchemaOnly());
    TableRegistry schemas;schemas.emplace("t",std::move(schema).ValueUnsafe());
    auto count=ParseSelect("SELECT count(*) FROM t",schemas);ASSERT_TRUE(count.ok());EXPECT_FALSE(ExecuteQuery(*count,true,true).ok());
    for(const std::string sql:{
        "SELECT sum(v) FROM t WHERE k IN (SELECT k FROM t WHERE v > 2)",
        "SELECT sum(v) FROM t WHERE v > (SELECT avg(v) FROM t)",
        "SELECT k, sum(v) FROM t GROUP BY k ORDER BY sum(v) DESC",
        "SELECT count(*) FROM t",
        "SELECT sum(v) FROM t WHERE k >= 2",
        "SELECT sum(v) FROM t WHERE k > 100"}) {
        auto original=ParseSelect(sql,t);ASSERT_TRUE(original.ok())<<sql<<": "<<original.status().Message();auto expected=ExecuteQuery(*original,true,true);ASSERT_TRUE(expected.ok());
        auto prepared=PrepareDirectQuery(directory,sql);ASSERT_TRUE(prepared.ok())<<prepared.status().Message();
        EXPECT_FALSE(prepared->tables.at("t")->ColumnIndex("unused"));
        if(sql=="SELECT sum(v) FROM t WHERE k >= 2") {
            ASSERT_EQ(prepared->tables.at("t")->RowGroupCount(),1);
            EXPECT_EQ(prepared->tables.at("t")->GetRowGroup(0).size,2);
        }
        if(sql=="SELECT sum(v) FROM t WHERE k > 100")
            EXPECT_EQ(prepared->tables.at("t")->RowGroupCount(),0);
        auto actual=ExecuteQuery(prepared->query,true,true);ASSERT_TRUE(actual.ok())<<actual.status().Message();EXPECT_EQ(actual->values,expected->values);
    }
}
