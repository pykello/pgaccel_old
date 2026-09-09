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
    auto schema=ColumnarTable::LoadSchema("t",path);ASSERT_TRUE(schema.ok());EXPECT_TRUE((*schema)->IsSchemaOnly());EXPECT_EQ((*schema)->SourceRowGroupCount(),1);EXPECT_EQ((*schema)->RowGroupCount(),0);
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

TEST(QueryColumns, DensePreparationRetainsBuffersAndRechecksPredicates) {
    char directory[]="/tmp/pgaccel-dense-preparation-XXXXXX";ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup{const char *p;~Cleanup(){std::filesystem::remove_all(p);}}cleanup{directory};
    std::vector<int64_t> keys(20000),values(20000,1);std::iota(keys.begin(),keys.end(),0);
    auto tables=test::Table({{test::Raw(keys),test::Raw(values)}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
    ASSERT_TRUE(tables.at("t")->Save(std::string(directory)+"/t.pgaccel").ok());
    for(int cutoff:{0,18999,19000,19999,20000}) {
        auto sql="SELECT sum(v) FROM t WHERE k<"+std::to_string(cutoff);
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok());auto expected=ExecuteQuery(*q,true,true);ASSERT_TRUE(expected.ok());
        auto prepared=PrepareDirectQuery(directory,sql);ASSERT_TRUE(prepared.ok());
        auto actual=ExecuteQuery(prepared->query,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
        int rows=0;for(int g=0;g<prepared->tables.at("t")->RowGroupCount();++g)rows+=prepared->tables.at("t")->GetRowGroup(g).size;
        EXPECT_EQ(rows,cutoff>=19000?20000:cutoff);
    }
}
