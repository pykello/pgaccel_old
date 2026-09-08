#include "direct_query.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
using namespace pgaccel;
TEST(DatePredicateColumns, PreserveComparisonBitsAndRejectObservedDates) {
    char directory[]="/tmp/pgaccel-date-predicate-XXXXXX";ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup {const char *p;~Cleanup(){std::filesystem::remove_all(p);}} cleanup{directory};
    auto tables=test::Table({{test::Raw({1,2,3,4}),test::Dict<DateType>({0,10,20,30}),test::Dict<DateType>({1,10,10,31})}},
        {{"k",std::make_shared<Int64Type>()},{"a",std::make_shared<DateType>()},{"b",std::make_shared<DateType>()}});
    ASSERT_TRUE(tables.at("t")->Save(std::string(directory)+"/t.pgaccel").ok());
    for(auto op:{"<","<=",">",">=","=","<>"}) {
        auto sql=std::string("SELECT count(*) FROM t WHERE a ")+op+" b";
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok());auto plan=DatePredicateOnlyColumns(*q);
        ASSERT_EQ(plan[tables.at("t").get()].size(),1);
        auto encoded=DatePredicateDictionaries(tables.at("t")->Schema(),tables.at("t")->GetRowGroup(0),plan.begin()->second);
        ASSERT_TRUE(encoded.ok());EXPECT_LE(static_cast<const DictColumnDataBase*>((*encoded).at("a").get())->dictSize(),2);
        auto prepared=PrepareDirectQuery(directory,sql);ASSERT_TRUE(prepared.ok())<<prepared.status().Message();
        auto expected=ExecuteQuery(*q,true,true),actual=ExecuteQuery(prepared->query,true,true);
        ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
    }
    for(const auto &sql:std::vector<std::string>{
        "SELECT a FROM t WHERE a<b ORDER BY a",
        "SELECT count(*) FROM t WHERE a<b AND a>'1970-01-01'",
        "SELECT count(*) FROM t WHERE a<b OR a=b",
        "SELECT count(*) FROM t WHERE a<b AND k IN (SELECT k FROM t WHERE a>'1970-01-01')"}) {
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok())<<q.status().Message();
        EXPECT_TRUE(DatePredicateOnlyColumns(*q).empty());
    }
    {
        auto q=ParseSelect("SELECT count(*) FROM t WHERE a<b AND b>a",tables);ASSERT_TRUE(q.ok());
        EXPECT_EQ(DatePredicateOnlyColumns(*q)[tables.at("t").get()].size(),1);
        // A comparison between two aliases can observe different source rows.
        // Build the bound shape explicitly to avoid relying on alias syntax.
        auto cross=std::move(q).ValueUnsafe();cross.tables.push_back(tables.at("t").get());
        auto e=std::make_shared<Expression>(*cross.predicates.front());
        e->right=std::make_shared<Expression>(*e->right);e->right->column->tableIdx=1;
        cross.predicates={e};
        EXPECT_TRUE(DatePredicateOnlyColumns(cross).empty());
    }
    // Row compaction must use the same original selection for both encoded
    // dates, even when representatives came from rows discarded by another filter.
    auto sql="SELECT count(*) FROM t WHERE a<b AND k>=3";
    auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok());auto prepared=PrepareDirectQuery(directory,sql);ASSERT_TRUE(prepared.ok());
    auto actual=ExecuteQuery(prepared->query,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,Rows({{"1"}}));
}
