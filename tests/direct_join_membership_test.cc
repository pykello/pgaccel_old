#include "direct_query.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
using namespace pgaccel;
TEST(DirectJoinMembership, PrunesTransitivelyWithoutChangingDuplicates) {
    char directory[]="/tmp/pgaccel-direct-membership-XXXXXX";ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup{const char *p;~Cleanup(){std::filesystem::remove_all(p);}}cleanup{directory};
    auto tables=test::Table({{test::Raw({1,2,3}),test::Raw({5,7,11})},{test::Raw({1,2,99}),test::Raw({13,17,19})}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
    auto d=test::Table({{test::Raw({1,1,2}),test::Raw({1,1,0})}},
        {{"k",std::make_shared<Int64Type>()},{"keep",std::make_shared<Int64Type>()}});
    auto bridge=test::Table({{test::Raw({1,2,3})}},{{"k",std::make_shared<Int64Type>()}});
    tables.emplace("d",std::move(d.at("t")));tables.emplace("b",std::move(bridge.at("t")));
    for(const auto &[name,table]:tables)ASSERT_TRUE(table->Save(std::string(directory)+"/"+name+".pgaccel").ok());
    for(auto predicate:{"d.keep=1","d.keep=0","d.keep=7"}) {
        auto sql=std::string("SELECT sum(t.v) FROM t,b,d WHERE t.k=b.k AND b.k=d.k AND ")+predicate;
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok());auto expected=ExecuteQuery(*q,true,true);ASSERT_TRUE(expected.ok());
        auto p=PrepareDirectQuery(directory,sql);ASSERT_TRUE(p.ok())<<p.status().Message();
        auto actual=ExecuteQuery(p->query,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
        uint64_t rows=0;for(int g=0;g<p->tables.at("t")->RowGroupCount();++g)rows+=p->tables.at("t")->GetRowGroup(g).size;
        EXPECT_EQ(rows,std::string(predicate)=="d.keep=7"?0:2);
        EXPECT_EQ(p->tables.at("d")->GetRowGroup(0).size,3);
    }
    auto sql="SELECT sum(t.v) FROM t LEFT JOIN d ON t.k=d.k AND d.keep=1";
    auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok());auto p=PrepareDirectQuery(directory,sql);ASSERT_TRUE(p.ok());
    EXPECT_EQ(p->tables.at("t")->RowGroupCount(),2);
    EXPECT_EQ(ExecuteQuery(p->query,true,true)->values,ExecuteQuery(*q,true,true)->values);
}
TEST(DirectJoinMembership, SparseExtremeDomainsDeclinePruning) {
    auto tables=test::Table({{test::Raw({INT64_MIN,INT64_MAX}),test::Raw({1,1})}},
        {{"k",std::make_shared<Int64Type>()},{"keep",std::make_shared<Int64Type>()}});
    auto fact=test::Table({{test::Raw({INT64_MIN,INT64_MAX})}},{{"k",std::make_shared<Int64Type>()}});
    tables.emplace("f",std::move(fact.at("t")));
    auto q=ParseSelect("SELECT count(*) FROM f,t WHERE f.k=t.k AND t.keep=1",tables);ASSERT_TRUE(q.ok());
    int f=std::find(q->tables.begin(),q->tables.end(),tables.at("f").get())-q->tables.begin();
    // The registry fixture's internal name is t; use an ordinary named source.
    auto memberships=PrepareDirectMemberships(*q,f,tables,true,1024);
    EXPECT_TRUE(memberships.empty());
}

TEST(DirectJoinMembership, CommonDisjunctFiltersAreNecessaryInEveryBranch) {
    auto tables=test::Table({{test::Raw({1,2,3}),test::Dict<StringType>({"yes","no","yes"})}},
        {{"k",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    auto common=ParseSelect("SELECT sum(k) FROM t WHERE (s='yes' AND k=1) OR (s='yes' AND k=3)",tables);
    ASSERT_TRUE(common.ok());auto required=DirectPreparationFilters(*common);
    ASSERT_EQ(required.filterClauses.size(),1);EXPECT_EQ(required.filterClauses[0].columnRef.Name(),"s");
    EXPECT_EQ(required.filterClauses[0].value,"yes");
    auto optional=ParseSelect("SELECT sum(k) FROM t WHERE (s='yes' AND k=1) OR k=2",tables);
    ASSERT_TRUE(optional.ok());EXPECT_TRUE(DirectPreparationFilters(*optional).filterClauses.empty());
}

TEST(DirectScopeFilters, IntersectsRequirementsAcrossEveryPhysicalUse) {
    char directory[]="/tmp/pgaccel-direct-scopes-XXXXXX";ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup{const char *p;~Cleanup(){std::filesystem::remove_all(p);}}cleanup{directory};
    auto tables=test::Table({{test::Raw({1,2,3,4}),test::Raw({5,7,11,13})}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
    ASSERT_TRUE(tables.at("t")->Save(std::string(directory)+"/t.pgaccel").ok());
    auto check=[&](const std::string &sql,uint64_t expectedRows) {
        auto original=ParseSelect(sql,tables);ASSERT_TRUE(original.ok())<<original.status().Message();
        auto prepared=PrepareDirectQuery(directory,sql);ASSERT_TRUE(prepared.ok())<<prepared.status().Message();
        auto expected=ExecuteQuery(*original,true,true);ASSERT_TRUE(expected.ok())<<expected.status().Message();
        auto actual=ExecuteQuery(prepared->query,true,true);ASSERT_TRUE(actual.ok())<<actual.status().Message();
        EXPECT_EQ(actual->values,expected->values);
        uint64_t rows=0;auto &table=*prepared->tables.at("t");
        for(int g=0;g<table.RowGroupCount();++g)rows+=table.GetRowGroup(g).size;
        EXPECT_EQ(rows,expectedRows);
    };
    check("SELECT sum(v) FROM t WHERE k>=3 AND v>(SELECT avg(v) FROM t WHERE k>=3)",2);
    check("SELECT sum(v) FROM t WHERE k>=3 AND v>(SELECT avg(v) FROM t)",4);
    check("SELECT sum(v) FROM t WHERE k>=3 AND v>(SELECT avg(v) FROM t WHERE k<=2)",4);
    check("SELECT sum(a.v) FROM t a,t b WHERE a.k=b.k AND a.k>=3",4);
    check("WITH x AS (SELECT k,v FROM t WHERE k>=3) SELECT sum(v) FROM x WHERE v>(SELECT avg(v) FROM x)",2);
}

TEST(DirectJoinMembership, ConditionalCountsPruneGroupedInputAndPreserveDimension) {
    char directory[]="/tmp/pgaccel-direct-counts-XXXXXX";ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup{const char *p;~Cleanup(){std::filesystem::remove_all(p);}}cleanup{directory};
    auto tables=test::Table({{test::Raw({1,1,2,3}),test::Dict<StringType>({"mail","mail","ship","mail"}),
        test::Raw({1,0,1,1})}},{{"k",std::make_shared<Int64Type>()},{"mode",std::make_shared<StringType>()},
        {"keep",std::make_shared<Int64Type>()}});
    auto d=test::Table({{test::Raw({1,2,3}),test::Dict<StringType>({"high","low","high"})}},
        {{"k",std::make_shared<Int64Type>()},{"priority",std::make_shared<StringType>()}});
    tables.emplace("d",std::move(d.at("t")));
    for(const auto &[name,table]:tables)ASSERT_TRUE(table->Save(std::string(directory)+"/"+name+".pgaccel").ok());
    for(const std::string join:{"JOIN","LEFT JOIN"}) {
        auto sql=std::string("SELECT t.mode,sum(CASE WHEN ")+(join=="JOIN"?"priority='high'":"t.keep=1")+
            " THEN 1 ELSE 0 END) FROM t"+
            (join=="JOIN"?",d WHERE t.k=d.k AND ":" LEFT JOIN d ON t.k=d.k WHERE ")+
            "t.keep=1 GROUP BY t.mode ORDER BY t.mode";
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok())<<q.status().Message()<<" SQL: "<<sql;auto expected=ExecuteQuery(*q,true,true);ASSERT_TRUE(expected.ok());
        auto p=PrepareDirectQuery(directory,sql);ASSERT_TRUE(p.ok())<<p.status().Message();
        auto actual=ExecuteQuery(p->query,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
        EXPECT_EQ(p->tables.at("t")->GetRowGroup(0).size,join=="JOIN"?3:4);
        EXPECT_EQ(p->tables.at("d")->GetRowGroup(0).size,3);
    }
}

TEST(DirectJoinMembership, SharedScalarScopesRequireIdenticalDomains) {
    char directory[]="/tmp/pgaccel-shared-domain-XXXXXX";ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup{const char *p;~Cleanup(){std::filesystem::remove_all(p);}}cleanup{directory};
    auto tables=test::Table({{test::Raw({1,2,3}),test::Raw({5,7,11})},
        {test::Raw({1,2,3}),test::Raw({13,17,19})}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
    auto d=test::Table({{test::Raw({1,2,3}),test::Raw({1,0,0})}},
        {{"k",std::make_shared<Int64Type>()},{"keep",std::make_shared<Int64Type>()}});
    tables.emplace("d",std::move(d.at("t")));
    for(const auto &[name,table]:tables)ASSERT_TRUE(table->Save(std::string(directory)+"/"+name+".pgaccel").ok());
    for(const std::string inner:{" AND d.keep=1"," AND d.keep=0",""}) {
        auto sql="SELECT t.k,sum(t.v) FROM t,d WHERE t.k=d.k AND d.keep=1 GROUP BY t.k HAVING sum(t.v)>"
            "(SELECT sum(t.v)*0.1 FROM t,d WHERE t.k=d.k"+inner+") ORDER BY t.k";
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok())<<q.status().Message();
        EXPECT_EQ(DirectSharedJoinScopes(*q),inner==" AND d.keep=1");
        auto expected=ExecuteQuery(*q,true,true);ASSERT_TRUE(expected.ok());
        auto prepared=PrepareDirectQuery(directory,sql);ASSERT_TRUE(prepared.ok())<<prepared.status().Message();
        auto actual=ExecuteQuery(prepared->query,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
        int rows=0;for(int g=0;g<prepared->tables.at("t")->RowGroupCount();++g)rows+=prepared->tables.at("t")->GetRowGroup(g).size;
        EXPECT_EQ(rows,inner==" AND d.keep=1"?2:6);
        EXPECT_EQ(prepared->tables.at("d")->GetRowGroup(0).size,3);
    }
}
