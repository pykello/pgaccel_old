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
