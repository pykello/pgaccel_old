#include "convergent_fixture.hpp"
#include "executor_independent_plan.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(IndependentPlan, AcceptsDistinctAliasesAndRejectsSharedChains) {
    auto tables=test::ConvergentTables();
    auto q=ParseSelect("SELECT SUM(t.x) FROM t,o,c,s,n a,n b,r WHERE t.o=o.k AND o.c=c.k "
        "AND c.n=a.k AND a.r=r.k AND t.s=s.k AND s.n=b.k",tables);ASSERT_TRUE(q.ok());
    Budget budget(1<<20);IndependentJoinPlan plan;
    ASSERT_TRUE(plan.prepare(*q,0,budget,true,true));EXPECT_EQ(plan.outgoing[0].size(),2);
    EXPECT_EQ(plan.order.size(),6);EXPECT_NE(plan.armOf[4],plan.armOf[5]);
    auto shared=ParseSelect("SELECT SUM(t.x) FROM t,o,c,s,n WHERE t.o=o.k AND o.c=c.k "
        "AND c.n=n.k AND t.s=s.k AND s.n=n.k",tables);ASSERT_TRUE(shared.ok());
    EXPECT_FALSE(plan.prepare(*shared,0,budget,true,true));
    ASSERT_TRUE(plan.prepare(*q,0,budget,false,false));EXPECT_EQ(plan.order.size(),6);
}
TEST(IndependentPlan, PairedRelationRequiresTwoDistinctChains) {
    auto integer=std::make_shared<Int64Type>();
    auto tables=test::Table({{test::Raw({1,1,2}),test::Raw({10,10,11})}},{{"a",integer},{"b",integer}});
    auto add=[&](std::string name,TableRegistry table){tables.emplace(name,std::move(table.at("t")));};
    add("a",test::Table({{test::Raw({1,2})}},{{"k",integer}}));
    add("b",test::Table({{test::Raw({10,11})}},{{"k",integer}}));
    add("p",test::Table({{test::Raw({1,1,2,2}),test::Raw({10,11,10,11})}},{{"a",integer},{"b",integer}}));
    auto q=ParseSelect("SELECT COUNT(*) FROM t,a,b,p WHERE t.a=a.k AND t.b=b.k AND p.a=a.k AND p.b=b.k",tables);
    ASSERT_TRUE(q.ok());Budget budget(1<<20);IndependentJoinPlan plan;
    EXPECT_FALSE(plan.prepare(*q,0,budget,true,true));
    EXPECT_TRUE(plan.prepare(*q,0,budget,true,true,3));
    EXPECT_EQ(plan.order.size(),2);EXPECT_EQ(plan.armOf[3],-1);
    EXPECT_FALSE(plan.prepare(*q,0,budget,true,true,0));
    EXPECT_FALSE(plan.prepare(*q,0,budget,true,true,4));
}
