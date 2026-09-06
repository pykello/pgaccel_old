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
