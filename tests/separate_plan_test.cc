#include "convergent_fixture.hpp"
#include "executor_separate_plan.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;

TEST(SeparatePlan, IndependentAliasesAndSparseIntermediateKeys) {
    auto tables=test::ConvergentTables();
    auto q=ParseSelect("SELECT n1.label,n2.label,SUM(t.x) FROM t,o,c,s,n n1,n n2 "
        "WHERE t.o=o.k AND o.c=c.k AND t.s=s.k AND c.n=n1.k AND s.n=n2.k "
        "GROUP BY n1.label,n2.label",tables);ASSERT_TRUE(q.ok());
    int fact=q->aggregateClauses[2].expression->column->tableIdx;
    std::array<int,2> centers{q->groupExpressions[0]->column->tableIdx,q->groupExpressions[1]->column->tableIdx};
    for(bool avx:{false,true})for(bool parallel:{false,true}) {
        Budget budget(1<<20);SeparateJoinPlan plan;
        ASSERT_TRUE(plan.prepare(*q,fact,centers,budget,avx,parallel));
        EXPECT_EQ(plan.order.size(),5);
        EXPECT_NE(plan.centerOf[plan.outgoing[fact][0].target],plan.centerOf[plan.outgoing[fact][1].target]);
        // Reusing a proof object must not retain its previous graph.
        ASSERT_TRUE(plan.prepare(*q,fact,centers,budget,avx,parallel));
    }
}
TEST(SeparatePlan, SharedTerminalAndUnconsumedEqualityKeepFallback) {
    auto tables=test::ConvergentTables();
    auto q=ParseSelect("SELECT n1.label,n2.label,SUM(t.x) FROM t,o,c,s,n n1,n n2 "
        "WHERE t.o=o.k AND o.c=c.k AND t.s=s.k AND c.n=n1.k AND s.n=n2.k AND c.n=s.n "
        "GROUP BY n1.label,n2.label",tables);ASSERT_TRUE(q.ok());
    int fact=q->aggregateClauses[2].expression->column->tableIdx;
    std::array<int,2> centers{q->groupExpressions[0]->column->tableIdx,q->groupExpressions[1]->column->tableIdx};
    Budget budget(1<<20);SeparateJoinPlan plan;
    EXPECT_FALSE(plan.prepare(*q,fact,centers,budget,true,true));
}
