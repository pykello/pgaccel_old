#include "convergent_fixture.hpp"
#include "executor_convergent_plan.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;

TEST(ConvergentPlan, ProvesTransitiveJoinsAndShuffledSparseKeys) {
    auto tables=test::ConvergentTables();
    auto q=ParseSelect(test::ConvergentSQL(),tables);ASSERT_TRUE(q.ok());
    int fact=q->aggregateClauses[1].expression->left->column->tableIdx;
    int center=q->groupExpressions.front()->column->tableIdx;
    for(bool avx:{false,true})for(bool parallel:{false,true}) {
        Budget budget(1<<20);ConvergentJoinPlan plan;
        ASSERT_TRUE(plan.prepare(*q,fact,center,budget,avx,parallel));
        EXPECT_EQ(plan.order.size(),5);EXPECT_EQ(plan.outgoing[fact].size(),2);
        EXPECT_EQ(plan.keys[center]->sequence.minimum(),-1);
        EXPECT_EQ(plan.keys[center]->sequence.size(),3);
    }
}
TEST(ConvergentPlan, DuplicateKeysAndAdditionalConstraintsKeepFallback) {
    for(bool duplicate:{false,true}) {
        auto tables=test::ConvergentTables(duplicate);
        auto q=ParseSelect(test::ConvergentSQL(duplicate?"":" AND o.c=s.k"),tables);
        ASSERT_TRUE(q.ok());
        int fact=q->aggregateClauses[1].expression->left->column->tableIdx;
        int center=q->groupExpressions.front()->column->tableIdx;
        Budget budget(1<<20);ConvergentJoinPlan plan;
        EXPECT_FALSE(plan.prepare(*q,fact,center,budget,true,true));
    }
}
