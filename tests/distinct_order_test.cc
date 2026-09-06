#include "test_table.hpp"
#include "../src/executor_distinct_order.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(DistinctOrder, NormalizedAndWideFallbackMatchMixedDirectionTuples) {
    auto registry=test::Table({{test::Dict<StringType>({"b","a","c"}),test::Raw({1,2,3})}},
        {{"g",std::make_shared<StringType>()},{"v",std::make_shared<Int64Type>()}});
    auto q=ParseSelect("SELECT g,count(DISTINCT v) AS n FROM t GROUP BY g ORDER BY n DESC,g DESC",registry);ASSERT_TRUE(q.ok());
    Budget budget(1<<20);SmallGroupKey groups(*q->tables[0],{*q->groupExpressions[0]->column},budget);
    for(uint64_t maximum:{uint64_t(10),UINT64_MAX}) {
        std::pmr::vector<uint64_t> counts({maximum-1,maximum,maximum-1},&budget);
        auto actual=OrderDistinctGroups(*q,groups,{0,-1},counts,budget);
        EXPECT_EQ(actual,(std::pmr::vector<uint32_t>{1,2,0}));
    }
    std::pmr::vector<uint64_t> counts(3,0,&budget);
    EXPECT_TRUE(OrderDistinctGroups(*q,groups,{0,-1},counts,budget).empty());
}
