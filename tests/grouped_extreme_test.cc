#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteGroupedExtreme(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteMaterialized(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
TEST(GroupedExtreme, SharedTotalsPreserveTiesAndDimensionFilters) {
    for(bool duplicate:{false,true})for(int64_t missingValue:{10,50}) {
        auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
        auto registry=test::Table({{test::Raw({1,2,3,4,9,1,2,3,4}),test::Raw({5,3,2,1,missingValue,5,7,8,9})}},
            {{"k",std::make_shared<Int64Type>()},{"x",decimal}});
        auto dimension=test::Table({{test::Raw({1,2,duplicate?2:3,4}),test::Dict<StringType>({"same","same","three","four"})}},
            {{"k",std::make_shared<Int64Type>()},{"name",std::make_shared<StringType>()}});
        registry.emplace("d",std::move(dimension.at("t")));
        for(const std::string extreme:{"max","min"})
        for(const std::string sourceFilter:{"", " WHERE x<0.00"})
        for(const std::string suffix:{"", " AND d.k>2"}) {
            auto parsed=ParseSelect("WITH totals AS (SELECT k AS key,sum(x*(1-0.05)) AS total FROM t"+sourceFilter+
                " GROUP BY k) SELECT d.k,d.name,total FROM d,totals WHERE d.k=key AND total=(SELECT "+extreme+
                "(total) FROM totals)"+suffix+" ORDER BY d.k LIMIT 3 OFFSET 1",registry);
            ASSERT_TRUE(parsed.ok())<<parsed.status().Message();const auto &q=*parsed;
            auto expected=ExecuteMaterialized(q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto direct=ExecuteGroupedExtreme(q,avx,parallel);
                EXPECT_EQ(direct.has_value(),!duplicate)<<extreme<<sourceFilter<<suffix<<" predicates="<<q.predicates.size();
                if(direct){ASSERT_TRUE(direct->ok())<<direct->status().Message();EXPECT_EQ((**direct).values,expected->values);}
                auto actual=ExecuteQuery(q,avx,parallel);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
            }
        }
    }
}
TEST(GroupedExtreme, IndependentSourcesAndStrictChildBudgetsRetainFallback) {
    auto registry=test::Table({{test::Raw({1,2,3}),test::Raw({-2,0,-2})}},
        {{"k",std::make_shared<Int64Type>()},{"x",std::make_shared<Int64Type>()}});
    auto parsed=ParseSelect("WITH totals AS (SELECT k,sum(x) AS total FROM t GROUP BY k) "
        "SELECT t.k,total FROM t,totals WHERE t.k=totals.k AND total=(SELECT max(total) FROM totals) ORDER BY t.k",registry);
    ASSERT_TRUE(parsed.ok())<<parsed.status().Message();
    auto q=*parsed;auto actual=ExecuteGroupedExtreme(q,true,true);ASSERT_TRUE(actual);ASSERT_TRUE(actual->ok());
    EXPECT_EQ((**actual).values,(Rows{{"2","0"}}));
    q.scalarQueries[0].query=std::make_shared<QueryDesc>(*q.scalarQueries[0].query);
    q.scalarQueries[0].query->materializedSources[0].query=std::make_shared<QueryDesc>(*q.materializedSources[0].query);
    EXPECT_FALSE(ExecuteGroupedExtreme(q,true,true));
    auto reference=ExecuteQuery(q,true,true);ASSERT_TRUE(reference.ok());EXPECT_EQ(reference->values,(**actual).values);
    q=*parsed;q.materializedSources[0].query->memoryBudgetBytes=1;q.materializedSources[0].query->allowSpill=false;
    EXPECT_FALSE(ExecuteGroupedExtreme(q,true,true));EXPECT_FALSE(ExecuteQuery(q,true,true).ok());
}
