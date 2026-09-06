#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteLeftCountHistogram(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteMaterialized(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
TEST(LeftCountHistogram, UnmatchedRowsAndFilteredFactMultiplicity) {
    for(bool duplicate:{false,true}) {
        auto registry=test::Table({{test::Raw({duplicate?2:4,5,6})},{test::Raw({1,2,3})}},{{"k",std::make_shared<Int64Type>()}});
        auto orders=test::Table({{test::Raw({1,1,2,4,9}),test::Raw({1,2,3,4,5}),
                test::Dict<StringType>({"ordinary","special requests","requests special","specialxxrequests","missing"})},
            {test::Raw({1,2,2,3}),test::Raw({6,7,8,9}),test::Dict<StringType>({"normal","special requests","special\nrequests","n"})}},
            {{"k",std::make_shared<Int64Type>()},{"id",std::make_shared<Int64Type>()},{"comment",std::make_shared<StringType>()}});
        registry.emplace("o",std::move(orders.at("t")));
        for(const std::string filter:{"", " AND o.comment NOT LIKE '%special%requests%'", " AND o.id<0", " AND o.id>3"})
        for(const std::string slice:{"", " LIMIT 2 OFFSET 1"}) {
            auto q=ParseSelect("SELECT n,count(*) AS frequency FROM (SELECT c.k,count(o.id) AS n FROM t c "
                "LEFT JOIN o ON c.k=o.k"+filter+" GROUP BY c.k) d GROUP BY n ORDER BY frequency DESC,n DESC"+slice,registry);
            ASSERT_TRUE(q.ok())<<q.status().Message();auto expected=ExecuteMaterialized(*q,false,false);
            ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto direct=ExecuteLeftCountHistogram(*q,avx,parallel);EXPECT_EQ(direct.has_value(),!duplicate);
                if(direct){ASSERT_TRUE(direct->ok())<<direct->status().Message();EXPECT_EQ((**direct).values,expected->values);}
                auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
            }
            q->materializedSources[0].query->memoryBudgetBytes=1;q->materializedSources[0].query->allowSpill=false;
            EXPECT_FALSE(ExecuteLeftCountHistogram(*q,true,true));EXPECT_FALSE(ExecuteQuery(*q,true,true).ok());
        }
    }
}
TEST(LeftCountHistogram, WideDomainsTailsAndHistogramGrowth) {
    std::vector<int64_t> dimension,foreign,ids;
    for(int i=0;i<259;++i)dimension.push_back(INT64_MIN+i);
    for(int i=0;i<513;++i){foreign.push_back(INT64_MIN+i%3);ids.push_back(i);}
    foreign.push_back(INT64_MAX);ids.push_back(513);
    auto registry=test::Table({{test::Raw(dimension)}},{{"k",std::make_shared<Int64Type>()}});
    auto orders=test::Table({{test::Raw(foreign),test::Raw(ids)}},{{"k",std::make_shared<Int64Type>()},{"id",std::make_shared<Int64Type>()}});
    registry.emplace("o",std::move(orders.at("t")));
    auto q=ParseSelect("SELECT n,count(*) FROM (SELECT c.k,count(o.id) AS n FROM t c LEFT JOIN o ON c.k=o.k "
        "GROUP BY c.k) d GROUP BY n ORDER BY n",registry);ASSERT_TRUE(q.ok());
    auto actual=ExecuteLeftCountHistogram(*q,true,true);ASSERT_TRUE(actual);ASSERT_TRUE(actual->ok());
    EXPECT_EQ((**actual).values,(Rows{{"0","256"},{"171","3"}}));
}
