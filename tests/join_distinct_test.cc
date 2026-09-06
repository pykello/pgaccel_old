#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteJoinDistinct(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
TEST(JoinDistinct, ExactGroupsMembershipDuplicatesAndSlices) {
    for(bool duplicate:{false,true}) {
        auto registry=test::Table({{test::Raw({4,5,6}),test::Dict<StringType>({"b","a","b"}),test::Raw({2,1,2})},
            {test::Raw({1,2,duplicate?2:3}),test::Dict<StringType>({"a","b","a"}),test::Raw({1,2,1})}},
            {{"k",std::make_shared<Int64Type>()},{"brand",std::make_shared<StringType>()},{"size",std::make_shared<Int64Type>()}});
        auto facts=test::Table({{test::Raw({1,1,2,4,9}),test::Raw({7,7,8,8,9})},
            {test::Raw({3,5,6,2,1}),test::Raw({7,10,11,11,12})}},
            {{"k",std::make_shared<Int64Type>()},{"s",std::make_shared<Int64Type>()}});
        auto excluded=test::Table({{test::Raw({11,11,99})}},{{"s",std::make_shared<Int64Type>()}});
        registry.emplace("f",std::move(facts.at("t")));registry.emplace("x",std::move(excluded.at("t")));
        for(const std::string filter:{"", " AND t.brand NOT LIKE '%b%'", " AND (t.size=1 OR t.size=2)", " AND t.size<0"})
        for(const std::string membership:{"", " AND f.s NOT IN (SELECT s FROM x)", " AND f.s IN (SELECT s FROM x)",
                " AND f.s NOT IN (SELECT s FROM x WHERE s<0)"})
        for(const std::string slice:{"", " LIMIT 1 OFFSET 1"}) {
            auto q=ParseSelect("SELECT t.brand,t.size,count(DISTINCT f.s) AS n FROM t,f WHERE t.k=f.k"+filter+membership+
                " GROUP BY t.brand,t.size ORDER BY n DESC,t.brand,t.size"+slice,registry);
            ASSERT_TRUE(q.ok())<<q.status().Message();auto reference=ExecuteRelational(*q,false,false);ASSERT_TRUE(reference.ok())<<reference.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto direct=ExecuteJoinDistinct(*q,avx,parallel);ASSERT_EQ(direct.has_value(),!duplicate);
                if(direct){ASSERT_TRUE(direct->ok())<<direct->status().Message();EXPECT_EQ((**direct).values,reference->values);}
                auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok())<<actual.status().Message();EXPECT_EQ(actual->values,reference->values);
            }
            if(!q->membershipQueries.empty()) {
                q->membershipQueries.front().query->memoryBudgetBytes=1;q->membershipQueries.front().query->allowSpill=false;
                EXPECT_FALSE(ExecuteQuery(*q,true,true).ok());
            }
        }
    }
}
TEST(JoinDistinct, SignedEndpointsAndUnsupportedDomains) {
    for(int64_t first:{INT64_MIN,INT64_MAX-2})for(bool wide:{false,true}) {
        auto registry=test::Table({{test::Raw({first,first+1,first+2}),test::Raw({-1,-1,0})}},
            {{"k",std::make_shared<Int64Type>()},{"g",std::make_shared<Int64Type>()}});
        auto facts=test::Table({{test::Raw({first,first+1,first+2,first}),test::Raw({first,first,first+1,wide?(first==INT64_MIN?INT64_MAX:INT64_MIN):first+2})}},
            {{"k",std::make_shared<Int64Type>()},{"s",std::make_shared<Int64Type>()}});
        registry.emplace("f",std::move(facts.at("t")));
        auto q=ParseSelect("SELECT t.g,count(DISTINCT f.s) FROM t,f WHERE t.k=f.k GROUP BY t.g ORDER BY t.g",registry);ASSERT_TRUE(q.ok());
        auto reference=ExecuteRelational(*q,false,false);ASSERT_TRUE(reference.ok());
        auto direct=ExecuteJoinDistinct(*q,true,true);EXPECT_EQ(direct.has_value(),!wide);
        auto actual=ExecuteQuery(*q,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,reference->values);
    }
}
