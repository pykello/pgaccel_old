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
        for(const std::string order:{" ORDER BY n DESC,t.brand,t.size", " ORDER BY t.brand DESC,t.size DESC,n", ""})
        for(const std::string slice:{"", " LIMIT 1 OFFSET 1"}) {
            auto q=ParseSelect("SELECT t.brand,t.size,count(DISTINCT f.s) AS n FROM t,f WHERE t.k=f.k"+filter+membership+
                " GROUP BY t.brand,t.size"+order+slice,registry);
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
TEST(JoinDistinct, MaskedGatherBoundsTailsAndMembershipRepresentations) {
    for(int64_t first:{INT64_MIN,INT64_MAX-8,int64_t(INT32_MIN),int64_t(INT32_MAX)-8}) {
        std::vector<int64_t> keys,groups,foreign,suppliers;
        for(int i=0;i<9;++i){keys.push_back(first+i);groups.push_back(i%3);}
        for(int i=0;i<259;++i) {
            foreign.push_back(i%13<9?first+i%13:(first==INT64_MIN?INT64_MAX:first==INT64_MAX-8?INT64_MIN:first==INT32_MIN?int64_t(INT32_MAX):int64_t(INT32_MIN)));
            suppliers.push_back(first+i%7);
        }
        auto registry=test::Table({{test::Raw(keys),test::Raw(groups)}},
            {{"k",std::make_shared<Int64Type>()},{"g",std::make_shared<Int64Type>()}});
        auto facts=test::Table({{test::Raw(foreign,first>=INT32_MIN && first<=INT32_MAX-8?4:8),test::Raw(suppliers,first>=INT32_MIN && first<=INT32_MAX-8?4:8)}},
            {{"k",std::make_shared<Int64Type>()},{"s",std::make_shared<Int64Type>()}});
        auto excluded=test::Table({{test::Raw({first,first+2,first+6,first+8,first<0?INT64_MAX:INT64_MIN})}},{{"s",std::make_shared<Int64Type>()}});
        registry.emplace("f",std::move(facts.at("t")));registry.emplace("x",std::move(excluded.at("t")));
        for(const std::string clause:{"", " AND f.s IN (SELECT s FROM x)", " AND f.s NOT IN (SELECT s FROM x)",
            " AND f.s IN (SELECT s FROM x WHERE s<0)", " AND f.s NOT IN (SELECT s FROM x WHERE s>0)",
            " AND f.s NOT IN (SELECT s FROM x WHERE s=0)"}) {
            auto q=ParseSelect("SELECT t.g,count(DISTINCT f.s) FROM t,f WHERE t.k=f.k"+clause+" GROUP BY t.g ORDER BY t.g",registry);ASSERT_TRUE(q.ok());
            auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto actual=ExecuteJoinDistinct(*q,avx,parallel);ASSERT_TRUE(actual);ASSERT_TRUE(actual->ok())<<actual->status().Message();
                EXPECT_EQ((**actual).values,expected->values);
            }
        }
    }
}
TEST(JoinDistinct, ParallelOutputWithHiddenOrderingAndLongLabels) {
    std::vector<int64_t> keys,suppliers;std::vector<std::string> labels;
    for(int i=0;i<2053;++i){keys.push_back(i+1);suppliers.push_back(i%11);labels.push_back(std::string(i%64,'x')+std::to_string(i));}
    auto registry=test::Table({{test::Raw(keys),test::Dict<StringType>(labels)}},
        {{"k",std::make_shared<Int64Type>()},{"g",std::make_shared<StringType>()}});
    auto facts=test::Table({{test::Raw(keys),test::Raw(suppliers)}},
        {{"k",std::make_shared<Int64Type>()},{"s",std::make_shared<Int64Type>()}});
    registry.emplace("f",std::move(facts.at("t")));
    for(const std::string projection:{"t.g,count(DISTINCT f.s)","count(DISTINCT f.s)"})
    for(const std::string slice:{"", " LIMIT 1500 OFFSET 259"}) {
        auto q=ParseSelect("SELECT "+projection+" FROM t,f WHERE t.k=f.k GROUP BY t.g ORDER BY t.g DESC"+slice,registry);ASSERT_TRUE(q.ok());
        auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok());
        for(bool parallel:{false,true}) {
            auto actual=ExecuteJoinDistinct(*q,true,parallel);ASSERT_TRUE(actual);ASSERT_TRUE(actual->ok())<<actual->status().Message();
            EXPECT_EQ((**actual).values,expected->values);
        }
    }
}
