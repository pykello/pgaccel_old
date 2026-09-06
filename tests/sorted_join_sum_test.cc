#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteSortedJoinSum(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
namespace {
TableRegistry JoinedSums(bool duplicate=false,bool unsorted=false) {
    auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
    auto registry=test::Table({{test::Raw({2,3,3,unsorted?0:4}),test::Raw({10,11,12,12}),test::Raw({5,6,-2,7}),test::Raw({2,3,4,1})},
        {test::Raw({-1,0,2}),test::Raw({10,999,12}),test::Raw({2,3,4}),test::Raw({1,1,2})},
        {test::Raw({2,2}),test::Raw({10,11}),test::Raw({6,-1}),test::Raw({2,2})}},
        {{"g",std::make_shared<Int64Type>()},{"sk",std::make_shared<Int64Type>()},{"x",decimal},{"y",std::make_shared<Int64Type>()}});
    auto facts=std::move(registry.at("t"));registry.clear();registry.emplace("f",std::move(facts));
    auto suppliers=test::Table({{test::Raw({10,duplicate?10:11,12}),test::Raw({20,21,20})}},
        {{"k",std::make_shared<Int64Type>()},{"nk",std::make_shared<Int64Type>()}});
    registry.emplace("s",std::move(suppliers.at("t")));
    auto nations=test::Table({{test::Raw({20,21}),test::Dict<StringType>({"keep","drop"})}},
        {{"k",std::make_shared<Int64Type>()},{"name",std::make_shared<StringType>()}});
    registry.emplace("n",std::move(nations.at("t")));return registry;
}
std::string JoinedSql(const std::string &country,const std::string &op,const std::string &factor,const std::string &inner="") {
    auto where=[](const std::string &c){return " FROM f,s,n WHERE f.sk=s.k AND s.nk=n.k AND n.name='"+c+"'";};
    return "SELECT f.g,sum(f.x*f.y)"+where(country)+" GROUP BY f.g HAVING sum(f.x*f.y) "+op+
        " (SELECT sum(f.x*f.y)*"+factor+where(inner.empty()?country:inner)+") ORDER BY sum(f.x*f.y) DESC,f.g";
}
}
TEST(SortedJoinSum, SharedTotalsBoundariesDecimalThresholdsAndEmptyDomains) {
    auto registry=JoinedSums();
    for (const std::string country:{"keep","drop","absent"})
        for (const std::string op:{">",">=","<","<=","=","!="})
            for (const std::string factor:{"0.1000000000","-0.1000000000","1.0"}) {
                auto sql=JoinedSql(country,op,factor);auto q=ParseSelect(sql,registry);ASSERT_TRUE(q.ok())<<q.status().Message();
                auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
                for (bool avx:{false,true}) for (bool parallel:{false,true}) {
                    auto fast=ExecuteSortedJoinSum(*q,avx,parallel);ASSERT_TRUE(fast);
                    ASSERT_TRUE(fast->ok())<<fast->status().Message();EXPECT_EQ((*fast)->values,expected->values);
                }
                auto sliced=ParseSelect(sql+" LIMIT 1 OFFSET 1",registry);ASSERT_TRUE(sliced.ok());
                auto actual=ExecuteQuery(*sliced,true,true),reference=ExecuteRelational(*sliced,false,false);
                ASSERT_TRUE(actual.ok());ASSERT_TRUE(reference.ok());EXPECT_EQ(actual->values,reference->values);
            }
}
TEST(SortedJoinSum, DifferentInputsDuplicateDimensionsAndUnsortedKeysFallback) {
    for (bool duplicate:{false,true}) for (bool unsorted:{false,true}) {
        auto registry=JoinedSums(duplicate,unsorted);
        auto q=ParseSelect(JoinedSql("keep",">","0.1"),registry);ASSERT_TRUE(q.ok());
        auto fast=ExecuteSortedJoinSum(*q,true,true);
        if (duplicate || unsorted) EXPECT_FALSE(fast);else ASSERT_TRUE(fast);
        auto actual=ExecuteQuery(*q,true,true),expected=ExecuteRelational(*q,false,false);
        ASSERT_TRUE(actual.ok());ASSERT_TRUE(expected.ok());EXPECT_EQ(actual->values,expected->values);
        auto different=ParseSelect(JoinedSql("keep",">","0.1","drop"),registry);ASSERT_TRUE(different.ok());
        EXPECT_FALSE(ExecuteSortedJoinSum(*different,true,true));
        auto strict=*q;strict.scalarQueries[0].query=std::make_shared<QueryDesc>(*q->scalarQueries[0].query);
        strict.scalarQueries[0].query->memoryBudgetBytes=1;
        EXPECT_FALSE(ExecuteSortedJoinSum(strict,true,true));EXPECT_FALSE(ExecuteQuery(strict,true,true).ok());
        if (!duplicate && !unsorted) {
            auto tiny=*q;tiny.memoryBudgetBytes=1;tiny.allowSpill=false;
            auto failed=ExecuteSortedJoinSum(tiny,true,true);ASSERT_TRUE(failed);EXPECT_FALSE(failed->ok());
        }
    }
}
