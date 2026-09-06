#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteCorrelatedMin(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
namespace {
TableRegistry MinTables(int width,bool duplicate,int64_t low) {
    std::vector<int64_t> keys;for(int k=1;k<=64;++k)keys.push_back(k);
    auto registry=test::Table({{test::Raw(keys,width)}},{{"k",std::make_shared<Int64Type>()}});
    auto part=std::move(registry.at("t"));registry.clear();registry.emplace("p",std::move(part));
    auto add=[&](const std::string &name,auto table){registry.emplace(name,std::move(table.at("t")));};
    add("s",test::Table({{test::Raw({10,duplicate?10:11},width),test::Raw({100,100},width)},
                        {test::Raw({12,13},width),test::Raw({101,102},width)}},
        {{"k",std::make_shared<Int64Type>()},{"nk",std::make_shared<Int64Type>()}}));
    add("n",test::Table({{test::Raw({100,101,102},width),test::Raw({200,201,200},width)}},
        {{"k",std::make_shared<Int64Type>()},{"rk",std::make_shared<Int64Type>()}}));
    add("r",test::Table({{test::Raw({200,201},width),test::Dict<StringType>({"west","east"})}},
        {{"k",std::make_shared<Int64Type>()},{"name",std::make_shared<StringType>()}}));
    auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
    add("ps",test::Table({{test::Raw({1,1,1,1,1,2},width),test::Raw({10,11,12,13,10,10},width),test::Raw({50,50,40,70,50,20})},
                         {test::Raw({2,2,2,3,1},width),test::Raw({12,13,11,10,999},width),test::Raw({20,low,0,1,-100})}},
        {{"pk",std::make_shared<Int64Type>()},{"sk",std::make_shared<Int64Type>()},{"cost",decimal}}));
    return registry;
}
std::string MinSql(const std::string &outer,const std::string &inner,const std::string &extra="") {
    return "SELECT p.k,s.k,n.k,r.name,ps.cost FROM p,s,ps,n,r WHERE p.k=ps.pk AND s.k=ps.sk "
        "AND s.nk=n.k AND n.rk=r.k AND p.k<=2 AND r.name='"+outer+"' AND ps.cost="
        "(SELECT min(ps2.cost) FROM ps ps2,s s2,n n2,r r2 WHERE ps2.sk=s2.k AND s2.nk=n2.k "
        "AND n2.rk=r2.k AND p.k=ps2.pk AND r2.name='"+inner+"'"+extra+") ORDER BY p.k,s.k,ps.cost";
}
}
TEST(CorrelatedMin, TiesDuplicateFactsDistinctDomainsAndMissingKeys) {
    for (int width:{4,8}) for (int64_t low:{int64_t(-5),INT64_MIN}) {
        auto registry=MinTables(width,false,low);
        for (const std::string outer:{"west","east"}) for (const std::string inner:{"west","east","absent"})
            for (const std::string extra:{""," AND ps2.sk!=13"}) {
                auto q=ParseSelect(MinSql(outer,inner,extra),registry);ASSERT_TRUE(q.ok())<<q.status().Message();
                auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
                for (bool avx:{false,true}) for (bool parallel:{false,true}) {
                    auto fast=ExecuteCorrelatedMin(*q,avx,parallel);ASSERT_TRUE(fast);
                    ASSERT_TRUE(fast->ok())<<fast->status().Message();EXPECT_EQ((*fast)->values,expected->values);
                }
                auto sliced=ParseSelect(MinSql(outer,inner,extra)+" LIMIT 2 OFFSET 1",registry);ASSERT_TRUE(sliced.ok());
                auto actual=ExecuteQuery(*sliced,true,true),reference=ExecuteRelational(*sliced,false,false);
                ASSERT_TRUE(actual.ok());ASSERT_TRUE(reference.ok());EXPECT_EQ(actual->values,reference->values);
            }
    }
}
TEST(CorrelatedMin, DuplicateDimensionAndBudgetFallbacks) {
    for (bool duplicate:{false,true}) {
        auto registry=MinTables(8,duplicate,-5);auto q=ParseSelect(MinSql("west","west"),registry);ASSERT_TRUE(q.ok());
        auto fast=ExecuteCorrelatedMin(*q,true,true);
        if (duplicate) EXPECT_FALSE(fast);else ASSERT_TRUE(fast);
        auto actual=ExecuteQuery(*q,true,true),expected=ExecuteRelational(*q,false,false);
        ASSERT_TRUE(actual.ok());ASSERT_TRUE(expected.ok());EXPECT_EQ(actual->values,expected->values);
        auto strict=*q;strict.scalarQueries[0].query=std::make_shared<QueryDesc>(*q->scalarQueries[0].query);
        strict.scalarQueries[0].query->memoryBudgetBytes=1;
        EXPECT_FALSE(ExecuteCorrelatedMin(strict,true,true));EXPECT_FALSE(ExecuteQuery(strict,true,true).ok());
        auto unordered=*q;unordered.orderBy.clear();EXPECT_FALSE(ExecuteCorrelatedMin(unordered,true,true));
        if (!duplicate) {
            auto tiny=*q;tiny.memoryBudgetBytes=1;tiny.allowSpill=false;
            auto failed=ExecuteCorrelatedMin(tiny,true,true);ASSERT_TRUE(failed);EXPECT_FALSE(failed->ok());
        }
    }
}
