#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace pgaccel { std::optional<Result<QueryOutput>> ExecuteCorrelatedSum(const QueryDesc &, bool, bool); }
namespace {
QueryDesc GenericSum(const QueryDesc &q) {
    auto reference = q;
    reference.scalarQueries[0].query = std::make_shared<QueryDesc>(*q.scalarQueries[0].query);
    auto truth = std::make_shared<Expression>(); truth->type = std::make_shared<Int64Type>(); truth->literal = "1";
    reference.scalarQueries[0].query->predicates.push_back(truth);
    return reference;
}
}
TEST(CorrelatedSum, CompositeKeysOuterDuplicatesDatesAndMissingGroups) {
    for (int64_t base : {int64_t(0), int64_t(-10), INT64_MIN + 10}) for (int width : {4, 8}) {
        if (width == 4 && base < INT32_MIN) continue;
        std::vector<int64_t> keys;
        std::vector<std::string> names;
        for (int k = 1; k <= 64; ++k) { keys.push_back(base + k); names.push_back(k <= 2 ? "forest tree" : "other"); }
        auto registry = test::Table({{test::Raw({base+1,base+1,base+1,base+2},width),
            test::Raw({1,1,2,1}),test::Raw({7,3,1,10})},
            {test::Raw({base+2,base+3,base+1,base+2},width),
            test::Raw({2,1,1,3}),test::Raw({20,99,7,1})}},
            {{"pk",std::make_shared<Int64Type>()},{"sk",std::make_shared<Int64Type>()},{"avail",std::make_shared<Int64Type>()}});
        auto parts = test::Table({{test::Raw(keys,width),test::Dict<StringType>(names)},
            {test::Raw({base+1,base+65},width),test::Dict<StringType>({"forest duplicate","forest unused"})}},
            {{"k",std::make_shared<Int64Type>()},{"name",std::make_shared<StringType>()}});
        registry.emplace("p",std::move(parts.at("t")));
        auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
        auto date=std::make_shared<DateType>();
        auto facts = test::Table({{test::Raw({base+1,base+1,base+1,base+2,base+2,base+3,
            width==4?INT32_MIN:INT64_MIN,width==4?INT32_MAX:INT64_MAX},width),
            test::Raw({1,1,2,1,2,1,1,1}),test::Raw({400,600,200,1000,10000,9900,10000,10000}),
            test::Dict<DateType>({ParseDate("1994-01-01"),ParseDate("1994-12-31"),ParseDate("1994-06-01"),
                ParseDate("1994-06-01"),ParseDate("1993-12-31"),ParseDate("1994-06-01"),ParseDate("1994-06-01"),ParseDate("1994-06-01")})},
            {test::Raw({base+1,base+1,base+2},width),test::Raw({99,1,3}),test::Raw({99900,200,0}),test::Dict<DateType>({ParseDate("1994-06-01"),ParseDate("1994-06-01"),ParseDate("1994-06-01")})}},
            {{"pk",std::make_shared<Int64Type>()},{"sk",std::make_shared<Int64Type>()},{"qty",decimal},{"day",date}});
        registry.emplace("f",std::move(facts.at("t")));
        for (bool reverse : {false,true}) for (const std::string op : {">",">=","<","="}) {
            auto correlation=reverse?"a.sk=t.sk AND a.pk=t.pk":"a.pk=t.pk AND a.sk=t.sk";
            auto sql="SELECT pk,sk,avail FROM t WHERE pk IN (SELECT k FROM p WHERE name LIKE 'forest%') AND avail "+op+
                " (SELECT 0.5*sum(a.qty) FROM f a WHERE "+correlation+
                " AND a.day>=DATE '1994-01-01' AND a.day<DATE '1995-01-01') ORDER BY pk,sk,avail";
            auto q=ParseSelect(sql,registry);ASSERT_TRUE(q.ok())<<q.status().Message();
            auto fast=ExecuteCorrelatedSum(*q,true,true);ASSERT_TRUE(fast);ASSERT_TRUE(fast->ok())<<fast->status().Message();
            auto expected=ExecuteQuery(GenericSum(*q),false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto result=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(result.ok())<<result.status().Message();
                EXPECT_EQ(result->values,expected->values)<<op<<" reverse="<<reverse;
            }
            if(op==">")EXPECT_EQ(expected->values,Rows({{std::to_string(base+1),"1","7"},{std::to_string(base+1),"1","7"},{std::to_string(base+2),"1","10"},{std::to_string(base+2),"3","1"}}));
            auto memberSql=sql.substr(0,sql.find(" ORDER BY"));
            memberSql.replace(7,11,"pk");
            auto parsedNumeric=ParseSelect(memberSql,registry);
            ASSERT_TRUE(parsedNumeric.ok())<<parsedNumeric.status().Message();
            auto numericQuery=*parsedNumeric;
            numericQuery.numericMembershipOutput=true;
            auto typed=ExecuteCorrelatedSum(numericQuery,true,true);
            ASSERT_TRUE(typed); ASSERT_TRUE(typed->ok());
            ASSERT_TRUE((*typed)->numericMembership);
            auto normal=ExecuteQuery(GenericSum(numericQuery),false,false);
            ASSERT_TRUE(normal.ok())<<normal.status().Message();
            std::vector<__int128_t> expectedKeys;
            for(const auto &row:normal->values) expectedKeys.push_back(std::stoll(row[0]));
            auto actualKeys=*(*typed)->numericMembership;
            std::sort(expectedKeys.begin(),expectedKeys.end());
            std::sort(actualKeys.begin(),actualKeys.end());
            EXPECT_EQ(actualKeys,expectedKeys);
            auto ordered=*q; ordered.numericMembershipOutput=true;
            auto orderedResult=ExecuteQuery(ordered,true,true);
            ASSERT_TRUE(orderedResult.ok()); EXPECT_FALSE(orderedResult->numericMembership);
            EXPECT_EQ(orderedResult->values,expected->values);
            auto parent=ParseSelect("SELECT k FROM p WHERE k IN ("+memberSql+") ORDER BY k",registry);
            ASSERT_TRUE(parent.ok())<<parent.status().Message();
            auto parentReference=*parent;
            parentReference.membershipQueries[0].query=std::make_shared<QueryDesc>(GenericSum(*parent->membershipQueries[0].query));
            auto parentActual=ExecuteQuery(*parent,true,true), parentExpected=ExecuteQuery(parentReference,false,false);
            ASSERT_TRUE(parentActual.ok())<<parentActual.status().Message(); ASSERT_TRUE(parentExpected.ok());
            EXPECT_EQ(parentActual->values,parentExpected->values);
            auto emptySql=sql; emptySql.replace(emptySql.find("forest%"),7,"absent%");
            auto empty=ParseSelect(emptySql,registry);ASSERT_TRUE(empty.ok());
            auto emptyResult=ExecuteQuery(*empty,true,true);ASSERT_TRUE(emptyResult.ok());EXPECT_TRUE(emptyResult->values.empty());
            auto childLimited=*q;
            childLimited.scalarQueries[0].query=std::make_shared<QueryDesc>(*q->scalarQueries[0].query);
            childLimited.scalarQueries[0].query->memoryBudgetBytes=1;
            EXPECT_FALSE(ExecuteCorrelatedSum(childLimited,true,true));
            EXPECT_FALSE(ExecuteQuery(childLimited,true,true).ok());
            auto sourceLimited=*q;
            sourceLimited.membershipQueries[0].query=std::make_shared<QueryDesc>(*q->membershipQueries[0].query);
            sourceLimited.membershipQueries[0].query->memoryBudgetBytes=1;
            EXPECT_FALSE(ExecuteCorrelatedSum(sourceLimited,true,true));
            EXPECT_FALSE(ExecuteQuery(sourceLimited,true,true).ok());
            auto limited=ParseSelect(sql+" LIMIT 1 OFFSET 1",registry);ASSERT_TRUE(limited.ok());
            auto actualLimit=ExecuteQuery(*limited,true,true),expectedLimit=ExecuteQuery(GenericSum(*limited),false,false);
            ASSERT_TRUE(actualLimit.ok());ASSERT_TRUE(expectedLimit.ok());EXPECT_EQ(actualLimit->values,expectedLimit->values);
        }
    }
}
