#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel { std::optional<Result<QueryOutput>> ExecuteStarSum(const QueryDesc &,bool,bool); }
using namespace pgaccel;
namespace {
TableRegistry StarTables(bool duplicate=false) {
    auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
    std::vector<int64_t> keys{INT64_MIN,-5,0,1,2,2,9,10,10,17,18,18,18,25,26,99,INT64_MAX};
    std::vector<std::vector<ColumnDataP>> facts;
    for(int g=0;g<3;++g) {
        std::vector<int64_t> values;for(int i=0;i<int(keys.size());++i)values.push_back((i-8)*(g+1)*100);
        facts.push_back({test::Raw(keys),test::Raw(values,2)});
    }
    auto registry=test::Table(std::move(facts),{{"k",std::make_shared<Int64Type>()},{"x",decimal}});
    auto bridge=test::Table({
        {test::Raw({17,duplicate?2:18,25,26}),test::Raw({4,5,99,-1}),test::Dict<DateType>({4,5,6,7})},
        {test::Raw({1,2,9,10}),test::Raw({0,1,2,3}),test::Dict<DateType>({0,1,2,3})}},
        {{"k",std::make_shared<Int64Type>()},{"c",std::make_shared<Int64Type>()},{"day",std::make_shared<DateType>()}});
    auto dimension=test::Table({{test::Raw({4,5}),test::Raw({0,1})},
        {test::Raw({0,1}),test::Raw({0,1})},{test::Raw({2,3}),test::Raw({0,1})}},
        {{"k",std::make_shared<Int64Type>()},{"keep",std::make_shared<Int64Type>()}});
    registry.emplace("b",std::move(bridge.at("t")));registry.emplace("c",std::move(dimension.at("t")));return registry;
}
}
TEST(StarSum, CascadedMembershipParallelGroupsDatesAndZeroSums) {
    auto registry=StarTables();
    for(const std::string suffix:{"", " AND f.x>=0.00", " AND f.x<-1000.00", " AND c.keep=2"})
    for(const std::string value:{"f.x*(1-0.05)","f.x-f.x"}) {
        auto q=ParseSelect("SELECT f.k,b.day,sum("+value+") AS revenue,count(*) FROM t f,b,c "
            "WHERE f.k=b.k AND b.c=c.k AND c.keep=1 AND b.day>=CAST('1970-01-01' AS date)"+suffix+
            " GROUP BY f.k,b.day ORDER BY revenue DESC,f.k LIMIT 10",registry);
        ASSERT_TRUE(q.ok())<<q.status().Message();
        auto direct=ExecuteStarSum(*q,true,true);ASSERT_TRUE(direct.has_value());ASSERT_TRUE(direct->ok())<<direct->status().Message();
        QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
        auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
        for(bool avx:{false,true})for(bool parallel:{false,true}) {
            auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok())<<actual.status().Message();
            EXPECT_EQ(actual->values,expected->values);
        }
    }
}
TEST(StarSum, DuplicateBridgeAndMissingGroupIdentityRetainFallback) {
    auto registry=StarTables(true);
    for(const std::string projection:{"f.k,b.day","b.day"}) {
        auto q=ParseSelect("SELECT "+projection+",sum(f.x),count(*) FROM t f,b,c "
            "WHERE f.k=b.k AND b.c=c.k AND c.keep=1 GROUP BY "+projection+" ORDER BY "+projection,registry);
        ASSERT_TRUE(q.ok())<<q.status().Message();EXPECT_FALSE(ExecuteStarSum(*q,true,true).has_value());
        QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
        auto expected=ExecuteQuery(reference,false,false);auto actual=ExecuteQuery(*q,true,true);
        ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
    }
    auto q=ParseSelect("SELECT f.k,sum(f.x) FROM t f,b,c WHERE f.k=b.k AND b.c=c.k AND c.keep=1 GROUP BY f.k",registry);
    ASSERT_TRUE(q.ok());QueryDesc limited=*q;limited.memoryBudgetBytes=1;limited.allowSpill=false;
    EXPECT_FALSE(ExecuteQuery(limited,true,true).ok());
}
