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

TEST(StarSum, SixteenLaneProbesMaskOutsideDomainAndPartialBlocks) {
    std::vector<int64_t> dimensionKeys,keep,bridgeKeys,customerKeys,factKeys,amounts;
    for(int i=0;i<258;++i) {dimensionKeys.push_back(i+7);keep.push_back(i%3!=0);}
    for(int i=0;i<257;++i) {
        bridgeKeys.push_back(100+i);customerKeys.push_back(i+6);
        factKeys.push_back(100+i);amounts.push_back(i-30);
    }
    for(int64_t key:{int64_t(INT32_MIN),int64_t(INT32_MAX),int64_t(99),int64_t(357)}) {
        factKeys.push_back(key);amounts.push_back(1);
    }
    for(int i=0;i<16;++i) {factKeys.push_back(INT32_MAX);amounts.push_back(1);}
    std::rotate(factKeys.begin(),factKeys.end()-20,factKeys.end());
    std::rotate(amounts.begin(),amounts.end()-20,amounts.end());
    auto registry=test::Table({{test::Raw(factKeys,4),test::Raw(amounts,4)},
        {test::Raw(factKeys,4),test::Raw(amounts,4)},
        {test::Raw(factKeys,4),test::Raw(amounts,4)}},
        {{"k",std::make_shared<Int64Type>()},{"x",std::make_shared<Int64Type>()}});
    auto bridge=test::Table({{test::Raw(bridgeKeys,4),test::Raw(customerKeys,4)}},
        {{"k",std::make_shared<Int64Type>()},{"c",std::make_shared<Int64Type>()}});
    auto dimension=test::Table({{test::Raw(dimensionKeys,4),test::Raw(keep,4)}},
        {{"k",std::make_shared<Int64Type>()},{"keep",std::make_shared<Int64Type>()}});
    registry.emplace("b",std::move(bridge.at("t")));registry.emplace("c",std::move(dimension.at("t")));
    for(const std::string suffix:{"", " LIMIT 7 OFFSET 3"})
    for(const std::string filter:{"", " AND f.x>=0", " AND b.k<150"}) {
        auto q=ParseSelect("SELECT f.k,sum(f.x),count(*) FROM t f,b,c "
            "WHERE f.k=b.k AND b.c=c.k AND c.keep=1"+filter+" GROUP BY f.k ORDER BY f.k"+suffix,registry);
        ASSERT_TRUE(q.ok())<<q.status().Message();
        auto direct=ExecuteStarSum(*q,true,true);ASSERT_TRUE(direct.has_value());ASSERT_TRUE(direct->ok());
        QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
        auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok());
        EXPECT_EQ((**direct).values,expected->values);
    }
}

TEST(StarSum, ProjectionOnlyGroupsNeedNoAggregateStorage) {
    auto registry=StarTables();
    auto q=ParseSelect("SELECT f.k,b.day FROM t f,b,c WHERE f.k=b.k AND b.c=c.k "
        "AND c.keep=1 GROUP BY f.k,b.day ORDER BY f.k",registry);
    ASSERT_TRUE(q.ok());auto actual=ExecuteStarSum(*q,true,true);
    ASSERT_TRUE(actual.has_value());ASSERT_TRUE(actual->ok());
    QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
    auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok());
    EXPECT_EQ((**actual).values,expected->values);
}

TEST(StarSum, DimensionProjectionUsesVerifiedBridgeIdentity) {
    for(bool duplicate:{false,true}) {
        auto registry=StarTables();
        auto dimension=test::Table({{test::Raw({0,1,duplicate?1:2,3,4,5}),
            test::Dict<StringType>({"zero","same","same","three","four","five"})}},
            {{"k",std::make_shared<Int64Type>()},{"name",std::make_shared<StringType>()}});
        registry.erase("c");registry.emplace("c",std::move(dimension.at("t")));
        for(const std::string identity:{"b.k,", ""})
        for(const std::string slice:{"", " LIMIT 2 OFFSET 1"}) {
            std::string keys=identity+"c.name,c.k,b.day";
            auto q=ParseSelect("SELECT "+keys+",sum(f.x),count(*) FROM t f,b,c "
                "WHERE f.k=b.k AND b.c=c.k GROUP BY "+keys+" ORDER BY "+keys+slice,registry);
            ASSERT_TRUE(q.ok())<<q.status().Message();
            QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
            auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok());
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto direct=ExecuteStarSum(*q,avx,parallel);
                EXPECT_EQ(direct.has_value(),!duplicate && !identity.empty());
                auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok())<<actual.status().Message();
                EXPECT_EQ(actual->values,expected->values);
            }
        }
    }
}

TEST(StarSum, SparseMembershipFindsGappedOrdersAndPreservesMultiplicity) {
    for(bool duplicate:{false,true}) {
        auto registry=StarTables(duplicate);
        auto members=test::Table({{test::Raw({2,2,18,25,99,INT64_MAX})}},{{"k",std::make_shared<Int64Type>()}});
        registry.emplace("m",std::move(members.at("t")));
        for(const std::string source:{"SELECT k FROM m", "SELECT k FROM m WHERE k<0",
                "SELECT k FROM m GROUP BY k HAVING sum(k)>3"})
        for(const std::string slice:{"", " LIMIT 1 OFFSET 1"}) {
            auto q=ParseSelect("SELECT b.k,c.k,b.day,sum(f.x),count(*) FROM t f,b,c "
                "WHERE f.k=b.k AND b.c=c.k AND b.k IN ("+source+") "
                "GROUP BY b.k,c.k,b.day ORDER BY b.k"+slice,registry);
            ASSERT_TRUE(q.ok())<<q.status().Message();
            QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
            auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto direct=ExecuteStarSum(*q,avx,parallel);
                if(!duplicate){ASSERT_TRUE(direct);ASSERT_TRUE(direct->ok())<<direct->status().Message();}
                auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok())<<actual.status().Message();
                EXPECT_EQ(actual->values,expected->values);
            }
            q->membershipQueries.front().query->memoryBudgetBytes=1;
            q->membershipQueries.front().query->allowSpill=false;
            EXPECT_FALSE(ExecuteQuery(*q,true,true).ok());
        }
    }
}

TEST(StarSum, HavingRetainsGeneralEvaluation) {
    auto registry=StarTables();
    auto q=ParseSelect("SELECT b.k,sum(f.x) AS total FROM t f,b,c WHERE f.k=b.k AND b.c=c.k "
        "GROUP BY b.k HAVING sum(f.x)>0.00 ORDER BY b.k",registry);
    ASSERT_TRUE(q.ok());EXPECT_FALSE(ExecuteStarSum(*q,true,true));
    QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
    auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok());
    auto actual=ExecuteQuery(*q,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
}
