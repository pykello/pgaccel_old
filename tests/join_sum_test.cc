#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel { std::optional<Result<QueryOutput>> ExecuteJoinSum(const QueryDesc &,bool,bool); }
using namespace pgaccel;
TEST(JoinSum, ExactConditionalArithmeticMasksAndEmptyInputs) {
    auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
    std::vector<int64_t> keys{INT64_MIN,-5,0};
    for(int i=1;i<=17;++i)keys.push_back(i);
    keys.insert(keys.end(),{18,99,INT64_MAX});
    auto registry=test::Table({{test::Raw(keys),test::Raw(std::vector<int64_t>(keys.size(),1000),2)},
        {test::Raw(keys),test::Raw(std::vector<int64_t>(keys.size(),2000),2)}},
        {{"k",std::make_shared<Int64Type>()},{"x",decimal}});
    std::vector<std::vector<ColumnDataP>> groups;
    for(const auto [begin,end]:std::vector<std::pair<int,int>>{{17,18},{1,9},{9,17}}) {
        std::vector<int64_t> ids;std::vector<std::string> labels;
        for(int i=begin;i<end;++i){ids.push_back(i);labels.push_back(i%2?"PROMO A":"OTHER");}
        groups.push_back({test::Raw(ids,1),test::Dict<StringType>(labels)});
    }
    auto other=test::Table(std::move(groups),{{"k",std::make_shared<Int64Type>()},{"label",std::make_shared<StringType>()}});
    registry.emplace("d",std::move(other.at("t")));
    for(const std::string suffix:{"", " AND t.x<0.00", " AND d.k>3"})
    for(const std::string pattern:{"PROMO%","NEVER%"}) {
        std::string sum="sum(CASE WHEN d.label LIKE '"+pattern+"' THEN t.x*(1-0.05) ELSE 0 END)";
        auto q=ParseSelect("SELECT count(*),sum(t.x),"+sum+",100.00*"+sum+"/sum(t.x) FROM t,d WHERE t.k=d.k"+suffix,registry);
        ASSERT_TRUE(q.ok())<<q.status().Message();
        if(suffix.empty()) ASSERT_TRUE(ExecuteJoinSum(*q,true,true).has_value());
        QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
        auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
        for(bool avx:{false,true}) for(bool parallel:{false,true}) {
            auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok())<<actual.status().Message();
            EXPECT_EQ(actual->values,expected->values);
        }
    }
    auto q=ParseSelect("SELECT count(*) FROM t,d WHERE t.k=d.k",registry);ASSERT_TRUE(q.ok());
    QueryDesc limited=*q;limited.memoryBudgetBytes=1;limited.allowSpill=false;
    EXPECT_FALSE(ExecuteQuery(limited,true,true).ok());
}
TEST(JoinSum, DuplicateBuildAndLazyUnsafeBranchesRetainFallback) {
    auto registry=test::Table({{test::Raw({1,1,2,3,4,5,6,7,8,9}),test::Raw({1,2,3,4,5,6,7,8,9,10})}},
        {{"k",std::make_shared<Int64Type>()},{"v",std::make_shared<Int64Type>()}});
    auto other=test::Table({{test::Raw({1,1,2}),test::Dict<StringType>({"a","a","a"})}},
        {{"k",std::make_shared<Int64Type>()},{"label",std::make_shared<StringType>()}});
    registry.emplace("d",std::move(other.at("t")));
    for(const auto &[sql,rows]:std::vector<std::pair<std::string,Rows>>{
        {"SELECT count(*),sum(v) FROM t,d WHERE t.k=d.k",{{"5","9"}}},
        {"SELECT sum(CASE WHEN label LIKE 'z%' THEN v/0 ELSE 0 END) FROM t,d WHERE t.k=d.k",{{"0.000000"}}}}) {
        auto q=ParseSelect(sql,registry);ASSERT_TRUE(q.ok());
        auto actual=ExecuteQuery(*q,true,true);ASSERT_TRUE(actual.ok())<<actual.status().Message();
        EXPECT_EQ(actual->values,rows);
    }
}
