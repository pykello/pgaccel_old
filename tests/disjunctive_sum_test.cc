#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel { std::optional<Result<QueryOutput>> ExecuteDisjunctiveSum(const QueryDesc &,bool,bool); }
using namespace pgaccel;
TEST(DisjunctiveSum, OverlappingBranchesPartialStripesMissingKeysAndDuplicates) {
    auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
    for(int width:{4,8})for(bool duplicate:{false,true}) {
        std::vector<std::vector<ColumnDataP>> groups;
        for(int g=0;g<3;++g) {
            std::vector<int64_t> keys,qty,price;std::vector<std::string> mode;
            for(int i=0;i<257;++i) {keys.push_back(i%15-2);qty.push_back((i%20)*100);price.push_back((i-10)*(g+1)*100);mode.push_back(i%3==0?"air":i%3==1?"rail":"road");}
            keys[6]=width==4?INT32_MIN:INT64_MIN;keys[12]=width==4?INT32_MAX:INT64_MAX;
            groups.push_back({test::Raw(keys,width),test::Raw(qty,2),test::Raw(price,4),test::Dict<StringType>(mode)});
        }
        auto registry=test::Table(std::move(groups),{{"k",std::make_shared<Int64Type>()},{"qty",decimal},{"price",decimal},{"mode",std::make_shared<StringType>()}});
        auto build=test::Table({{test::Raw({duplicate?2:9,10},1),test::Dict<StringType>({"a","b"})},
            {test::Raw({1,2,3,4},1),test::Dict<StringType>({"a","b","c","a"})},
            {test::Raw({5,6,7,8},1),test::Dict<StringType>({"b","a","c","b"})}},
            {{"k",std::make_shared<Int64Type>()},{"brand",std::make_shared<StringType>()}});
        registry.emplace("b",std::move(build.at("t")));
        for(const std::string brand:{"a","never"}) {
            auto q=ParseSelect("SELECT sum(f.price*(1-0.05)),count(*) FROM t f,b WHERE "
                "(b.k=f.k AND b.brand='"+brand+"' AND f.qty>=1 AND f.qty<=1+10 AND (f.mode='air' OR f.mode='rail')) OR "
                "(b.k=f.k AND b.brand='"+brand+"' AND f.qty>=5 AND f.qty<=15 AND f.mode='air')",registry);
            ASSERT_TRUE(q.ok())<<q.status().Message();auto direct=ExecuteDisjunctiveSum(*q,true,true);EXPECT_EQ(direct.has_value(),!duplicate);
            QueryDesc reference=*q;reference.predicates.push_back(reference.predicates.front());
            auto expected=ExecuteQuery(reference,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto actual=ExecuteQuery(*q,avx,parallel);ASSERT_TRUE(actual.ok())<<actual.status().Message();EXPECT_EQ(actual->values,expected->values);
            }
            QueryDesc limited=*q;limited.memoryBudgetBytes=1;limited.allowSpill=false;EXPECT_FALSE(ExecuteQuery(limited,true,true).ok());
        }
    }
}
