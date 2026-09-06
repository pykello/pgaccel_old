#include "convergent_fixture.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteIndependentRatio(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
namespace {
TableRegistry RatioTables(bool duplicate=false) {
    auto tables=test::ConvergentTables(duplicate);auto integer=std::make_shared<Int64Type>();
    std::vector<int64_t> orders,suppliers,parts,amounts;
    for(int i=0;i<521;++i){orders.push_back(100+2*(i%11));suppliers.push_back(20+i%7);parts.push_back(30+i%5);amounts.push_back(i+1);}
    auto fact=test::Table({{test::Raw(orders,4),test::Raw(suppliers,4),test::Raw(parts,4),test::Raw(amounts,4)},
                         {test::Raw(orders,4),test::Raw(suppliers,4),test::Raw(parts,4),test::Raw(amounts,4)}},
        {{"o",integer},{"s",integer},{"p",integer},{"x",integer}});
    tables.at("t")=std::move(fact.at("t"));
    auto order=test::Table({{test::Raw({108,110,112,114}),test::Raw({14,15,10,10}),test::Dict<DateType>({0,365,0,-1})},
                            {test::Raw({100,102,104,106}),test::Raw({10,11,12,13}),test::Dict<DateType>({-366,-1,0,365})}},
        {{"k",integer},{"c",integer},{"day",std::make_shared<DateType>()}});
    tables.at("o")=std::move(order.at("t"));
    auto part=test::Table({{test::Raw({30,31,32,33}),test::Raw({1,0,1,0})}},{{"k",integer},{"keep",integer}});
    tables.emplace("p",std::move(part.at("t")));return tables;
}
std::string RatioSQL(std::string extra="",std::string label="other") {
    return "SELECT EXTRACT(year FROM o.day) AS y,SUM(CASE WHEN n2.label='"+label+"' THEN t.x*(1-0.05) ELSE 0 END) / "
        "SUM(t.x*(1-0.05)) AS share FROM t,o,c,s,n n1,n n2,r,p WHERE t.o=o.k AND o.c=c.k "
        "AND c.n=n1.k AND n1.r=r.k AND t.s=s.k AND s.n=n2.k AND t.p=p.k AND p.keep=1 AND r.keep=1"+extra+
        " GROUP BY EXTRACT(year FROM o.day) ORDER BY y";
}
}
TEST(IndependentRatio, ThreeArmsDatesConditionsAndSlicesMatchReference) {
    auto tables=RatioTables();
    for(const std::string extra:{"", " AND o.day>=CAST('1970-01-01' AS date)", " AND t.x>300", " AND p.k=30", " AND r.k<0", " AND n2.k<0"})
    for(const std::string label:{"other","same","missing"})for(const std::string slice:{"", " LIMIT 2 OFFSET 1"}) {
        SCOPED_TRACE(extra+label+slice);auto q=ParseSelect(RatioSQL(extra,label)+slice,tables);ASSERT_TRUE(q.ok())<<q.status().Message();
        auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
        for(bool avx:{false,true})for(bool parallel:{false,true}) {
            auto actual=ExecuteIndependentRatio(*q,avx,parallel);ASSERT_TRUE(actual.has_value());
            ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
        }
    }
}
TEST(IndependentRatio, DuplicateKeysAndCrossArmPredicatesRetainFallback) {
    for(bool duplicate:{false,true}) {
        auto tables=RatioTables(duplicate);auto q=ParseSelect(RatioSQL(duplicate?"":" AND n1.k=n2.k"),tables);ASSERT_TRUE(q.ok());
        EXPECT_FALSE(ExecuteIndependentRatio(*q,true,true).has_value());
        auto expected=ExecuteRelational(*q,false,false),actual=ExecuteQuery(*q,true,true);
        ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
    }
}
TEST(IndependentRatio, EnforcesQueryBudget) {
    auto tables=RatioTables();auto parsed=ParseSelect(RatioSQL(),tables);ASSERT_TRUE(parsed.ok());
    auto q=std::move(parsed).ValueUnsafe();q.memoryBudgetBytes=1;q.allowSpill=false;
    auto actual=ExecuteIndependentRatio(q,true,true);ASSERT_TRUE(actual.has_value());EXPECT_FALSE(actual->ok());
}
