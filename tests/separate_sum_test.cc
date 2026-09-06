#include "convergent_fixture.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteSeparateSum(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
namespace {
TableRegistry SeparateTables(bool duplicate=false) {
    auto tables=test::ConvergentTables(duplicate);
    std::vector<int64_t> orders,suppliers,amounts;std::vector<int32_t> days;
    for(int i=0;i<259;++i) {
        orders.push_back(i%11<8?100+2*(i%11):999);
        suppliers.push_back(20+i%7);amounts.push_back(i-130);days.push_back(-366+(i%5)*366);
    }
    auto fact=test::Table({{test::Raw(orders,4),test::Raw(suppliers,4),test::Raw(amounts,4),test::Dict<DateType>(days)},
                          {test::Raw(orders,4),test::Raw(suppliers,4),test::Raw(amounts,4),test::Dict<DateType>(days)}},
        {{"o",std::make_shared<Int64Type>()},{"s",std::make_shared<Int64Type>()},
         {"x",std::make_shared<Int64Type>()},{"day",std::make_shared<DateType>()}});
    tables.at("t")=std::move(fact.at("t"));return tables;
}
std::string SeparateSQL(const std::string &extra="") {
    return "SELECT n1.label AS a,n2.label AS b,EXTRACT(year FROM t.day) AS y,SUM(t.x*(1-0.05)) AS amount "
        "FROM t,o,c,s,n n1,n n2 WHERE t.o=o.k AND o.c=c.k AND t.s=s.k AND c.n=n1.k AND s.n=n2.k "
        "AND ((n1.label='same' AND n2.label='other') OR (n1.label='other' AND n2.label='same'))"+extra+
        " GROUP BY n1.label,n2.label,EXTRACT(year FROM t.day) ORDER BY a,b,y";
}
}
TEST(SeparateSum, CoupledLabelsYearsFiltersAndSlicesRemainExact) {
    auto tables=SeparateTables();
    for(const std::string filter:{"", " AND o.keep=1", " AND t.day>=CAST('1970-01-01' AS date)",
                                 " AND t.x>0", " AND n1.k<0", " AND n2.k<0", " AND n1.k<-10"})
    for(const std::string slice:{"", " LIMIT 3 OFFSET 1"}) {
        SCOPED_TRACE(filter+slice);
        auto q=ParseSelect(SeparateSQL(filter)+slice,tables);ASSERT_TRUE(q.ok())<<q.status().Message();
        auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
        for(bool avx:{false,true})for(bool parallel:{false,true}) {
            auto actual=ExecuteSeparateSum(*q,avx,parallel);ASSERT_TRUE(actual.has_value());
            ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
        }
    }
}
TEST(SeparateSum, DuplicateKeysAndExtraConstraintsRetainFallback) {
    for(bool duplicate:{false,true}) {
        auto tables=SeparateTables(duplicate);auto q=ParseSelect(SeparateSQL(duplicate?"":" AND c.n=s.n"),tables);
        ASSERT_TRUE(q.ok());EXPECT_FALSE(ExecuteSeparateSum(*q,true,true).has_value());
        auto expected=ExecuteRelational(*q,false,false),actual=ExecuteQuery(*q,true,true);
        ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
    }
}
TEST(SeparateSum, EnforcesAllocationLimit) {
    auto tables=SeparateTables();auto parsed=ParseSelect(SeparateSQL(),tables);ASSERT_TRUE(parsed.ok());
    auto q=std::move(parsed).ValueUnsafe();q.memoryBudgetBytes=1;q.allowSpill=false;
    auto result=ExecuteSeparateSum(q,true,true);ASSERT_TRUE(result.has_value());EXPECT_FALSE(result->ok());
}
