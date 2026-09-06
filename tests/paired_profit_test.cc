#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecutePairedProfit(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
namespace {
TableRegistry ProfitTables(bool duplicatePairs=false,bool brokenRun=false) {
    auto integer=std::make_shared<Int64Type>();
    auto decimal=[](int scale){auto type=std::make_shared<DecimalType>();type->scale=scale;return type;};
    std::vector<int64_t> p,s,o,x,qty;
    for(int i=0;i<259;++i){p.push_back(10+i%6);s.push_back(20+i%6);o.push_back(100+2*(i%6));x.push_back(i-130);qty.push_back(i%7);}
    auto tables=test::Table({{test::Raw(p,4),test::Raw(s,4),test::Raw(o,4),test::Raw(x,4),test::Raw(qty,4)},
                            {test::Raw(p,4),test::Raw(s,4),test::Raw(o,4),test::Raw(x,4),test::Raw(qty,4)}},
        {{"p",integer},{"s",integer},{"o",integer},{"x",integer},{"qty",decimal(1)}});
    auto add=[&](std::string name,TableRegistry table){tables.emplace(name,std::move(table.at("t")));};
    add("p",test::Table({{test::Raw({10,11,12,13}),test::Raw({1,1,0,1})}},{{"k",integer},{"keep",integer}}));
    add("s",test::Table({{test::Raw({20,21,22,23}),test::Raw({-1,0,1,0})}},{{"k",integer},{"n",integer}}));
    add("n",test::Table({{test::Raw({-1,0,1}),test::Dict<StringType>({"same","same","other"})}},
        {{"k",integer},{"label",std::make_shared<StringType>()}}));
    add("o",test::Table({{test::Raw({104,106}),test::Dict<DateType>({365,0})},
                         {test::Raw({100,102}),test::Dict<DateType>({-1,0})}},
        {{"k",integer},{"day",std::make_shared<DateType>()}}));
    std::vector<std::vector<ColumnDataP>> frames;
    for(int stripe:{1,0}) {
        std::vector<int64_t> parts,suppliers,cost,keep;
        for(int i=0;i<8;++i) {
            int index=stripe*8+i;parts.push_back(10+index/4);suppliers.push_back(20+(duplicatePairs && index%4==2?1:index%4));
            cost.push_back(index*17-40);keep.push_back(index%3);
        }
        if(brokenRun && stripe==0)parts[2]=11;
        frames.push_back({test::Raw(parts,4),test::Raw(suppliers,4),test::Raw(cost,4),test::Raw(keep,4)});
    }
    add("ps",test::Table(std::move(frames),{{"p",integer},{"s",integer},{"cost",decimal(2)},{"keep",integer}}));
    return tables;
}
std::string ProfitSQL(std::string extra="") {
    return "SELECT n.label,EXTRACT(year FROM o.day) AS y,SUM(t.x*(1-0.05)-ps.cost*t.qty) AS profit "
        "FROM t,p,s,ps,o,n WHERE t.p=p.k AND t.p=ps.p AND t.s=s.k AND t.s=ps.s AND t.o=o.k AND s.n=n.k "
        "AND p.keep=1"+extra+" GROUP BY n.label,EXTRACT(year FROM o.day) ORDER BY n.label,y DESC";
}
}
TEST(PairedProfit, RepeatedPairsPreserveMultiplicityFiltersAndDecimalScales) {
    for(bool duplicate:{false,true}) {
        auto tables=ProfitTables(duplicate);
        for(const std::string extra:{"", " AND ps.keep=1", " AND n.k<0", " AND p.k<0", " AND t.x>0", " AND o.day>=CAST('1970-01-01' AS date)"})
        for(const std::string slice:{"", " LIMIT 2 OFFSET 1"}) {
            SCOPED_TRACE(extra+slice);auto q=ParseSelect(ProfitSQL(extra)+slice,tables);ASSERT_TRUE(q.ok())<<q.status().Message();
            auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto actual=ExecutePairedProfit(*q,avx,parallel);ASSERT_TRUE(actual.has_value());
                ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
            }
        }
    }
}
TEST(PairedProfit, BrokenRunAndExtraPredicatesRetainFallback) {
    for(bool broken:{false,true}) {
        auto tables=ProfitTables(false,broken);auto q=ParseSelect(ProfitSQL(broken?"":" AND ps.cost>t.x"),tables);ASSERT_TRUE(q.ok());
        EXPECT_FALSE(ExecutePairedProfit(*q,true,true).has_value());
        auto expected=ExecuteRelational(*q,false,false),actual=ExecuteQuery(*q,true,true);
        ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
    }
}
TEST(PairedProfit, EnforcesQueryAllocationLimit) {
    auto tables=ProfitTables();auto parsed=ParseSelect(ProfitSQL(),tables);ASSERT_TRUE(parsed.ok());
    auto q=std::move(parsed).ValueUnsafe();q.memoryBudgetBytes=1;q.allowSpill=false;
    auto actual=ExecutePairedProfit(q,true,true);ASSERT_TRUE(actual.has_value());EXPECT_FALSE(actual->ok());
}
