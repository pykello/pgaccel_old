#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecutePrefixAntiJoin(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
namespace {
TableRegistry PrefixTables(int first=100,bool duplicate=false,bool emptyAverage=false) {
    auto integer=std::make_shared<Int64Type>();auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
    std::vector<std::vector<ColumnDataP>> frames;
    for(int g=0;g<2;++g) {
        std::vector<int64_t> keys,balance;std::vector<std::string> phones;
        for(int i=0;i<259;++i) {
            int id=g*259+i;keys.push_back(first+id-(duplicate && id==517));
            balance.push_back(emptyAverage?-1:id%7==0?-id:id%11==0?0:id%31+1);
            const std::string prefixes[]={"13","31","99","","1","13é","éa","31λ"};
            phones.push_back(prefixes[id%8]+(id%8==3 || id%8==4?"":std::to_string(id)));
        }
        frames.push_back({test::Raw(keys,4),test::Raw(balance,4),test::Dict<StringType>(phones)});
    }
    std::reverse(frames.begin(),frames.end());
    auto tables=test::Table(std::move(frames),{{"k",integer},{"balance",decimal},{"phone",std::make_shared<StringType>()}});
    std::vector<int64_t> foreign,keep;
    for(int i=0;i<1100;++i){foreign.push_back(i%9==0?first-100:first+(i*3)%540);keep.push_back(i%3);}
    auto orders=test::Table({{test::Raw({foreign.begin(),foreign.begin()+550},4),test::Raw({keep.begin(),keep.begin()+550},4)},
                             {test::Raw({foreign.begin()+550,foreign.end()},4),test::Raw({keep.begin()+550,keep.end()},4)}},{{"c",integer},{"keep",integer}});
    tables.emplace("o",std::move(orders.at("t")));return tables;
}
std::string PrefixSQL(std::string extra="",std::string inner="",std::string average="") {
    return "SELECT SUBSTRING(c.phone FROM 1 FOR 2) AS prefix,COUNT(*) AS customers,SUM(c.balance) AS total FROM t c "
        "WHERE SUBSTRING(c.phone FROM 1 FOR 2) IN ('13','31') AND c.balance>(SELECT AVG(a.balance) FROM t a "
        "WHERE a.balance>0.00 AND SUBSTRING(a.phone FROM 1 FOR 2) IN ('31','13')"+average+
        ") AND NOT EXISTS (SELECT * FROM o WHERE o.c=c.k"+inner+")"+extra+
        " GROUP BY SUBSTRING(c.phone FROM 1 FOR 2) ORDER BY prefix";
}
}
TEST(PrefixAntiJoin, PrefixesAverageAndRepeatedOrdersMatchReference) {
    for(int first:{-300,100})for(bool empty:{false,true}) {
        auto tables=PrefixTables(first,false,empty);
        for(const std::string outer:{"", " AND c.balance<0.25"})
        for(const std::string inner:{"", " AND o.keep=1"})for(const std::string avg:{"", " AND a.balance<0.20"}) {
            SCOPED_TRACE(outer+inner+avg);auto q=ParseSelect(PrefixSQL(outer,inner,avg),tables);ASSERT_TRUE(q.ok())<<q.status().Message();
            auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto actual=ExecutePrefixAntiJoin(*q,avx,parallel);ASSERT_TRUE(actual.has_value());
                ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
            }
        }
    }
}
TEST(PrefixAntiJoin, UnsupportedKeysAndPrefixShapesRetainFallback) {
    for(int scenario=0;scenario<3;++scenario) {
        auto tables=PrefixTables(100,scenario==0);auto sql=PrefixSQL();
        if(scenario==1) {
            auto at=sql.find("IN ('31','13')");sql.replace(at,std::string("IN ('31','13')").size(),"IN ('31','99')");
        }
        if(scenario==2) {
            size_t at=0;while((at=sql.find("FROM 1 FOR 2",at))!=std::string::npos){sql.replace(at,12,"FROM 2 FOR 2");at+=12;}
        }
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok())<<q.status().Message();EXPECT_FALSE(ExecutePrefixAntiJoin(*q,true,true).has_value());
        auto expected=ExecuteRelational(*q,false,false),actual=ExecuteQuery(*q,true,true);
        ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
    }
}
TEST(PrefixAntiJoin, EnforcesQueryBudget) {
    auto tables=PrefixTables();auto parsed=ParseSelect(PrefixSQL(),tables);ASSERT_TRUE(parsed.ok());
    auto q=std::move(parsed).ValueUnsafe();q.memoryBudgetBytes=1;q.allowSpill=false;
    auto actual=ExecutePrefixAntiJoin(q,true,true);ASSERT_TRUE(actual.has_value());EXPECT_FALSE(actual->ok());
}

TEST(PrefixAntiJoin, PreservesRoundedScalarAverageAtFineDecimalBoundaries) {
    auto integer=std::make_shared<Int64Type>();auto decimal=std::make_shared<DecimalType>();decimal->scale=13;
    auto tables=test::Table({{test::Raw({1,2,3}),test::Raw({1,1,2}),test::Dict<StringType>({"13a","13b","13c"})}},
        {{"k",integer},{"balance",decimal},{"phone",std::make_shared<StringType>()}});
    auto orders=test::Table({{test::Raw({999})}},{{"c",integer}});tables.emplace("o",std::move(orders.at("t")));
    auto q=ParseSelect(PrefixSQL(),tables);ASSERT_TRUE(q.ok())<<q.status().Message();
    auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok());
    auto actual=ExecutePrefixAntiJoin(*q,true,true);ASSERT_TRUE(actual.has_value());ASSERT_TRUE(actual->ok());
    EXPECT_EQ((**actual).values,expected->values);ASSERT_EQ(expected->values.size(),1);EXPECT_EQ(expected->values[0][1],"3");
}

TEST(PrefixAntiJoin, AsciiPrefixDoesNotHideInvalidUtf8Suffix) {
    auto integer=std::make_shared<Int64Type>();auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
    auto tables=test::Table({{test::Raw({1,2,3}),test::Raw({100,200,300}),
        test::Dict<StringType>({std::string("13")+char(0x80),"31a","13b"})}},
        {{"k",integer},{"balance",decimal},{"phone",std::make_shared<StringType>()}});
    auto orders=test::Table({{test::Raw({999})}},{{"c",integer}});
    tables.emplace("o",std::move(orders.at("t")));
    auto q=ParseSelect(PrefixSQL(),tables);ASSERT_TRUE(q.ok());
    EXPECT_FALSE(ExecutePrefixAntiJoin(*q,true,true).has_value());
}
