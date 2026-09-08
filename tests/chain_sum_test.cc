#include "convergent_fixture.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteChainSum(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
namespace {
std::string ChainSQL(std::string extra="") {
    return "SELECT c.k,n.label,c.n,r.keep,SUM(t.x*(1-0.05)) AS amount FROM t,o,c,n,r "
        "WHERE t.o=o.k AND o.c=c.k AND c.n=n.k AND n.r=r.k"+extra+
        " GROUP BY c.k,n.label,c.n,r.keep ORDER BY amount DESC,c.k";
}
}
TEST(ChainSum, UniqueGroupsKeepDependentLabelsFiltersAndTopResultsExact) {
    auto tables=test::ConvergentTables();
    for(const std::string extra:{"", " AND o.keep=1", " AND r.keep=1", " AND t.x>0", " AND n.k<-10", " AND c.k=10"})
    for(const std::string slice:{"", " LIMIT 2 OFFSET 1", " LIMIT 0"}) {
        SCOPED_TRACE(extra+slice);auto q=ParseSelect(ChainSQL(extra)+slice,tables);ASSERT_TRUE(q.ok())<<q.status().Message();
        auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
        for(bool avx:{false,true})for(bool parallel:{false,true}) {
            auto actual=ExecuteChainSum(*q,avx,parallel);ASSERT_TRUE(actual.has_value());
            ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
        }
    }
}
TEST(ChainSum, DuplicateKeysAndUnprovedGroupsRetainFallback) {
    for(bool duplicate:{false,true}) {
        auto tables=test::ConvergentTables(duplicate);auto sql=ChainSQL();
        if(!duplicate) {
            sql="SELECT n.label,SUM(t.x) AS amount FROM t,o,c,n WHERE t.o=o.k AND o.c=c.k AND c.n=n.k GROUP BY n.label ORDER BY amount DESC";
        }
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok());EXPECT_FALSE(ExecuteChainSum(*q,true,true).has_value());
        auto expected=ExecuteRelational(*q,false,false),actual=ExecuteQuery(*q,true,true);
        ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
    }
}
TEST(ChainSum, RejectsGroupsAboveTheUniqueKeyInTheChain) {
    auto tables=test::ConvergentTables();auto q=ParseSelect("SELECT c.k,o.keep,SUM(t.x) FROM t,o,c WHERE t.o=o.k AND o.c=c.k GROUP BY c.k,o.keep",tables);
    ASSERT_TRUE(q.ok());EXPECT_FALSE(ExecuteChainSum(*q,true,true).has_value());
}
TEST(ChainSum, EnforcesQueryBudget) {
    auto tables=test::ConvergentTables();auto parsed=ParseSelect(ChainSQL(),tables);ASSERT_TRUE(parsed.ok());
    auto q=std::move(parsed).ValueUnsafe();q.memoryBudgetBytes=1;q.allowSpill=false;
    auto actual=ExecuteChainSum(q,true,true);ASSERT_TRUE(actual.has_value());EXPECT_FALSE(actual->ok());
}
TEST(ChainSum, NarrowStateAndWidePrefixesRemainExact) {
    auto tables=test::ConvergentTables();
    uint64_t narrowPeak=0,widePeak=0;
    for(const auto &values:std::vector<std::vector<int64_t>>{
            {1,-2,3}, {INT64_MAX,INT64_MAX,-INT64_MAX},
            {INT64_MIN,INT64_MIN,INT64_MAX}, {INT64_MIN,0,0},
            {INT64_MAX,INT64_MAX,-INT64_MAX,-INT64_MAX}}) {
        auto fact=test::Table({{test::Raw(std::vector<int64_t>(values.size(),100)),test::Raw(values)},
                              {test::Raw(std::vector<int64_t>(values.size(),100)),test::Raw(values)}},
                             {{"o",std::make_shared<Int64Type>()},{"x",std::make_shared<Int64Type>()}});
        tables["t"]=std::move(fact.at("t"));
        auto parsed=ParseSelect("SELECT c.k,SUM(t.x) FROM t,o,c WHERE t.o=o.k AND o.c=c.k GROUP BY c.k",tables);
        ASSERT_TRUE(parsed.ok());auto expected=ExecuteRelational(*parsed,false,false);ASSERT_TRUE(expected.ok());
        for(bool avx:{false,true})for(bool parallel:{false,true}) {
            auto actual=ExecuteChainSum(*parsed,avx,parallel);ASSERT_TRUE(actual.has_value());
            ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
            if(avx && parallel) {
                if(values[0]==1)narrowPeak=(**actual).peakMemoryBytes;
                if(values[0]==INT64_MAX)widePeak=(**actual).peakMemoryBytes;
            }
        }
    }
    EXPECT_GT(widePeak,narrowPeak);
}
TEST(ChainSum, SharedNarrowStateKeepsConcurrentUpdatesToOneGroup) {
    auto tables=test::ConvergentTables();
    std::vector<std::vector<ColumnDataP>> stripes;
    for(int i=0;i<8;++i)stripes.push_back({test::Raw(std::vector<int64_t>(8192,100),4),
                                         test::Raw(std::vector<int64_t>(8192,1),1)});
    auto fact=test::Table(std::move(stripes),{{"o",std::make_shared<Int64Type>()},{"x",std::make_shared<Int64Type>()}});
    tables["t"]=std::move(fact.at("t"));
    auto q=ParseSelect("SELECT c.k,SUM(t.x) FROM t,o,c WHERE t.o=o.k AND o.c=c.k GROUP BY c.k",tables);
    ASSERT_TRUE(q.ok());auto actual=ExecuteChainSum(*q,true,true);ASSERT_TRUE(actual.has_value());
    ASSERT_TRUE(actual->ok());EXPECT_EQ((**actual).values,Rows({{"10","65536"}}));
}
