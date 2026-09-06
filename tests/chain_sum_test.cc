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
