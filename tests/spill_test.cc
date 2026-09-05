#include "test_table.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <set>
using namespace pgaccel;
static std::set<std::string> SpillDirectories() {
    std::set<std::string> result;
    for (const auto &entry : std::filesystem::directory_iterator(std::filesystem::temp_directory_path()))
        if (entry.path().filename().string().starts_with("pgaccel-spill-"))
            result.insert(entry.path().string());
    return result;
}
TEST(Spill, ExactSortedAggregationWithinSmallBudget) {
    const auto directories = SpillDirectories();
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g=0;g<3;++g) {
        std::vector<int64_t> values(60000),key(60000,7);
        for (int r=0;r<60000;++r) values[r]=g*30000+r;
        groups.push_back({test::Raw(values),test::Raw(key)});
    }
    auto table=test::Table(groups,{{"v",std::make_shared<Int64Type>()},{"g",std::make_shared<Int64Type>()}});
    for (const auto &sql: {
        "SELECT count(distinct v),count(*),sum(v),min(v),max(v) FROM t",
        "SELECT count(distinct v),count(*),sum(v),min(v),max(v) FROM t GROUP BY g"}) {
        auto parsed=ParseSelect(sql,table);ASSERT_TRUE(parsed.ok());auto q=std::move(parsed).ValueUnsafe();
        q.memoryBudgetBytes=1<<20;
        auto output=ExecuteQuery(q,true,true);ASSERT_TRUE(output.ok())<<output.status().Message();
        EXPECT_EQ(output->values,Rows({{"120000","180000","10799910000","0","119999"}}));
        EXPECT_GT(output->spillBytes,0);EXPECT_LE(output->peakMemoryBytes,q.memoryBudgetBytes);
        q.spillByteBudget=1;
        auto failed=ExecuteQuery(q,true,true);EXPECT_FALSE(failed.ok());
        EXPECT_NE(failed.status().Message().find("spill byte budget"),std::string::npos);
        q.allowSpill=false;
        EXPECT_FALSE(ExecuteQuery(q,true,true).ok());
        EXPECT_EQ(SpillDirectories(), directories);
    }
    auto parsed=ParseSelect("SELECT v,count(*) AS n FROM t GROUP BY v ORDER BY n DESC,v LIMIT 10",table);
    ASSERT_TRUE(parsed.ok());auto q=std::move(parsed).ValueUnsafe();q.memoryBudgetBytes=1<<20;
    auto output=ExecuteQuery(q,true,true);ASSERT_TRUE(output.ok())<<output.status().Message();
    EXPECT_GT(output->spillBytes,0);Rows expected;
    for(int i=30000;i<30010;++i)expected.push_back({std::to_string(i),"2"});
    EXPECT_EQ(output->values,expected);
    auto composite=ParseSelect("SELECT g,v,count(*) AS n FROM t GROUP BY g,v ORDER BY n DESC,v LIMIT 10",table);
    ASSERT_TRUE(composite.ok());auto compositeQuery=std::move(composite).ValueUnsafe();compositeQuery.memoryBudgetBytes=1<<20;
    auto compositeOutput=ExecuteQuery(compositeQuery,true,true);
    ASSERT_TRUE(compositeOutput.ok())<<compositeOutput.status().Message();
    EXPECT_GT(compositeOutput->spillBytes,0);
    for(auto &row:expected)row.insert(row.begin(),"7");
    EXPECT_EQ(compositeOutput->values,expected);
    EXPECT_EQ(SpillDirectories(), directories);
}
