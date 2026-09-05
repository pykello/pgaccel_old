#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(ConditionalGroupKey, ExistingAndVirtualLabelsPreserveBothCaseBranches)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g = 0; g < 8; ++g) {
        std::vector<int64_t> flags;
        std::vector<std::string> source, target;
        for (int i = 0; i < 100; ++i) {
            flags.push_back(i % 3);
            source.push_back((i + g) % 5 == 0 ? "" : (i + g) % 2 ? "a" : "z");
            target.push_back("target" + std::to_string((i + g) % 7));
        }
        groups.push_back({test::Raw(flags, 1), test::Dict<StringType>(source), test::Dict<StringType>(target)});
    }
    auto registry = test::Table(std::move(groups), {{"f", std::make_shared<Int64Type>()},
        {"s", std::make_shared<StringType>()}, {"d", std::make_shared<StringType>()}});
    std::vector<std::string> cases{
        "CASE WHEN f=0 THEN s ELSE '' END",
        "CASE WHEN f=0 THEN s ELSE 'missing' END",
        "CASE WHEN f=0 THEN 'missing' ELSE s END",
        "CASE WHEN (f=0 AND 1/0=0) THEN s ELSE 'missing' END"};
    std::vector<Rows> expected;
    auto sql = [&](const std::string &expression) {
        return "SELECT f," + expression + " AS k,d,count(*) AS c FROM t WHERE " +
            (expression.find("1/0") == std::string::npos ? "f<3" : "f<>0") +
            " GROUP BY f,k,d ORDER BY c DESC,f,k,d";
    };
    for (const auto &expression : cases) {
        auto q = ParseSelect(sql(expression), registry); ASSERT_TRUE(q.ok());
        auto result = ExecuteQuery(*q, true, false); ASSERT_TRUE(result.ok());
        expected.push_back(result->values);
    }
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    for (size_t i = 0; i < cases.size(); ++i) {
        auto q = ParseSelect(sql(cases[i]), registry); ASSERT_TRUE(q.ok());
        for (bool parallel : {false,true}) {
            auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, expected[i]);
        }
    }
}
TEST(ConditionalGroupKey, EqualBranchesStillEvaluateAnUnsafeCondition)
{
    auto registry = test::Table({{test::Dict<StringType>({"a","a"})}}, {{"s", std::make_shared<StringType>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 20).ok());
    auto q = ParseSelect("SELECT CASE WHEN 1/0=0 THEN s ELSE 'a' END AS k,count(*) FROM t GROUP BY k", registry);
    ASSERT_TRUE(q.ok());
    EXPECT_FALSE(ExecuteQuery(*q, true, true).ok());
}
