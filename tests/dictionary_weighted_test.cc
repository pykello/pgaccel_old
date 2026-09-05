#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;

TEST(DictionaryWeighted, WeightsMergeComputedKeysAndSelectedExtrema)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g = 0; g < 8; ++g)
        groups.push_back({test::Dict<StringType>(g % 2
            ? std::vector<std::string>{"bx", "ax", "ax", "", "bx"}
            : std::vector<std::string>{"ax", "ax", "bxx", "", "bxx"}),
            test::Raw({1, 1, 1, 0, 0}, 1)});
    auto registry = test::Table(std::move(groups), {{"s", std::make_shared<StringType>()},
        {"keep", std::make_shared<Int64Type>()}});
    std::vector<std::string> sql{
        "SELECT REGEXP_REPLACE(s,'x+','long-owned-suffix') AS k,avg(length(s)),sum(length(s)),count(*),min(s),max(s) FROM t WHERE keep=1 GROUP BY k ORDER BY k",
        "SELECT length(s) AS k,count(*) FROM t WHERE s<>'' GROUP BY k HAVING count(*)>5 ORDER BY k DESC LIMIT 1 OFFSET 1",
        "SELECT 10/length(s) AS k,count(*) FROM t WHERE s<>'' GROUP BY k ORDER BY k",
        "SELECT REGEXP_REPLACE(s,'x','xx') AS k,avg(length(s)) FROM t WHERE keep=9 GROUP BY k",
        "SELECT CASE WHEN s='' THEN 'empty' ELSE REGEXP_REPLACE(s,'x','xx') END AS k,count(*) FROM t GROUP BY k ORDER BY k"};
    std::vector<Rows> expected;
    for (const auto &text : sql) {
        auto q = ParseSelect(text, registry); ASSERT_TRUE(q.ok()) << q.status().Message();
        auto r = ExecuteQuery(*q, false, false); ASSERT_TRUE(r.ok()) << r.status().Message();
        expected.push_back(r->values);
    }
    EXPECT_EQ(expected[0], Rows({{"along-owned-suffix", "2", "32", "16", "ax", "ax"},
        {"blong-owned-suffix", "2.5", "20", "8", "bx", "bxx"}}));
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    for (size_t i = 0; i < sql.size(); ++i) {
        auto q = ParseSelect(sql[i], registry); ASSERT_TRUE(q.ok());
        for (bool avx : {false, true}) for (bool parallel : {false, true}) {
            auto r = ExecuteQuery(*q, avx, parallel); ASSERT_TRUE(r.ok()) << r.status().Message();
            EXPECT_EQ(r->values, expected[i]);
        }
    }
    auto unsafe = ParseSelect("SELECT 10/length(s) AS k,count(*) FROM t GROUP BY k", registry);
    ASSERT_TRUE(unsafe.ok()); EXPECT_FALSE(ExecuteQuery(*unsafe, true, true).ok());
    auto limited = ParseSelect(sql[0], registry); ASSERT_TRUE(limited.ok());
    QueryDesc bounded = *limited; bounded.memoryBudgetBytes = 32; bounded.allowSpill = false;
    EXPECT_FALSE(ExecuteQuery(bounded, true, true).ok());
}
