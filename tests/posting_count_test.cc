#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(PostingCount, PaginationHavingAndAffineProjectionsUseCompleteGroups)
{
    auto registry = test::Table({{test::Raw({INT64_MIN, 1, 2, 1})}, {test::Raw({2, 3, 1, INT64_MAX})}},
        {{"k", std::make_shared<Int64Type>()}});
    std::vector<std::string> queries{
        "SELECT k,count(*) AS c FROM t GROUP BY k ORDER BY c DESC,k LIMIT 3 OFFSET 1",
        "SELECT k,k-1,k-2,count(*) AS c FROM t GROUP BY k,k-1,k-2 ORDER BY c DESC,k",
        "SELECT k,count(k) AS c FROM t GROUP BY k HAVING count(k)>1 ORDER BY k",
        "SELECT k,count(*) FROM t WHERE k>0 GROUP BY k ORDER BY k",
        "SELECT k,sum(k) FROM t GROUP BY k ORDER BY k"};
    std::vector<Rows> expected;
    for (const auto &sql : queries) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok());
        auto result = ExecuteQuery(*q, true, false); ASSERT_TRUE(result.ok());
        expected.push_back(result->values);
    }
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 20).ok());
    for (size_t i = 0; i < queries.size(); ++i) {
        auto q = ParseSelect(queries[i], registry); ASSERT_TRUE(q.ok());
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, expected[i]);
        }
    }
}
