#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(NearUniqueGroups, DuplicateExceptionsRetainCompleteCompositeAggregates)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g = 0; g < 8; ++g) {
        std::vector<int64_t> keys, amounts;
        std::vector<std::string> labels;
        for (int i = 0; i < 512; ++i) {
            keys.push_back(g * 512 + i);
            amounts.push_back((g + i) % 17);
            labels.push_back(i % 2 ? "b" : "a");
        }
        if (g % 2 == 1) {
            // Some repetitions coalesce, others have different secondary keys.
            keys.push_back(0);
            amounts.push_back(g);
            labels.push_back(g < 4 ? "a" : "b");
        }
        groups.push_back({test::Raw(keys), test::Dict<StringType>(labels), test::Raw(amounts)});
    }
    auto registry = test::Table(std::move(groups),
        {{"k", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()},
         {"v", std::make_shared<Int64Type>()}});
    std::vector<std::string> queries{
        "SELECT k,s,count(*) AS c,sum(v),avg(v) FROM t GROUP BY k,s ORDER BY c DESC,k,s LIMIT 10 OFFSET 3",
        "SELECT k,s,count(*) AS c,min(v),max(v) FROM t WHERE v > 2 GROUP BY k,s ORDER BY c DESC,k,s LIMIT 10",
        "SELECT k,s,count(*) AS c FROM t GROUP BY k,s HAVING count(*) > 1 ORDER BY k,s",
        "SELECT k,s,count(DISTINCT v) AS c FROM t GROUP BY k,s ORDER BY c DESC,k,s LIMIT 10"};
    std::vector<Rows> expected;
    for (const auto &sql : queries) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok());
        auto result = ExecuteQuery(*q, true, false); ASSERT_TRUE(result.ok());
        expected.push_back(result->values);
    }
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    ASSERT_TRUE(registry.at("t")->Dictionary(0)->HasSparsePostings());
    for (size_t i = 0; i < queries.size(); ++i) {
        auto q = ParseSelect(queries[i], registry); ASSERT_TRUE(q.ok());
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, expected[i]);
        }
    }
    auto unsafe = ParseSelect("SELECT k,s,sum(v*9223372036854775807*9223372036854775807) FROM t GROUP BY k,s", registry);
    ASSERT_TRUE(unsafe.ok());
    EXPECT_FALSE(ExecuteQuery(*unsafe, true, true).ok());
}
