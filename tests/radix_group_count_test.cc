#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(RadixGroupCount, CompletePartitionsPreserveFiltersAndGlobalOrdering)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g = 0; g < 8; ++g) {
        std::vector<int64_t> keys;
        std::vector<std::string> labels;
        for (int i = 0; i < 4096; ++i) {
            keys.push_back((i * 13 + g) % 511 - 255);
            labels.push_back(i % 17 == 0 ? "" : "label" + std::to_string((i + g * 7) % 31));
        }
        groups.push_back({test::Raw(keys, 4), test::Dict<StringType>(labels)});
    }
    auto registry = test::Table(std::move(groups),
        {{"k", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()}});
    std::vector<std::string> queries{
        "SELECT k,s,count(*) AS c FROM t GROUP BY k,s ORDER BY c DESC,k,s LIMIT 10 OFFSET 7",
        "SELECT k,s,count(*) AS c FROM t WHERE s<>'' GROUP BY k,s HAVING count(*)>1 ORDER BY c DESC,k,s LIMIT 30",
        "SELECT k,s,count(*) AS c FROM t WHERE k>1000 GROUP BY k,s ORDER BY c DESC LIMIT 10",
        "SELECT k,s,count(*) FROM t GROUP BY k,s LIMIT 10"};
    std::vector<Rows> expected;
    for (const auto &sql : queries) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok());
        auto result = ExecuteQuery(*q, true, false); ASSERT_TRUE(result.ok());
        expected.push_back(result->values);
    }
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    for (size_t i = 0; i < queries.size(); ++i) {
        auto q = ParseSelect(queries[i], registry); ASSERT_TRUE(q.ok());
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, expected[i]);
        }
    }
}
