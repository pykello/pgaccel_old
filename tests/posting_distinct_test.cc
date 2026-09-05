#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(PostingDistinct, CrossStripeDuplicatesFiltersAndCompositeKeysAreExact)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g = 0; g < 8; ++g) {
        std::vector<int64_t> users, keys;
        std::vector<std::string> labels;
        for (int i = 0; i < 300; ++i) {
            users.push_back(i % 31 == 0 ? INT64_MIN : i % 31 == 1 ? INT64_MAX : i % 31);
            keys.push_back((i + g) % 7 - 3);
            labels.push_back((i * 3 + g) % 5 == 0 ? "" : (i + g) % 2 ? "z" : "a");
        }
        groups.push_back({test::Raw(users), test::Raw(keys, 1), test::Dict<StringType>(labels)});
    }
    auto registry = test::Table(std::move(groups), {{"u", std::make_shared<Int64Type>()},
        {"k", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()}});
    std::vector<std::string> queries{
        "SELECT k,count(DISTINCT u) AS c FROM t GROUP BY k ORDER BY c DESC,k",
        "SELECT s,count(DISTINCT u) AS c FROM t WHERE s<>'' GROUP BY s ORDER BY c DESC,s",
        "SELECT k,s,count(DISTINCT u) AS c FROM t WHERE s<>'' GROUP BY k,s ORDER BY c DESC,k,s LIMIT 5 OFFSET 2",
        "SELECT k,count(DISTINCT u) AS c FROM t WHERE k>0 GROUP BY k ORDER BY k",
        "SELECT k,count(DISTINCT u) FROM t WHERE k>100 GROUP BY k ORDER BY k"};
    std::vector<Rows> expected;
    for (const auto &sql : queries) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok()) << sql << ": " << q.status().Message();
        auto result = ExecuteQuery(*q, true, false); ASSERT_TRUE(result.ok());
        expected.push_back(result->values);
    }
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    ASSERT_TRUE(registry.at("t")->Dictionary(0)->HasDensePostings());
    for (size_t i = 0; i < queries.size(); ++i) {
        auto q = ParseSelect(queries[i], registry); ASSERT_TRUE(q.ok());
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*q, true, parallel); ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, expected[i]);
        }
    }
}
