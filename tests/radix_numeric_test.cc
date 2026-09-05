#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(RadixNumeric, ExactWideSumsAndScaledAveragesSurvivePartitioning)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g = 0; g < 8; ++g) {
        std::vector<int64_t> keys, values, decimals;
        std::vector<std::string> labels;
        for (int i = 0; i < 500; ++i) {
            keys.push_back(i % 13 - 6); labels.push_back((i + g) % 3 ? "a" : "z");
            values.push_back(i % 5 ? INT64_MAX : INT64_MIN);
            decimals.push_back((i + g) % 11 * 101 - 505);
        }
        groups.push_back({test::Raw(keys, 4), test::Dict<StringType>(labels),
            test::Raw(values), test::Raw(decimals)});
    }
    auto decimal = std::make_shared<DecimalType>(); decimal->scale = 2;
    auto registry = test::Table(std::move(groups), {{"k", std::make_shared<Int64Type>()},
        {"s", std::make_shared<StringType>()}, {"v", std::make_shared<Int64Type>()},
        {"d", decimal}});
    std::vector<std::string> queries{
        "SELECT k,s,sum(v) FROM t GROUP BY k,s ORDER BY k,s",
        "SELECT k,s,count(*) AS c,sum(v),avg(d) FROM t GROUP BY k,s ORDER BY c DESC,k,s LIMIT 10 OFFSET 2",
        "SELECT k,s,count(*) AS c,sum(v),avg(d),sum(d) FROM t WHERE k>0 GROUP BY k,s HAVING count(*)>5 ORDER BY c DESC,k,s",
        "SELECT k,s,sum(v),avg(d) FROM t WHERE k>1000 GROUP BY k,s ORDER BY k,s"};
    std::vector<Rows> expected;
    for (const auto &sql : queries) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q, true, false); ASSERT_TRUE(result.ok()) << result.status().Message();
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
