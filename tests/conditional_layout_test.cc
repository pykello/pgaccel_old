#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(ConditionalLayout, BranchSpecificFieldsRetainCompleteTuplesAndMissingGuardFallback)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int g = 0; g < 8; ++g) {
        std::vector<int64_t> guards, hidden;
        std::vector<std::string> source, a, b, c, d;
        for (int row = 0; row < 512; ++row) {
            int i = g * 512 + row, key = i % 2048;
            guards.push_back(key % 2 ? INT16_MAX : INT16_MIN);
            hidden.push_back(i / 2048);
            source.push_back(key % 17 ? "source" + std::to_string(key) : "");
            a.push_back("a" + std::to_string(key));
            b.push_back("b" + std::to_string(key * 3 % 2048));
            c.push_back("c" + std::to_string(key * 5 % 2048));
            d.push_back("d" + std::to_string(key * 7 % 2048));
        }
        groups.push_back({test::Raw(guards,2),test::Raw(hidden,1),test::Dict<StringType>(source),
            test::Dict<StringType>(a),test::Dict<StringType>(b),test::Dict<StringType>(c),test::Dict<StringType>(d)});
    }
    auto registry = test::Table(std::move(groups), {{"g",std::make_shared<Int64Type>()},
        {"h",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()},
        {"a",std::make_shared<StringType>()},{"b",std::make_shared<StringType>()},
        {"c",std::make_shared<StringType>()},{"d",std::make_shared<StringType>()}});
    std::vector<std::string> queries{
        "SELECT g,CASE WHEN g=-32768 THEN s ELSE '' END AS src,a,b,c,d,count(*) AS n FROM t GROUP BY g,src,a,b,c,d ORDER BY n DESC,g,src,a,b,c,d LIMIT 20 OFFSET 3",
        "SELECT g,CASE WHEN (g=-32768 AND h=0) THEN s ELSE '' END AS src,a,b,c,d,count(*) AS n FROM t GROUP BY g,src,a,b,c,d ORDER BY n DESC,g,src,a,b,c,d"};
    std::vector<Rows> expected;
    for (const auto &sql : queries) {
        auto q = ParseSelect(sql, registry); ASSERT_TRUE(q.ok()) << q.status().Message();
        auto result = ExecuteQuery(*q,true,false); ASSERT_TRUE(result.ok());
        expected.push_back(result->values);
    }
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    for (size_t i = 0; i < queries.size(); ++i) {
        auto q = ParseSelect(queries[i],registry); ASSERT_TRUE(q.ok());
        for (bool parallel : {false,true}) {
            auto result = ExecuteQuery(*q,true,parallel); ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values,expected[i]);
        }
    }
}
