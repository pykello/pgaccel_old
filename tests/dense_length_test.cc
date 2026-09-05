#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;

TEST(DenseLength, IndexedKeysPreserveByteLengthsFiltersAndHaving)
{
    std::vector<std::vector<ColumnDataP>> stripes;
    for (int g = 0; g < 8; ++g)
        stripes.push_back({test::Raw({-5, 8, -5, 8, 99}, 4),
            test::Dict<StringType>({"abc", "", "é", "longer", "z"})});
    auto registry = test::Table(std::move(stripes), {{"k", std::make_shared<Int64Type>()},
        {"s", std::make_shared<StringType>()}});
    std::vector<std::string> sql{
        "SELECT k,avg(length(s)),count(*) FROM t GROUP BY k ORDER BY k",
        "SELECT k,sum(length(s)),avg(length(s)),count(*) FROM t WHERE s<>'' GROUP BY k HAVING count(*)>8 ORDER BY k DESC LIMIT 1",
        "SELECT k,avg(length(s)) FROM t WHERE k>999 GROUP BY k",
        "SELECT k,sum(length(s)),avg(length(s)),sum(length(s)) FROM t GROUP BY k ORDER BY k LIMIT 2 OFFSET 1"};
    std::vector<Rows> expected;
    for (auto &text : sql) {
        auto q = ParseSelect(text, registry); ASSERT_TRUE(q.ok());
        auto r = ExecuteQuery(*q, false, false); ASSERT_TRUE(r.ok()); expected.push_back(r->values);
    }
    EXPECT_EQ(expected[0], Rows({{"-5", "2.5", "16"}, {"8", "3", "16"}, {"99", "1", "8"}}));
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    for (size_t i = 0; i < sql.size(); ++i) {
        auto q = ParseSelect(sql[i], registry); ASSERT_TRUE(q.ok());
        for (bool parallel : {false, true}) for (bool avx : {false, true}) {
            auto r = ExecuteQuery(*q, avx, parallel); ASSERT_TRUE(r.ok()) << r.status().Message();
            EXPECT_EQ(r->values, expected[i]);
        }
    }
}
