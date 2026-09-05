#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;

TEST(ProjectionBoundary, InclusiveThresholdPreservesTiesDirectionsAndOffset)
{
    std::vector<std::vector<ColumnDataP>> stripes;
    for (int g = 0; g < 24; ++g) {
        std::vector<int64_t> keys;
        std::vector<std::string> labels;
        for (int i = 0; i < 29; ++i) {
            keys.push_back((i + g * 7) % 13 - 6);
            labels.push_back(std::to_string((g * 29 + i) % 37) + (i % 3 ? "-needle" : "-other"));
        }
        stripes.push_back({test::Raw(keys, 4), test::Dict<StringType>(labels)});
    }
    auto registry = test::Table(std::move(stripes), {{"k", std::make_shared<Int64Type>()},
        {"s", std::make_shared<StringType>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    for (const std::string order : {"k,s", "k DESC,s", "s,k DESC", "s DESC,k"}) {
        auto all = ParseSelect("SELECT k,s FROM t WHERE s LIKE '%needle%' ORDER BY " + order, registry);
        ASSERT_TRUE(all.ok()); auto full = ExecuteQuery(*all, false, false); ASSERT_TRUE(full.ok());
        Rows expected(full->values.begin() + 3, full->values.begin() + 14);
        auto limited = ParseSelect("SELECT k,s FROM t WHERE s LIKE '%needle%' ORDER BY " + order + " LIMIT 11 OFFSET 3", registry);
        ASSERT_TRUE(limited.ok());
        for (bool parallel : {false, true}) for (bool avx : {false, true}) {
            auto result = ExecuteQuery(*limited, avx, parallel); ASSERT_TRUE(result.ok()) << result.status().Message();
            EXPECT_EQ(result->values, expected) << order;
        }
    }
}
