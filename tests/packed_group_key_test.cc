#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <map>
#include <tuple>
#include <limits>

using namespace pgaccel;
TEST(PackedGroups, CrossStripeIdsAndNegativeEpochPartsPreserveIdentity)
{
    const std::vector<int64_t> epochs{INT64_MIN, INT64_MAX, -3601, -61, -60, -1, 0, 59, 60, 3599, 3600};
    for (const std::string part : {"hour", "minute", "second"}) {
        std::vector<std::vector<ColumnDataP>> groups;
        std::map<std::tuple<int64_t, std::string, int64_t>, int64_t> counts;
        for (int g = 0; g < 8; ++g) {
            std::vector<int64_t> keys, times;
            std::vector<std::string> labels;
            for (size_t i = 0; i < epochs.size(); ++i) {
                int64_t key = (i + g) % 2 ? -128 : 127;
                std::string label = (i + g) % 3 ? "z" : "a";
                int64_t unit = part == "hour" ? 3600 : part == "minute" ? 60 : 1;
                int64_t modulus = part == "hour" ? 24 : 60;
                __int128 seconds = epochs[i];
                __int128 quotient = seconds / unit;
                if (seconds % unit < 0) --quotient;
                int64_t value = (quotient % modulus + modulus) % modulus;
                ++counts[{key, label, value}];
                keys.push_back(key); labels.push_back(label); times.push_back(epochs[i]);
            }
            groups.push_back({test::Raw(keys, 1), test::Dict<StringType>(labels), test::Raw(times)});
        }
        auto registry = test::Table(std::move(groups), {{"k", std::make_shared<Int64Type>()},
            {"s", std::make_shared<StringType>()}, {"ts", std::make_shared<TimestampType>()}});
        Rows expected;
        for (const auto &[key, count] : counts) expected.push_back({std::to_string(std::get<0>(key)),
            std::get<1>(key), std::to_string(std::get<2>(key)), std::to_string(count)});
        for (bool indexed : {false, true}) {
            if (indexed) ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
            std::string expression = "EXTRACT(" + part + " FROM ts)";
            auto query = ParseSelect("SELECT k,s," + expression + ",count(*) FROM t GROUP BY k,s," +
                expression + " ORDER BY k,s," + expression, registry);
            ASSERT_TRUE(query.ok());
            for (bool parallel : {false, true}) {
                auto result = ExecuteQuery(*query, true, parallel);
                ASSERT_TRUE(result.ok()) << result.status().Message();
                EXPECT_EQ(result->values, expected);
            }
        }
    }
}
TEST(PackedGroups, WideCompositeKeysRetainAllBits)
{
    auto registry = test::Table({{test::Raw({INT64_MIN, INT64_MAX, INT64_MIN, INT64_MAX}),
        test::Raw({INT64_MAX, INT64_MIN, INT64_MIN, INT64_MAX})}},
        {{"a", std::make_shared<Int64Type>()}, {"b", std::make_shared<Int64Type>()}});
    auto query = ParseSelect("SELECT a,b,count(*) FROM t GROUP BY a,b ORDER BY a,b", registry);
    ASSERT_TRUE(query.ok());
    auto result = ExecuteQuery(*query, true, true);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->values, Rows({{"-9223372036854775808","-9223372036854775808","1"},
        {"-9223372036854775808","9223372036854775807","1"},
        {"9223372036854775807","-9223372036854775808","1"},
        {"9223372036854775807","9223372036854775807","1"}}));
}
