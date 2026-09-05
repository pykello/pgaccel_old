#include "test_table.hpp"
#include <gtest/gtest.h>
#include <array>

using namespace pgaccel;
TEST(DictionaryMembership, GatherAndScalarMapsMatchIndependentRowsAndTails)
{
    for (int n : {1, 15, 16, 17, 63, 64, 65, 255, 256, 257, 65535, 65536}) {
        std::vector<std::string> values;
        for (int i = 0; i < n; ++i)
            values.push_back((i % 7 ? "ordinary-" : "google-") + std::to_string(i));
        auto registry = test::Table({{test::Dict<StringType>(values)}}, {{"s", std::make_shared<StringType>()}});
        const auto &rg = registry.at("t")->GetRowGroup(0);
        for (int predicate = 0; predicate < 3; ++predicate) {
            std::string clause = predicate == 0 ? "s LIKE '%google%'" : predicate == 1
                ? "s NOT LIKE '%google%'" : "s IN ('google-0','ordinary-1','ordinary-2','absent')";
            auto query = ParseSelect("SELECT count(*) FROM t WHERE " + clause, registry);
            ASSERT_TRUE(query.ok());
            for (bool avx : {false, true}) {
                auto filter = CreateFilterNode(query->filterClauses, avx);
                std::array<uint8_t, RowGroupSize / 8 + 8> bitmap;
                for (bool intersect : {false, true}) {
                    bitmap.fill(0xa5);
                    if (intersect) {
                        std::fill(bitmap.begin(), bitmap.begin() + (n + 7) / 8, 0);
                        for (int i = 0; i < n; i += 3) bitmap[i / 8] |= 1u << (i % 8);
                    }
                    int actual = intersect ? filter->ExecuteAnd(rg, bitmap.data()) : filter->ExecuteSet(rg, bitmap.data());
                    int expected = 0;
                    for (int i = 0; i < n; ++i) {
                        bool match = predicate == 0 ? values[i].find("google") != std::string::npos
                            : predicate == 1 ? values[i].find("google") == std::string::npos : i < 3;
                        match &= !intersect || i % 3 == 0;
                        expected += match;
                        EXPECT_EQ(bool(bitmap[i / 8] & (1u << (i % 8))), match) << n << ':' << i;
                    }
                    EXPECT_EQ(actual, expected);
                    if (!intersect) EXPECT_EQ(filter->ExecuteCount(rg), expected);
                    if (n % 8) EXPECT_EQ(bitmap[n / 8] >> (n % 8), 0);
                    EXPECT_EQ(bitmap[(n + 7) / 8], 0xa5);
                }
            }
        }
    }
}
