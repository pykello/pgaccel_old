#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(PostingFilter, EqualitySetCountAndIntersectionPreserveTailBytes)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int n : {65, 129, 513}) {
        std::vector<int64_t> keys(n, 0), other(n);
        keys[0] = INT64_MIN; keys[n-1] = INT64_MIN;
        for (int i = 0; i < n; ++i) other[i] = i % 2;
        groups.push_back({test::Raw(keys), test::Raw(other)});
    }
    auto registry = test::Table(std::move(groups),
        {{"k", std::make_shared<Int64Type>()}, {"o", std::make_shared<Int64Type>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 20).ok());
    for (bool avx : {false, true}) for (int64_t value : {INT64_MIN, int64_t(0), int64_t(42)}) {
        auto q = ParseSelect("SELECT count(*) FROM t WHERE k=" + std::to_string(value), registry);
        ASSERT_TRUE(q.ok());
        auto filter = CreateFilterNode(q->filterClauses, avx);
        for (int g = 0; g < 3; ++g) {
            const auto &rg = registry.at("t")->GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8 + 1];
            memset(bitmap, 0xa5, sizeof(bitmap));
            int expected = value == INT64_MIN ? 2 : value == 0 ? rg.size - 2 : 0;
            EXPECT_EQ(filter->ExecuteCount(rg), expected);
            EXPECT_EQ(filter->ExecuteSet(rg, bitmap), expected);
            EXPECT_EQ(bitmap[(rg.size + 7) / 8], 0xa5);
            for (int i = 0; i < rg.size; ++i) {
                bool matches = value == INT64_MIN ? (i == 0 || i == rg.size-1) :
                    value == 0 ? (i != 0 && i != rg.size-1) : false;
                EXPECT_EQ(bool(bitmap[i/8] & (1u << (i%8))), matches);
            }
            memset(bitmap, 0x55, sizeof(bitmap));
            int count = filter->ExecuteAnd(rg, bitmap), independent = 0;
            for (int i = 0; i < rg.size; ++i) {
                bool matches = value == INT64_MIN ? (i == 0 || i == rg.size-1) :
                    value == 0 ? (i != 0 && i != rg.size-1) : false;
                bool selected = i % 2 == 0 && matches; independent += selected;
                EXPECT_EQ(bool(bitmap[i/8] & (1u << (i%8))), selected);
            }
            EXPECT_EQ(count, independent);
            EXPECT_EQ(bitmap[(rg.size + 7) / 8], 0x55);
        }
    }
}
