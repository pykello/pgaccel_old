#include "test_table.hpp"
#include "../src/executor_membership.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(MembershipBitmap, SignedDomainsMasksTailsAndAntiMembership) {
    for (int64_t start : {int64_t(-200), int64_t(0), INT64_MIN, INT64_MAX - 500}) {
        Budget budget(1 << 20); ScopedValueResource resource(budget);
        MembershipSet set(budget, 0);
        for (int i = 0; i < 500; i += 3) set.values.insert(Cell{Wide(start) + i});
        set.compact(budget); ASSERT_FALSE(set.bits.empty());
        for (int width : {4, 8}) {
            if (width == 4 && (start < INT32_MIN || start > INT32_MAX - 500)) continue;
            std::vector<int64_t> keys;
            for (int i = 0; i < 513; ++i) keys.push_back(start + (i % 500));
            keys[0] = width == 4 ? INT32_MIN : INT64_MIN;
            keys[1] = width == 4 ? INT32_MAX : INT64_MAX;
            auto registry = test::Table({{test::Raw(keys, width)}}, {{"k", std::make_shared<Int64Type>()}});
            auto q = ParseSelect("SELECT k FROM t", registry); ASSERT_TRUE(q.ok());
            auto column = *q->aggregateClauses[0].columnRef;
            for (bool avx : {false, true}) for (bool anti : {false, true}) {
                std::array<uint8_t, 65> bitmap{};
                for (int i = 0; i < 513; ++i) if (i % 5) bitmap[i / 8] |= 1 << (i % 8);
                set.intersect(q->tables[0]->GetRowGroup(0), column, anti, avx, bitmap.data());
                for (int i = 0; i < 513; ++i) {
                    Wide delta = Wide(keys[i]) - start;
                    bool found = delta >= 0 && delta < 500 && delta % 3 == 0;
                    EXPECT_EQ(bool(bitmap[i / 8] >> (i % 8) & 1), i % 5 && (found != anti));
                }
                EXPECT_EQ(bitmap.back() & 0xfe, 0);
            }
        }
    }
}
TEST(MembershipBitmap, UnboundedKeysAndLimitedMemoryKeepHashFallback) {
    Budget budget(4096); ScopedValueResource resource(budget);
    MembershipSet set(budget, 0);
    set.values.insert(Cell{INT64_MIN}); set.values.insert(Cell{INT64_MAX});
    set.compact(budget); EXPECT_TRUE(set.bits.empty());
    EXPECT_TRUE(set.contains(Cell{INT64_MIN})); EXPECT_FALSE(set.contains(Cell{0}));
    MembershipSet sparse(budget, 0);
    for (int i = 0; i < 10; ++i) sparse.values.insert(Cell{i * 2000});
    sparse.compact(budget); EXPECT_TRUE(sparse.bits.empty());
    EXPECT_TRUE(sparse.contains(Cell{4000})); EXPECT_FALSE(sparse.contains(Cell{4001}));
}
TEST(MembershipBitmap, StringsWideNumbersAndDecimalNormalization) {
    Budget budget(1 << 20); ScopedValueResource resource(budget);
    MembershipSet strings(budget, 0);
    strings.values.insert(Cell{0, "alpha", true}); strings.compact(budget);
    EXPECT_TRUE(strings.bits.empty());
    EXPECT_TRUE(strings.contains(Cell{0, "alpha", true}));
    EXPECT_FALSE(strings.contains(Cell{0, "beta", true}));
    MembershipSet wide(budget, 0);
    wide.values.insert(Cell{Wide(INT64_MAX) + 1}); wide.compact(budget);
    EXPECT_TRUE(wide.bits.empty()); EXPECT_TRUE(wide.contains(Cell{Wide(INT64_MAX) + 1}));
    MembershipSet decimal(budget, 2);
    decimal.values.insert(Cell{100}); decimal.values.insert(Cell{200}); decimal.compact(budget);
    auto registry = test::Table({{test::Raw({-1, 0, 1, 2, 3}, 4)}}, {{"k", std::make_shared<Int64Type>()}});
    auto q = ParseSelect("SELECT k FROM t", registry); ASSERT_TRUE(q.ok());
    for (bool avx : {false, true}) {
        uint8_t mask = 0x1f;
        decimal.intersect(q->tables[0]->GetRowGroup(0), *q->aggregateClauses[0].columnRef, false, avx, &mask);
        EXPECT_EQ(mask, 0x0c);
    }
    MembershipSet empty(budget, 0); empty.compact(budget);
    EXPECT_FALSE(empty.contains(Cell{0}));
}
