#include "executor_flat_map.hpp"
#include <gtest/gtest.h>
#include <map>

using namespace pgaccel;
namespace {
struct CollidingHash {
    size_t operator()(uint64_t value) const { return value % 3; }
};
}
TEST(FlatMap, ExactCollisionsStableIndexesAndDuplicateUpdates)
{
    Budget budget(1 << 20);
    FlatMap<uint64_t, uint64_t, CollidingHash, std::equal_to<uint64_t>> values(budget);
    std::map<uint64_t, uint32_t> indexes;
    for (uint64_t key = 0; key < 1000; ++key)
    {
        auto [index, added] = values.get(key, [=] { return key * 7; });
        ASSERT_TRUE(added);
        indexes.emplace(key, index);
    }
    for (uint64_t key = 0; key < 1000; ++key)
    {
        auto [index, added] = values.get(key, [] { return UINT64_MAX; });
        EXPECT_FALSE(added);
        EXPECT_EQ(index, indexes.at(key));
        EXPECT_EQ(values.at(index).second, key * 7);
    }
    EXPECT_EQ(values.size(), 1000);
    values.release();
    EXPECT_EQ(budget.used.load(), 0);
}

TEST(FlatMap, AllocationFailureLeavesExistingEntriesReadable)
{
    Budget budget(2048);
    FlatMap<uint64_t, uint64_t, CollidingHash, std::equal_to<uint64_t>> values(budget);
    uint64_t completed = 0;
    try {
        for (; completed < 1000; ++completed)
            values.get(completed, [=] { return completed + 17; });
        FAIL() << "Expected a bounded allocation failure";
    } catch (const std::runtime_error &) {}
    ASSERT_GT(completed, 0);
    EXPECT_EQ(values.size(), completed);
    for (uint64_t key = 0; key < completed; ++key)
    {
        auto [index, added] = values.get(key, [] { return uint64_t(0); });
        EXPECT_FALSE(added);
        EXPECT_EQ(values.at(index).second, key + 17);
    }
    values.release();
    EXPECT_EQ(budget.used.load(), 0);
}

TEST(FlatMap, ConstLookupPreservesCollisionChainsWithoutAllocating)
{
    Budget budget(1 << 20);
    FlatMap<uint64_t, uint64_t, CollidingHash, std::equal_to<uint64_t>> values(budget);
    const auto &read = values;
    EXPECT_EQ(read.find(0), decltype(values)::missing);
    for (uint64_t key = 0; key < 1000; key += 2) values.get(key, [=] { return key + 7; });
    auto bytes = budget.used.load();
    for (uint64_t key = 0; key < 1001; ++key) {
        auto index = read.find(key);
        if (key < 1000 && key % 2 == 0) {
            ASSERT_NE(index, decltype(values)::missing);
            EXPECT_EQ(read.at(index).second, key + 7);
        } else EXPECT_EQ(index, decltype(values)::missing);
    }
    EXPECT_EQ(budget.used.load(), bytes);
    values.release(); EXPECT_EQ(read.find(0), decltype(values)::missing);
}

TEST(FlatMap, ReservationPreservesSlotsAndAvoidsInsertionGrowth)
{
    Budget budget(1 << 20);
    FlatMap<uint64_t, uint64_t, CollidingHash, std::equal_to<uint64_t>> values(budget);
    for (uint64_t key = 0; key < 10; ++key) values.get(key, [=] { return key + 7; });
    values.reserve(1000); auto bytes = budget.used.load();
    for (uint64_t key = 0; key < 1000; ++key) {
        auto [slot, added] = values.get(key, [=] { return key + 7; });
        EXPECT_EQ(slot, key); EXPECT_EQ(added, key >= 10);
        EXPECT_EQ(values.at(slot).second, key + 7);
    }
    EXPECT_EQ(budget.used.load(), bytes);
    EXPECT_THROW(values.reserve(size_t(UINT32_MAX) + 1), std::runtime_error);
    EXPECT_EQ(values.find(999), 999);
    values.release(); EXPECT_EQ(budget.used.load(), 0);
    Budget tiny(1024);
    FlatMap<uint64_t, uint64_t, CollidingHash, std::equal_to<uint64_t>> limited(tiny);
    limited.get(3, [] { return uint64_t(9); });
    EXPECT_THROW(limited.reserve(1000), std::runtime_error);
    EXPECT_EQ(limited.at(limited.find(3)).second, 9);
}
