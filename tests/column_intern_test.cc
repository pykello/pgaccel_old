#include "column_intern.hpp"
#include "executor_state.hpp"
#include <gtest/gtest.h>
#include <map>

using namespace pgaccel;
namespace {
struct Collisions { size_t operator()(int64_t value) const { return uint64_t(value) % 3; } };
}
TEST(ColumnIntern, StableExactIdentitiesThroughCollisionsAndGrowth)
{
    Budget budget(1 << 20);
    ColumnIntern<int64_t, Collisions> values(budget);
    std::map<int64_t, uint32_t> expected;
    for (int64_t value = -500; value <= 500; ++value)
        expected.emplace(value, values.intern(value));
    for (const auto &[value, id] : expected) {
        EXPECT_EQ(values.intern(value), id);
        EXPECT_EQ(values.find(value), id);
        EXPECT_EQ(values.keys()[id], value);
    }
    EXPECT_EQ(values.find(9999), decltype(values)::missing);
    EXPECT_EQ(values.keys().size(), expected.size());
}
TEST(ColumnIntern, FailedGrowthPreservesExistingIds)
{
    Budget budget(2048);
    ColumnIntern<int64_t, Collisions> values(budget);
    int64_t inserted = 0;
    try {
        for (; inserted < 1000; ++inserted) values.intern(inserted);
        FAIL() << "Expected bounded allocation failure";
    } catch (const std::runtime_error &) {}
    ASSERT_GT(inserted, 0);
    EXPECT_EQ(values.keys().size(), inserted);
    for (int64_t value = 0; value < inserted; ++value) {
        EXPECT_EQ(values.find(value), uint32_t(value));
        EXPECT_EQ(values.keys()[value], value);
    }
}
