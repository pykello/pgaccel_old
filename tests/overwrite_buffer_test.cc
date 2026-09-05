#include "executor_buffer.hpp"
#include "executor_state.hpp"
#include <gtest/gtest.h>
#include <limits>

using namespace pgaccel;
TEST(OverwriteBuffer, WrittenElementsSurviveMoveAndReleaseTheBudget)
{
    Budget memory(4096);
    {
        OverwriteBuffer<uint64_t> first(128, memory);
        EXPECT_EQ(memory.used.load(), 1024);
        for (size_t i = 0; i < first.size(); ++i) first[i] = i * 991;
        OverwriteBuffer<uint64_t> second(std::move(first));
        EXPECT_EQ(first.size(), 0);
        for (size_t i = 0; i < second.size(); ++i) EXPECT_EQ(second[i], i * 991);
        EXPECT_EQ(memory.used.load(), 1024);
    }
    EXPECT_EQ(memory.used.load(), 0);
    OverwriteBuffer<uint64_t> empty(0, memory);
    EXPECT_EQ(empty.data(), nullptr);
    EXPECT_THROW((OverwriteBuffer<uint64_t>(1024, memory)), std::runtime_error);
    EXPECT_EQ(memory.used.load(), 0);
    EXPECT_THROW((OverwriteBuffer<uint64_t>(std::numeric_limits<size_t>::max(), memory)), std::bad_array_new_length);
}
