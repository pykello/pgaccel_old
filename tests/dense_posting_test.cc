#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;
TEST(DensePostings, EveryPhysicalRowAppearsOnceUnderItsExactValue)
{
    std::vector<std::vector<int64_t>> values{{INT64_MIN, 7, 7, INT64_MAX}, {}, {0, 7, INT64_MIN}, {INT64_MAX, 0}};
    std::vector<std::vector<ColumnDataP>> groups;
    for (const auto &stripe : values) groups.push_back({test::Raw(stripe)});
    auto registry = test::Table(std::move(groups), {{"k", std::make_shared<Int64Type>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 20).ok());
    auto index = registry.at("t")->Dictionary(0);
    ASSERT_TRUE(index->HasDensePostings());
    EXPECT_FALSE(index->HasSparsePostings());
    std::vector<int64_t> flattened;
    for (const auto &stripe : values) flattened.insert(flattened.end(), stripe.begin(), stripe.end());
    std::vector<int> seen(flattened.size());
    for (uint32_t id = 0; id < index->size(); ++id) {
        uint32_t previous = 0; bool first = true;
        for (uint32_t row : index->PostingRows(id)) {
            ASSERT_LT(row, flattened.size());
            EXPECT_EQ(flattened[row], index->Numbers()[id]);
            EXPECT_TRUE(first || previous < row);
            previous = row; first = false; ++seen[row];
        }
    }
    for (int count : seen) EXPECT_EQ(count, 1);
    EXPECT_EQ(index->PostingOffsets().back(), flattened.size());
    EXPECT_THROW(index->PostingRows(index->size()), std::out_of_range);
}
TEST(DensePostings, FailedConstructionPublishesNoPartialIndex)
{
    auto registry = test::Table({{test::Raw({1, 1, 2, 3, 2})}}, {{"k", std::make_shared<Int64Type>()}});
    auto full = GlobalDictionary::Build(*registry.at("t"), 0, 1 << 20);
    ASSERT_TRUE(full.ok());
    auto insufficient = registry.at("t")->BuildDictionaries((*full)->bytes() - 1);
    EXPECT_FALSE(insufficient.ok());
    EXPECT_EQ(registry.at("t")->Dictionary(0), nullptr);
    EXPECT_TRUE(registry.at("t")->BuildDictionaries(1 << 20).ok());
}
