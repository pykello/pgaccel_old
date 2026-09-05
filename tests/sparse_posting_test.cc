#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <map>

using namespace pgaccel;
TEST(SparsePostings, RepeatedBoundariesPreserveActualRowLocations)
{
    auto value = [](int i) -> int64_t {
        if (i == 0) return INT64_MIN;
        if (i == 4095) return INT64_MAX;
        return (i * 7919) % 4096 - 2048;
    };
    std::vector<std::vector<int64_t>> input(3);
    for (int i = 0; i < 100; ++i) input[0].push_back(value(i));
    input[1].push_back(value(5));
    for (int i = 100; i < 3000; ++i) input[1].push_back(value(i));
    input[2] = {value(2999), value(0)};
    for (int i = 3000; i < 4096; ++i) input[2].push_back(value(i));
    input[2].push_back(value(4095));
    std::vector<std::vector<ColumnDataP>> groups;
    std::map<int64_t, std::vector<uint32_t>> expected;
    uint32_t position = 0;
    for (const auto &values : input) {
        groups.push_back({test::Raw(values)});
        for (int64_t value : values) expected[value].push_back(position++);
    }
    auto registry = test::Table(std::move(groups), {{"v", std::make_shared<Int64Type>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 22).ok());
    auto index = registry.at("t")->Dictionary(0);
    ASSERT_TRUE(index->HasSparsePostings());
    EXPECT_EQ(index->RowOffset(1), input[0].size());
    EXPECT_EQ(index->RowOffset(2), input[0].size() + input[1].size());
    std::vector<uint32_t> repeated;
    for (const auto &[value, positions] : expected) {
        uint32_t id = index->Find(value);
        std::vector<uint32_t> actual{index->FirstRow(id)};
        for (const auto &row : index->AdditionalRows(id)) {
            EXPECT_EQ(row.id, id);
            actual.push_back(row.row);
        }
        EXPECT_EQ(actual, positions);
        if (positions.size() > 1) repeated.insert(repeated.end(), positions.begin(), positions.end());
    }
    std::sort(repeated.begin(), repeated.end());
    EXPECT_EQ(std::vector<uint32_t>(index->RepeatedRows().begin(), index->RepeatedRows().end()), repeated);
}
TEST(SparsePostings, FrequentDuplicatesDoNotQualify)
{
    auto registry = test::Table({{test::Raw({1, 1, 2, 2, 1})}}, {{"v", std::make_shared<Int64Type>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 20).ok());
    EXPECT_FALSE(registry.at("t")->Dictionary(0)->HasSparsePostings());
}
