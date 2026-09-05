#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;

TEST(GlobalDictionary, NumericBitPatternsAndIndependentLocalCodeSpaces)
{
    auto registry = test::Table(
        {{test::Raw({INT64_MIN, 0, INT64_MAX}), test::Dict<StringType>({"b", "", "b"}),
          test::Dict<DateType>({-1, 2, -1})},
         {test::Raw({INT64_MAX, 7, INT64_MIN}), test::Dict<StringType>({"a", "b", ""}),
          test::Dict<DateType>({2, 7, -1})}},
        {{"v", std::make_shared<Int64Type>()}, {"s", std::make_shared<StringType>()},
         {"d", std::make_shared<DateType>()}});
    auto &table = *registry.at("t");
    auto built = table.BuildDictionaries(1 << 20);
    ASSERT_TRUE(built.ok()) << built.status().Message();
    ASSERT_EQ(built->columns.size(), 3);
    EXPECT_GE(built->peakBytes, built->bytes);
    auto numbers = table.Dictionary(0);
    ASSERT_NE(numbers, nullptr);
    EXPECT_EQ(numbers->size(), 4);
    EXPECT_EQ(numbers->Codes(0)[0], numbers->Codes(1)[2]);
    EXPECT_EQ(numbers->Codes(0)[2], numbers->Codes(1)[0]);
    for (int g = 0; g < 2; ++g) for (int row = 0; row < 3; ++row) {
        auto raw = static_cast<const RawColumnDataBase *>(table.GetRowGroup(g).columns[0].get());
        EXPECT_EQ(numbers->Numbers()[numbers->Codes(g)[row]], reinterpret_cast<const int64_t *>(raw->values)[row]);
        for (int column : {1, 2}) {
            auto dictionary = static_cast<const DictColumnDataBase *>(table.GetRowGroup(g).columns[column].get());
            int code = dictionary->values[row];
            auto index = table.Dictionary(column);
            if (column == 1) {
                const auto &text = static_cast<const DictColumnData<StringType> *>(dictionary)->dict[code];
                EXPECT_EQ(index->Strings()[index->Codes(g)[code]], text);
                EXPECT_EQ(index->Find(std::string_view(text)), index->Codes(g)[code]);
            } else {
                int64_t date = static_cast<const DictColumnData<DateType> *>(dictionary)->dict[code];
                EXPECT_EQ(index->Numbers()[index->Codes(g)[code]], date);
                EXPECT_EQ(index->Find(date), index->Codes(g)[code]);
            }
        }
    }
    auto repeated = table.BuildDictionaries(1 << 20);
    ASSERT_TRUE(repeated.ok());
    EXPECT_TRUE(repeated->columns.empty());
    EXPECT_EQ(repeated->bytes, built->bytes);
}

TEST(GlobalDictionary, FailedConstructionIsNotPublished)
{
    auto data = test::Dict<StringType>({"a", "b", "a"});
    auto registry = test::Table({{data}}, {{"s", std::make_shared<StringType>()}});
    auto &table = *registry.at("t");
    auto failed = table.BuildDictionaries(1);
    EXPECT_FALSE(failed.ok());
    EXPECT_EQ(table.Dictionary(0), nullptr);
    auto built = table.BuildDictionaries(1 << 20);
    ASSERT_TRUE(built.ok());
    auto index = table.Dictionary(0);
    EXPECT_EQ(index->size(), 2);
    EXPECT_EQ(index->Find(std::string_view("absent")), UINT32_MAX);
}
