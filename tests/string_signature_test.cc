#include "string_signature.hpp"
#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <random>

using namespace pgaccel;
TEST(StringSignature, EveryActualSubstringRemainsAPossibleMatch)
{
    std::mt19937 random(991);
    for (int sample = 0; sample < 100; ++sample) {
        std::string value;
        for (int i = 0; i < sample; ++i) value.push_back(char(random() % 256));
        auto signature = StringSignature::Build(value);
        for (size_t first = 0; first <= value.size(); ++first)
            for (size_t size = 0; size <= value.size() - first; ++size)
                EXPECT_TRUE(signature.mayContain(StringSignature::Build(std::string_view(value).substr(first, size))));
    }
}
TEST(StringSignature, IndexedLikeStillChecksExactBytesAndNegation)
{
    std::vector<std::string> values{
        "", "go", "google", "Google", "goxogle", "prefix-google-suffix", "ordinary",
        std::string("a\0google", 8), "goog", "oogle", "кириллица-google", "αβγ"};
    std::mt19937 random(101);
    for (int i = 0; i < 2000; ++i) {
        std::string value;
        for (int j = 0; j < i % 100; ++j) value.push_back('a' + random() % 26);
        if (i % 17 == 0) value += "google";
        values.push_back(value);
    }
    auto registry = test::Table({{test::Dict<StringType>(values)}}, {{"s", std::make_shared<StringType>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 22).ok());
    for (const std::string needle : {"google", "Google", "go", "ordinary", "γ"}) {
        uint64_t matching = 0;
        for (const auto &value : values) matching += value.find(needle) != std::string::npos;
        for (bool negate : {false, true}) {
            auto query = ParseSelect("SELECT count(*) FROM t WHERE s " + std::string(negate ? "NOT LIKE" : "LIKE") +
                                     " '%" + needle + "%'", registry);
            ASSERT_TRUE(query.ok());
            for (bool avx : {false, true}) {
                auto result = ExecuteQuery(*query, avx, true);
                ASSERT_TRUE(result.ok());
                EXPECT_EQ(result->values, Rows({{std::to_string(negate ? values.size() - matching : matching)}}));
            }
        }
    }
}

TEST(StringSignature, LocalLayoutsFollowIndependentStripeLabelOrders)
{
    auto registry = test::Table({{test::Dict<StringType>({"z-google", "aa", "z-google"})},
        {test::Dict<StringType>({"aa", "b-google", "zz"})}},
        {{"s", std::make_shared<StringType>()}});
    auto table = registry.at("t").get();
    ASSERT_TRUE(table->BuildDictionaries(1 << 20).ok());
    for (int g = 0; g < table->RowGroupCount(); ++g) {
        auto data = static_cast<const DictColumnData<StringType> *>(table->GetRowGroup(g).columns[0].get());
        ASSERT_EQ(data->localStringSignatures.size(), data->dict.size());
        for (size_t i = 0; i < data->dict.size(); ++i)
            EXPECT_EQ(data->localStringSignatures[i].words, StringSignature::Build(data->dict[i]).words);
    }
    for (bool parallel : {false, true}) for (bool avx : {false, true}) {
        auto query = ParseSelect("SELECT count(*) FROM t WHERE s LIKE '%google%'", registry);
        ASSERT_TRUE(query.ok()); auto result = ExecuteQuery(*query, avx, parallel);
        ASSERT_TRUE(result.ok()); EXPECT_EQ(result->values, Rows({{"3"}}));
    }
}
