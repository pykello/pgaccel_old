#include "column_index.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <map>

using namespace pgaccel;
TEST(PreparedFilter, QueryLocalLabelMasksRespectPatternsAndCrossStripeCodes)
{
    std::vector<std::vector<ColumnDataP>> groups;
    std::vector<std::string> all;
    for (int g = 0; g < 8; ++g) {
        std::vector<std::string> values;
        for (int i = 0; i < 300; ++i) {
            std::string value = std::to_string(i + g * 100) + (i % 7 == 0 ? "google" : i % 11 == 0 ? "ordinary" : "");
            values.push_back(value); all.push_back(value);
        }
        groups.push_back({test::Dict<StringType>(values)});
    }
    auto registry = test::Table(std::move(groups), {{"s", std::make_shared<StringType>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 24).ok());
    for (std::string needle : {"google", "ordinary", "missing"}) for (bool negate : {false, true}) {
        uint64_t count = 0;
        for (auto &s : all) count += (s.find(needle) != std::string::npos) != negate;
        auto q = ParseSelect("SELECT count(*) FROM t WHERE s<>'' AND s " +
            std::string(negate ? "NOT LIKE" : "LIKE") + " '%" + needle + "%'", registry);
        ASSERT_TRUE(q.ok());
        for (bool avx : {false, true}) for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*q, avx, parallel);
            ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, Rows({{std::to_string(count)}}));
            EXPECT_GT(result->peakMemoryBytes, 0);
        }
    }
}
TEST(PreparedFilter, FailedPreparationDoesNotPublishAnIncompleteMask)
{
    auto registry = test::Table({{test::Dict<StringType>({"google", "ordinary", "other"})}},
        {{"s", std::make_shared<StringType>()}});
    ASSERT_TRUE(registry.at("t")->BuildDictionaries(1 << 20).ok());
    auto q = ParseSelect("SELECT count(*) FROM t WHERE s LIKE '%google%'", registry);
    ASSERT_TRUE(q.ok());
    std::pmr::monotonic_buffer_resource noSpace(std::pmr::null_memory_resource());
    auto filter = CreateFilterNode(q->filterClauses, true);
    EXPECT_THROW(filter->Prepare(*registry.at("t"), noSpace, 1), std::bad_alloc);
    EXPECT_EQ(filter->ExecuteCount(registry.at("t")->GetRowGroup(0)), 1);
}
