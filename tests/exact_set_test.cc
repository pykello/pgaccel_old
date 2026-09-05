#include "executor_state.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <set>

using namespace pgaccel;

TEST(ExactSet, SingletonIntegerAndWidePromotionRemainExact)
{
    Budget budget(1 << 20);
    ExactSet values(budget);
    EXPECT_EQ(values.size(), 0);
    values.insert(Cell{INT64_MIN});
    values.insert(Cell{INT64_MIN});
    EXPECT_EQ(values.size(), 1);
    values.insert(Cell{INT64_MAX});
    values.insert(Cell{0});
    EXPECT_EQ(values.size(), 3);
    values.insert(Cell{Wide(INT64_MAX) + 1});
    values.insert(Cell{Wide(INT64_MIN) - 1});
    values.insert(Cell{INT64_MIN});
    std::set<Wide> actual;
    values.forEach([&](const Cell &v) { actual.insert(v.number); });
    EXPECT_EQ(actual, std::set<Wide>({INT64_MIN, INT64_MAX, 0,
                                    Wide(INT64_MAX) + 1, Wide(INT64_MIN) - 1}));
    EXPECT_EQ(values.size(), actual.size());
}

TEST(ExactSet, FailedGeneralPromotionRetainsIntegerValues)
{
    Budget budget(512);
    ExactSet values(budget);
    for (int i = 0; i < 5; ++i) values.insert(Cell{i});
    auto used = budget.used.load();
    budget.limit = used;
    EXPECT_THROW(values.insert(Cell{Wide(INT64_MAX) + 1}), std::runtime_error);
    EXPECT_EQ(values.size(), 5);
    EXPECT_EQ(budget.used.load(), used);
    std::set<Wide> actual;
    values.forEach([&](const Cell &v) { actual.insert(v.number); });
    EXPECT_EQ(actual, std::set<Wide>({0, 1, 2, 3, 4}));
}

TEST(ExactSet, GroupedWideExpressionsMergeAcrossWorkers)
{
    auto registry = test::Table(
        {{test::Raw({1, 1, 1, 2}), test::Raw({INT64_MIN, 0, INT64_MAX, 7})},
         {test::Raw({1, 1, 2, 2}), test::Raw({INT64_MIN, 0, 7, 8})}},
        {{"g", std::make_shared<Int64Type>()}, {"v", std::make_shared<Int64Type>()}});
    for (const char *expression : {"v", "v+1", "v-1"}) {
        auto query = ParseSelect(std::string("SELECT g,count(DISTINCT ") + expression +
                                 ") FROM t GROUP BY g ORDER BY g", registry);
        ASSERT_TRUE(query.ok());
        for (bool parallel : {false, true}) {
            auto result = ExecuteQuery(*query, true, parallel);
            ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, Rows({{"1", "3"}, {"2", "2"}}));
        }
    }
}
