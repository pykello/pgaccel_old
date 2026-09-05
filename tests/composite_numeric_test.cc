#include "test_table.hpp"
#include <gtest/gtest.h>

using namespace pgaccel;

TEST(CompositeNumeric, InlineAritiesAndDynamicFallbackAcrossWorkers)
{
    std::vector<std::vector<ColumnDataP>> groups;
    for (int group = 0; group < 8; ++group)
        groups.push_back({test::Raw({1, 1, 2}), test::Raw({3, 3, 4}), test::Raw({5, -2, 9})});
    auto registry = test::Table(std::move(groups),
        {{"a", std::make_shared<Int64Type>()}, {"b", std::make_shared<Int64Type>()},
         {"v", std::make_shared<Int64Type>()}});
    for (int aggregates = 1; aggregates <= 5; ++aggregates)
    {
        std::string sql = "SELECT a,b";
        Row first{"1", "3"}, second{"2", "4"};
        for (int i = 0; i < aggregates; ++i)
        {
            sql += ",sum(v)";
            first.push_back("24");
            second.push_back("72");
        }
        sql += " FROM t GROUP BY a,b ORDER BY a,b";
        auto query = ParseSelect(sql, registry);
        ASSERT_TRUE(query.ok());
        for (bool parallel : {false, true})
        {
            auto result = ExecuteQuery(*query, true, parallel);
            ASSERT_TRUE(result.ok());
            EXPECT_EQ(result->values, Rows({first, second}));
        }
    }
}
