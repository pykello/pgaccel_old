#include "test_table.hpp"
#include "../src/executor_correlation_domain.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(CorrelationDomain, FilteredDimensionRestrictsChildAndEquivalentProbe) {
    for (int width : {4, 8}) {
        std::vector<int64_t> keys, flags, factKeys, quantities;
        std::vector<std::string> labels;
        for (int i = 1; i <= 32; ++i) { keys.push_back(i); flags.push_back(i == 1); labels.push_back(i == 1 ? "keep" : "drop"); }
        keys.push_back(1); flags.push_back(1); labels.push_back("keep");
        for (int i = 1; i <= 32; ++i) for (int value : {1, 2, 9}) {
            factKeys.push_back(i); quantities.push_back(value);
        }
        auto registry = test::Table({{test::Raw(keys), test::Raw(flags), test::Dict<StringType>(labels)}},
            {{"k", std::make_shared<Int64Type>()}, {"flag", std::make_shared<Int64Type>()}, {"label", std::make_shared<StringType>()}});
        std::shared_ptr<AccelType> type = std::make_shared<Int64Type>();
        auto fact = test::Table({{test::Raw(factKeys, width), test::Raw(quantities)}},
            {{"k", type}, {"v", std::make_shared<Int64Type>()}});
        registry.emplace("u", std::move(fact.at("t")));
        for (bool avx : {false, true}) for (const auto &[filter, expected] : std::vector<std::pair<std::string, Rows>>{
            {"t.flag=1 AND t.label LIKE 'keep%'", {{"6"}}}, {"t.flag=99", {{"NULL"}}}}) {
            auto q = ParseSelect("SELECT sum(u.v) FROM t,u WHERE t.k=u.k AND " + filter +
                " AND u.v<(SELECT avg(a.v) FROM u a WHERE a.k=t.k)", registry);
            ASSERT_TRUE(q.ok()) << q.status().Message();
            auto restricted = CorrelationDomains(*q, avx); ASSERT_TRUE(restricted);
            EXPECT_EQ(restricted->membershipQueries.size(), 2);
            EXPECT_TRUE(q->membershipQueries.empty()); // Source plan is immutable.
            for (const QueryDesc *plan : std::array<const QueryDesc *, 2>{&*q, &*restricted}) {
                auto result = ExecuteQuery(*plan, avx, false);
                ASSERT_TRUE(result.ok()) << result.status().Message();
                EXPECT_EQ(result->values, expected);
            }
        }
        auto broad = ParseSelect("SELECT sum(u.v) FROM t,u WHERE t.k=u.k AND t.flag>=0"
            " AND u.v<(SELECT avg(a.v) FROM u a WHERE a.k=t.k)", registry);
        ASSERT_TRUE(broad.ok()); EXPECT_FALSE(CorrelationDomains(*broad, true));
    }
}
