#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(PrefixLike, SortedDictionaryRangesPreserveExactRegexSemantics) {
    std::vector<std::string> labels{"for", "forest", "forest green", "forestz",
        "forest\nx", std::string("forest\0x", 8), std::string("forest\xff", 7),
        "forest😀", std::string("forest\xc0\xaf", 8), std::string("forest\x80", 7),
        std::string("forest\xed\xa0\x80", 9), std::string("forest\xf4\x90\x80\x80", 10),
        std::string("forest\xc2", 7), std::string("forest\xe0\x80\x80", 9),
        std::string("forest\xf0\x90\x80\x80", 10),
        "foret", "alpha", "é", "éclair", "zebra"};
    std::vector<std::string> values;
    for (int i = 0; i < 513; ++i) values.push_back(labels[i % labels.size()]);
    std::vector<int64_t> keys; for (int i = 0; i < 513; ++i) keys.push_back(i);
    auto registry = test::Table({{test::Dict<StringType>(values), test::Raw(keys, 4)}},
        {{"s", std::make_shared<StringType>()}, {"k", std::make_shared<Int64Type>()}});
    for (bool avx : {false, true}) for (bool anti : {false, true})
        for (const std::string prefix : {"forest%", "é%", "absent%", "z%", "%", "fo_est%"})
            for (const std::string restriction : {"", "k<5 AND "}) {
                std::string predicate = "s " + std::string(anti ? "NOT LIKE" : "LIKE") + " '" + prefix + "'";
                auto native = ParseSelect("SELECT count(*) FROM t WHERE " + restriction + predicate, registry);
                auto reference = ParseSelect("SELECT count(*) FROM t WHERE " + restriction + "(" + predicate + " OR 1=0)", registry);
                ASSERT_TRUE(native.ok()); ASSERT_TRUE(reference.ok());
                auto actual = ExecuteQuery(*native, avx, true), expected = ExecuteQuery(*reference, false, false);
                ASSERT_TRUE(actual.ok()) << actual.status().Message(); ASSERT_TRUE(expected.ok()) << expected.status().Message();
                EXPECT_EQ(actual->values, expected->values) << predicate << " " << restriction;
            }
}
