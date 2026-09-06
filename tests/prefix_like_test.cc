#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(PrefixLike, SortedDictionaryRangesPreserveExactRegexSemantics) {
    std::vector<std::string> labels{"for", "forest", "forest green", "forestz",
        "forest\nx", std::string("forest\0x", 8), std::string("forest\xff", 7),
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

TEST(PrefixLike, ContiguousCodeIntervalsCoverWidthsEndsAndPriorMasks) {
    for (int distinct : {255, 256, 65536}) {
        std::vector<std::string> values;
        int size=distinct<65536?513:65536;
        for(int i=0;i<size;++i) {
            int label=i%distinct;
            values.push_back(std::string(label%2?"z":"a")+std::to_string(label));
        }
        std::vector<std::string> tail{"zlast","afirst","zlast","other","afirst"};
        auto registry=test::Table({{test::Dict<StringType>(values)},{test::Dict<StringType>(tail)}},
            {{"s",std::make_shared<StringType>()}});
        for(const std::string prefix:{"z","a","absent"})for(bool anti:{false,true})for(bool avx:{false,true}) {
            auto q=ParseSelect("SELECT count(*) FROM t WHERE s "+std::string(anti?"NOT LIKE":"LIKE")+" '"+prefix+"%'",registry);
            ASSERT_TRUE(q.ok());auto filter=CreateFilterNode(q->filterClauses,avx);
            for(int g=0;g<2;++g)for(bool intersect:{false,true}) {
                const auto &input=g?tail:values;const auto &rg=q->tables[0]->GetRowGroup(g);
                std::array<uint8_t,RowGroupSize/8> bitmap{};
                for(size_t row=0;row<input.size();++row)if(row%5)bitmap[row/8]|=uint8_t(1)<<(row%8);
                int actual=intersect?filter->ExecuteAnd(rg,bitmap.data()):filter->ExecuteSet(rg,bitmap.data());
                int expected=0;
                for(size_t row=0;row<input.size();++row) {
                    bool keep=(!intersect||row%5)&&(input[row].starts_with(prefix)!=anti);
                    EXPECT_EQ(bool(bitmap[row/8]>>(row%8)&1),keep);expected+=keep;
                }
                EXPECT_EQ(actual,expected);
            }
        }
    }
}
