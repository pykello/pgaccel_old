#include "executor_bitmap_probe.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
namespace pgaccel { std::optional<Result<QueryOutput>> ExecuteCorrelatedAvg(const QueryDesc &, bool, bool); }
namespace {
QueryDesc GenericAverage(const QueryDesc &q) {
    auto reference = q;
    reference.scalarQueries[0].query = std::make_shared<QueryDesc>(*q.scalarQueries[0].query);
    auto truth = std::make_shared<Expression>(); truth->type = std::make_shared<Int64Type>(); truth->literal = "1";
    reference.scalarQueries[0].query->predicates.push_back(truth);
    return reference;
}
}
TEST(CorrelatedAverage, ExactThresholdsSparseGroupsAndDuplicateFallback) {
    for (bool duplicate : {false, true}) for (int width : {4, 8}) {
        std::vector<int64_t> keys, flags;
        for (int i = 64; i > 0; --i) { keys.push_back(i); flags.push_back(i <= 2); }
        if (duplicate) { keys.push_back(1); flags.push_back(1); }
        auto registry = test::Table({{test::Raw(keys, width), test::Raw(flags)}},
            {{"k", std::make_shared<Int64Type>()}, {"flag", std::make_shared<Int64Type>()}});
        auto decimal = std::make_shared<DecimalType>(); decimal->scale = 2;
        std::vector<std::vector<ColumnDataP>> groups;
        for (int g = 0; g < 3; ++g) {
            std::vector<int64_t> fk, quantity, price;
            for (int i = 0; i < 129; ++i) {
                fk.push_back(i % 5 == 0 ? 1 : i % 5 == 1 ? 2 : 99);
                quantity.push_back((i % 7 - 2) * 100);
                price.push_back((i % 11 - 3) * 125);
            }
            fk[0] = width == 4 ? INT32_MIN : INT64_MIN;
            fk[1] = width == 4 ? INT32_MAX : INT64_MAX;
            groups.push_back({test::Raw(fk, width), test::Raw(quantity), test::Raw(price)});
        }
        auto facts = test::Table(std::move(groups), {{"k", std::make_shared<Int64Type>()},
            {"qty", decimal}, {"price", decimal}});
        registry.emplace("f", std::move(facts.at("t")));
        for (const std::string op : {"<", "<=", ">", ">=", "=", "<>"})
            for (const std::string factor : {"0.2", "1.0", "0.333333"}) {
                auto q = ParseSelect("SELECT sum(f.price)/7.0,count(*) FROM t,f WHERE t.k=f.k AND t.flag=1 AND f.qty " + op +
                    " (SELECT " + factor + "*avg(a.qty) FROM f a WHERE a.k=t.k)", registry);
                ASSERT_TRUE(q.ok()) << q.status().Message();
                auto specialized = ExecuteCorrelatedAvg(*q, true, true);
                if (duplicate) EXPECT_FALSE(specialized);
                else { ASSERT_TRUE(specialized); ASSERT_TRUE(specialized->ok()) << specialized->status().Message(); }
                auto expected = ExecuteQuery(GenericAverage(*q), false, false);
                ASSERT_TRUE(expected.ok()) << expected.status().Message();
                for (bool avx : {false, true}) for (bool parallel : {false, true}) {
                    auto actual = ExecuteQuery(*q, avx, parallel);
                    ASSERT_TRUE(actual.ok()) << actual.status().Message();
                    EXPECT_EQ(actual->values, expected->values) << op << " " << factor;
                }
            }
        auto empty = ParseSelect("SELECT sum(f.price),count(*) FROM t,f WHERE t.k=f.k AND t.flag=99 AND f.qty < "
            "(SELECT 0.2*avg(a.qty) FROM f a WHERE a.k=t.k)", registry);
        ASSERT_TRUE(empty.ok()); auto result = ExecuteQuery(*empty, true, true);
        ASSERT_TRUE(result.ok()); EXPECT_EQ(result->values, Rows({{"NULL", "0"}}));
    }
}
TEST(CorrelatedAverage, RoundedAverageThresholdAndReversedComparison) {
    std::vector<int64_t> keys, flags;
    for (int k = 1; k <= 32; ++k) { keys.push_back(k); flags.push_back(k == 1); }
    auto registry = test::Table({{test::Raw(keys), test::Raw(flags)}},
        {{"k", std::make_shared<Int64Type>()}, {"flag", std::make_shared<Int64Type>()}});
    auto decimal = std::make_shared<DecimalType>(); decimal->scale = 2;
    auto facts = test::Table({{test::Raw({1, 1, 1}), test::Raw({100, 0, 0})}},
        {{"k", std::make_shared<Int64Type>()}, {"qty", decimal}});
    registry.emplace("f", std::move(facts.at("t")));
    // Rounded AVG is 0.333333333333, so multiplying by 3 remains below 1.
    for (const std::string condition : {
        "f.qty<(SELECT 3.0*avg(a.qty) FROM f a WHERE a.k=t.k)",
        "(SELECT 3.0*avg(a.qty) FROM f a WHERE a.k=t.k)>f.qty"}) {
        auto q = ParseSelect("SELECT count(*) FROM t,f WHERE t.k=f.k AND t.flag=1 AND " + condition, registry);
        ASSERT_TRUE(q.ok()) << q.status().Message();
        for (bool parallel : {false, true}) {
            auto actual = ExecuteQuery(*q, true, parallel);
            ASSERT_TRUE(actual.ok()) << actual.status().Message(); EXPECT_EQ(actual->values, Rows({{"2"}}));
            auto expected = ExecuteQuery(GenericAverage(*q), false, false);
            ASSERT_TRUE(expected.ok()); EXPECT_EQ(actual->values, expected->values);
        }
        auto childLimited = *q;
        childLimited.scalarQueries[0].query = std::make_shared<QueryDesc>(*q->scalarQueries[0].query);
        childLimited.scalarQueries[0].query->memoryBudgetBytes = 1;
        EXPECT_FALSE(ExecuteCorrelatedAvg(childLimited, true, true));
        EXPECT_FALSE(ExecuteQuery(childLimited, true, true).ok());
        auto limited = *q; limited.memoryBudgetBytes = 1; limited.allowSpill = false;
        EXPECT_FALSE(ExecuteQuery(limited, true, true).ok());
    }
}

TEST(BitmapProbe, HierarchicalSparseWordsMasksAndTails) {
    constexpr int first=13,last=200019;
    std::vector<uint64_t> bits((last-first+64)/64);
    for(int key:{first,first+31,first+32,first+1023,first+1024,last}) {
        int offset=key-first;bits[offset/64]|=uint64_t(1)<<(offset%64);
    }
    size_t words=(last-first+32)/32;
    std::vector<uint32_t> summary((words+31)/32);
    auto narrow=reinterpret_cast<const uint32_t *>(bits.data());
    for(size_t i=0;i<words;++i) if(narrow[i]) summary[i/32]|=uint32_t(1)<<(i%32);
    BitmapDomain domain{first,last,bits.data(),summary.data()};
    std::vector<int32_t> values(320);
    const int32_t cases[]={INT32_MIN,first-1,first,first+31,first+32,first+1023,
        first+1024,first+1025,last,last+1,INT32_MAX,100000};
    for(size_t i=0;i<values.size();++i)values[i]=cases[i%12];
    uint8_t bitmap[40];memset(bitmap,0xaa,sizeof(bitmap));
    uint32_t output[320];
    for(int begin:{0,16,32}) for(int n:{0,1,15,16,17,63,64,65,255,256})
        for(bool filtered:{false,true}) {
            std::vector<uint32_t> expected;
            for(int row=begin;row<begin+n;++row)
                if((!filtered || (bitmap[row/8]>>(row%8)&1)) && domain.contains(values[row]))expected.push_back(row);
            int count=BitmapSelectRange32<true>(domain,values.data(),begin,n,filtered?bitmap:nullptr,output);
            EXPECT_EQ((std::vector<uint32_t>(output,output+count)),expected);
            int flat=BitmapSelectRange32(domain,values.data(),begin,n,filtered?bitmap:nullptr,output);
            EXPECT_EQ((std::vector<uint32_t>(output,output+flat)),expected);
        }
}
