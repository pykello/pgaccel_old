#include "test_table.hpp"
#include "../src/executor_bounded_sum.hpp"
#include "../src/executor_result.hpp"
#include <gtest/gtest.h>
namespace pgaccel {Result<QueryOutput> ExecuteModern(const QueryDesc &,bool,bool);}
using namespace pgaccel;
TEST(BoundedSum, SignedWidthsFiltersRepeatedKeysAndZeroGroups) {
    for(int width:{1,2,4,8}) {
        std::vector<std::vector<ColumnDataP>> groups;
        for(int g=0;g<3;++g) {
            std::vector<int64_t> keys,values;
            for(int i=0;i<259;++i){keys.push_back(i%23-11);values.push_back((i%19-9)*(g+1));}
            groups.push_back({test::Raw(keys,width),test::Raw(values,2)});
        }
        auto decimal=std::make_shared<DecimalType>();decimal->scale=2;
        auto registry=test::Table(std::move(groups),{{"k",std::make_shared<Int64Type>()},{"x",decimal}});
        for(const std::string filter:{"", " WHERE x>0.00", " WHERE x>100.00"})
        for(const std::string value:{"x*(1-0.05)","x-x"}) {
            auto parsed=ParseSelect("SELECT k,sum("+value+") FROM t"+filter+" GROUP BY k ORDER BY k",registry);
            ASSERT_TRUE(parsed.ok())<<parsed.status().Message();const auto &q=*parsed;
            auto expected=ExecuteModern(q,false,false);ASSERT_TRUE(expected.ok());
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                Budget budget(1<<20);ScopedValueResource memory(budget);
                auto native=q.filterClauses.empty()?FilterNodeP{}:CreateFilterNode(q.filterClauses,avx);
                auto sums=ScanBoundedSums(*q.tables[0],*q.aggregateClauses[0].columnRef,
                    *q.aggregateClauses[1].expression,native.get(),budget,avx,parallel);
                QueryResults result(q,budget);
                for(size_t slot=0;slot<sums.sums.size();++slot)if(sums.contains(slot))result.addLazy([&](size_t output) {
                    return FinalCell{Cell{output?sums.sums[slot]:Wide(sums.first)+slot},q.aggregateClauses[output].expression->type.get()};
                });
                EXPECT_EQ(result.finish().values,expected->values);
            }
        }
    }
}
TEST(BoundedSum, ExtremeKeysWideFallbackAndBudgets) {
    for(int64_t first:{INT64_MIN,INT64_MAX-2}) {
        auto registry=test::Table({{test::Raw({first,first+2,first}),test::Raw({4,-7,-4})}},
            {{"k",std::make_shared<Int64Type>()},{"x",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k,sum(x) FROM t GROUP BY k",registry);ASSERT_TRUE(q.ok());
        Budget budget(1<<20);auto sums=ScanBoundedSums(*q->tables[0],*q->aggregateClauses[0].columnRef,
            *q->aggregateClauses[1].expression,nullptr,budget,true,true);
        ASSERT_EQ(sums.sums.size(),3);EXPECT_TRUE(sums.contains(0));EXPECT_EQ(sums.sums[0],0);
        EXPECT_FALSE(sums.contains(1));EXPECT_TRUE(sums.contains(2));EXPECT_EQ(sums.sums[2],-7);
        Budget tiny(1);EXPECT_THROW(ScanBoundedSums(*q->tables[0],*q->aggregateClauses[0].columnRef,
            *q->aggregateClauses[1].expression,nullptr,tiny,true,true),std::runtime_error);
    }
    for(const auto &columns:std::vector<std::vector<ColumnDataP>>{
        {test::Raw({INT64_MIN,INT64_MAX}),test::Raw({1,2})},
        {test::Raw({1,1}),test::Raw({INT64_MAX,INT64_MAX})}}) {
        auto registry=test::Table({columns},{{"k",std::make_shared<Int64Type>()},{"x",std::make_shared<Int64Type>()}});
        auto q=ParseSelect("SELECT k,sum(x) FROM t GROUP BY k",registry);ASSERT_TRUE(q.ok());
        Budget budget(1<<20);EXPECT_THROW(ScanBoundedSums(*q->tables[0],*q->aggregateClauses[0].columnRef,
            *q->aggregateClauses[1].expression,nullptr,budget,true,true),NotDense);
    }
}
