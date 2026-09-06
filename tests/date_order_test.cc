#include "test_table.hpp"
#include "executor_date_order.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(DateOrder, DistinctDictionariesWidthsEqualityAndSelectedTails) {
    for(int n:{23,513}) {
        std::vector<int32_t> left,right;
        for(int i=0;i<n;++i) {left.push_back((i%257)*2-300);right.push_back(((i*31)%300)*3-400);}
        auto registry=test::Table({{test::Dict<DateType>(left),test::Dict<DateType>(right)}},
            {{"a",std::make_shared<DateType>()},{"b",std::make_shared<DateType>()}});
        for(const std::string op:{"<","<=",">",">="}) {
            auto q=ParseSelect("SELECT count(*) FROM t WHERE a"+op+"b",registry);ASSERT_TRUE(q.ok());
            ASSERT_EQ(q->predicates.size(),1);auto &e=*q->predicates[0];
            ASSERT_TRUE(DateOrderPredicate::supports(e,0));
            Budget budget(1<<20);DateOrderPredicate predicate(e,registry.at("t")->GetRowGroup(0),budget);
            for(bool avx:{false,true})for(int step:{1,3}) {
                std::vector<uint32_t> selected,expected;
                for(int i=0;i<n;i+=step) {
                    selected.push_back(i);
                    bool keep=op=="<"?left[i]<right[i]:op=="<="?left[i]<=right[i]:op==">"?left[i]>right[i]:left[i]>=right[i];
                    if(keep)expected.push_back(i);
                }
                int count=predicate.select(selected.data(),selected.size(),selected.data(),avx);
                selected.resize(count);EXPECT_EQ(selected,expected);
            }
        }
    }
}
