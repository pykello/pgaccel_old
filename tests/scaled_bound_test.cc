#include "../src/executor_scaled_bound.hpp"
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(ScaledBound, DirectedRoundingMatchesExactCrossProducts) {
    for (auto op:{FilterClause::FILTER_EQ,FilterClause::FILTER_NE,FilterClause::FILTER_GT,
                  FilterClause::FILTER_GTE,FilterClause::FILTER_LT,FilterClause::FILTER_LTE})
        for (int rowScale:{0,2,5}) for (int boundScale:{0,1,4})
            for (int64_t bound:{INT64_MIN,int64_t(-101),int64_t(-10),int64_t(-1),int64_t(0),int64_t(1),int64_t(10),int64_t(101),INT64_MAX}) {
                ScaledBound compare(op,bound,boundScale,rowScale);
                for (int64_t value:{INT64_MIN,int64_t(-101),int64_t(-10),int64_t(-1),int64_t(0),int64_t(1),int64_t(10),int64_t(101),INT64_MAX}) {
                    int scale=std::max(rowScale,boundScale);
                    Wide a=Wide(value)*Power(scale-rowScale),b=Wide(bound)*Power(scale-boundScale);
                    bool expected=false;
                    switch(op) {
                    case FilterClause::FILTER_EQ:expected=a==b;break;
                    case FilterClause::FILTER_NE:expected=a!=b;break;
                    case FilterClause::FILTER_GT:expected=a>b;break;
                    case FilterClause::FILTER_GTE:expected=a>=b;break;
                    case FilterClause::FILTER_LT:expected=a<b;break;
                    case FilterClause::FILTER_LTE:expected=a<=b;break;
                    default:break;
                    }
                    EXPECT_EQ(compare.matches(value),expected);
                }
                ScaledBound null(op,bound,boundScale,rowScale,true);EXPECT_FALSE(null.matches(0));
            }
}
