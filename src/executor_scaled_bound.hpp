#pragma once
#include "executor_value.hpp"
namespace pgaccel {
namespace {
// Compare a scaled integer to a fixed decimal bound without multiplying
// every row. Directed rounding preserves strict and inclusive comparisons.
struct ScaledBound {
    FilterClause::Op op;
    Wide floor,ceil;
    bool integral,valid;
    ScaledBound(FilterClause::Op comparison,Wide value,int boundScale,int rowScale,bool null=false)
        :op(comparison),floor(value),ceil(value),integral(true),valid(!null) {
        if (!valid) return;
        if (boundScale>rowScale) {
            Wide divisor=Power(boundScale-rowScale),remainder=value%divisor;
            floor=value/divisor;if (remainder<0) --floor;
            integral=remainder==0;ceil=floor+!integral;
        } else floor=ceil=Multiply(value,Power(rowScale-boundScale));
    }
    bool matches(Wide value) const {
        if (!valid) return false;
        switch (op) {
        case FilterClause::FILTER_EQ:return integral && value==floor;
        case FilterClause::FILTER_NE:return !integral || value!=floor;
        case FilterClause::FILTER_GT:return value>floor;
        case FilterClause::FILTER_GTE:return value>=ceil;
        case FilterClause::FILTER_LT:return value<ceil;
        case FilterClause::FILTER_LTE:return value<=floor;
        default:return false;
        }
    }
};
}
}
