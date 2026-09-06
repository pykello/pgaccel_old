#pragma once
#include "executor_arithmetic_program.hpp"
#include "executor_state.hpp"

namespace pgaccel {
namespace {
// Translate one stripe-local dictionary into exact thresholds in the other.
// Comparing raw dictionary IDs from different columns would be incorrect.
class DateOrderPredicate {
    const DictColumnData<DateType> *left,*right;
    int leftWidth,rightWidth;
    std::pmr::vector<uint32_t> thresholds;
public:
    static bool supports(const Expression &e,int table) {
        return (e.kind==Expression::LT || e.kind==Expression::LTE ||
                e.kind==Expression::GT || e.kind==Expression::GTE) &&
            e.left && e.right && e.left->column && e.right->column &&
            e.left->column->tableIdx==table && e.right->column->tableIdx==table &&
            e.left->type->type_num()==DATE_TYPE && e.right->type->type_num()==DATE_TYPE;
    }
    DateOrderPredicate(const Expression &e,const RowGroup &rg,Budget &budget):thresholds(&budget) {
        auto a=rg.columns[e.left->column->columnIdx].get(),b=rg.columns[e.right->column->columnIdx].get();
        if(a->type!=ColumnDataBase::DICT_COLUMN_DATA || b->type!=ColumnDataBase::DICT_COLUMN_DATA)throw NotDense{};
        if(e.kind==Expression::GT || e.kind==Expression::GTE)std::swap(a,b);
        left=static_cast<const DictColumnData<DateType> *>(a);right=static_cast<const DictColumnData<DateType> *>(b);
        leftWidth=left->bytesPerValue();rightWidth=right->bytesPerValue();
        bool strict=e.kind==Expression::LT || e.kind==Expression::GT;
        thresholds.resize(left->dictSize());size_t j=0;
        for(size_t i=0;i<thresholds.size();++i) {
            while(j<right->dictSize() && (strict?right->dict[j]<=left->dict[i]:right->dict[j]<left->dict[i]))++j;
            thresholds[i]=j;
        }
    }
    int select(const uint32_t *input,int n,uint32_t *output,bool avx) const {
        int row=0,count=0;
        auto code=[](const DictColumnData<DateType> *d,int width,uint32_t row) {
            return width==1?uint32_t(d->values[row]):uint32_t(reinterpret_cast<const uint16_t *>(d->values)[row]);
        };
        if(avx)for(;row+16<=n;row+=16) {
            alignas(64)uint32_t a[16],b[16];
            for(int lane=0;lane<16;++lane) {a[lane]=code(left,leftWidth,input[row+lane]);b[lane]=code(right,rightWidth,input[row+lane]);}
            auto limits=_mm512_i32gather_epi32(_mm512_load_si512(a),thresholds.data(),4);
            auto mask=_mm512_cmpge_epi32_mask(_mm512_load_si512(b),limits);
            _mm512_mask_compressstoreu_epi32(output+count,mask,_mm512_loadu_si512(input+row));
            count+=__builtin_popcount(mask);
        }
        for(;row<n;++row)if(code(right,rightWidth,input[row])>=thresholds[code(left,leftWidth,input[row])])output[count++]=input[row];
        return count;
    }
};
}
}
