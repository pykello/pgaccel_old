#pragma once
#include "executor_value.hpp"
#include <array>

namespace pgaccel {
namespace {
constexpr int Block = 256;
struct NotDense {};
struct Operation {
    Expression::Kind kind;
    int left = -1, right = -1;
    int64_t lm = 1, rm = 1, constant = 0;
    const RawColumnDataBase *column = nullptr;
    Wide low = 0, high = 0;
};
template<class T,bool Gather=false,bool VectorGather=false>
void DecodeNumbers(const RawColumnDataBase *column,int begin,int n,int64_t *out,bool avx,const uint32_t *selection=nullptr) {
    const auto source=reinterpret_cast<const T *>(column->values)+begin;
    if constexpr(Gather) {
        int i=0;
        if constexpr(VectorGather && (sizeof(T)==4 || sizeof(T)==8)) {
            if(avx)for(;i+8<=n;i+=8) {
                auto indices=_mm256_loadu_si256(reinterpret_cast<const __m256i *>(selection+i));
                __m512i values;
                if constexpr(sizeof(T)==4)
                    values=_mm512_cvtepi32_epi64(_mm256_i32gather_epi32(source,indices,4));
                else values=_mm512_i32gather_epi64(indices,source,8);
                _mm512_storeu_si512(out+i,values);
            }
        }
        for(;i<n;++i) out[i]=source[selection[i]];
        return;
    }
    int j=0;
    if(avx) for(;j+8<=n;j+=8) {
        __m512i values;
        if constexpr(sizeof(T)==1)
            values=_mm512_cvtepi8_epi64(_mm_loadl_epi64(reinterpret_cast<const __m128i *>(source+j)));
        else if constexpr(sizeof(T)==2)
            values=_mm512_cvtepi16_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(source+j)));
        else if constexpr(sizeof(T)==4)
            values=_mm512_cvtepi32_epi64(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(source+j)));
        else values=_mm512_loadu_si512(source+j);
        _mm512_store_si512(out+j,values);
    }
    for(;j<n;++j) out[j]=source[j];
}
// A stripe-specific range proof keeps every arithmetic intermediate and every
// stripe sum in int64. Other shapes retain checked 128-bit interpretation.
struct Program {
    std::vector<Operation> ops;
    int compile(const Expression &e, const RowGroup &rg) {
        Operation op{e.kind};
        if (e.kind == Expression::COLUMN) {
            if (e.type->type_num() != INT32_TYPE && e.type->type_num() != INT64_TYPE &&
                e.type->type_num() != DECIMAL_TYPE) throw NotDense{};
            auto data = rg.columns[e.column->columnIdx].get();
            if (data->type != ColumnDataBase::RAW_COLUMN_DATA) throw NotDense{};
            op.column = static_cast<const RawColumnDataBase *>(data);
            if (e.type->type_num() == INT32_TYPE) {
                auto d = static_cast<const RawColumnData<Int32Type> *>(data);
                op.low = d->minValue; op.high = d->maxValue;
            } else if (e.type->type_num() == DECIMAL_TYPE) {
                auto d = static_cast<const RawColumnData<DecimalType> *>(data);
                op.low = d->minValue; op.high = d->maxValue;
            } else {
                auto d = static_cast<const RawColumnData<Int64Type> *>(data);
                op.low = d->minValue; op.high = d->maxValue;
            }
        } else if (e.kind == Expression::CONSTANT) {
            if (e.type->type_num() != INT64_TYPE && e.type->type_num() != DECIMAL_TYPE)
                throw NotDense{};
            op.low = op.high = ParseDecimal(Scale(e.type.get()), e.literal);
            if (op.low < INT64_MIN || op.high > INT64_MAX) throw NotDense{};
            op.constant = int64_t(op.low);
        } else {
            if (e.kind != Expression::ADD && e.kind != Expression::SUBTRACT &&
                e.kind != Expression::MULTIPLY) throw NotDense{};
            op.left = compile(*e.left, rg); op.right = compile(*e.right, rg);
            auto l = ops[op.left], r = ops[op.right];
            if (e.kind == Expression::MULTIPLY) {
                std::array<Wide, 4> products{l.low*r.low, l.low*r.high,
                                           l.high*r.low, l.high*r.high};
                op.low = *std::min_element(products.begin(), products.end());
                op.high = *std::max_element(products.begin(), products.end());
            } else {
                Wide lm = Power(Scale(e.type.get())-Scale(e.left->type.get()));
                Wide rm = Power(Scale(e.type.get())-Scale(e.right->type.get()));
                if (lm > INT64_MAX || rm > INT64_MAX) throw NotDense{};
                op.lm = int64_t(lm); op.rm = int64_t(rm);
                l.low *= lm; l.high *= lm; r.low *= rm; r.high *= rm;
                if (l.low < INT64_MIN || l.high > INT64_MAX ||
                    r.low < INT64_MIN || r.high > INT64_MAX) throw NotDense{};
                op.low = e.kind == Expression::ADD ? l.low+r.low : l.low-r.high;
                op.high = e.kind == Expression::ADD ? l.high+r.high : l.high-r.low;
            }
        }
        if (op.low < INT64_MIN || op.high > INT64_MAX) throw NotDense{};
        for (size_t i=0;i<ops.size();++i) {
            const auto &p=ops[i];
            if (p.kind==op.kind && p.left==op.left && p.right==op.right &&
                p.lm==op.lm && p.rm==op.rm && p.constant==op.constant &&
                p.column==op.column) return i;
        }
        if (ops.size()==32) throw NotDense{};
        ops.push_back(op); return ops.size()-1;
    }
    template<bool Gather=false,bool VectorGather=false>
    void evaluate(int begin, int n, int64_t *buffers, bool avx,const uint32_t *selection=nullptr) const {
        for (size_t i=0;i<ops.size();++i) {
            const auto &p=ops[i]; auto out=buffers+i*Block;
            if (p.column) {
#define READ_WIDTH(T) DecodeNumbers<T,Gather,VectorGather>(p.column,begin,n,out,avx,selection)
                switch(p.column->bytesPerValue) {
                case 1: READ_WIDTH(int8_t); break;
                case 2: READ_WIDTH(int16_t); break;
                case 4: READ_WIDTH(int32_t); break;
                case 8: READ_WIDTH(int64_t); break;
                default: throw NotDense{};
                }
#undef READ_WIDTH
            } else if(p.kind==Expression::CONSTANT) {
                std::fill_n(out,n,p.constant);
            } else {
                const auto l=buffers+p.left*Block, r=buffers+p.right*Block;
                if(p.kind==Expression::MULTIPLY)
                    for(int j=0;j<n;++j) out[j]=l[j]*r[j];
                else if(ops[p.left].kind==Expression::CONSTANT && p.rm==1) {
                    const int64_t value=ops[p.left].constant*p.lm;
                    if(p.kind==Expression::ADD)
                        for(int j=0;j<n;++j) out[j]=value+r[j];
                    else for(int j=0;j<n;++j) out[j]=value-r[j];
                } else if(ops[p.right].kind==Expression::CONSTANT && p.lm==1) {
                    const int64_t value=ops[p.right].constant*p.rm;
                    if(p.kind==Expression::ADD)
                        for(int j=0;j<n;++j) out[j]=l[j]+value;
                    else for(int j=0;j<n;++j) out[j]=l[j]-value;
                } else if(p.lm==1 && p.rm==1) {
                    if(p.kind==Expression::ADD)
                        for(int j=0;j<n;++j) out[j]=l[j]+r[j];
                    else for(int j=0;j<n;++j) out[j]=l[j]-r[j];
                } else if(p.kind==Expression::ADD)
                    for(int j=0;j<n;++j) out[j]=l[j]*p.lm+r[j]*p.rm;
                else for(int j=0;j<n;++j) out[j]=l[j]*p.lm-r[j]*p.rm;
            }
        }
    }
};
}
}
