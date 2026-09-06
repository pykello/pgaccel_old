#pragma once
#include "executor_arithmetic_program.hpp"
#include "executor_state.hpp"

namespace pgaccel {
namespace {
// A bounded DAG shares repeated native filters across boolean branches.
// Each instruction writes one stripe bitmap into caller-budgeted storage.
class BooleanBitmapProgram {
    struct Op {
        enum Kind { LEAF, AND, OR, TRUE_VALUE, FALSE_VALUE } kind;
        int left=-1,right=-1;
        std::optional<FilterClause> clause;
        FilterNodeP filter;
        explicit Op(Kind k):kind(k) {}
    };
    Budget &budget;bool avx;int attempts=0;
    std::pmr::vector<Op> ops;
    static bool constant(const Expression &e,int depth=0) {
        if(depth>64)return false;
        if(e.kind==Expression::CONSTANT)return true;
        return (e.kind==Expression::ADD || e.kind==Expression::SUBTRACT || e.kind==Expression::MULTIPLY) &&
            e.left && e.right && constant(*e.left,depth+1) && constant(*e.right,depth+1);
    }
    int append(Op op) {if(ops.size()==64)throw NotDense{};ops.push_back(std::move(op));return ops.size()-1;}
public:
    BooleanBitmapProgram(Budget &memory,bool useAvx):budget(memory),avx(useAvx),ops(&memory) {}
    size_t bytes() const {return ops.size()*(RowGroupSize/8);}
    int truth(bool value=true) {
        auto kind=value?Op::TRUE_VALUE:Op::FALSE_VALUE;
        for(size_t i=0;i<ops.size();++i)if(ops[i].kind==kind)return i;
        return append(Op(kind));
    }
    int combine(bool conjunction,int left,int right) {
        if(left==right)return left;auto kind=conjunction?Op::AND:Op::OR;
        if(left>right)std::swap(left,right);
        for(size_t i=0;i<ops.size();++i)if(ops[i].kind==kind && ops[i].left==left && ops[i].right==right)return i;
        Op op(kind);op.left=left;op.right=right;return append(std::move(op));
    }
    int leaf(const FilterClause &clause) {
        for(size_t i=0;i<ops.size();++i)if(ops[i].clause) {
            const auto &f=*ops[i].clause;
            if(f.op==clause.op && f.columnRef==clause.columnRef && f.value==clause.value && f.values==clause.values)return i;
        }
        uint64_t strings=clause.value.size()+clause.columnRef.columnDesc.name.size();
        for(const auto &v:clause.values)strings+=v.size();budget.charge(3*strings+512);
        Op op(Op::LEAF);op.clause=clause;op.filter=CreateFilterNode({clause},avx);return append(std::move(op));
    }
    int compile(const Expression &e,int table) {
        if(++attempts>128)throw NotDense{};
        if(e.kind==Expression::AND || e.kind==Expression::OR) {
            int a=compile(*e.left,table),b=compile(*e.right,table);return combine(e.kind==Expression::AND,a,b);
        }
        if(e.kind==Expression::CONSTANT && e.type->type_num()==INT64_TYPE)return truth(Evaluate(e,RowGroup{},0).number!=0);
        if(e.kind<Expression::EQ || e.kind>Expression::GTE || !e.left || !e.right)throw NotDense{};
        auto column=e.left,value=e.right;bool reverse=!column->column;if(reverse)std::swap(column,value);
        if(!column->column || column->column->tableIdx!=table || !constant(*value))throw NotDense{};
        FilterClause f;f.columnRef=*column->column;
        switch(e.kind) {
        case Expression::EQ:f.op=FilterClause::FILTER_EQ;break;case Expression::NE:f.op=FilterClause::FILTER_NE;break;
        case Expression::LT:f.op=reverse?FilterClause::FILTER_GT:FilterClause::FILTER_LT;break;
        case Expression::LTE:f.op=reverse?FilterClause::FILTER_GTE:FilterClause::FILTER_LTE;break;
        case Expression::GT:f.op=reverse?FilterClause::FILTER_LT:FilterClause::FILTER_GT;break;
        case Expression::GTE:f.op=reverse?FilterClause::FILTER_LTE:FilterClause::FILTER_GTE;break;
        default:throw NotDense{};
        }
        Cell bound;
        try {bound=Evaluate(*value,RowGroup{},0);}catch(const std::exception &){throw NotDense{};}
        auto type=column->type->type_num();
        if(type==STRING_TYPE) {
            if(!bound.string)throw NotDense{};f.value=bound.text;
        } else if(type==DATE_TYPE) {
            if(value->type->type_num()!=DATE_TYPE || bound.number<INT32_MIN || bound.number>INT32_MAX)throw NotDense{};
            f.value=ToString(column->type.get(),int64_t(bound.number));
        } else {
            if((type!=INT32_TYPE && type!=INT64_TYPE && type!=DECIMAL_TYPE) || bound.string ||
                (value->type->type_num()!=INT32_TYPE && value->type->type_num()!=INT64_TYPE && value->type->type_num()!=DECIMAL_TYPE))throw NotDense{};
            int scale=Scale(column->type.get()),source=Scale(value->type.get());
            if(source>scale) {auto divisor=Power(source-scale);if(bound.number%divisor)throw NotDense{};bound.number/=divisor;}
            else try {bound.number=Multiply(bound.number,Power(scale-source));}catch(const std::exception &){throw NotDense{};}
            if(bound.number<INT64_MIN || bound.number>INT64_MAX || (type==INT32_TYPE && (bound.number<INT32_MIN || bound.number>INT32_MAX)))throw NotDense{};
            f.value=Number(bound.number,scale);
        }
        return leaf(f);
    }
    const uint8_t *bitmap(const uint8_t *scratch,int root) const {return scratch+root*(RowGroupSize/8);}
    void evaluate(const RowGroup &rg,uint8_t *scratch,std::array<int,64> &counts) const {
        const int words=(rg.size+63)/64,bytes=(rg.size+7)/8;
        for(size_t i=0;i<ops.size();++i) {
            const auto &op=ops[i];auto out=scratch+i*(RowGroupSize/8);
            if(op.kind==Op::LEAF) {
                if(rg.size%64)std::memset(out+(rg.size/64)*8,0,8);
                counts[i]=op.filter->ExecuteSet(rg,out);
                if(rg.size%8)out[bytes-1]&=(1u<<(rg.size%8))-1;
                continue;
            }
            if(op.kind==Op::TRUE_VALUE || op.kind==Op::FALSE_VALUE) {
                std::memset(out,op.kind==Op::TRUE_VALUE?0xff:0,words*8);
                if(rg.size%64)reinterpret_cast<uint64_t *>(out)[words-1]&=(uint64_t(1)<<(rg.size%64))-1;
                counts[i]=op.kind==Op::TRUE_VALUE?rg.size:0;continue;
            }
            auto a=reinterpret_cast<const uint64_t *>(bitmap(scratch,op.left)),b=reinterpret_cast<const uint64_t *>(bitmap(scratch,op.right));
            auto dest=reinterpret_cast<uint64_t *>(out);int count=0;
            for(int word=0;word<words;++word) {dest[word]=op.kind==Op::AND?a[word]&b[word]:a[word]|b[word];count+=__builtin_popcountll(dest[word]);}
            counts[i]=count;
        }
    }
};
}
}
