#pragma once
#include "executor.h"
#include <atomic>
#include <cstring>
#include <iomanip>
#include <numeric>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <memory_resource>
#include <re2/re2.h>

namespace pgaccel
{
namespace
{
using Wide = __int128_t;
thread_local std::pmr::memory_resource *valueResource=std::pmr::new_delete_resource();
struct ScopedValueResource {
    std::pmr::memory_resource *previous;
    explicit ScopedValueResource(std::pmr::memory_resource &memory):previous(valueResource) {valueResource=&memory;}
    ~ScopedValueResource(){valueResource=previous;}
};
struct Cell
{
    Wide number = 0;
    std::string_view text;
    bool string = false;
    std::shared_ptr<const std::pmr::string> owner;
    bool operator==(const Cell &b) const
    {
        return string == b.string && (string ? text == b.text : number == b.number);
    }
};
struct Hash
{
    size_t operator()(const Cell &v) const
    {
        if (v.string)
            return std::hash<std::string_view>{}(v.text);
        uint64_t x = uint64_t(v.number) ^ uint64_t(v.number >> 64);
        x ^= x >> 30;
        x *= 0xbf58476d1ce4e5b9ULL;
        x ^= x >> 27;
        x *= 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }
};
bool Less(const Cell &a, const Cell &b) { return a.string ? a.text < b.text : a.number < b.number; }
int Scale(const AccelType *t)
{
    return t->type_num() == DECIMAL_TYPE ? static_cast<const DecimalType *>(t)->scale : 0;
}
Wide Power(int n)
{
    Wide v = 1;
    while (n-- > 0)
        v *= 10;
    return v;
}
Wide Add(Wide a, Wide b)
{
    Wide c;
    if (__builtin_add_overflow(a, b, &c))
        throw std::overflow_error("128-bit accumulator overflow");
    return c;
}
Wide Multiply(Wide a, Wide b)
{
    Wide c;
    if (__builtin_mul_overflow(a, b, &c))
        throw std::overflow_error("128-bit expression overflow");
    return c;
}
int Code(const DictColumnDataBase *d, int i)
{
    return d->bytesPerValue() == 1 ? d->values[i]
                                   : reinterpret_cast<const uint16_t *>(d->values)[i];
}
Cell Label(const DictColumnDataBase *d, int i, TypeNum type)
{
    if (type == STRING_TYPE)
        return {0, static_cast<const DictColumnData<StringType> *>(d)->dict[i], true};
    return {static_cast<const DictColumnData<DateType> *>(d)->dict[i]};
}
Cell Read(const RowGroup &rg, const ColumnRef &c, int row)
{
    auto data = rg.columns[c.columnIdx].get();
    if (data->type == ColumnDataBase::DICT_COLUMN_DATA)
    {
        auto d = static_cast<const DictColumnDataBase *>(data);
        return Label(d, Code(d, row), c.Type()->type_num());
    }
    auto d = static_cast<const RawColumnDataBase *>(data);
    switch (d->bytesPerValue)
    {
    case 1:
        return {reinterpret_cast<const int8_t *>(d->values)[row]};
    case 2:
        return {reinterpret_cast<const int16_t *>(d->values)[row]};
    case 4:
        return {reinterpret_cast<const int32_t *>(d->values)[row]};
    case 8:
        return {reinterpret_cast<const int64_t *>(d->values)[row]};
    default:
        throw std::runtime_error("Invalid storage width");
    }
}
Cell Evaluate(const Expression &e, const RowGroup &rg, int row)
{
    if (e.kind == Expression::COLUMN)
        return Read(rg, *e.column, row);
    if (e.kind == Expression::CONSTANT)
    {
        if (e.type->type_num() == STRING_TYPE)
            return {0, e.literal, true};
        return {ParseDecimal(Scale(e.type.get()), e.literal)};
    }
    if (e.kind==Expression::REGEXP_REPLACE)
    {
        Cell input=Evaluate(*e.left,rg,row);
        re2::StringPiece pieces[10];
        if (!e.regexp->Match(re2::StringPiece(input.text.data(),input.text.size()),0,input.text.size(),
                             re2::RE2::UNANCHORED,pieces,10)) return input;
        const auto &rewrite=e.replacement;
        if (rewrite.size()==2 && rewrite[0]=='\\' && rewrite[1]>='0' && rewrite[1]<='9' && pieces[0].size()==input.text.size())
        {
            auto capture=pieces[rewrite[1]-'0'];
            return {0,std::string_view(capture.data(),capture.size()),true,input.owner};
        }
        auto output=std::allocate_shared<std::pmr::string>(std::pmr::polymorphic_allocator<std::pmr::string>(valueResource));
        size_t begin=pieces[0].data()-input.text.data(),end=begin+pieces[0].size();
        output->append(input.text.data(),begin);
        for(size_t i=0;i<rewrite.size();++i)
        {
            if(rewrite[i]!='\\')output->push_back(rewrite[i]);
            else {
                char escaped=rewrite[++i];
                if(escaped=='\\')output->push_back(escaped);
                else if(pieces[escaped-'0'].size())output->append(pieces[escaped-'0'].data(),pieces[escaped-'0'].size());
            }
        }
        output->append(input.text.data()+end,input.text.size()-end);
        return {0,std::string_view(*output),true,output};
    }
    if (e.kind==Expression::EXTRACT || e.kind==Expression::DATE_TRUNC)
    {
        Wide seconds=Evaluate(*e.left,rg,row).number;
        Wide unit=e.literal=="day"?86400:e.literal=="hour"?3600:e.literal=="minute"?60:1;
        Wide remainder=seconds%unit;
        if (remainder<0) remainder+=unit;
        Wide rounded=seconds-remainder;
        if (e.kind==Expression::DATE_TRUNC) return {rounded};
        Wide value=(rounded/unit)%(e.literal=="hour"?24:60);
        if (value<0) value+=e.literal=="hour"?24:60;
        return {value};
    }
    if (e.kind==Expression::LENGTH)
        return {Wide(Evaluate(*e.left,rg,row).text.size())};
    auto l = Evaluate(*e.left, rg, row).number, r = Evaluate(*e.right, rg, row).number;
    int ls = Scale(e.left->type.get()), rs = Scale(e.right->type.get()), s = Scale(e.type.get());
    if (e.kind == Expression::MULTIPLY)
        return {Multiply(l, r)};
    if (e.kind == Expression::DIVIDE)
    {
        if (!r)
            throw std::invalid_argument("Division by zero");
        auto numerator = Multiply(l, Power(s + rs - ls));
        if (numerator == std::numeric_limits<Wide>::min() && r == -1)
            throw std::overflow_error("Division overflow");
        return {numerator / r};
    }
    l = Multiply(l, Power(s - ls));
    r = Multiply(r, Power(s - rs));
    if (e.kind == Expression::SUBTRACT)
    {
        Wide v;
        if (__builtin_sub_overflow(l, r, &v))
            throw std::overflow_error("Subtraction overflow");
        return {v};
    }
    return {Add(l, r)};
}
std::string Number(Wide n, int scale)
{
    bool negative = n < 0;
    __uint128_t magnitude = negative ? __uint128_t(-(n + 1)) + 1 : __uint128_t(n);
    std::string s;
    do
    {
        s += char('0' + magnitude % 10);
        magnitude /= 10;
    } while (magnitude);
    std::reverse(s.begin(), s.end());
    if (scale)
    {
        if (s.size() <= scale)
            s.insert(0, scale + 1 - s.size(), '0');
        s.insert(s.size() - scale, 1, '.');
    }
    return (negative ? "-" : "") + s;
}
} // namespace
} // namespace pgaccel
