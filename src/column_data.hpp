#pragma once

#include <vector>
#include <map>
#include <execution>
#include <algorithm>
#include <set>
#include <cstdint>
#include <iostream>

#include "result_type.hpp"
#include "types.hpp"

namespace pgaccel
{

/*
 * Data Structures
*/
const int RowGroupSize = 1 << 16;

struct ColumnDataBase;
typedef std::unique_ptr<ColumnDataBase> ColumnDataP;

struct ColumnDataBase {
    enum Type {
        DICT_COLUMN_DATA = 0,
        RAW_COLUMN_DATA = 1
    } type;
    int size;

    virtual Result<bool> Save(std::ostream &out) const = 0;

    virtual ~ColumnDataBase() {};

    static Result<ColumnDataP> Load(std::istream &in, AccelType *type);
};

template<class Ty>
struct DictColumnData: public ColumnDataBase {
    using DictTy = typename Ty::c_type;
    std::vector<DictTy> dict;
    uint8_t *values = NULL;

    virtual Result<bool> Save(std::ostream &out) const;

    virtual ~DictColumnData() {
        if (values)
            free(values);
    }

private:
    Result<bool> SaveValue(std::ostream &out, const DictTy &value) const;
};

struct RawColumnDataBase: public ColumnDataBase {
    uint8_t *values = NULL;
    int bytesPerValue;

    virtual ~RawColumnDataBase() {
        if (values)
            free(values);
    }
};

template<class Ty>
struct RawColumnData: public RawColumnDataBase {
    typename Ty::c_type minValue, maxValue;

    virtual Result<bool> Save(std::ostream &out) const;
};

// Save functions
template<typename AccelTy>
Result<bool>
RawColumnData<AccelTy>::Save(std::ostream &out) const
{
    out.write((char *) &type, sizeof(type));
    out.write((char *) &size, sizeof(size));
    out.write((char *) &bytesPerValue, sizeof(bytesPerValue));
    out.write((char *) &minValue, sizeof(minValue));
    out.write((char *) &maxValue, sizeof(maxValue));
    out.write((char *) values, size * bytesPerValue);
    return true;
}

template<typename AccelTy>
Result<bool>
DictColumnData<AccelTy>::Save(std::ostream &out) const
{
    out.write((char *) &type, sizeof(type));
    int dictSize = dict.size();
    out.write((char *) &dictSize, sizeof(dictSize));
    for (auto v: dict)
    {
        SaveValue(out, v);
    }
    out.write((char *) &size, sizeof(size));
    int bytesPerValue = (dictSize < 256) ? 1 : 2;
    out.write((char *) values, size * bytesPerValue);
    return true;
}

template<typename AccelTy>
Result<bool>
DictColumnData<AccelTy>::SaveValue(std::ostream &out,
                                   const typename AccelTy::c_type &value) const
{
    out.write((char *) &value, sizeof(value));
    return true;
}

// declare specialized version for StringType, define in .cc
template<>
Result<bool> DictColumnData<StringType>::SaveValue(
    std::ostream &out,
    const std::string &value) const;

};
