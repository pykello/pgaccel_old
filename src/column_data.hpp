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
typedef std::shared_ptr<ColumnDataBase> ColumnDataP;

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

struct DictColumnDataBase: public ColumnDataBase {
    uint8_t *values = NULL;
    std::shared_ptr<AccelType> valueType;
    virtual int bytesPerValue() const = 0;
    virtual int dictSize() const = 0;
    virtual std::vector<std::string> labels() const = 0;
    virtual std::string label(int) const = 0;

    void to_16(uint16_t *out);
};

template<class Ty>
struct DictColumnData: public DictColumnDataBase {
    using DictTy = typename Ty::c_type;
    std::vector<DictTy> dict;

    virtual Result<bool> Save(std::ostream &out) const;

    virtual ~DictColumnData() {
        if (values)
            free(values);
    }

    virtual int bytesPerValue() const {
        return dict.size() < 256 ? 1 : 2;
    }

    virtual int dictSize() const {
        return dict.size();
    }

    virtual std::vector<std::string> labels() const {
        std::vector<std::string> result;
        for (const auto& v: dict)
            result.push_back(ToString(valueType.get(), v));
        return result;
    }

    virtual std::string label(int idx) const {
        return ToString(valueType.get(), dict[idx]);
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
