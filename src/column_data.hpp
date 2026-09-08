#pragma once

#include <vector>
#include <map>
#include <execution>
#include <algorithm>
#include <set>
#include <cstdint>
#include <iostream>
#include <span>

#include "result_type.hpp"
#include "types.hpp"

namespace pgaccel
{

/*
 * Data Structures
*/
inline void* AllocateAligned(size_t alignment, size_t bytes) {
    auto p = std::aligned_alloc(alignment, ((std::max(bytes, size_t(1)) + alignment - 1) / alignment) * alignment);
    if (!p) throw std::bad_alloc();
    return p;
}

const int RowGroupSize = 1 << 16;

class GlobalDictionary;
struct StringSignature;
struct ColumnDataBase;
typedef std::shared_ptr<ColumnDataBase> ColumnDataP;

struct ColumnDataBase {
    enum Type {
        DICT_COLUMN_DATA = 0,
        RAW_COLUMN_DATA = 1
    } type;
    int size;
    // Optional immutable auxiliary encoding, attached before query readers.
    const GlobalDictionary *globalDictionary = nullptr;
    std::span<const uint32_t> globalIds;
    std::span<const StringSignature> localStringSignatures;
    uint64_t globalRowOffset = 0;

    virtual Result<bool> Save(std::ostream &out, bool aligned = false) const = 0;

    virtual ~ColumnDataBase() {};

    static Result<ColumnDataP> Load(std::istream &in, AccelType *type, bool aligned = false);
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

    virtual Result<bool> Save(std::ostream &out, bool aligned = false) const;

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

    virtual Result<bool> Save(std::ostream &out, bool aligned = false) const;
};

// Version 2 pads value vectors to the allocation alignment, including their
// final vector tail. Version 1 retains its original byte layout.
inline void PadNativeValues(std::ostream &out) {
    auto position = out.tellp();
    if (position < 0) { out.setstate(std::ios::failbit); return; }
    const char zeros[512]{};
    size_t padding = (512 - uint64_t(position) % 512) % 512;
    out.write(zeros, padding);
}

// Save functions
template<typename AccelTy>
Result<bool>
RawColumnData<AccelTy>::Save(std::ostream &out, bool aligned) const
{
    out.write((char *) &type, sizeof(type));
    out.write((char *) &size, sizeof(size));
    out.write((char *) &bytesPerValue, sizeof(bytesPerValue));
    out.write((char *) &minValue, sizeof(minValue));
    out.write((char *) &maxValue, sizeof(maxValue));
    if (aligned) PadNativeValues(out);
    out.write((char *) values, size * bytesPerValue);
    if (aligned) PadNativeValues(out);
    return true;
}

template<typename AccelTy>
Result<bool>
DictColumnData<AccelTy>::Save(std::ostream &out, bool aligned) const
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
    if (aligned) PadNativeValues(out);
    out.write((char *) values, size * bytesPerValue);
    if (aligned) PadNativeValues(out);
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
