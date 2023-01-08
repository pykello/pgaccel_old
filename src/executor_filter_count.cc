#include "column_data.hpp"
#include "types.hpp"
#include "executor.h"
#include <immintrin.h>
#include <future>

namespace pgaccel
{

template<int REGW, int N>
struct AvxTraits {};

template<>
struct AvxTraits<512, 8> {
    using mask_type = __mmask64;
};

template<>
struct AvxTraits<512, 16> {
    using mask_type = __mmask32;
};

template<>
struct AvxTraits<512, 32> {
    using mask_type = __mmask16;
};

template<>
struct AvxTraits<512, 64> {
    using mask_type = __mmask8;
};

#define Define_CountMatchesAvx(REGW, N, TYPE) \
template<bool countMatches, bool fillBitmap> \
int FilterMatchesRawAVX ## REGW ## _ ## N(const uint8_t *buf, int size, TYPE value, uint8_t *bitmap) \
{ \
    using RegType = __m ## REGW ## i; \
    using MaskType = AvxTraits<REGW, N>::mask_type; \
    RegType comparator = _mm ## REGW ## _set1_epi ## N(value); \
    auto valuesR = reinterpret_cast<const RegType *>(buf); \
\
    int avxCnt = size / (REGW / N); \
    int matches = 0; \
    MaskType *bitmapTyped = (MaskType *) bitmap; \
\
    for (int i = 0; i < avxCnt; i++) { \
        MaskType mask = _mm ## REGW ## _cmp_epi ## N ## _mask(valuesR[i], comparator, _MM_CMPINT_EQ); \
        if constexpr(countMatches) \
            matches += __builtin_popcountll(mask); \
        if constexpr(fillBitmap) \
            bitmapTyped[i] = mask; \
    } \
\
    auto values8 = reinterpret_cast<const TYPE *>(buf); \
    for (int i = (REGW / N) * avxCnt; i < size; i++) {\
        if (values8[i] == value) \
        { \
            if constexpr(countMatches) \
                matches++; \
            if constexpr(fillBitmap) \
                bitmap[i >> 3] |= (1 << (i & 7)); \
        } \
    } \
\
    return matches; \
}

Define_CountMatchesAvx(512, 8, uint8_t)
Define_CountMatchesAvx(512, 16, uint16_t)
Define_CountMatchesAvx(512, 32, uint32_t)
Define_CountMatchesAvx(512, 64, uint64_t)

template<class AccelTy>
int CountMatchesDictAVX(const DictColumnData<AccelTy> &columnData, 
                        typename AccelTy::c_type value)
{
    int dictIdx = DictIndex(columnData, value);
    if (dictIdx == -1)
        return 0;

    if (columnData.dict.size() < 256) {
        return FilterMatchesRawAVX512_8<true, false>(
                    columnData.values, columnData.size, dictIdx, nullptr);
    } else {
        return FilterMatchesRawAVX512_16<true, false>(
                    columnData.values, columnData.size, dictIdx, nullptr);
    }
}

template<class AccelTy>
int CountMatchesDict(const DictColumnData<AccelTy> &columnData, 
                     typename AccelTy::c_type value,
                     bool useAvx)
{
    if (useAvx)
        return CountMatchesDictAVX(columnData, value);

    int dictIdx = DictIndex(columnData, value);
    if (dictIdx == -1)
        return 0;

    if (columnData.dict.size() < 256) {
        int matches = 0;
        for (int i = 0; i < columnData.size; i++)
            if (columnData.values[i] == dictIdx)
                matches++;
        return matches;
    } else {
        auto values16 = reinterpret_cast<const uint16_t *>(columnData.values);
        int matches = 0;
        for (int i = 0; i < columnData.size; i++)
            if (values16[i] == dictIdx)
                matches++;
        return matches;
    }
}

template<class AccelTy, class storageType, bool returnCount, bool fillBitmap>
int FilterMatchesRaw(const RawColumnData<AccelTy> &columnData,
                     storageType value, uint8_t *bitmap)
{
    int count = 0;
    auto values = reinterpret_cast<const storageType *>(columnData.values);
    for (int i = 0; i < columnData.size; i++) {
        if (values[i] == value)
        {
            if constexpr (returnCount)
                count++;
            if constexpr (fillBitmap)
                bitmap[i >> 3] |= (1 << (i & 7));
        }
    }
    return count;
}

template<class AccelTy, bool returnCount, bool fillBitmap>
int FilterMatchesRaw(const RawColumnData<AccelTy> &columnData,
                     const typename AccelTy::c_type &value,
                     uint8_t *bitmap,
                     bool useAvx)
{
    if(value < columnData.minValue || value > columnData.maxValue)
        return 0;

    switch (columnData.bytesPerValue) {
        case 1:
            if (useAvx)
                return FilterMatchesRawAVX512_8<returnCount, fillBitmap>(
                            columnData.values,
                            columnData.size,
                            (uint8_t) value,
                            bitmap);
            else
                return FilterMatchesRaw<AccelTy, int8_t, returnCount, fillBitmap>(
                    columnData, value, bitmap);
        case 2:
            if (useAvx)
                return FilterMatchesRawAVX512_16<returnCount, fillBitmap>(
                            columnData.values,
                            columnData.size,
                            (uint16_t) value,
                            bitmap);
            else
                return FilterMatchesRaw<AccelTy, int16_t, returnCount, fillBitmap>(
                    columnData, value, bitmap);
        case 4:
            if (useAvx)
                return FilterMatchesRawAVX512_32<returnCount, fillBitmap>(
                            columnData.values,
                            columnData.size,
                            (uint32_t) value,
                            bitmap);
            else
                return FilterMatchesRaw<AccelTy, int16_t, returnCount, fillBitmap>(
                    columnData, value, bitmap);
        case 8:
            if (useAvx)
                return FilterMatchesRawAVX512_64<returnCount, fillBitmap>(
                            columnData.values,
                            columnData.size,
                            (uint64_t) value,
                            bitmap);
            else
                return FilterMatchesRaw<AccelTy, int16_t, returnCount, fillBitmap>(
                    columnData, value, bitmap);
    }

    return 0;
}

template<typename AccelTy>
class FilterRawDataNode: public FilterNode {
public:
    FilterRawDataNode(const typename AccelTy::c_type &value,
                      CompareOp op,
                      bool useAvx): 
        value(value),
        op(op),
        useAvx(useAvx) {}

    int ExecuteCount(ColumnDataBase *columnData) {
        auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData);
        return FilterMatchesRaw<AccelTy, true, false>(
            *typedColumnData, value, nullptr, useAvx);
    }

    int ExecuteSet(ColumnDataBase *columnData, uint8_t *bitmask) {
        return 0;
    }

    int ExecuteAnd(ColumnDataBase *columnData, uint8_t *bitmask) {
        return 0;
    }

private:
    typename AccelTy::c_type value;
    CompareOp op;
    bool useAvx;
};

template<typename AccelTy>
class FilterDictDataNode: public FilterNode {
public:
    FilterDictDataNode(const typename AccelTy::c_type &value,
                       CompareOp op,
                       bool useAvx): 
        value(value),
        op(op),
        useAvx(useAvx) {}

    int ExecuteCount(ColumnDataBase *columnData) {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return CountMatchesDict<AccelTy>(*typedColumnData, value, useAvx);
    }

    int ExecuteSet(ColumnDataBase *columnData, uint8_t *bitmask) {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return 0;
    }

    int ExecuteAnd(ColumnDataBase *columnData, uint8_t *bitmask) {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return 0;
    }

private:
    typename AccelTy::c_type value;
    CompareOp op;
    bool useAvx;
};

std::unique_ptr<FilterNode>
CreateRawFilterNode(const ColumnDesc &columnDesc,
                    const std::string &valueStr,
                    CompareOp op,
                    bool useAvx)
{
    switch (columnDesc.type->type_num())
    {
        case INT32_TYPE:
            return std::make_unique<FilterRawDataNode<Int32Type>>(
                columnDesc.type->asInt32Type()->Parse(valueStr),
                op,
                useAvx);
        case INT64_TYPE:
            return std::make_unique<FilterRawDataNode<Int64Type>>(
                columnDesc.type->asInt64Type()->Parse(valueStr),
                op,
                useAvx);
        case DECIMAL_TYPE:
            return std::make_unique<FilterRawDataNode<DecimalType>>(
                columnDesc.type->asDecimalType()->Parse(valueStr),
                op,
                useAvx);
    }

    return {};
}

std::unique_ptr<FilterNode>
CreateDictFilterNode(const ColumnDesc &columnDesc,
                     const std::string &valueStr,
                     CompareOp op,
                     bool useAvx)
{
    switch (columnDesc.type->type_num())
    {
        case STRING_TYPE:
            return std::make_unique<FilterDictDataNode<StringType>>(
                columnDesc.type->asStringType()->Parse(valueStr),
                op,
                useAvx);
        case DATE_TYPE:
            return std::make_unique<FilterDictDataNode<DateType>>(
                columnDesc.type->asDateType()->Parse(valueStr),
                op,
                useAvx);
    }

    return {};
}

std::unique_ptr<FilterNode>
FilterNode::Create(const ColumnDesc &columnDesc,
                   const std::string &valueStr,
                   CompareOp op,
                   bool useAvx)
{
    switch (columnDesc.layout)
    {
        case ColumnDataBase::DICT_COLUMN_DATA:
            return CreateDictFilterNode(columnDesc, valueStr, op, useAvx);
        case ColumnDataBase::RAW_COLUMN_DATA:
            return CreateRawFilterNode(columnDesc, valueStr, op, useAvx);
    }

    return {};
}

};
