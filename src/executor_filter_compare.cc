#include "column_data.hpp"
#include "types.hpp"
#include "executor.h"
#include <immintrin.h>
#include <future>

namespace pgaccel
{

template<FilterClause::Op op, typename ty>
struct OperatorTraits {};

template<typename ty>
struct OperatorTraits<FilterClause::FILTER_EQ, ty> {
    static inline bool compare(const ty& a, const ty& b)
    {
        return a == b;
    }
    const static int AvxOp = _MM_CMPINT_EQ;
};

template<typename ty>
struct OperatorTraits<FilterClause::FILTER_LT, ty> {
    static inline bool compare(const ty& a, const ty& b)
    {
        return a < b;
    }
    const static int AvxOp = _MM_CMPINT_LT;
};

template<typename ty>
struct OperatorTraits<FilterClause::FILTER_LTE, ty> {
    static inline bool compare(const ty& a, const ty& b)
    {
        return a <= b;
    }
    const static int AvxOp = _MM_CMPINT_LE;
};

template<typename ty>
struct OperatorTraits<FilterClause::FILTER_GT, ty> {
    static inline bool compare(const ty& a, const ty& b)
    {
        return a > b;
    }
    const static int AvxOp = _MM_CMPINT_GT;
};

template<typename ty>
struct OperatorTraits<FilterClause::FILTER_GTE, ty> {
    static inline bool compare(const ty& a, const ty& b)
    {
        return a >= b;
    }
    const static int AvxOp = _MM_CMPINT_GE;
};

template<typename ty>
struct OperatorTraits<FilterClause::FILTER_NE, ty> {
    static inline bool compare(const ty& a, const ty& b)
    {
        return a != b;
    }
    const static int AvxOp = _MM_CMPINT_NE;
};

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

enum BitmapAction {
    BITMAP_NOOP,
    BITMAP_SET,
    BITMAP_AND,
};

template<class storageType, bool returnCount, BitmapAction bitmapAction>
int FilterMatchesRaw(const uint8_t *valueBuffer, int size,
                     storageType value, FilterClause::Op op,
                     uint8_t *bitmap,
                     bool useAvx);

#define Define_CountMatchesAvx(REGW, N, TYPE) \
template<bool countMatches, BitmapAction bitmapAction, FilterClause::Op op> \
int FilterMatchesRawAVX ## REGW ## _ ## N(const uint8_t *buf, int size, TYPE value, uint8_t *bitmap) \
{ \
    using RegType = __m ## REGW ## i; \
    using MaskType = AvxTraits<REGW, N>::mask_type; \
    using OpTraits = OperatorTraits<op, TYPE>; \
    RegType comparator = _mm ## REGW ## _set1_epi ## N(value); \
    auto valuesR = reinterpret_cast<const RegType *>(buf); \
\
    int avxCnt = size / (REGW / N); \
    int matches = 0; \
    MaskType *bitmapTyped = (MaskType *) bitmap; \
\
    for (int i = 0; i < avxCnt; i++) { \
        MaskType mask; \
        if constexpr(bitmapAction == BITMAP_AND) \
            mask = _mm ## REGW ## _mask_cmp_epi ## N ## _mask(bitmapTyped[i], valuesR[i], comparator, OpTraits::AvxOp); \
        else \
            mask = _mm ## REGW ## _cmp_epi ## N ## _mask(valuesR[i], comparator, OpTraits::AvxOp); \
        if constexpr(countMatches) \
            matches += __builtin_popcountll(mask); \
        if constexpr(bitmapAction != BITMAP_NOOP) \
            bitmapTyped[i] = mask; \
    } \
    int processed = (REGW / N) * avxCnt; \
\
    matches += \
       FilterMatchesRaw<TYPE, countMatches, bitmapAction>( \
        buf + (processed * (N / 8)), size - processed, \
        value, op, \
        bitmap == nullptr ? nullptr : bitmap + (processed / 8), \
        false); \
\
    return matches; \
}

Define_CountMatchesAvx(512, 8, uint8_t)
Define_CountMatchesAvx(512, 16, uint16_t)
Define_CountMatchesAvx(512, 32, uint32_t)
Define_CountMatchesAvx(512, 64, uint64_t)

template<class storageType, bool returnCount, BitmapAction bitmapAction, FilterClause::Op op>
int FilterMatchesRaw(const uint8_t *valueBuffer, int size,
                     storageType value,
                     uint8_t *bitmap,
                     bool useAvx)
{
    if (useAvx)
    {
        if constexpr(sizeof(storageType) == 1)
            return FilterMatchesRawAVX512_8<returnCount, bitmapAction, op>(
                valueBuffer, size, value, bitmap);

        if constexpr(sizeof(storageType) == 2)
            return FilterMatchesRawAVX512_16<returnCount, bitmapAction, op>(
                valueBuffer, size, value, bitmap);

        if constexpr(sizeof(storageType) == 4)
            return FilterMatchesRawAVX512_32<returnCount, bitmapAction, op>(
                valueBuffer, size, value, bitmap);

        if constexpr(sizeof(storageType) == 8)
            return FilterMatchesRawAVX512_64<returnCount, bitmapAction, op>(
                valueBuffer, size, value, bitmap);
    }

    using OpTraits = OperatorTraits<op, storageType>;

    int count = 0;
    auto values = reinterpret_cast<const storageType *>(valueBuffer);
    for (int i = 0; i < size; i++) {
        if constexpr (bitmapAction == BITMAP_NOOP)
        {
            if constexpr (returnCount)
                if (OpTraits::compare(values[i], value))
                    count++;
        }
        else if constexpr (bitmapAction == BITMAP_SET)
        {
            if (OpTraits::compare(values[i], value))
            {
                bitmap[i >> 3] |= (1 << (i & 7));
                if constexpr (returnCount)
                    count++;
            }
            else
            {
                bitmap[i >> 3] &= ~(1 << (i & 7));
            }
        }
        else if constexpr (bitmapAction == BITMAP_AND)
        {
            if (OpTraits::compare(values[i], value))
            {
                if constexpr (returnCount)
                    if (bitmap[i >> 3] & (1 << (i & 7)))
                        count++;
            }
            else
            {
                bitmap[i >> 3] &= ~(1 << (i & 7));
            }
        }
    }

    return count;
}

template<class storageType, bool returnCount, BitmapAction bitmapAction>
int FilterMatchesRaw(const uint8_t *valueBuffer, int size,
                     storageType value,
                     FilterClause::Op op,
                     uint8_t *bitmap,
                     bool useAvx)
{
    switch (op)
    {
    #define FILTER_MATCHES_RAW_DISPATCH_CASE(op) \
        case op: \
            return FilterMatchesRaw<storageType, returnCount, bitmapAction, op>( \
                valueBuffer, size, value, bitmap, useAvx);

        FILTER_MATCHES_RAW_DISPATCH_CASE(FilterClause::FILTER_EQ);
        FILTER_MATCHES_RAW_DISPATCH_CASE(FilterClause::FILTER_NE);
        FILTER_MATCHES_RAW_DISPATCH_CASE(FilterClause::FILTER_LT);
        FILTER_MATCHES_RAW_DISPATCH_CASE(FilterClause::FILTER_LTE);
        FILTER_MATCHES_RAW_DISPATCH_CASE(FilterClause::FILTER_GT);
        FILTER_MATCHES_RAW_DISPATCH_CASE(FilterClause::FILTER_GTE);
    }

    return 0;
}

template<class AccelTy, bool countMatches, BitmapAction bitmapAction>
int FilterMatchesDict(const DictColumnData<AccelTy> &columnData, 
                      typename AccelTy::c_type value,
                      FilterClause::Op op,
                      uint8_t *bitmap,
                      bool useAvx)
{
    int dictIdx = DictIndex(columnData, value);
    if (dictIdx == -1)
        return 0;

    switch (columnData.bytesPerValue())
    {
        case 1:
            return FilterMatchesRaw<uint8_t, countMatches, bitmapAction>(
                columnData.values, columnData.size, (uint8_t) dictIdx, op, bitmap, useAvx);

        case 2:
            return FilterMatchesRaw<uint16_t, countMatches, bitmapAction>(
                columnData.values, columnData.size, (uint16_t) dictIdx, op, bitmap, useAvx);
    }

    return 0;
}

template<class AccelTy, bool returnCount, BitmapAction bitmapAction>
int FilterMatchesRaw(const RawColumnData<AccelTy> &columnData,
                     const typename AccelTy::c_type &value,
                     FilterClause::Op op,
                     uint8_t *bitmap,
                     bool useAvx)
{
    if(value < columnData.minValue || value > columnData.maxValue)
        return 0;

    switch (columnData.bytesPerValue) {
        case 1:
            return FilterMatchesRaw<int8_t, returnCount, bitmapAction>(
                columnData.values, columnData.size, value, op, bitmap, useAvx);

        case 2:
            return FilterMatchesRaw<int16_t, returnCount, bitmapAction>(
                columnData.values, columnData.size, value, op, bitmap, useAvx);

        case 4:
            return FilterMatchesRaw<int32_t, returnCount, bitmapAction>(
                columnData.values, columnData.size, value, op, bitmap, useAvx);

        case 8:
            return FilterMatchesRaw<int64_t, returnCount, bitmapAction>(
                columnData.values, columnData.size, value, op, bitmap, useAvx);
    }

    return 0;
}

class CompareFilterNode: public FilterNode {
public:
    virtual int ExecuteCount(ColumnDataBase *columnData) const = 0;
    virtual int ExecuteSet(ColumnDataBase *columnData, uint8_t *bitmask) const = 0;
    virtual int ExecuteAnd(ColumnDataBase *columnData, uint8_t *bitmask)const  = 0;

    virtual int ExecuteCount(const RowGroup &rowGroup) const
    {
        return ExecuteCount(rowGroup.columns[columnIndex].get());
    }

    virtual int ExecuteSet(const RowGroup &rowGroup, uint8_t *bitmask) const
    {
        return ExecuteSet(rowGroup.columns[columnIndex].get(), bitmask);
    }

    virtual int ExecuteAnd(const RowGroup &rowGroup, uint8_t *bitmask) const
    {
        return ExecuteAnd(rowGroup.columns[columnIndex].get(), bitmask);
    }

    int columnIndex;
};

template<typename AccelTy>
class FilterRawDataNode: public CompareFilterNode {
public:
    FilterRawDataNode(const typename AccelTy::c_type &value,
                      FilterClause::Op op,
                      bool useAvx): 
        value(value),
        op(op),
        useAvx(useAvx) {}

    int ExecuteCount(ColumnDataBase *columnData) const
    {
        auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData);
        return FilterMatchesRaw<AccelTy, true, BITMAP_NOOP>(
            *typedColumnData, value, op, nullptr, useAvx);
    }

    int ExecuteSet(ColumnDataBase *columnData, uint8_t *bitmask) const
    {
        auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData);
        return FilterMatchesRaw<AccelTy, true, BITMAP_SET>(
            *typedColumnData, value, op, bitmask, useAvx);
    }

    int ExecuteAnd(ColumnDataBase *columnData, uint8_t *bitmask) const
    {
        auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData);
        return FilterMatchesRaw<AccelTy, true, BITMAP_AND>(
            *typedColumnData, value, op, bitmask, useAvx);
    }

private:
    typename AccelTy::c_type value;
    FilterClause::Op op;
    bool useAvx;
};

template<typename AccelTy>
class FilterDictDataNode: public CompareFilterNode {
public:
    FilterDictDataNode(const typename AccelTy::c_type &value,
                       FilterClause::Op op,
                       bool useAvx): 
        value(value),
        op(op),
        useAvx(useAvx) {}

    int ExecuteCount(ColumnDataBase *columnData) const
    {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return FilterMatchesDict<AccelTy, true, BITMAP_NOOP>(
            *typedColumnData, value, op, nullptr, useAvx);
    }

    int ExecuteSet(ColumnDataBase *columnData, uint8_t *bitmask) const
    {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return FilterMatchesDict<AccelTy, true, BITMAP_SET>(
            *typedColumnData, value, op, bitmask, useAvx);
    }

    int ExecuteAnd(ColumnDataBase *columnData, uint8_t *bitmask) const
    {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return FilterMatchesDict<AccelTy, true, BITMAP_AND>(
            *typedColumnData, value, op, bitmask, useAvx);
    }

private:
    typename AccelTy::c_type value;
    FilterClause::Op op;
    bool useAvx;
};

std::unique_ptr<CompareFilterNode>
CreateRawFilterNode(const ColumnDesc &columnDesc,
                    const std::string &valueStr,
                    FilterClause::Op op,
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

std::unique_ptr<CompareFilterNode>
CreateDictFilterNode(const ColumnDesc &columnDesc,
                     const std::string &valueStr,
                     FilterClause::Op op,
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
FilterNode::CreateSimpleCompare(const ColumnRef &colRef,
                                const std::string &valueStr,
                                FilterClause::Op op,
                                bool useAvx)
{
    std::unique_ptr<CompareFilterNode> result;

    const auto &columnDesc = colRef.table->Schema()[colRef.columnIdx];
    switch (columnDesc.layout)
    {
        case ColumnDataBase::DICT_COLUMN_DATA:
            result = CreateDictFilterNode(columnDesc, valueStr, op, useAvx);
            break;

        case  ColumnDataBase::RAW_COLUMN_DATA:
            result = CreateRawFilterNode(columnDesc, valueStr, op, useAvx);
            break;
    }

    result->columnIndex = colRef.columnIdx;

    return std::move(result);
}

};
