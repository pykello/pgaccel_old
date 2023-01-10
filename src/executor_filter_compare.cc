#include "column_data.hpp"
#include "types.hpp"
#include "executor.h"
#include "avx_traits.hpp"
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

template<int REGW, int N, bool sign, 
         bool countMatches,
         BitmapAction bitmapAction,
         FilterClause::Op op>
int FilterMatchesRawAVX(
    const uint8_t *buf,
    int size,
    typename AvxTraits<REGW, N, sign>::atom_type value,
    uint8_t *bitmap)
{
    using Traits = AvxTraits<REGW, N, sign>;
    using RegType = Traits::register_type;
    using MaskType = Traits::mask_type;
    using AtomType = Traits::atom_type;
    using OpTraits = OperatorTraits<op, AtomType>;
    RegType comparator = Traits::set1(value);
    auto valuesR = reinterpret_cast<const RegType *>(buf);

    int avxCnt = size / (REGW / N);
    int matches = 0;
    MaskType *bitmapTyped = (MaskType *) bitmap;

    for (int i = 0; i < avxCnt; i++) {
        MaskType mask;
        if constexpr(bitmapAction == BITMAP_AND)
            mask = Traits::mask_compare(bitmapTyped[i], valuesR[i], comparator, OpTraits::AvxOp);
        else
            mask = Traits::compare(valuesR[i], comparator, OpTraits::AvxOp);
        if constexpr(countMatches)
            matches += __builtin_popcountll(mask);
        if constexpr(bitmapAction != BITMAP_NOOP)
            bitmapTyped[i] = mask;
    }
    int processed = (REGW / N) * avxCnt;

    matches +=
       FilterMatchesRaw<AtomType, countMatches, bitmapAction>(
        buf + (processed * (N / 8)), size - processed,
        value, op,
        bitmap == nullptr ? nullptr : bitmap + (processed / 8),
        false);

    return matches;
}

static int CountSetBits(int size, uint8_t *bitmap)
{
    int result = 0;
    for (int i = 0; i < size / 8; i++)
        result += __builtin_popcount(bitmap[i]);
    for (int i = 0; i < size % 8; i++)
        if ((1 << i) & bitmap[size / 8])
            result++;
    return result;
}

template<class storageType, bool returnCount, BitmapAction bitmapAction, FilterClause::Op op>
int FilterMatchesRaw(const uint8_t *valueBuffer, int size,
                     storageType value,
                     uint8_t *bitmap,
                     bool useAvx)
{
    if (useAvx)
    {
    #define FilterMatchesRawCase(N) \
        if constexpr(sizeof(storageType) == N) \
            return FilterMatchesRawAVX< \
                    512 /* reg width */, 8 * N /* bits per value */, \
                    std::is_signed<storageType>::value, \
                    returnCount, bitmapAction, op> \
                (valueBuffer, size, value, bitmap);

        FilterMatchesRawCase(1);
        FilterMatchesRawCase(2);
        FilterMatchesRawCase(4);
        FilterMatchesRawCase(8);
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

template<BitmapAction bitmapAction>
int FilterNone(int size, uint8_t *bitmap)
{
    if constexpr(bitmapAction != BITMAP_NOOP)
        memset(bitmap, 0, (size + 7) / 8);
    return 0;
}

template<BitmapAction bitmapAction>
int FilterAll(int size, uint8_t *bitmap)
{
    if constexpr(bitmapAction == BITMAP_NOOP)
        return size;

    if constexpr(bitmapAction == BITMAP_SET)
    {
        memset(bitmap, -1, (size + 7) / 8);
        return size;
    }

    if constexpr(bitmapAction == BITMAP_AND)
    {
        return CountSetBits(size, bitmap);
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
    int dictIdx = DictIndex(columnData, value, op);
    if (dictIdx == -1)
    {
        switch (op)
        {
            case FilterClause::FILTER_EQ:
            case FilterClause::FILTER_LT:
            case FilterClause::FILTER_LTE:
                return FilterNone<bitmapAction>(columnData.size, bitmap);

            case FilterClause::FILTER_GT:
            case FilterClause::FILTER_GTE:
                return FilterAll<bitmapAction>(columnData.size, bitmap);
        }
    }
    else if (dictIdx >= columnData.dict.size())
    {
        switch (op)
        {
            case FilterClause::FILTER_EQ:
            case FilterClause::FILTER_GT:
            case FilterClause::FILTER_GTE:
                return FilterNone<bitmapAction>(columnData.size, bitmap);

            case FilterClause::FILTER_LT:
            case FilterClause::FILTER_LTE:
                return FilterAll<bitmapAction>(columnData.size, bitmap);
        }
    }

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
    if (value < columnData.minValue)
    {
        switch (op)
        {
            case FilterClause::FILTER_EQ:
            case FilterClause::FILTER_LT:
            case FilterClause::FILTER_LTE:
                return FilterNone<bitmapAction>(columnData.size, bitmap);
        }
    }

    if (value > columnData.maxValue)
    {
        switch (op)
        {
            case FilterClause::FILTER_EQ:
            case FilterClause::FILTER_GT:
            case FilterClause::FILTER_GTE:
                return FilterNone<bitmapAction>(columnData.size, bitmap);
        }
    }

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
