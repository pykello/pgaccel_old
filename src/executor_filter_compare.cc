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
                     storageType fusedVal, FilterClause::Op fusedOp,
                     uint8_t *bitmap,
                     bool useAvx);

template<int REGW, int N, bool sign, 
         bool countMatches,
         BitmapAction bitmapAction,
         FilterClause::Op op,
         FilterClause::Op fusedOp>
int FilterMatchesRawAVX(
    const uint8_t *buf,
    int size,
    typename AvxTraits<REGW, N, sign>::atom_type value,
    typename AvxTraits<REGW, N, sign>::atom_type fusedVal,
    uint8_t *bitmap)
{
    using Traits = AvxTraits<REGW, N, sign>;
    using RegType = typename Traits::register_type;
    using MaskType = typename Traits::mask_type;
    using AtomType = typename Traits::atom_type;
    using OpTraits = OperatorTraits<op, AtomType>;
    RegType comparator = Traits::set1(value);
    auto valuesR = reinterpret_cast<const RegType *>(buf);

    RegType comparator2;
    if constexpr (fusedOp != FilterClause::INVALID)
        comparator2 = Traits::set1(fusedVal);

    int avxCnt = size / (REGW / N);
    int matches = 0;
    MaskType *bitmapTyped = (MaskType *) bitmap;

    for (int i = 0; i < avxCnt; i++)
    {
        MaskType mask;
        if constexpr(bitmapAction == BITMAP_AND)
        {
            mask = Traits::mask_compare(bitmapTyped[i], valuesR[i], comparator, OpTraits::AvxOp);
            if constexpr (fusedOp != FilterClause::INVALID)
                mask = Traits::mask_compare(mask, valuesR[i], comparator2,
                                            OperatorTraits<fusedOp, AtomType>::AvxOp);
        }
        else
        {
            mask = Traits::compare(valuesR[i], comparator, OpTraits::AvxOp);
            if constexpr (fusedOp != FilterClause::INVALID)
                mask = Traits::mask_compare(mask, valuesR[i], comparator2,
                                            OperatorTraits<fusedOp, AtomType>::AvxOp);
        }

        if constexpr(countMatches)
            matches += __builtin_popcountll(mask);
        if constexpr(bitmapAction != BITMAP_NOOP)
            bitmapTyped[i] = mask;
    }

    int processed = (REGW / N) * avxCnt;
    matches +=
       FilterMatchesRaw<AtomType, countMatches, bitmapAction>(
        buf + (processed * (N / 8)), size - processed,
        value, op, fusedVal, fusedOp,
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

template<class storageType,
         bool returnCount,
         BitmapAction bitmapAction,
         FilterClause::Op op,
         FilterClause::Op fusedOp>
int FilterMatchesRaw(const uint8_t *valueBuffer, int size,
                     storageType value,
                     storageType fusedVal,
                     uint8_t *bitmap,
                     bool useAvx)
{
    static_assert(fusedOp == FilterClause::INVALID ||
                  fusedOp == FilterClause::FILTER_LT ||
                  fusedOp == FilterClause::FILTER_LTE);

    if (useAvx)
    {
    #define FilterMatchesRawCase(N) \
        if constexpr(sizeof(storageType) == N) \
            return FilterMatchesRawAVX< \
                    512 /* reg width */, 8 * N /* bits per value */, \
                    std::is_signed<storageType>::value, \
                    returnCount, bitmapAction, op, fusedOp> \
                (valueBuffer, size, value, fusedVal, bitmap);

        FilterMatchesRawCase(1);
        FilterMatchesRawCase(2);
        FilterMatchesRawCase(4);
        FilterMatchesRawCase(8);
    }

    using OpTraits = OperatorTraits<op, storageType>;

    int count = 0;
    auto values = reinterpret_cast<const storageType *>(valueBuffer);
    for (int i = 0; i < size; i++) {

        bool eval = OpTraits::compare(values[i], value);
        if constexpr(fusedOp != FilterClause::INVALID)
            eval = eval && OperatorTraits<fusedOp, storageType>::compare(values[i], fusedVal);

        if constexpr (bitmapAction == BITMAP_NOOP)
        {
            if constexpr (returnCount)
                if (eval)
                    count++;
        }
        else if constexpr (bitmapAction == BITMAP_SET)
        {
            if (eval)
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
            if (eval)
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

template<class storageType,
         bool returnCount,
         BitmapAction bitmapAction,
         FilterClause::Op op>
int FilterMatchesRaw(const uint8_t *valueBuffer, int size,
                     storageType value,
                     storageType fusedVal,
                     FilterClause::Op fusedOp,
                     uint8_t *bitmap,
                     bool useAvx)
{
    switch (fusedOp)
    {
        case FilterClause::FILTER_LT:
            return FilterMatchesRaw<storageType, returnCount, bitmapAction, op, FilterClause::FILTER_LT>( \
                valueBuffer, size, value, fusedVal, bitmap, useAvx);

        case FilterClause::FILTER_LTE:
            return FilterMatchesRaw<storageType, returnCount, bitmapAction, op, FilterClause::FILTER_LTE>( \
                valueBuffer, size, value, fusedVal, bitmap, useAvx);

        default:
            return FilterMatchesRaw<storageType, returnCount, bitmapAction, op, FilterClause::INVALID>( \
                valueBuffer, size, value, fusedVal, bitmap, useAvx);
    }
}

template<class storageType, bool returnCount, BitmapAction bitmapAction>
int FilterMatchesRaw(const uint8_t *valueBuffer, int size,
                     storageType value,
                     FilterClause::Op op,
                     storageType fusedVal,
                     FilterClause::Op fusedOp,
                     uint8_t *bitmap,
                     bool useAvx)
{
    switch (op)
    {
    #define FILTER_MATCHES_RAW_DISPATCH_CASE(op) \
        case op: \
            return FilterMatchesRaw<storageType, returnCount, bitmapAction, op>( \
                valueBuffer, size, value, fusedVal, fusedOp, bitmap, useAvx);

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

enum SkipAction {
    FILTER_NONE,
    FILTER_ALL,
    CANNOT_SKIP
};

template<typename T>
static SkipAction
ComputeSkipAction(T value, FilterClause::Op op,
                  T fusedVal, FilterClause::Op fusedOp,
                  T minValue, T maxValue)
{
    switch (op)
    {
        case FilterClause::FILTER_EQ:
            if (value < minValue || value > maxValue)
                return FILTER_NONE;
            break;
        
        case FilterClause::FILTER_LT:
        case FilterClause::FILTER_LTE:
            if (fusedOp == FilterClause::INVALID)
            {
                if (value < minValue)
                    return FILTER_NONE;

                if (value > maxValue)
                    return FILTER_ALL;
            }
            else if (value < minValue)
            {
                // filter range starts before vector range, should check end

                // case 1: filter range ends before vector range. no overlaps
                if (fusedVal < minValue)
                    return FILTER_NONE;

                // case 2: filter range ends after vector range. full overlap.
                if (fusedVal > maxValue)
                    return FILTER_ALL;
            }
            else if (value > maxValue)
            {
                // filter range starts after vector range. no overlaps.
                return FILTER_NONE;
            }

            break;

        case FilterClause::FILTER_GT:
        case FilterClause::FILTER_GTE:
            if (value < minValue)
            {
                return FILTER_ALL;
            }
            else if (value > maxValue)
            {
                return FILTER_NONE;
            }

            break;
    }

    return CANNOT_SKIP;
}

template<class AccelTy, bool countMatches, BitmapAction bitmapAction>
int FilterMatchesDict(const DictColumnData<AccelTy> &columnData, 
                      typename AccelTy::c_type value,
                      FilterClause::Op op,
                      typename AccelTy::c_type fusedVal,
                      FilterClause::Op fusedOp,
                      uint8_t *bitmap,
                      bool useAvx)
{
    /*
     * We assume fusedOp is not INVALID only for BETWEEN cases.
     */

    int dictIdx = DictIndex(columnData, value, op);
    int dictIdx2 = fusedOp == FilterClause::INVALID ? -1 : DictIndex(columnData, fusedVal, fusedOp);
    int dictSize = columnData.dict.size();

    switch (ComputeSkipAction(dictIdx, op, dictIdx2, fusedOp, 0, dictSize - 1))
    {
        case FILTER_NONE:
            return FilterNone<bitmapAction>(columnData.size, bitmap);

        case FILTER_ALL:
            return FilterAll<bitmapAction>(columnData.size, bitmap);
    }

    switch (columnData.bytesPerValue())
    {
        case 1:
            return FilterMatchesRaw<uint8_t, countMatches, bitmapAction>(
                columnData.values, columnData.size,
                (uint8_t) dictIdx, op,
                (uint8_t) dictIdx2, fusedOp,
                bitmap, useAvx);

        case 2:
            return FilterMatchesRaw<uint16_t, countMatches, bitmapAction>(
                columnData.values, columnData.size,
                (uint16_t) dictIdx, op,
                (uint16_t) dictIdx2, fusedOp,
                bitmap, useAvx);
    }

    return 0;
}

template<class AccelTy, bool returnCount, BitmapAction bitmapAction>
int FilterMatchesRaw(const RawColumnData<AccelTy> &columnData,
                     const typename AccelTy::c_type &value,
                     FilterClause::Op op,
                     const typename AccelTy::c_type &fusedVal,
                     FilterClause::Op fusedOp,
                     uint8_t *bitmap,
                     bool useAvx)
{
    switch (ComputeSkipAction(value, op, fusedVal, fusedOp, columnData.minValue, columnData.maxValue))
    {
        case FILTER_NONE:
            return FilterNone<bitmapAction>(columnData.size, bitmap);

        case FILTER_ALL:
            return FilterAll<bitmapAction>(columnData.size, bitmap);
    }

    switch (columnData.bytesPerValue) {
    #define FILTER_MATCHES_RAW_DISPATCH_BY_SIZE(SIZE, TYPE) \
        case SIZE: \
            return FilterMatchesRaw<TYPE, returnCount, bitmapAction>( \
                columnData.values, columnData.size, value, op, fusedVal, fusedOp, bitmap, useAvx);

        FILTER_MATCHES_RAW_DISPATCH_BY_SIZE(1, int8_t);
        FILTER_MATCHES_RAW_DISPATCH_BY_SIZE(2, int16_t);
        FILTER_MATCHES_RAW_DISPATCH_BY_SIZE(4, int32_t);
        FILTER_MATCHES_RAW_DISPATCH_BY_SIZE(8, int64_t);
    }

    return 0;
}

class CompareFilterNode: public FilterNodeImpl {
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
                      const typename AccelTy::c_type &fusedVal,
                      FilterClause::Op fusedOp,
                      bool useAvx): 
        value(value),
        op(op),
        fusedVal(fusedVal),
        fusedOp(fusedOp),
        useAvx(useAvx) {}

    int ExecuteCount(ColumnDataBase *columnData) const
    {
        auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData);
        return FilterMatchesRaw<AccelTy, true, BITMAP_NOOP>(
            *typedColumnData, value, op, fusedVal, fusedOp, nullptr, useAvx);
    }

    int ExecuteSet(ColumnDataBase *columnData, uint8_t *bitmask) const
    {
        auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData);
        return FilterMatchesRaw<AccelTy, true, BITMAP_SET>(
            *typedColumnData, value, op, fusedVal, fusedOp, bitmask, useAvx);
    }

    int ExecuteAnd(ColumnDataBase *columnData, uint8_t *bitmask) const
    {
        auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData);
        return FilterMatchesRaw<AccelTy, true, BITMAP_AND>(
            *typedColumnData, value, op, fusedVal, fusedOp, bitmask, useAvx);
    }

private:
    typename AccelTy::c_type value, fusedVal;
    FilterClause::Op op, fusedOp;
    bool useAvx;
};

template<typename AccelTy>
class FilterDictDataNode: public CompareFilterNode {
public:
    FilterDictDataNode(const typename AccelTy::c_type &value,
                       FilterClause::Op op,
                       const typename AccelTy::c_type &fusedVal,
                       FilterClause::Op fusedOp,
                       bool useAvx): 
        value(value),
        op(op),
        fusedVal(fusedVal),
        fusedOp(fusedOp),
        useAvx(useAvx) {}

    int ExecuteCount(ColumnDataBase *columnData) const
    {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return FilterMatchesDict<AccelTy, true, BITMAP_NOOP>(
            *typedColumnData, value, op, fusedVal, fusedOp, nullptr, useAvx);
    }

    int ExecuteSet(ColumnDataBase *columnData, uint8_t *bitmask) const
    {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return FilterMatchesDict<AccelTy, true, BITMAP_SET>(
            *typedColumnData, value, op, fusedVal, fusedOp, bitmask, useAvx);
    }

    int ExecuteAnd(ColumnDataBase *columnData, uint8_t *bitmask) const
    {
        auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData);
        return FilterMatchesDict<AccelTy, true, BITMAP_AND>(
            *typedColumnData, value, op, fusedVal, fusedOp, bitmask, useAvx);
    }

private:
    typename AccelTy::c_type value, fusedVal;
    FilterClause::Op op, fusedOp;
    bool useAvx;
};

std::unique_ptr<CompareFilterNode>
CreateRawFilterNode(const ColumnDesc &columnDesc,
                    const std::string &valueStr,
                    FilterClause::Op op,
                    const std::string &value2Str,
                    FilterClause::Op fusedOp,
                    bool useAvx)
{
    switch (columnDesc.type->type_num())
    {
        case INT32_TYPE:
            return std::make_unique<FilterRawDataNode<Int32Type>>(
                columnDesc.type->asInt32Type()->Parse(valueStr),
                op,
                fusedOp == FilterClause::INVALID ? 
                    0 : columnDesc.type->asInt32Type()->Parse(value2Str),
                fusedOp,
                useAvx);

        case INT64_TYPE:
            return std::make_unique<FilterRawDataNode<Int64Type>>(
                columnDesc.type->asInt64Type()->Parse(valueStr),
                op,
                fusedOp == FilterClause::INVALID ? 
                    0 : columnDesc.type->asInt64Type()->Parse(value2Str),
                fusedOp,
                useAvx);

        case DECIMAL_TYPE:
            return std::make_unique<FilterRawDataNode<DecimalType>>(
                columnDesc.type->asDecimalType()->Parse(valueStr),
                op,
                fusedOp == FilterClause::INVALID ? 
                    0 : columnDesc.type->asDecimalType()->Parse(value2Str),
                fusedOp,
                useAvx);
    }

    return {};
}

std::unique_ptr<CompareFilterNode>
CreateDictFilterNode(const ColumnDesc &columnDesc,
                     const std::string &valueStr,
                     FilterClause::Op op,
                     const std::string &value2Str,
                     FilterClause::Op fusedOp,
                     bool useAvx)
{
    switch (columnDesc.type->type_num())
    {
        case STRING_TYPE:
            return std::make_unique<FilterDictDataNode<StringType>>(
                columnDesc.type->asStringType()->Parse(valueStr),
                op,
                fusedOp == FilterClause::INVALID ? 
                    std::string() : columnDesc.type->asStringType()->Parse(value2Str),
                fusedOp,
                useAvx);

        case DATE_TYPE:
            return std::make_unique<FilterDictDataNode<DateType>>(
                columnDesc.type->asDateType()->Parse(valueStr),
                op,
                fusedOp == FilterClause::INVALID ? 
                    0 : columnDesc.type->asDateType()->Parse(value2Str),
                fusedOp,
                useAvx);
    }

    return {};
}

std::unique_ptr<FilterNodeImpl>
FilterNodeImpl::CreateSimpleCompare(const ColumnRef &colRef,
                                const std::string &valueStr,
                                FilterClause::Op op,
                                const std::string &fusedValueStr,
                                FilterClause::Op fusedOp,
                                bool useAvx)
{
    std::unique_ptr<CompareFilterNode> result;

    const auto &columnDesc = colRef.columnDesc;
    switch (columnDesc.layout)
    {
        case ColumnDataBase::DICT_COLUMN_DATA:
            result = CreateDictFilterNode(
                        columnDesc, valueStr, op, fusedValueStr, fusedOp, useAvx);
            break;

        case ColumnDataBase::RAW_COLUMN_DATA:
            result = CreateRawFilterNode(
                        columnDesc, valueStr, op, fusedValueStr, fusedOp, useAvx);
            break;
    }

    result->columnIndex = colRef.columnIdx;

    return std::move(result);
}

};
