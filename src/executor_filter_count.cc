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

template<class AccelTy>
int CountMatchesDict(const ColumnDataP &columnData,
                     const typename AccelTy::c_type &value,
                     bool useAvx)
{
    auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData.get());
    return CountMatchesDict<AccelTy>(*typedColumnData, value, useAvx);
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

template<class AccelTy, bool returnCount, bool fillBitmap>
int FilterMatchesRaw(const ColumnDataP &columnData,
                     const typename AccelTy::c_type &value,
                     uint8_t *bitmap,
                     bool useAvx)
{
    auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData.get());
    return FilterMatchesRaw<AccelTy, returnCount, fillBitmap>(
        *typedColumnData, value, bitmap, useAvx);
}

template<bool returnCount, bool fillBitmap>
int FilterMatches(const ColumnDataP &columnData,
                  const std::string &valueStr,
                  const pgaccel::AccelType *type,
                  uint8_t *bitmap,
                  bool useAvx)
{
    switch (type->type_num())
    {
        case pgaccel::STRING_TYPE:
        {
            return CountMatchesDict<pgaccel::StringType>(columnData, valueStr, useAvx);
        }
        case pgaccel::DATE_TYPE:
        {
            int32_t value = pgaccel::ParseDate(valueStr);
            return CountMatchesDict<pgaccel::DateType>(columnData, value, useAvx);
        }
        case pgaccel::INT32_TYPE:
        {
            int32_t value = std::stol(valueStr);
            return FilterMatchesRaw<pgaccel::Int32Type, returnCount, fillBitmap>(
                columnData, value, bitmap, useAvx);
        }
        case pgaccel::INT64_TYPE:
        {
            int64_t value = std::stoll(valueStr);
            return FilterMatchesRaw<pgaccel::Int64Type, returnCount, fillBitmap>(
                columnData, value, bitmap, useAvx);
        }
        case pgaccel::DECIMAL_TYPE:
        {
            auto decimalType = static_cast<const pgaccel::DecimalType *>(type);
            int64_t value = pgaccel::ParseDecimal(decimalType->scale, valueStr);
            return FilterMatchesRaw<pgaccel::DecimalType, returnCount, fillBitmap>(
                columnData, value, bitmap, useAvx);
        }
        default:
            std::cout << "???" << std::endl;
            return 0;
    }
}

int CountMatches(const std::vector<ColumnDataP>& columnDataVec, 
                 const std::string &valueStr,
                 const pgaccel::AccelType *type,
                 bool useAvx,
                 bool useParallelism)
{
    int count = 0;

    int numThreads = 8;
    if (useParallelism)
    {
        std::vector<std::future<int>> futureResults;
        for (int i = 0; i < numThreads; i++)
        {
            futureResults.push_back(
                std::async([&](int m) {
                    int cnt = 0;
                    for (int j = 0; j < columnDataVec.size(); j++)
                        if (j % numThreads == m)
                            cnt += FilterMatches<true, false>(
                                columnDataVec[j], valueStr, type, nullptr, useAvx);
                    return cnt;
                }, i));
        }
        for (auto &f: futureResults)
            count += f.get();
    }
    else
    {
        for (auto &columnData: columnDataVec) {
            count += FilterMatches<true, false>(columnData, valueStr, type, nullptr, useAvx);
        }
    }
    return count;
}

};
