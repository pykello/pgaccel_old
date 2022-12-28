#pragma once

#include "column_data.hpp"
#include <immintrin.h>

#define Define_CountMatchesAvx(REGW, N, TYPE) \
int CountMatchesRawAVX ## REGW ## _ ## N(const uint8_t *buf, int size, TYPE value) \
{ \
    using RegType = __m ## REGW ## i; \
    RegType comparator = _mm ## REGW ## _set1_epi ## N(value); \
    auto valuesR = reinterpret_cast<const RegType *>(buf); \
\
    int avxCnt = size / (REGW / N); \
    int matches = 0; \
\
    for (int i = 0; i < avxCnt; i++) { \
        __mmask64 mask = _mm ## REGW ## _cmp_epi ## N ## _mask(valuesR[i], comparator, _MM_CMPINT_EQ); \
        matches += __builtin_popcountll(mask); \
    } \
\
    auto values8 = reinterpret_cast<const TYPE *>(buf); \
    for (int i = (REGW / N) * avxCnt; i < size; i++) \
        if (values8[i] == value) \
            matches++; \
\
    return matches; \
}

Define_CountMatchesAvx(512, 8, uint8_t)
Define_CountMatchesAvx(512, 16, uint16_t)
Define_CountMatchesAvx(512, 32, uint32_t)
Define_CountMatchesAvx(512, 64, uint64_t)


template<class AccelTy>
int DictIndex(const DictColumnData<AccelTy> &columnData, 
              typename AccelTy::c_type value)
{
    int left = 0, right = columnData.dict.size() - 1;
    while (left <= right) {
        int mid = (left + right) / 2;
        if (columnData.dict[mid] == value)
            return mid;
        else if (value < columnData.dict[mid]) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return -1;
}

template<class AccelTy>
int CountMatchesDictAVX(const DictColumnData<AccelTy> &columnData, 
                        typename AccelTy::c_type value)
{
    int dictIdx = DictIndex(columnData, value);

    if (columnData.dict.size() < 256) {
        return CountMatchesRawAVX512_8(columnData.values, columnData.size, dictIdx);
    } else {
        return CountMatchesRawAVX512_16(columnData.values, columnData.size, dictIdx);
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

template<class AccelTy, class storageType>
int CountMatchesRaw(const RawColumnData<AccelTy> &columnData,
                    storageType value)
{
    int count = 0;
    auto values = reinterpret_cast<const storageType *>(columnData.values);
    for (int i = 0; i < columnData.size; i++) {
        if (values[i] == value)
            count++;
    }
    return count;
}


template<class AccelTy>
int CountMatchesRaw(const RawColumnData<AccelTy> &columnData,
                    typename AccelTy::c_type value,
                    bool useAvx)
{
    if(value < columnData.minValue || value > columnData.maxValue)
        return 0;

    switch (columnData.bytesPerValue) {
        case 1:
            if (useAvx)
                return CountMatchesRawAVX512_8(columnData.values,
                                               columnData.size,
                                               (uint8_t) value);
            else
                return CountMatchesRaw<AccelTy, int8_t>(columnData, value);
        case 2:
            if (useAvx)
                return CountMatchesRawAVX512_16(columnData.values,
                                                columnData.size,
                                                (uint16_t) value);
            else
                return CountMatchesRaw<AccelTy, int16_t>(columnData, value);
        case 4:
            if (useAvx)
                return CountMatchesRawAVX512_32(columnData.values,
                                                columnData.size,
                                                (uint32_t) value);
            else
                return CountMatchesRaw<AccelTy, int32_t>(columnData, value);
        case 8:
            if (useAvx)
                return CountMatchesRawAVX512_64(columnData.values,
                                                columnData.size,
                                                (uint64_t) value);
            else
                return CountMatchesRaw<AccelTy, int64_t>(columnData, value);
    }

    return 0;
}
