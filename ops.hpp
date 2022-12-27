#pragma once

#include "column_data.hpp"
#include <immintrin.h>

#define Define_CountMatchesAvx(N, TYPE) \
int CountMatchesRawAVX_##N(const uint8_t *buf, int size, TYPE value) \
{ \
    __m512i comparator = _mm512_set1_epi ## N(value); \
    auto values512 = reinterpret_cast<const __m512i *>(buf); \
\
    int avxCnt = size / (512 / N); \
    int matches = 0; \
\
    for (int i = 0; i < avxCnt; i++) { \
        __mmask64 mask = _mm512_cmp_epi ## N ## _mask(values512[i], comparator, _MM_CMPINT_EQ); \
        matches += __builtin_popcountll(mask); \
    } \
\
    auto values8 = reinterpret_cast<const TYPE *>(buf); \
    for (int i = (512 / N) * avxCnt; i < size; i++) \
        if (values8[i] == value) \
            matches++; \
\
    return matches; \
}

Define_CountMatchesAvx(8, uint8_t)
Define_CountMatchesAvx(16, uint16_t)
Define_CountMatchesAvx(32, uint32_t)
Define_CountMatchesAvx(64, uint64_t)


template<class PhyTy>
int DictIndex(const DictColumnData<PhyTy> &columnData, 
              typename pgaccel_type_traits<PhyTy::type_num>::dict_type value)
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

template<class PhyTy>
int CountMatchesDictAVX(const DictColumnData<PhyTy> &columnData, 
                        typename pgaccel_type_traits<PhyTy::type_num>::dict_type value)
{
    int dictIdx = DictIndex(columnData, value);

    if (columnData.dict.size() < 256) {
        return CountMatchesRawAVX_8(columnData.values, columnData.size, dictIdx);
    } else {
        return CountMatchesRawAVX_16(columnData.values, columnData.size, dictIdx);
    }
}

template<class PhyTy>
int CountMatchesDict(const DictColumnData<PhyTy> &columnData, 
                     typename pgaccel_type_traits<PhyTy::type_num>::dict_type value,
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

template<class PhyTy, class storageTy>
int CountMatchesRaw(const RawColumnData<PhyTy> &columnData,
                    typename PhyTy::c_type value)
{
    int count = 0;
    auto values = reinterpret_cast<const storageTy *>(columnData.values);
    for (int i = 0; i < columnData.size; i++) {
        if (values[i] == value)
            count++;
    }
    return count;
}


template<class PhyTy>
int CountMatchesRaw(const RawColumnData<PhyTy> &columnData,
                    typename PhyTy::c_type value,
                    bool useAvx)
{
    if(value < columnData.minValue || value > columnData.maxValue)
        return 0;

    switch (columnData.bytesPerValue) {
        case 1:
            if (useAvx)
                return CountMatchesRawAVX_8(columnData.values,
                                            columnData.size,
                                            (uint8_t) value);
            else
                return CountMatchesRaw<PhyTy, int8_t>(columnData, value);
        case 2:
            if (useAvx)
                return CountMatchesRawAVX_16(columnData.values,
                                             columnData.size,
                                             (uint16_t) value);
            else
                return CountMatchesRaw<PhyTy, int16_t>(columnData, value);
        case 4:
            if (useAvx)
                return CountMatchesRawAVX_32(columnData.values,
                                             columnData.size,
                                             (uint32_t) value);
            else
                return CountMatchesRaw<PhyTy, int32_t>(columnData, value);
        case 8:
            if (useAvx)
                return CountMatchesRawAVX_64(columnData.values,
                                             columnData.size,
                                             (uint64_t) value);
            else
                return CountMatchesRaw<PhyTy, int64_t>(columnData, value);
    }

    return 0;
}
