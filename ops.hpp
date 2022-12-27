#pragma once

#include "column_data.hpp"
#include <immintrin.h>

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
        __m512i comparator = _mm512_set1_epi8(dictIdx);
        auto values512 = reinterpret_cast<const __m512i *>(columnData.values);

        int avxCnt = columnData.size / 64;
        int matches = 0;

        for (int i = 0; i < avxCnt; i++) {
            __mmask64 mask = _mm512_cmp_epi8_mask(values512[i], comparator, _MM_CMPINT_EQ);
            matches += __builtin_popcountll(mask);
        }

        for (int i = 64 * avxCnt; i < columnData.size; i++)
            if (columnData.values[i] == dictIdx)
                matches++;

        return matches;
    } else {
        __m512i comparator = _mm512_set1_epi16(dictIdx);
        auto values512 = reinterpret_cast<const __m512i *>(columnData.values);

        int avxCnt = columnData.size / 32;
        int matches = 0;

        for (int i = 0; i < avxCnt; i++) {
            __mmask32 mask = _mm512_cmp_epi16_mask(values512[i], comparator, _MM_CMPINT_EQ);
            matches += __builtin_popcount(mask);
        }

        auto values16 = reinterpret_cast<const uint16_t *>(columnData.values);
        for (int i = 32 * avxCnt; i < columnData.size; i++)
            if (values16[i] == dictIdx)
                matches++;

        return matches;
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
int CountMatchesRawAVX(const RawColumnData<PhyTy> &columnData,
                       typename PhyTy::c_type value)
{
    __m512i comparator = _mm512_set1_epi16((int16_t) value);
    auto values512 = reinterpret_cast<const __m512i *>(columnData.values);

    int avxCnt = columnData.size / 32;
    int matches = 0;

    for (int i = 0; i < avxCnt; i++) {
        __mmask32 mask = _mm512_cmp_epi16_mask(values512[i], comparator, _MM_CMPINT_EQ);
        matches += __builtin_popcount(mask);
    }

    auto values16 = reinterpret_cast<const uint16_t *>(columnData.values);
    for (int i = 32 * avxCnt; i < columnData.size; i++)
        if (values16[i] == value)
            matches++;

    return matches;
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
            return CountMatchesRaw<PhyTy, int8_t>(columnData, value);
        case 2:
            if (useAvx)
                return CountMatchesRawAVX<PhyTy>(columnData, value);
            else
                return CountMatchesRaw<PhyTy, int16_t>(columnData, value);
        case 4:
            return CountMatchesRaw<PhyTy, int32_t>(columnData, value);
        case 8:
            return CountMatchesRaw<PhyTy, int64_t>(columnData, value);
    }

    return 0;
}