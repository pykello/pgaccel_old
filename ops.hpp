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
int CountMatchesAVX(const DictColumnData<PhyTy> &columnData, 
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
int CountMatches(const DictColumnData<PhyTy> &columnData, 
                 typename pgaccel_type_traits<PhyTy::type_num>::dict_type value,
                 bool useAvx)
{
    if (useAvx)
        return CountMatchesAVX(columnData, value);

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
