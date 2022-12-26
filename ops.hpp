#pragma once

#include "column_data.hpp"
#include <immintrin.h>

template<class PhyTy>
int CountMatches(const ColumnData<PhyTy> &columnData, 
                 typename pgaccel_type_traits<PhyTy::type_num>::dict_type value)
{
    int dictIdx = 0;
    for (int i = 0; i < columnData.dict.size(); i++)
        if (columnData.dict[i] == value)
            dictIdx = i;
    int matches = 0;
    for (int i = 0; i < columnData.size; i++)
        if (columnData.values[i] == dictIdx)
            matches++;
    return matches;
}

template<class PhyTy>
int CountMatchesAVX(const ColumnData<PhyTy> &columnData, 
                    typename pgaccel_type_traits<PhyTy::type_num>::dict_type value)
{
    int dictIdx = 0;
    for (int i = 0; i < columnData.dict.size(); i++)
        if (columnData.dict[i] == value)
            dictIdx = i;

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
}
