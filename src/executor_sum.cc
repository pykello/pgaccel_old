#include "executor.h"
#include <immintrin.h>

namespace pgaccel
{

int32_t
SumAllAvx512_16(uint8_t *valuesRaw, int size)
{
    auto valuesR = reinterpret_cast<const __m256i*>(valuesRaw);

    int avxCnt = size / (256 / 16);
    int32_t sum = 0;

    __m512i result = _mm512_set1_epi32(0);

    for (int i = 0; i < avxCnt; i++) {
        __m512i v = _mm512_cvtepi16_epi32(valuesR[i]);
        result = _mm512_add_epi32(result, v);
    }

    auto avxVec = (int32_t *) &result;
    for (int i = 0; i < (256 / 16); i++)
        sum += avxVec[i];
    
    auto values16 = reinterpret_cast<const int16_t *>(valuesRaw);
    for (int i = (256 / 16) * avxCnt; i < size; i++) {
        sum += values16[i];
    }

    return sum;
}

template<class storageType>
int64_t
SumAllRaw(const RawColumnDataBase *columnData,
          const pgaccel::AccelType *type,
          bool useAvx)
{
    int64_t result = 0;
    auto values = reinterpret_cast<const storageType *>(columnData->values);
    for (int i = 0; i < columnData->size; i++) {
        result += values[i];
    }
    return result;
}

int64_t
SumAllRaw(const RawColumnDataBase *columnData,
          const pgaccel::AccelType *type,
          bool useAvx)
{
    switch (columnData->bytesPerValue)
    {
        case 1:
            return SumAllRaw<int8_t>(columnData, type, useAvx);
        case 2:
            if (useAvx)
                return SumAllAvx512_16(columnData->values, columnData->size);
            else
                return SumAllRaw<int16_t>(columnData, type, useAvx);
        case 4:
            return SumAllRaw<int32_t>(columnData, type, useAvx);
        case 8:
            return SumAllRaw<int64_t>(columnData, type, useAvx);
    }
    return 0;
}

int64_t
SumAll(const ColumnDataP& columnData,
       const pgaccel::AccelType *type,
       bool useAvx)
{
    switch (columnData->type)
    {
        case ColumnDataBase::RAW_COLUMN_DATA:
        {
            auto rawColumnData = static_cast<RawColumnDataBase *>(columnData.get());
            return SumAllRaw(rawColumnData, type, useAvx);
        }
        case ColumnDataBase::DICT_COLUMN_DATA:
            // std::cout << "Sum of dict columns not supported yet" << std::endl;
            break;
    }
    return 0;
}

int64_t
SumAll(const std::vector<ColumnDataP>& columnDataVec,
       const pgaccel::AccelType *type,
       bool useAvx)
{
    uint64_t result = 0;
    for (auto &columnData: columnDataVec) {
        result += SumAll(columnData, type, useAvx);
    }
    return result;
}

};
