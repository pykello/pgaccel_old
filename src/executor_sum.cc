#include "executor.h"
#include <immintrin.h>
#include <future>

namespace pgaccel
{

// good gains if noOverflowCnt >= 64.
int32_t
SumAllAvx512_16(uint8_t *valuesRaw, int size, int noOverflowCnt)
{
    auto valuesR = reinterpret_cast<const __m512i*>(valuesRaw);

    int avxCnt = size / (512 / 16);
    int32_t sum = 0;

    for (int i = 0; i < avxCnt; i += noOverflowCnt)
    {
        __m512i result = _mm512_set1_epi16(0);
        int x = std::min(avxCnt, i + noOverflowCnt);
        for (int j = i; j < x; j++) {
            result = _mm512_add_epi16(result, valuesR[j]);
        }

        auto avxVec = (int16_t *) &result;
        for (int i = 0; i < (512 / 16); i++)
            sum += avxVec[i];
    }

    auto values16 = reinterpret_cast<const int16_t *>(valuesRaw);
    for (int i = (512 / 16) * avxCnt; i < size; i++) {
        sum += values16[i];
    }

    return sum;
}

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
       bool useAvx,
       bool useParallelism)
{
    int64_t result = 0;
    int numThreads = 16;
    if (useParallelism)
    {
        std::vector<std::future<int64_t>> futureResults;
        for (int i = 0; i < numThreads; i++)
        {
            futureResults.push_back(
                std::async([&](int m) {
                    int64_t sum = 0;
                    for (int j = 0; j < columnDataVec.size(); j++)
                        if (j % numThreads == m)
                            sum += SumAll(columnDataVec[j], type, useAvx);
                    return sum;
                }, i));
        }
        for (auto &f: futureResults)
            result += f.get();
    }
    else
    {
        for (auto &columnData: columnDataVec) {
            result += SumAll(columnData, type, useAvx);
        }
    }

    return result;
}

};
