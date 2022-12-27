#pragma once

#include <parquet/types.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <arrow/api.h>

#include "util.hpp"

#include <vector>
#include <map>
#include <execution>
#include <algorithm>
#include <set>
#include <cstdint>

/*
 * Data Structures
*/
const int RowGroupSize = 1 << 16;

struct ColumnDataBase {
    enum Type {
        DICT_COLUMN_DATA = 0,
        RAW_COLUMN_DATA = 1
    } type;
};

template<class PhyTy>
struct DictColumnData: public ColumnDataBase {
    // using PhyTy = parquet::ByteArrayType;
    using DictTy = typename pgaccel_type_traits<PhyTy::type_num>::dict_type;
    std::vector<DictTy> dict;
    alignas(512) uint8_t values[RowGroupSize * 2];
    int size;
};

template<class PhyTy>
struct RawColumnData: public ColumnDataBase {
    alignas(512) typename PhyTy::c_type values[RowGroupSize * 8];
    int size;
    int bytesPerValue;
    typename PhyTy::c_type minValue, maxValue;
};

typedef std::unique_ptr<ColumnDataBase> ColumnDataP;

/*
 * Functions
*/

template<class PhyTy>
void
GenerateRawColumnData(parquet::ColumnReader &untypedReader,
                      std::vector<ColumnDataP> &result)
{
    // using PhyTy = parquet::Int64Type;
    using ReaderType = parquet::TypedColumnReader<PhyTy>&;
    ReaderType& typedReader = static_cast<ReaderType>(untypedReader);

    const int N = RowGroupSize;

    std::vector<typename PhyTy::c_type> allValues;
    while (true) {
        int16_t rep_levels[N];
        int16_t def_levels[N];
        typename PhyTy::c_type values[N];
        int64_t valuesRead = 0;
        typedReader.ReadBatch(N, def_levels, rep_levels, values, &valuesRead);
        if (valuesRead == 0)
            break;

        for (int i = 0; i < valuesRead; i++) {
            allValues.push_back(pgaccel_type_traits<PhyTy::type_num>::convert(values[i]));
        }
    }

    for (int offset = 0; offset < allValues.size(); offset += RowGroupSize)
    {
        int rowGroupSize = std::min((int) allValues.size() - offset, RowGroupSize);
        auto columnData = std::make_unique<RawColumnData<PhyTy>>();
        columnData->type = ColumnDataBase::RAW_COLUMN_DATA;

        typename PhyTy::c_type maxValue = allValues[offset], minValue = allValues[offset];
        for (int i = 0; i < rowGroupSize; i++) {
            minValue = std::min(minValue, allValues[i + offset]);
            maxValue = std::max(maxValue, allValues[i + offset]);
        }
#define FILL_RAW_DATA(type) \
        { \
        auto valuesTyped = reinterpret_cast<type *>(columnData->values); \
        for (int i = 0; i < rowGroupSize; i++) \
            valuesTyped[i] = allValues[i + offset]; \
        }

        if (maxValue <= INT8_MAX && minValue >= INT8_MIN) {
            columnData->bytesPerValue = 1;
            FILL_RAW_DATA(int8_t);
        } else if (maxValue <= INT16_MAX && minValue >= INT16_MIN) {
            columnData->bytesPerValue = 2;
            FILL_RAW_DATA(int16_t);
        } else if (maxValue <= INT32_MAX && minValue >= INT32_MIN) {
            columnData->bytesPerValue = 4;
            FILL_RAW_DATA(int32_t);
        } else {
            columnData->bytesPerValue = 8;
            FILL_RAW_DATA(int64_t);
        }

        columnData->size = rowGroupSize;
        columnData->minValue = minValue;
        columnData->maxValue = maxValue;

        result.push_back(std::move(columnData));
    }
}

template<class PhyTy>
void
GenerateDictColumnData(parquet::ColumnReader &untypedReader,
                       std::vector<ColumnDataP> &result)
{
    using ReaderType = parquet::TypedColumnReader<PhyTy>&;
    using DictTy = typename pgaccel_type_traits<PhyTy::type_num>::dict_type;
    ReaderType& typedReader = static_cast<ReaderType>(untypedReader);

    const int N = RowGroupSize;

    std::vector<DictTy> convertedValues;
    while (true) {
        int16_t rep_levels[N];
        int16_t def_levels[N];
        typename PhyTy::c_type values[N];
        int64_t valuesRead = 0;
        typedReader.ReadBatch(N, def_levels, rep_levels, values, &valuesRead);
        if (valuesRead == 0)
            break;

        for (int i = 0; i < valuesRead; i++) {
            convertedValues.push_back(pgaccel_type_traits<PhyTy::type_num>::convert(values[i]));
        }
    }

    for (int offset = 0; offset < convertedValues.size(); offset += RowGroupSize)
    {
        int rowGroupSize = std::min((int) convertedValues.size() - offset, RowGroupSize);
        auto columnData = std::make_unique<DictColumnData<PhyTy>>();
        columnData->type = ColumnDataBase::DICT_COLUMN_DATA;
        std::set<DictTy> distinctValues;
        std::map<DictTy, int> dictIndexMap;

        for (int i = offset; i < offset + rowGroupSize; i++) {
            distinctValues.insert(convertedValues[i]);
        }
        int dictSize = 0;
        for (const auto &distinctValue: distinctValues) {
            dictIndexMap[distinctValue] = dictSize++;
            columnData->dict.push_back(distinctValue);
        }

        if (dictSize < 256) {
            uint8_t *values8 = columnData->values;
            for (int i = offset; i < offset + rowGroupSize; i++) {
                values8[i - offset] = dictIndexMap[convertedValues[i]];
            }
        } else {
            uint16_t *values16 = reinterpret_cast<uint16_t *>(columnData->values);
            for (int i = offset; i < offset + rowGroupSize; i++) {
                values16[i - offset] = dictIndexMap[convertedValues[i]];
            }
        }

        columnData->size = rowGroupSize;

        result.push_back(std::move(columnData));
    }
}

template<class PhyTy>
std::vector<ColumnDataP>
GenerateDictColumnData(parquet::ParquetFileReader &fileReader, int column)
{
    // using PhyTy = parquet::ByteArrayType;
    std::vector<ColumnDataP> result;
    auto fileMetadata = fileReader.metadata();
    for (int rowGroup = 0; rowGroup < fileMetadata->num_row_groups(); rowGroup++) {
        std::cout << "Row group: " << rowGroup << std::endl;
        auto rowGroupReader = fileReader.RowGroup(rowGroup);
        auto columnReader = rowGroupReader->Column(column);
        GenerateDictColumnData<PhyTy>(*columnReader, result);
    }
    return result;
}

template<class PhyTy>
std::vector<ColumnDataP>
GenerateRawColumnData(parquet::ParquetFileReader &fileReader, int column)
{
    // using PhyTy = parquet::ByteArrayType;
    std::vector<ColumnDataP> result;
    auto fileMetadata = fileReader.metadata();
    for (int rowGroup = 0; rowGroup < fileMetadata->num_row_groups(); rowGroup++) {
        std::cout << "Row group: " << rowGroup << std::endl;
        auto rowGroupReader = fileReader.RowGroup(rowGroup);
        auto columnReader = rowGroupReader->Column(column);
        GenerateRawColumnData<PhyTy>(*columnReader, result);
    }
    return result;
}
