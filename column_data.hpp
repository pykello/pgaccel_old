#pragma once

#include <parquet/types.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <arrow/api.h>

#include "util.hpp"

#include <vector>
#include <map>
#include <set>

/*
 * Data Structures
*/
const int RowGroupSize = 1 << 16;

class ColumnDataBase {};

template<class PhyTy>
struct ColumnData: public ColumnDataBase {
    // using PhyTy = parquet::ByteArrayType;
    using DictTy = typename pgaccel_type_traits<PhyTy::type_num>::dict_type;
    std::vector<DictTy> dict;
    alignas(512) uint8_t values[RowGroupSize * 2];
    int size;
};

typedef std::unique_ptr<ColumnDataBase> ColumnDataP;

/*
 * Functions
*/

template<class PhyTy>
void
GenerateColumnData(parquet::ColumnReader &untypedReader,
                   std::vector<ColumnDataP> &result)
{
    // using PhyTy = parquet::ByteArrayType;
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
        auto columnData = std::make_unique<ColumnData<PhyTy>>();
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
GenerateColumnData(parquet::ParquetFileReader &fileReader, int column)
{
    // using PhyTy = parquet::ByteArrayType;
    std::vector<ColumnDataP> result;
    auto fileMetadata = fileReader.metadata();
    for (int rowGroup = 0; rowGroup < fileMetadata->num_row_groups(); rowGroup++) {
        std::cout << "Row group: " << rowGroup << std::endl;
        auto rowGroupReader = fileReader.RowGroup(rowGroup);
        auto columnReader = rowGroupReader->Column(column);
        GenerateColumnData<PhyTy>(*columnReader, result);
    }
    return result;
}
