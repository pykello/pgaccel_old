#include "columnar_table.h"

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/util/bit_stream_utils.h>

#include <parquet/types.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <iostream>
#include <algorithm>
#include <cctype>
#include <string>

namespace pgaccel 
{

std::string
ToLower(const std::string& s)
{
    std::string result;
    for (auto ch: s)
        result.push_back(tolower(ch));
    return result;
}

std::optional<int>
ColumnarTable::ColumnIndex(const std::string& name) const
{
    auto lcName = ToLower(name);
    for (int i = 0; i < schema_.size(); i++) {
        if (lcName == ToLower(schema_[i].name))
            return i;
    }

    return {};
}

std::optional<ColumnarTable> 
ColumnarTable::ImportParquet(const std::string &path,
                             std::optional<std::set<std::string>> maybeFields)
{
    arrow::fs::LocalFileSystem fs;
    auto openResult = fs.OpenInputFile(path);
    if (!openResult.ok()) {
        std::cout << "could not open file" << std::endl;
        return {};
    }

    auto input = *openResult;
    auto fileReader = parquet::ParquetFileReader::Open(input, parquet::default_reader_properties());
    auto fileMetadata = fileReader->metadata();
    auto parquetSchema = fileMetadata->schema();

    std::set<std::string> fieldsToLoad;
    if (maybeFields.has_value())
    {
        for(auto f: maybeFields.value())
            fieldsToLoad.insert(ToLower(f));
    }
    else
    {
        for(size_t colIdx = 0; colIdx < parquetSchema->num_columns(); colIdx++)
            fieldsToLoad.insert(ToLower(parquetSchema->Column(colIdx)->name()));
    }

    ColumnarTable result;

    for (size_t colIdx = 0; colIdx < parquetSchema->num_columns(); colIdx++)
    {
        auto column = parquetSchema->Column(colIdx);

        if (0 == fieldsToLoad.count(ToLower(column->name())))
            continue;

        std::vector<ColumnDataP> columnDataVec;
        ColumnDesc columnDesc;
        columnDesc.name = column->name();

        auto phyType = column->physical_type();
        auto logicalType = column->logical_type();

        switch (phyType) {
            case parquet::Type::BYTE_ARRAY:
                columnDataVec = 
                    GenerateDictColumnData<parquet::ByteArrayType, pgaccel::StringType>(
                        *fileReader, colIdx);
                columnDesc.type = std::make_unique<pgaccel::StringType>();
                break;
            case parquet::Type::INT32:
                if (logicalType->is_date())
                {
                    columnDataVec =
                        GenerateDictColumnData<parquet::Int32Type, pgaccel::DateType>(
                            *fileReader, colIdx);
                    columnDesc.type = std::make_unique<pgaccel::DateType>();
                }
                else
                {
                    columnDataVec =
                        GenerateRawColumnData<parquet::Int32Type, pgaccel::Int32Type>(
                            *fileReader, colIdx);
                    columnDesc.type = std::make_unique<pgaccel::Int32Type>();
                }
                break;
            case parquet::Type::INT64:
                if (logicalType->is_decimal())
                {
                    columnDataVec = 
                        GenerateRawColumnData<parquet::Int64Type, pgaccel::DecimalType>(
                            *fileReader, colIdx);
                    auto parquetDecimalType =
                        static_cast<const parquet::DecimalLogicalType *>(logicalType.get());
                    auto accelDecimalType = std::make_unique<pgaccel::DecimalType>();
                    accelDecimalType->scale = parquetDecimalType->scale();
                    columnDesc.type = std::move(accelDecimalType);
                }
                else
                {
                    columnDataVec = 
                        GenerateRawColumnData<parquet::Int64Type, pgaccel::Int64Type>(
                            *fileReader, colIdx);
                    columnDesc.type = std::make_unique<pgaccel::Int64Type>();
                }
                break;
            default:
                std::cout << "Unsupported type: " << parquet::TypeToString(phyType) << std::endl;
                return {};
        }

        result.schema_.push_back(std::move(columnDesc));
        result.column_data_.push_back(std::move(columnDataVec));
    }

    return result;
}

};
