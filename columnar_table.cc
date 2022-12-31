#include "columnar_table.h"
#include "util.h"

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/util/bit_stream_utils.h>

#include <parquet/types.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cctype>
#include <string>

namespace pgaccel 
{

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

std::unique_ptr<ColumnarTable> 
ColumnarTable::ImportParquet(const std::string &tableName,
                             const std::string &path,
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

    auto result = std::unique_ptr<ColumnarTable>(new ColumnarTable);
    result->name_ = tableName;

    for (size_t colIdx = 0; colIdx < parquetSchema->num_columns(); colIdx++)
    {
        auto column = parquetSchema->Column(colIdx);

        if (0 == fieldsToLoad.count(ToLower(column->name())))
            continue;

        std::cout << "Loading column " << column->name() << " ..." << std::endl;

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

        result->schema_.push_back(std::move(columnDesc));
        result->column_data_vecs_.push_back(std::move(columnDataVec));
    }

    return result;
}

Result<bool>
ColumnarTable::Save(const std::string &path)
{
    std::ofstream dataStream(path);
    std::ofstream metadataStream(path + ".metadata");
    return Save(metadataStream, dataStream);
}


Result<bool>
ColumnarTable::Save(std::ostream& metadataStream,
                    std::ostream& dataStream) const
{
    std::vector<uint64_t> column_positions;

    int numCols = schema_.size();

    uint64_t current_pos = 0;
    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        column_positions.push_back(dataStream.tellp());

        const auto &columnDataVec = column_data_vecs_[colIdx];
        for (const auto &columnDataP: columnDataVec)
        {
            RAISE_IF_FAILS(columnDataP->Save(dataStream));
        }
    }

    metadataStream << numCols << std::endl;
    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        AccelType *type = schema_[colIdx].type.get();
        metadataStream << column_positions[colIdx];
        metadataStream << " " << schema_[colIdx].name;
        metadataStream << " " << type->type_num();
        switch (type->type_num())
        {
            case TypeNum::DECIMAL_TYPE:
                auto decimalType = static_cast<DecimalType *>(type);
                metadataStream << " " << decimalType->scale;
                break;
        }

        metadataStream << std::endl;
    }

    return true;
}

Result<ColumnarTableP>
ColumnarTable::Load(const std::string &path)
{
    std::ifstream dataStream(path);
    std::ifstream metadataStream(path + ".metadataStream");
    return Load(metadataStream, dataStream);
}

Result<ColumnarTableP>
ColumnarTable::Load(std::istream& metadataStream,
                    std::istream& dataStream)
{
    return Status::Invalid("load failed");
}

};
