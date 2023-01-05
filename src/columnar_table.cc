#include "columnar_table.h"
#include "util.h"

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/util/bit_stream_utils.h>

#include <parquet/types.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <execution>
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

std::vector<RowGroup>
LoadParquetRowGroup(parquet::RowGroupReader& rowGroupReader,
                    const ColumnarTable &columnarTable)
{
    std::vector<RowGroup> result;

    int parquetColCount = rowGroupReader.metadata()->num_columns();
    for (size_t colIdx = 0; colIdx < parquetColCount; colIdx++)
    {
        auto columnReader = rowGroupReader.Column(colIdx);
        auto name = columnReader->descr()->name();
        auto maybeColumnIdx = columnarTable.ColumnIndex(name);
        if (!maybeColumnIdx.has_value())
            continue;

        std::vector<ColumnDataP> columnDataVec;

        const auto &accelType = columnarTable.Schema()[*maybeColumnIdx].type;

        switch (accelType->type_num()) {
            case STRING_TYPE:
                columnDataVec =
                    GenerateDictColumnData<parquet::ByteArrayType, pgaccel::StringType>(
                        *columnReader);
                break;

            case DATE_TYPE:
                columnDataVec =
                    GenerateDictColumnData<parquet::Int32Type, pgaccel::DateType>(
                        *columnReader);
                break;

            case INT32_TYPE:
                columnDataVec =
                    GenerateRawColumnData<parquet::Int32Type, pgaccel::Int32Type>(
                        *columnReader);
                break;

            case DECIMAL_TYPE:
                columnDataVec =
                    GenerateRawColumnData<parquet::Int64Type, pgaccel::DecimalType>(
                        *columnReader);
                break;

            case INT64_TYPE:
                columnDataVec =
                    GenerateRawColumnData<parquet::Int64Type, pgaccel::Int64Type>(
                        *columnReader);
                break;
        }

        while (result.size() < columnDataVec.size())
            result.push_back({});

        for (int i = 0; i < columnDataVec.size(); i++)
            result[i].columns.push_back(std::move(columnDataVec[i]));
    }

    return std::move(result);
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

    for (size_t col = 0; col < parquetSchema->num_columns(); col++)
    {
        auto column = parquetSchema->Column(col);
        if (0 == fieldsToLoad.count(ToLower(column->name())))
            continue;

        ColumnDesc columnDesc;
        columnDesc.name = column->name();

        auto phyType = column->physical_type();
        auto logicalType = column->logical_type();

        switch (phyType) {
            case parquet::Type::BYTE_ARRAY:
                columnDesc.type = std::make_unique<pgaccel::StringType>();
                break;

            case parquet::Type::INT32:
                if (logicalType->is_date())
                {
                    columnDesc.type = std::make_unique<pgaccel::DateType>();
                }
                else
                {
                    columnDesc.type = std::make_unique<pgaccel::Int32Type>();
                }
                break;

            case parquet::Type::INT64:
                if (logicalType->is_decimal())
                {
                    auto parquetDecimalType =
                        static_cast<const parquet::DecimalLogicalType *>(logicalType.get());
                    auto accelDecimalType = std::make_unique<pgaccel::DecimalType>();
                    accelDecimalType->scale = parquetDecimalType->scale();
                    columnDesc.type = std::move(accelDecimalType);
                }
                else
                {
                    columnDesc.type = std::make_unique<pgaccel::Int64Type>();
                }
                break;

            default:
                std::cout << "Unsupported type: " << parquet::TypeToString(phyType) << std::endl;
                return {};
        }

        result->schema_.push_back(std::move(columnDesc));
    }

    std::vector<int> rowGroupIdxs;
    for (size_t i = 0; i < fileMetadata->num_row_groups(); i++)
        rowGroupIdxs.push_back(i);

    std::mutex push_mutex;
    std::mutex log_mutex;
    std::for_each(
        std::execution::par_unseq,
        rowGroupIdxs.begin(),
        rowGroupIdxs.end(),
        [&](int parquetGroup)
        {
            {
                std::lock_guard lock(push_mutex);
                std::cout << "Loading group " << parquetGroup << std::endl;
            }
            auto rowGroupReader = fileReader->RowGroup(parquetGroup);
            auto rowGroups = LoadParquetRowGroup(*rowGroupReader, *result);

            std::lock_guard lock(push_mutex);
            for(auto &rowGroup: rowGroups)
                result->row_groups_.push_back(std::move(rowGroup));
        });

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

        for (const auto &rowGroup: row_groups_)
        {
            RAISE_IF_FAILS(rowGroup.columns[colIdx]->Save(dataStream));
        }
    }

    metadataStream << numCols << std::endl;
    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        AccelType *type = schema_[colIdx].type.get();
        metadataStream << column_positions[colIdx];
        metadataStream << " " << row_groups_.size();
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
ColumnarTable::Load(const std::string &tableName,
                    const std::string &path,
                    std::optional<std::set<std::string>> fields)
{
    std::ifstream dataStream(path);
    std::ifstream metadataStream(path + ".metadata");
    return Load(tableName, metadataStream, dataStream, fields);
}

Result<ColumnarTableP>
ColumnarTable::Load(const std::string &tableName,
                    std::istream& metadataStream,
                    std::istream& dataStream,
                    std::optional<std::set<std::string>> maybeFields)
{
    auto result = std::unique_ptr<ColumnarTable>(new ColumnarTable);
    result->name_ = tableName;

    bool loadAll = false;
    std::set<std::string> fieldsToLoad;
    if (maybeFields.has_value())
    {
        for(auto f: maybeFields.value())
            fieldsToLoad.insert(ToLower(f));
    }
    else
    {
        loadAll = true;
    }

    std::vector<uint64_t> column_positions;
    std::vector<int> column_groups;
    std::vector<ColumnDesc> column_descs;

    int numCols;
    metadataStream >> numCols;
    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        uint64_t position;
        int groupCount;
        std::string columnName;
        int typeNum;

        metadataStream >> position >> groupCount >> columnName >> typeNum;

        ColumnDesc columnDesc;
        columnDesc.name = ToLower(columnName);

        switch (typeNum)
        {
            case TypeNum::INT32_TYPE:
                columnDesc.type = std::make_unique<Int32Type>();
                break;
            case TypeNum::INT64_TYPE:
                columnDesc.type = std::make_unique<Int64Type>();
                break;
            case TypeNum::STRING_TYPE:
                columnDesc.type = std::make_unique<StringType>();
                break;
            case TypeNum::DATE_TYPE:
                columnDesc.type = std::make_unique<DateType>();
                break;
            case TypeNum::DECIMAL_TYPE:
            {
                auto decimalType = std::make_unique<DecimalType>();
                int scale;
                metadataStream >> scale;
                decimalType->scale = scale;
                columnDesc.type = std::move(decimalType);
                break;
            }
            default:
                return Status::Invalid("Unknown type number: ", typeNum);
        }

        column_positions.push_back(position);
        column_groups.push_back(groupCount);
        column_descs.push_back(std::move(columnDesc));
    }

    for (int colIdx = 0; colIdx < numCols; colIdx++)
    {
        const ColumnDesc &columnDesc = column_descs[colIdx];

        if (!loadAll && !fieldsToLoad.count(columnDesc.name))
        {
            continue;
        }

        uint64_t position = column_positions[colIdx];
        int groupCount = column_groups[colIdx];
        dataStream.seekg(position);

        while(result->row_groups_.size() < groupCount)
            result->row_groups_.push_back({});

        for (int group = 0; group < groupCount; group++)
        {
            auto &rowGroup = result->row_groups_[group];
            auto columnData = ColumnDataBase::Load(dataStream, columnDesc.type.get());
            RAISE_IF_FAILS(columnData);
            rowGroup.columns.push_back(std::move(columnData).ValueUnsafe());
        }

        result->schema_.push_back(std::move(column_descs[colIdx]));
    }
    
    return result;
}

};
