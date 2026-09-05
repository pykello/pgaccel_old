#include <arrow/api.h>
#include <arrow/filesystem/api.h>


#include <parquet/types.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>

#include <execution>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <future>
#include <limits>

#include "columnar_table.h"
#include "util.h"

namespace pgaccel
{

template<class ParquetTy, class AccelTy>
std::vector<ColumnDataP>
GenerateRawColumnData(parquet::ColumnReader &untypedReader, int64_t divisor = 1, int64_t minimum = INT64_MIN, int64_t maximum = INT64_MAX)
{
    using ReaderType = parquet::TypedColumnReader<ParquetTy>&;
    ReaderType& typedReader = static_cast<ReaderType>(untypedReader);

    const int N = RowGroupSize;
    std::vector<ColumnDataP> result;

    std::vector<typename AccelTy::c_type> allValues;
    while (true) {
        int16_t rep_levels[N];
        int16_t def_levels[N];
        typename ParquetTy::c_type values[N];
        int64_t valuesRead = 0;
        auto levelsRead = typedReader.ReadBatch(N, def_levels, rep_levels, values, &valuesRead);
        if (levelsRead != valuesRead) throw std::invalid_argument("NULL or repeated values are not supported");
        if (valuesRead == 0)
            break;

        for (int i = 0; i < valuesRead; i++) {
            auto value = AccelTy::FromParquet(values[i]);
            if (value < minimum || value > maximum) throw std::invalid_argument("Value outside declared integer range");
            if (value % divisor) throw std::invalid_argument("Subsecond timestamps are not supported");
            allValues.push_back(value / divisor);
        }
    }

    for (int offset = 0; offset < allValues.size(); offset += RowGroupSize)
    {
        int rowGroupSize = std::min((int) allValues.size() - offset, RowGroupSize);
        auto columnData = std::make_unique<RawColumnData<AccelTy>>();
        columnData->type = ColumnDataBase::RAW_COLUMN_DATA;

        typename AccelTy::c_type maxValue = allValues[offset], minValue = allValues[offset];
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
            columnData->values = (uint8_t *) AllocateAligned(512, rowGroupSize);
            FILL_RAW_DATA(int8_t);
        } else if (maxValue <= INT16_MAX && minValue >= INT16_MIN) {
            columnData->bytesPerValue = 2;
            columnData->values = (uint8_t *) AllocateAligned(512, 2 * rowGroupSize);
            FILL_RAW_DATA(int16_t);
        } else if (maxValue <= INT32_MAX && minValue >= INT32_MIN) {
            columnData->bytesPerValue = 4;
            columnData->values = (uint8_t *) AllocateAligned(512, 4 * rowGroupSize);
            FILL_RAW_DATA(int32_t);
        } else {
            columnData->bytesPerValue = 8;
            columnData->values = (uint8_t *) AllocateAligned(512, 8 * rowGroupSize);
            FILL_RAW_DATA(int64_t);
        }

        columnData->size = rowGroupSize;
        columnData->minValue = minValue;
        columnData->maxValue = maxValue;

        result.push_back(std::move(columnData));
    }

    return std::move(result);
}

template<class ParquetTy, class AccelTy>
std::vector<ColumnDataP>
GenerateDictColumnData(parquet::ColumnReader &untypedReader)
{
    using ReaderType = parquet::TypedColumnReader<ParquetTy>&;
    using DictTy = typename AccelTy::c_type;
    ReaderType& typedReader = static_cast<ReaderType>(untypedReader);
    std::vector<ColumnDataP> result;

    const int N = RowGroupSize;

    std::vector<DictTy> convertedValues;
    while (true) {
        int16_t rep_levels[N];
        int16_t def_levels[N];
        typename ParquetTy::c_type values[N];
        int64_t valuesRead = 0;
        auto levelsRead = typedReader.ReadBatch(N, def_levels, rep_levels, values, &valuesRead);
        if (levelsRead != valuesRead) throw std::invalid_argument("NULL or repeated values are not supported");
        if (valuesRead == 0)
            break;

        for (int i = 0; i < valuesRead; i++) {
            convertedValues.push_back(AccelTy::FromParquet(values[i]));
        }
    }

    std::mutex push_mutex;

    for (int offset = 0; offset < convertedValues.size(); offset += RowGroupSize)
    {
        int rowGroupSize = std::min((int) convertedValues.size() - offset, RowGroupSize);
        auto columnData = std::make_unique<DictColumnData<AccelTy>>();
        columnData->type = ColumnDataBase::DICT_COLUMN_DATA;
        columnData->valueType = std::make_unique<AccelTy>();
        std::unordered_set<DictTy> distinctValues;

        // ordered map: 5.3s, 5.4s
        // unordered map: 4.2s
        // + unordered set + sort: 3.4s
        std::unordered_map<DictTy, int> dictIndexMap;

        for (int i = offset; i < offset + rowGroupSize; i++) {
            distinctValues.insert(convertedValues[i]);
        }

        std::vector<DictTy> distinctValuesSorted(
            distinctValues.begin(),
            distinctValues.end());
        std::sort(distinctValuesSorted.begin(), distinctValuesSorted.end());

        int dictSize = 0;
        for (const auto &distinctValue: distinctValuesSorted) {
            dictIndexMap[distinctValue] = dictSize++;
            columnData->dict.push_back(distinctValue);
        }

        if (dictSize < 256) {
            uint8_t *values8 = (uint8_t *) AllocateAligned(512, rowGroupSize);
            for (int i = offset; i < offset + rowGroupSize; i++) {
                values8[i - offset] = dictIndexMap[convertedValues[i]];
            }
            columnData->values = values8;
        } else {
            uint16_t *values16 = (uint16_t *) AllocateAligned(512, 2 * rowGroupSize);
            for (int i = offset; i < offset + rowGroupSize; i++) {
                values16[i - offset] = dictIndexMap[convertedValues[i]];
            }
            columnData->values = (uint8_t *) values16;
        }

        columnData->size = rowGroupSize;

        std::lock_guard lock(push_mutex);
        result.push_back(std::move(columnData));
    }

    return std::move(result);
}


std::vector<RowGroup>
LoadParquetRowGroup(parquet::RowGroupReader& rowGroupReader,
                    const ColumnarTable &columnarTable,
                    const std::map<std::string,std::string> &declaredTypes)
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
            {
                auto it = declaredTypes.find(ToLower(name));
                bool small = it != declaredTypes.end() && ToLower(it->second) == "smallint";
                columnDataVec =
                    GenerateRawColumnData<parquet::Int32Type, pgaccel::Int32Type>(
                        *columnReader, 1, small ? INT16_MIN : INT32_MIN, small ? INT16_MAX : INT32_MAX);
                break;
            }

            case DECIMAL_TYPE:
                columnDataVec =
                    GenerateRawColumnData<parquet::Int64Type, pgaccel::DecimalType>(
                        *columnReader);
                break;

            case TIMESTAMP_TYPE:
            {
                int64_t divisor = 1;
                auto logical = columnReader->descr()->logical_type();
                if (logical->is_timestamp()) {
                    auto timestamp = static_cast<const parquet::TimestampLogicalType*>(logical.get());
                    switch (timestamp->time_unit()) {
                    case parquet::LogicalType::TimeUnit::MILLIS: divisor = 1000; break;
                    case parquet::LogicalType::TimeUnit::MICROS: divisor = 1000000; break;
                    case parquet::LogicalType::TimeUnit::NANOS: divisor = 1000000000; break;
                    default: throw std::invalid_argument("Unknown timestamp unit");
                    }
                }
                columnDataVec = GenerateRawColumnData<parquet::Int64Type,pgaccel::Int64Type>(*columnReader,divisor);
                break;
            }
            case INT64_TYPE:
                columnDataVec =
                    GenerateRawColumnData<parquet::Int64Type, pgaccel::Int64Type>(
                        *columnReader);
                break;
        }

        while (result.size() < columnDataVec.size())
            result.push_back({});

        for (int i = 0; i < columnDataVec.size(); i++) {
            result[i].columns.push_back(std::move(columnDataVec[i]));
            result[i].size = result[i].columns.back()->size;
        }
    }

    return std::move(result);
}

std::unique_ptr<ColumnarTable> 
ColumnarTable::ImportParquet(const std::string &tableName,
                             const std::string &path,
                             std::optional<std::set<std::string>> maybeFields,
                             const std::map<std::string,std::string> &declaredTypes)
{
    try {
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
        if (column->max_repetition_level()) throw std::invalid_argument("Repeated columns are not supported");
        if (logicalType->is_decimal() && phyType != parquet::Type::INT64)
            throw std::invalid_argument("Only INT64 decimals are supported");
        std::string declared;
        if (!declaredTypes.empty()) {
            auto found = declaredTypes.find(ToLower(column->name()));
            if (found == declaredTypes.end()) throw std::invalid_argument("Missing declared type for " + column->name());
            declared = ToLower(found->second);
            bool compatible = (declared == "text" && phyType == parquet::Type::BYTE_ARRAY) ||
                ((declared == "smallint" || declared == "integer" || declared == "date") && phyType == parquet::Type::INT32) ||
                ((declared == "bigint" || declared == "timestamp") && phyType == parquet::Type::INT64);
            if (!compatible) throw std::invalid_argument("Incompatible declared type for " + column->name());
        }

        switch (phyType) {
            case parquet::Type::BYTE_ARRAY:
                columnDesc.type = std::make_shared<pgaccel::StringType>();
                columnDesc.layout = ColumnDataBase::DICT_COLUMN_DATA;
                break;

            case parquet::Type::INT32:
                if (logicalType->is_date() || declared == "date")
                {
                    columnDesc.type = std::make_shared<pgaccel::DateType>();
                    columnDesc.layout = ColumnDataBase::DICT_COLUMN_DATA;
                }
                else
                {
                    columnDesc.type = std::make_shared<pgaccel::Int32Type>();
                    columnDesc.layout = ColumnDataBase::RAW_COLUMN_DATA;
                }
                break;

            case parquet::Type::INT64:
                if (logicalType->is_timestamp() || declared == "timestamp")
                {
                    if (logicalType->is_timestamp() && !static_cast<const parquet::TimestampLogicalType*>(logicalType.get())->is_adjusted_to_utc())
                        throw std::invalid_argument("Timestamp must declare UTC adjustment");
                    columnDesc.type = std::make_shared<pgaccel::TimestampType>();
                    columnDesc.layout = ColumnDataBase::RAW_COLUMN_DATA;
                }
                else if (logicalType->is_decimal())
                {
                    auto parquetDecimalType =
                        static_cast<const parquet::DecimalLogicalType *>(logicalType.get());
                    auto accelDecimalType = std::make_shared<pgaccel::DecimalType>();
                    accelDecimalType->scale = parquetDecimalType->scale();
                    columnDesc.type = std::move(accelDecimalType);
                    columnDesc.layout = ColumnDataBase::RAW_COLUMN_DATA;
                }
                else
                {
                    columnDesc.type = std::make_shared<pgaccel::Int64Type>();
                    columnDesc.layout = ColumnDataBase::RAW_COLUMN_DATA;
                }
                break;

            default:
                std::cout << "Unsupported type: " << parquet::TypeToString(phyType) << std::endl;
                return {};
        }

        result->schema_.push_back(std::move(columnDesc));
    }

    if (result->schema_.empty()) throw std::invalid_argument("No matching columns");
    for (const auto &field : fieldsToLoad) if (!result->ColumnIndex(field))
        throw std::invalid_argument("Unknown requested column: " + field);
    for (const auto &entry : declaredTypes) {
        bool found = false;
        for (int c = 0; c < parquetSchema->num_columns(); ++c)
            found |= ToLower(parquetSchema->Column(c)->name()) == entry.first;
        if (!found) throw std::invalid_argument("Unknown declared column: " + entry.first);
    }
    int count = fileMetadata->num_row_groups();
    std::vector<std::vector<RowGroup>> loaded(count);
    std::vector<std::future<void>> workers;
    for (int worker = 0; worker < std::min(8,count); ++worker)
        workers.push_back(std::async(std::launch::async,[&,worker] {
            for (int group = worker; group < count; group += 8) {
                auto reader = fileReader->RowGroup(group);
                loaded[group] = LoadParquetRowGroup(*reader,*result,declaredTypes);
            }
        }));
    for (auto &worker : workers) worker.get();
    for (auto &groups : loaded) for (auto &group : groups) result->row_groups_.push_back(std::move(group));

    return result;
    } catch (const std::exception &e) {
        std::cerr << "Parquet import: " << e.what() << std::endl;
        return {};
    }
}

};
