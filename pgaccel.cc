#include <iostream>
#include <string>
#include <chrono>
#include <ctime>

using namespace std::chrono;

#include "column_data.hpp"
#include "ops.hpp"
#include "util.hpp"

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/util/bit_stream_utils.h>

#include <parquet/types.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>

const std::string path = "/home/hadi/data/tpch/16/parquet/lineitem.parquet";

using namespace std;
using arrow::Result;

class EqFilterBase {
public:
    int column;
    parquet::Type::type type;

    std::string filterValueStr;
};

typedef std::unique_ptr<EqFilterBase> EqFilterUP;

template<typename T>
class EqFilter: public EqFilterBase {
public:
    typename T::c_type filterValue;
};

static uint64_t pow(uint64_t p, int q) {
    uint64_t result = 1;
    for (int i = 0; i < q; i++)
        result *= p;
    return result;
}


template<class PhyTy>
static typename PhyTy::c_type
ParseDecimal(const parquet::DecimalLogicalType &decimalType,
             const std::string &valueStr)
{
    int scale = decimalType.scale();
    uint32_t whole, decimal;
    size_t pos = valueStr.find(".");
    if (pos == std::string::npos) {
        whole = std::stoul(valueStr);
        decimal = 0;
    } else {
        std::string wholeStr = valueStr.substr(0, pos);
        std::string decimalStr = valueStr.substr(pos + 1);
        // if decimal part has more digits than scale, trim
        if (decimalStr.length() > scale) {
            decimalStr = decimalStr.substr(0, scale);
        }

        // pad decimal part until its length is equal to scale
        while (decimalStr.length() < scale) {
            decimalStr += "0";
        }

        whole = std::stoul(wholeStr);
        decimal = std::stoul(decimalStr);
    }

    return whole * pow(10, scale) + decimal;
}

static Result<EqFilterUP>
parseFilterDecimal(const parquet::DecimalLogicalType &decimalType,
                   parquet::Type::type phyTy,
                   int colIndex, const std::string &filterValue)
{
    int scale = decimalType.scale();
    uint32_t whole, decimal;
    size_t pos = filterValue.find(".");
    if (pos == std::string::npos) {
        whole = std::stoul(filterValue);
        decimal = 0;
    } else {
        std::string wholeStr = filterValue.substr(0, pos);
        std::string decimalStr = filterValue.substr(pos + 1);
        // if decimal part has more digits than scale, trim
        if (decimalStr.length() > scale) {
            decimalStr = decimalStr.substr(0, scale);
        }

        // pad decimal part until its length is equal to scale
        while (decimalStr.length() < scale) {
            decimalStr += "0";
        }

        whole = std::stoul(wholeStr);
        decimal = std::stoul(decimalStr);
    }

    switch (phyTy) {

#define DECIMAL_TO_INT_CASE(T) \
        case T::type_num: { \
            auto eqFilter = std::make_unique<EqFilter<T>>(); \
            eqFilter->column = colIndex; \
            eqFilter->type = T::type_num; \
            eqFilter->filterValue = whole * pow(10, scale) + decimal; \
            return Result<EqFilterUP>(std::move(eqFilter)); \
        }

        DECIMAL_TO_INT_CASE(parquet::Int64Type);
        DECIMAL_TO_INT_CASE(parquet::Int32Type);

        // TODO:
        // case parquet::FixedLenByteArray
        // case parquet::ByteArrayType

        default: {
            std::cout << "Unsupported Physical Type for Decimals: "
                      << parquet::TypeToString(phyTy) << std::endl;
            exit(-1);
        }
    }
}

static Result<EqFilterUP>
parseFilter(const std::string& filter, const parquet::SchemaDescriptor &schema)
{
    size_t pos = filter.find("=");
    std::string colName = filter.substr(0, pos);
    std::string filterValue = filter.substr(pos + 1);

    int colIndex = -1;
    for (int i = 0; i < schema.num_columns(); i++) {
        if (schema.Column(i)->name() == colName) {
            colIndex = i;
            break;
        }
    }

    if (colIndex == -1) {
        return arrow::Status::Invalid("Invalid filter");
    }

    auto phyTy = schema.Column(colIndex)->physical_type();
    auto logicalType = schema.Column(colIndex)->logical_type();

    if (logicalType->is_decimal()) {
        auto decimalType = static_cast<const parquet::DecimalLogicalType *>(logicalType.get());
        return parseFilterDecimal(*decimalType, phyTy, colIndex, filterValue);
    }

    switch (phyTy) {
        case parquet::Type::BYTE_ARRAY: {
            auto eqFilter = std::make_unique<EqFilter<parquet::ByteArrayType>>();
            eqFilter->column = colIndex;
            eqFilter->type = parquet::Type::BYTE_ARRAY;
            eqFilter->filterValueStr = filterValue;
            eqFilter->filterValue =
                parquet::ByteArray(filterValue.length(),
                                   (const uint8_t *) eqFilter->filterValueStr.c_str());
            return eqFilter;
        }
#define NUMERIC_FILTER_CASE(phyTy, fromStr) \
        case phyTy::type_num: { \
            auto eqFilter = std::make_unique<EqFilter<phyTy>>(); \
            eqFilter->column = colIndex; \
            eqFilter->type = phyTy::type_num; \
            eqFilter->filterValueStr = filterValue; \
            eqFilter->filterValue = fromStr(filterValue); \
            return Result<EqFilterUP>(std::move(eqFilter)); \
        }

        NUMERIC_FILTER_CASE(parquet::Int32Type, std::stol);
        NUMERIC_FILTER_CASE(parquet::Int64Type, std::stoll);
        NUMERIC_FILTER_CASE(parquet::BooleanType, std::stol);

        default: {
            std::cout << "Unsupported Filter column type: " <<
                parquet::TypeToString(phyTy) << std::endl;
            exit(01);
        }
    }
}

int32_t
ParseDate(const std::string &s)
{
    auto year = s.substr(0, 4);
    auto month = s.substr(5, 2);
    auto day = s.substr(8);
    std::tm tm{};
    tm.tm_year = std::stol(year) - 1900;
    tm.tm_mon = std::stol(month) - 1;
    tm.tm_mday = std::stol(day);
    tm.tm_hour = 10;
    tm.tm_min = 15;
    std::time_t t = std::mktime(&tm); 
    return t/(60*60*24);
}


template<typename PhyTy>
int CountMatches(parquet::ColumnReader &untypedReader,
                 const typename PhyTy::c_type &filterValue)
{
    using ReaderType = parquet::TypedColumnReader<PhyTy>&;
    ReaderType& typedReader = static_cast<ReaderType>(untypedReader);

    const int N = 1024;
    int16_t rep_levels[N];
    int16_t def_levels[N];
    typename PhyTy::c_type values[N];

    int64_t valuesRead = 0;
    int matches = 0;

    while (true) {
        typedReader.ReadBatch(N, def_levels, rep_levels, values, &valuesRead);
        if (valuesRead == 0)
            break;
        for (int i = 0; i < valuesRead; i++) {
            if (values[i] == filterValue)
            {
                matches++;
            }
        }
    }

    return matches;
}

void FileDebugInfo(parquet::FileMetaData &fileMetadata)
{
    auto schema = fileMetadata.schema();
    cout << "Row groups: " << fileMetadata.num_row_groups() << endl;
    cout << "Columns: " << fileMetadata.num_columns() << endl;
    for (int col = 0; col < schema->num_columns(); col++) {
        auto column = schema->Column(col);
        cout << "  " << column->name() << "," << parquet::TypeToString(column->physical_type()) << endl;
    }
}

template<class PhyTy>
int CountMatchesDict(const ColumnDataP &columnData,
                     const typename pgaccel_type_traits<PhyTy::type_num>::dict_type &value,
                     bool useAvx)
{
    auto typedColumnData = static_cast<DictColumnData<PhyTy> *>(columnData.get());
    return CountMatchesDict<PhyTy>(*typedColumnData, value, useAvx);
}

template<class PhyTy>
int CountMatchesRaw(const ColumnDataP &columnData,
                     const typename PhyTy::c_type &value,
                     bool useAvx)
{
    auto typedColumnData = static_cast<RawColumnData<PhyTy> *>(columnData.get());
    return CountMatchesRaw<PhyTy>(*typedColumnData, value, useAvx);
}

int CountMatches(const std::vector<ColumnDataP>& columnDataVec, 
                 const std::string &valueStr,
                 const parquet::ColumnDescriptor &desc,
                 bool useAvx)
{
    int count = 0;
    for (auto &columnData: columnDataVec) {
        switch (desc.physical_type())
        {
            case parquet::ByteArrayType::type_num:
            {
                count += CountMatchesDict<parquet::ByteArrayType>(columnData, valueStr, useAvx);
                break;
            }
            case parquet::Int32Type::type_num:
            {
                int32_t value;
                if (desc.logical_type()->is_date()) {
                    value = ParseDate(valueStr);
                    count += CountMatchesDict<parquet::Int32Type>(columnData, value, useAvx);
                } else {
                    value = std::stol(valueStr);
                    count += CountMatchesRaw<parquet::Int32Type>(columnData, value, useAvx);
                }
                break;
            }
            case parquet::Int64Type::type_num:
            {
                int64_t value;
                if (desc.logical_type()->is_decimal()) {
                    auto decimalType = static_cast<const parquet::DecimalLogicalType *>(desc.logical_type().get());
                    value = ParseDecimal<parquet::Int64Type>(*decimalType, valueStr);
                } else {
                    value = std::stoll(valueStr);
                }
                count += CountMatchesRaw<parquet::Int64Type>(columnData, value, useAvx);
                break;
            }
            default:
                std::cout << "???" << std::endl;
        }
    }
    return count;
}

void MeasurePerf(const std::function<void()> &body)
{
    auto start = high_resolution_clock::now();

    body();

    auto stop = high_resolution_clock::now();
    auto duration  = duration_cast<microseconds>(stop - start);

    std::cout << "Duration: " << duration.count() / 1000 << "ms" << std::endl;
}

int main() {
    arrow::fs::LocalFileSystem fs;
    auto openResult = fs.OpenInputFile(path);
    if (!openResult.ok()) {
        std::cout << "open failed" << std::endl;
        return -1;
    }
    auto input = *openResult;
    auto fileReader = parquet::ParquetFileReader::Open(input, parquet::default_reader_properties());
    auto fileMetadata = fileReader->metadata();
    auto schema = fileMetadata->schema();

    FileDebugInfo(*fileMetadata);

    // std::string columnName = "L_SHIPMODE";
    // std::string value = "AIR";
    // std::string columnName = "L_SHIPDATE";
    // std::string value = "1996-02-12";
    // std::string columnName = "L_QUANTITY";
    // std::string value = "1";
    std::string columnName = "L_ORDERKEY";
    std::string value = "1";
    int columnIdx = schema->ColumnIndex(columnName);

    cout << "columnIdx=" << columnIdx << endl;

    std::vector<ColumnDataP> columnDataVec;
    
    auto phyType = schema->Column(columnIdx)->physical_type();
    switch (phyType) {
        case parquet::Type::BYTE_ARRAY:
            columnDataVec = GenerateDictColumnData<parquet::ByteArrayType>(*fileReader, columnIdx);
            break;
        case parquet::Type::INT32:
            if (schema->Column(columnIdx)->logical_type()->is_date())
                columnDataVec = GenerateDictColumnData<parquet::Int32Type>(*fileReader, columnIdx);
            else
                columnDataVec = GenerateRawColumnData<parquet::Int32Type>(*fileReader, columnIdx);
            break;
        case parquet::Type::INT64:
            columnDataVec = GenerateRawColumnData<parquet::Int64Type>(*fileReader, columnIdx);
            break;
        default:
            cout << "Unsupported type: " << parquet::TypeToString(phyType) << endl;
    }

    cout << "output groups: " << columnDataVec.size() << endl;

    MeasurePerf([&]() {
        int total = CountMatches(columnDataVec, value, *schema->Column(columnIdx), false);
        cout << "matches (no avx): " << total << endl;
    });

    MeasurePerf([&]() {
        int total = CountMatches(columnDataVec, value, *schema->Column(columnIdx), true);
        cout << "matches (avx): " << total << endl;
    });

    return 0;
}
