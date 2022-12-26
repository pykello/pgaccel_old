#include <iostream>
#include <string>
#include <chrono>
using namespace std::chrono;

#include "column_data.hpp"
#include "ops.hpp"

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

int exmain() {
    arrow::fs::LocalFileSystem fs;
    auto openResult = fs.OpenInputFile(path);
    if (!openResult.ok()) {
        std::cout << "open failed" << std::endl;
        return -1;
    }
    auto input = *openResult;

    auto fileReader = parquet::ParquetFileReader::Open(input, parquet::default_reader_properties());
    auto fileMetadata = fileReader->metadata();

    for (int rowGroup = 0; rowGroup < fileMetadata->num_row_groups(); rowGroup++) {
        auto rowGroupReader = fileReader->RowGroup(rowGroup);
        auto rowGroupMetadata = rowGroupReader->metadata();

        for (int column = 0; column < rowGroupMetadata->num_columns(); column++) {
            auto columnChunk = rowGroupMetadata->ColumnChunk(column);
            auto columnReader = rowGroupReader->Column(column);
        }
    }

    return 0;
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

    // FileDebugInfo(*fileMetadata);

    std::string columnName = "L_SHIPMODE";
    std::string value = "AIR";
    int columnIdx = schema->ColumnIndex(columnName);

    cout << "columnIdx=" << columnIdx << endl;

    auto columnDataVec = GenerateColumnData<parquet::ByteArrayType>(*fileReader, columnIdx);

    cout << "output groups: " << columnDataVec.size() << endl;

    auto start = high_resolution_clock::now();

    int total = 0;
    for (auto &columnData: columnDataVec) {
        using PhyTy = parquet::ByteArrayType;
        auto typedColumnData = static_cast<ColumnData<PhyTy> *>(columnData.get());
        total += CountMatches<PhyTy>(*typedColumnData, value);
    }

    auto stop1 = high_resolution_clock::now();

    int total2 = 0;
    for (auto &columnData: columnDataVec) {
        using PhyTy = parquet::ByteArrayType;
        auto typedColumnData = static_cast<ColumnData<PhyTy> *>(columnData.get());
        total2 += CountMatchesAVX<PhyTy>(*typedColumnData, value);
    }


    auto stop2 = high_resolution_clock::now();

    auto duration1 = duration_cast<microseconds>(stop1 - start);
    auto duration2 = duration_cast<microseconds>(stop2 - stop1);

    cout << "matches 1: " << total << " (" << duration1.count() / 1000 << "ms)" << endl;
    cout << "matches avx: " << total2 << " (" << duration2.count() / 1000 << "ms)" << endl;

    return 0;
}
