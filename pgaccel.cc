#include <iostream>
#include <string>
#include <chrono>
#include <ctime>

using namespace std::chrono;

#include "column_data.hpp"
#include "ops.hpp"
#include "types.hpp"
#include "columnar_table.h"

const std::string path = "/home/hadi/data/tpch/1/parquet/lineitem.parquet";

using namespace std;

static uint64_t pow(uint64_t p, int q) {
    uint64_t result = 1;
    for (int i = 0; i < q; i++)
        result *= p;
    return result;
}


static int64_t
ParseDecimal(int scale, const std::string &valueStr)
{
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

template<class AccelTy>
int CountMatchesDict(const ColumnDataP &columnData,
                     const typename AccelTy::c_type &value,
                     bool useAvx)
{
    auto typedColumnData = static_cast<DictColumnData<AccelTy> *>(columnData.get());
    return CountMatchesDict<AccelTy>(*typedColumnData, value, useAvx);
}

template<class AccelTy>
int CountMatchesRaw(const ColumnDataP &columnData,
                     const typename AccelTy::c_type &value,
                     bool useAvx)
{
    auto typedColumnData = static_cast<RawColumnData<AccelTy> *>(columnData.get());
    return CountMatchesRaw<AccelTy>(*typedColumnData, value, useAvx);
}

int CountMatches(const std::vector<ColumnDataP>& columnDataVec, 
                 const std::string &valueStr,
                 const pgaccel::AccelType *type,
                 bool useAvx)
{
    int count = 0;
    for (auto &columnData: columnDataVec) {
        switch (type->type_num())
        {
            case pgaccel::STRING_TYPE:
            {
                count += CountMatchesDict<pgaccel::StringType>(columnData, valueStr, useAvx);
                break;
            }
            case pgaccel::DATE_TYPE:
            {
                int32_t value = ParseDate(valueStr);
                count += CountMatchesDict<pgaccel::DateType>(columnData, value, useAvx);
                break;
            }
            case pgaccel::INT32_TYPE:
            {
                int32_t value = std::stol(valueStr);
                count += CountMatchesRaw<pgaccel::Int32Type>(columnData, value, useAvx);
                break;
            }
            case pgaccel::INT64_TYPE:
            {
                int64_t value = std::stoll(valueStr);
                count += CountMatchesRaw<pgaccel::Int64Type>(columnData, value, useAvx);
                break;
            }
            case pgaccel::DECIMAL_TYPE:
            {
                auto decimalType = static_cast<const pgaccel::DecimalType *>(type);
                int64_t value = ParseDecimal(decimalType->scale, valueStr);
                count += CountMatchesRaw<pgaccel::DecimalType>(columnData, value, useAvx);
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
    std::vector<std::pair<std::string, std::string>> queries = {
        {"L_SHIPMODE", "AIR"},          // 858104
        {"L_SHIPDATE", "1996-02-12"},   // 2441
        {"L_QUANTITY", "1"},            // 120401
        {"L_ORDERKEY", "1"},            // 6
    };

    std::set<std::string> fieldsToLoad;
    for (auto q: queries)
        fieldsToLoad.insert(q.first);
    
    auto columnarTable = pgaccel::ColumnarTable::ImportParquet(path, fieldsToLoad);
    if (!columnarTable.has_value()) {
        std::cout << "Failed to load parquet file" << std::endl;
        exit(-1);
    }

    for (auto q: queries) {
        std::string columnName = q.first;
        std::string value = q.second;

        auto maybeColumnIdx = columnarTable->ColumnIndex(columnName);
        if (!maybeColumnIdx.has_value()) {
            std::cout << "Column not found" << std::endl;
            exit(-1);
        }

        int columnIdx = *maybeColumnIdx;
        const auto &columnDataVec = columnarTable->ColumnData(columnIdx);
        const auto &columnDesc = columnarTable->Schema()[columnIdx];

        MeasurePerf([&]() {
            int total = CountMatches(columnDataVec, value, columnDesc.type.get(), false);
            cout << "matches (no avx): " << total << endl;
        });

        MeasurePerf([&]() {
            int total = CountMatches(columnDataVec, value, columnDesc.type.get(), true);
            cout << "matches (avx): " << total << endl;
        });
    }

    return 0;
}
