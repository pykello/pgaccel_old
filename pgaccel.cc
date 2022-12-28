#include <iostream>
#include <string>
#include <chrono>

using namespace std::chrono;

#include "column_data.hpp"
#include "executor.h"
#include "types.hpp"
#include "columnar_table.h"

const std::string path = "/home/hadi/data/tpch/1/parquet/lineitem.parquet";

using namespace std;


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
