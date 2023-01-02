#pragma once

#include "column_data.hpp"
#include "types.hpp"
#include "result_type.hpp"
#include "parser.h"
#include <vector>
#include <string>

namespace pgaccel
{

typedef std::vector<std::string> Row;

struct QueryOutput {
    Row fieldNames;
    std::vector<Row> values;
};

Result<QueryOutput> ExecuteQuery(
    const QueryDesc &query,
    bool useAvx,
    bool useParallelism);

// count
int CountMatches(const std::vector<ColumnDataP>& columnDataVec, 
                 const std::string &valueStr,
                 const pgaccel::AccelType *type,
                 bool useAvx,
                 bool useParallel);
uint64_t CountAll(const std::vector<ColumnDataP>& columnDataVec);

// sum
int64_t SumAll(const std::vector<ColumnDataP>& columnDataVec,
               const pgaccel::AccelType *type,
               bool useAvx,
               bool useParallelism);

template<class AccelTy>
int DictIndex(const DictColumnData<AccelTy> &columnData, 
              typename AccelTy::c_type value)
{
    int left = 0, right = columnData.dict.size() - 1;
    while (left <= right) {
        int mid = (left + right) / 2;
        if (columnData.dict[mid] == value)
            return mid;
        else if (value < columnData.dict[mid]) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return -1;
}

};
