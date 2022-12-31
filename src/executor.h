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

Result<QueryOutput> ExecuteQuery(const QueryDesc &query, bool useAvx);

int CountMatches(const std::vector<ColumnDataP>& columnDataVec, 
                 const std::string &valueStr,
                 const pgaccel::AccelType *type,
                 bool useAvx);

};
