#pragma once

#include "column_data.hpp"
#include "columnar_table.h"
#include "types.hpp"
#include "result_type.hpp"
#include <vector>
#include <string>


namespace pgaccel
{

struct ColumnRef {
    int tableIdx;
    int columnIdx;
};

struct FilterClause {
    enum {
        FILTER_EQ,
        FILTER_LE,
        FILTER_LTE,
        FILTER_GT,
        FILTER_GTE,
        FILTER_BETWEEN_00,
        FILTER_BETWEEN_01,
        FILTER_BETWEEN_10,
        FILTER_BETWEEN_11,
    } type;

    ColumnRef columnRef;
    std::string value[2];
};

struct AggregateClause {
    enum {
        AGGREGATE_COUNT,
        AGGREGATE_COUNT_DISTINCT,
        AGGREGATE_SUM,
        AGGREGATE_MIN,
        AGGREGATE_MAX,
    } type;

    // todo: expression
};

struct QueryDesc {
    std::vector<ColumnarTable *> tables;
    std::vector<FilterClause> filterClauses;
    std::vector<ColumnRef> groupBy;
    std::vector<AggregateClause> aggregateClauses;
};

typedef std::unique_ptr<QueryDesc> QueryDescP;
typedef std::map<std::string, std::unique_ptr<pgaccel::ColumnarTable>> TableRegistry;

Result<QueryDesc> ParseSelect(const std::string &queryStr);

};