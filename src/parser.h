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
    ColumnarTable *table;

    int tableIdx;
    int columnIdx;
    AccelType *type;

    std::string ToString() const;
};

struct FilterClause {
    enum Op {
        FILTER_EQ,
        FILTER_NE,
        FILTER_LT,
        FILTER_LTE,
        FILTER_GT,
        FILTER_GTE,
        FILTER_BETWEEN_00,
        FILTER_BETWEEN_01,
        FILTER_BETWEEN_10,
        FILTER_BETWEEN_11,
    } op;

    ColumnRef columnRef;
    std::string value[2];

    std::string ToString() const;
};

struct AggregateClause {
    enum Type {
        AGGREGATE_COUNT,
        AGGREGATE_COUNT_DISTINCT,
        AGGREGATE_SUM,
        AGGREGATE_AVG,
        AGGREGATE_MIN,
        AGGREGATE_MAX,
    } type;

    std::optional<ColumnRef> columnRef;
    // todo: expression

    std::string ToString() const;
};

struct QueryDesc {
    std::vector<ColumnarTable *> tables;
    std::vector<FilterClause> filterClauses;
    std::vector<ColumnRef> groupBy;
    std::vector<AggregateClause> aggregateClauses;

    std::string ToString() const;
};

typedef std::unique_ptr<QueryDesc> QueryDescP;
typedef std::map<std::string, std::unique_ptr<pgaccel::ColumnarTable>> TableRegistry;

Result<QueryDesc> ParseSelect(const std::string &query, const TableRegistry &registry);
};
