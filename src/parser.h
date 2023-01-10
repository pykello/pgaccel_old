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

    bool operator<(const ColumnRef &b) const
    {
        if (tableIdx != b.tableIdx)
            return tableIdx < b.tableIdx;
        return columnIdx < b.columnIdx;
    }

    bool operator==(const ColumnRef &b) const
    {
        return tableIdx == b.tableIdx && columnIdx == b.columnIdx;
    }

    std::string ToString() const;
};

struct FilterClause {
    enum Op {
        FILTER_EQ,
        FILTER_NE,
        FILTER_GT,
        FILTER_GTE,
        FILTER_LT,
        FILTER_LTE,
        INVALID
    } op;

    ColumnRef columnRef;
    std::string value;

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
