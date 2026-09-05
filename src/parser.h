#pragma once

#include "column_data.hpp"
#include "columnar_table.h"
#include "types.hpp"
#include "result_type.hpp"
#include <vector>
#include <string>


namespace re2 { class RE2; }

namespace pgaccel
{

struct ColumnRef {
    ColumnDesc columnDesc;

    int tableIdx;
    int columnIdx;

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

    bool operator!=(const ColumnRef &b) const
    {
        return !operator==(b);
    }


    std::string Name() const
    {
        return columnDesc.name;
    }

    const std::shared_ptr<AccelType>& Type() const
    {
        return columnDesc.type;
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
        FILTER_LIKE, FILTER_NOT_LIKE, FILTER_IN, FILTER_NOT_IN,
        INVALID
    } op;

    ColumnRef columnRef;
    std::string value;
    std::vector<std::string> values;

    std::string ToString() const;
};

struct Expression {
    enum Kind { COLUMN, CONSTANT, ADD, SUBTRACT, MULTIPLY, DIVIDE, LENGTH, EXTRACT, DATE_TRUNC, REGEXP_REPLACE, CASE, EQ, NE, LT, LTE, GT, GTE, AND } kind = CONSTANT;
    std::optional<ColumnRef> column;
    std::string literal = "0";
    std::string replacement;
    std::shared_ptr<const re2::RE2> regexp;
    std::shared_ptr<const re2::RE2> capturePrefix;
    std::shared_ptr<Expression> left, right, condition;
    std::shared_ptr<AccelType> type;
};

struct AggregateClause {
    enum Type {
        AGGREGATE_COUNT,
        AGGREGATE_COUNT_DISTINCT,
        AGGREGATE_SUM,
        AGGREGATE_AVG,
        AGGREGATE_MIN,
        AGGREGATE_MAX,
        AGGREGATE_PROJECT
    } type;

    std::optional<ColumnRef> columnRef;
    std::shared_ptr<Expression> expression;
    std::string alias;
    std::string sql;

    std::string ToString() const;
};

struct QueryDesc {
    std::vector<ColumnarTable *> tables;
    std::vector<FilterClause> filterClauses;
    std::vector<ColumnRef> groupBy;
    std::vector<std::shared_ptr<Expression>> groupExpressions;
    std::vector<AggregateClause> aggregateClauses;
    struct Ordering { size_t output; bool descending; };
    std::vector<Ordering> orderBy;
    struct Having { size_t output; FilterClause::Op op; int64_t value; };
    std::vector<Having> having;
    std::optional<size_t> outputColumnCount;
    std::optional<uint64_t> limit;
    uint64_t offset = 0;
    bool grouped = false;
    bool allowSpill = true;
    uint64_t spillByteBudget = uint64_t(64) << 30;
    // Conservative accounting for hash groups, exact sets, and output buffers.
    uint64_t memoryBudgetBytes = uint64_t(8) << 30;

    std::string ToString() const;
};

typedef std::unique_ptr<QueryDesc> QueryDescP;
typedef std::map<std::string, std::unique_ptr<pgaccel::ColumnarTable>> TableRegistry;

Result<QueryDesc> ParseSelect(const std::string &query, const TableRegistry &registry);
};
