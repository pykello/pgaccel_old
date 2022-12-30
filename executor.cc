#include "executor.h"

namespace pgaccel
{

Result<QueryOutput>
ExecuteQuery(const QueryDesc &query, bool useAvx)
{
    if (query.groupBy.size())
        return Status::Invalid("group by not supported yet");

    // ensure single equality filter
    int filterCount = query.filterClauses.size();
    if (filterCount != 1)
        return Status::Invalid(filterCount, " filters not supported yet");
    if (query.filterClauses[0].op != FilterClause::FILTER_EQ)
        return Status::Invalid("non-equality filters not supported yet");

    // ensure single count(*) aggregate
    int aggregateCount = query.aggregateClauses.size();
    if (aggregateCount != 1)
        return Status::Invalid(aggregateCount, " aggregates not suppprted yet");
    if (query.aggregateClauses[0].type != AggregateClause::AGGREGATE_COUNT)
        return Status::Invalid("non-count(*) aggregates not supported yet");


    // TODO: execute SELECT count(*) FROM tbl WHERE field=xyz;
    const std::string &value = query.filterClauses[0].value[0];
    ColumnRef col = query.filterClauses[0].columnRef;
    auto columnarTable = query.tables[col.tableIdx];
    const auto &columnDataVec = columnarTable->ColumnData(col.columnIdx);

    int matchCount = CountMatches(columnDataVec, value, col.type, useAvx);

    QueryOutput output;
    output.fieldNames.push_back("count");
    output.values.push_back({ std::to_string(matchCount) });

    return output;
}

};
