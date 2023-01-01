#include "executor.h"

namespace pgaccel
{

Result<QueryOutput>
ExecuteQuery(const QueryDesc &query, bool useAvx)
{
    if (query.groupBy.size())
        return Status::Invalid("group by not supported yet");

    // ensure single count(*) aggregate
    int aggregateCount = query.aggregateClauses.size();
    if (aggregateCount != 1)
        return Status::Invalid(aggregateCount, " aggregates not suppprted yet");

    int filterCount = query.filterClauses.size();
    if (filterCount == 0)
    {
        auto columnarTable = query.tables[0];

        switch (query.aggregateClauses[0].type)
        {
            case AggregateClause::AGGREGATE_COUNT:
            {
                const auto &columnDataVec = columnarTable->ColumnData(0);
                int totalCount = CountAll(columnDataVec);

                QueryOutput output;
                output.fieldNames.push_back("count");
                output.values.push_back({ std::to_string(totalCount) });
                return output;
            }
            case AggregateClause::AGGREGATE_SUM:
            {
                const auto &columnDataVec = columnarTable->ColumnData(0);
                auto totalSum = SumAll(columnDataVec);

                QueryOutput output;
                output.fieldNames.push_back("sum");
                output.values.push_back({ std::to_string(totalSum) });
                return output;
            }
            default:
                return Status::Invalid("Unsupported aggregate type");
        }
    }
    else if (filterCount == 1)
    {
        if (query.filterClauses[0].op != FilterClause::FILTER_EQ)
            return Status::Invalid("non-equality filters not supported yet");

        switch (query.aggregateClauses[0].type)
        {
            case AggregateClause::AGGREGATE_COUNT:
            {
                // execute SELECT count(*) FROM tbl WHERE field=xyz;
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
            default:
                return Status::Invalid("Unsupported aggregate type");
        }
    }
    else
    {
        return Status::Invalid(filterCount, " filters not supported yet");
    }
}

uint64_t CountAll(const std::vector<ColumnDataP>& columnDataVec)
{
    uint64_t result = 0;
    for(const auto &vec: columnDataVec)
    {
        result += vec->size;
    }
    return result;
}

};
