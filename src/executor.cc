#include "executor.h"
#include <functional>

namespace pgaccel
{

Result<QueryOutput>
ExecuteQuery(const QueryDesc &query, bool useAvx, bool useParallelism)
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
        const auto &agg = query.aggregateClauses[0];
        switch (agg.type)
        {
            case AggregateClause::AGGREGATE_COUNT:
            {
                // SELECT count(*) FROM table
                auto columnarTable = query.tables[0];
                QueryOutput output;
                output.fieldNames.push_back("count");
                output.values = ExecuteAgg<int32_t>(
                    [](const RowGroup& r) { return r.columns[0]->size; },
                    [](int32_t a, int32_t b) { return a + b; },
                    [](int32_t a) {
                        return Rows({{ std::to_string(a) }});
                    },
                    *columnarTable,
                    useParallelism
                );

                return output;
            }
            case AggregateClause::AGGREGATE_SUM:
            {
                // SELECT sum(col) FROM table
                ColumnRef colRef = *agg.columnRef;
                auto columnarTable = query.tables[colRef.tableIdx];

                QueryOutput output;
                output.values = ExecuteAgg<int64_t>(
                    [&](const RowGroup& r) {
                        return SumAll(r.columns[colRef.columnIdx],
                                      colRef.type,
                                      useAvx);
                    },
                    [](int64_t a, int64_t b) { return a + b; },
                    [&](int64_t totalSum) {
                        return Rows({{ ToString(colRef.type, totalSum) }});
                    },
                    *columnarTable,
                    useParallelism
                );

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

                QueryOutput output;
                output.fieldNames.push_back("count");
                output.values = ExecuteAgg<int32_t>(
                    [&](const RowGroup& r) {
                        return CountMatches(r.columns[col.columnIdx],
                                            value,
                                            col.type,
                                            useAvx);
                    },
                    [](int32_t a, int32_t b) { return a + b; },
                    [](int32_t a) {
                        return Rows({{ std::to_string(a) }});
                    },
                    *columnarTable,
                    useParallelism
                );

                return output;
            }
            case AggregateClause::AGGREGATE_SUM:
            {
                // SELECT sum(col) FROM table WHERE field=xyz;
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

};
