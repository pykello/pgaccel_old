#include "executor.h"
#include <functional>

namespace pgaccel
{

static FilterNodeP CreateFilterNode(const QueryDesc &query, bool useAvx);
static Rows SingleFilterCount(const QueryDesc &query,
                              const FilterNodeP &filterNode,
                              bool useParallelism);

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
                    [](const RowGroup& r, uint8_t *bitmap) {
                        return r.columns[0]->size;
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
                // SELECT sum(col) FROM table
                ColumnRef colRef = *agg.columnRef;
                auto columnarTable = query.tables[colRef.tableIdx];

                QueryOutput output;
                output.values = ExecuteAgg<int64_t>(
                    [&](const RowGroup& r, uint8_t *bitmap) {
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

    auto filterNode = CreateFilterNode(query, useAvx);

    switch (query.aggregateClauses[0].type)
    {
        case AggregateClause::AGGREGATE_COUNT:
        {
            QueryOutput output;
            output.fieldNames.push_back("count");
            output.values = SingleFilterCount(query, filterNode, useParallelism);

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

static FilterNodeP
CreateFilterNode(const QueryDesc &query, bool useAvx)
{
    std::vector<FilterNodeP> filterNodes;

    for (const auto &filterClause: query.filterClauses)
    {
        const std::string &value = filterClause.value[0];
        ColumnRef col = filterClause.columnRef;
        auto columnarTable = query.tables[col.tableIdx];

        filterNodes.push_back(
            FilterNode::CreateSimpleCompare(col, value, filterClause.op, useAvx));
    }

    if (filterNodes.size() == 1)
        return std::move(filterNodes[0]);

    return FilterNode::CreateAndNode(std::move(filterNodes));
}

static Rows
SingleFilterCount(const QueryDesc &query,
                  const FilterNodeP &filterNode,
                  bool useParallelism)
{
    return
    ExecuteAgg<int32_t>(
        [&](const RowGroup& r, uint8_t *bitmap) {
                return filterNode->ExecuteCount(r);
            },
            [](int32_t a, int32_t b) { return a + b; },
            [](int32_t a) {
                return Rows({{ std::to_string(a) }});
            },
            *query.tables[0],
            useParallelism
        );
}

};
