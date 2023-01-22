#include "executor.h"
#include "executor_groupby.h"
#include "nodes.h"
#include <functional>

namespace pgaccel
{

static Result<QueryOutput> ExecuteAggNoGroupByNoFilter(
    const QueryDesc &query, bool useAvx, bool useParallelism);
static Result<QueryOutput> ExecuteAggNoGroupByWithFilter(
    const QueryDesc &query, bool useAvx, bool useParallelism);
static Rows SingleFilterCount(const QueryDesc &query,
                              const FilterNodeP &filterNode,
                              bool useParallelism);
static Rows ExecuteGroupBy(const AggregateNode &aggNode,
                           bool useParallelism);
static Row FieldNames(const std::vector<ColumnDesc> &schema);

Result<QueryOutput>
ExecuteQuery(const QueryDesc &query, bool useAvx, bool useParallelism)
{
    if (query.groupBy.size() == 0)
    {
        int filterCount = query.filterClauses.size();
        if (filterCount == 0)
        {
            return ExecuteAggNoGroupByNoFilter(query, useAvx, useParallelism);
        }
        else
        {
            return ExecuteAggNoGroupByWithFilter(query, useAvx, useParallelism);
        }
    }
    else
    {
        ExecutionParams params { useAvx };
        PartitionedNodeP partitionedNode =
            std::make_unique<ScanNode>(
                query.tables[0],
                FieldNames(query.tables[0]->Schema()));
        if (query.filterClauses.size())
        {
            partitionedNode =
                std::make_unique<FilterNode>(
                    std::move(partitionedNode),
                    query.filterClauses,
                    params
                );
        }
        auto aggNode = std::make_unique<AggregateNode>(
            std::move(partitionedNode),
            query.aggregateClauses,
            query.groupBy, params);

        QueryOutput result;
        result.fieldNames = FieldNames(aggNode->Schema());
        result.values = ExecuteGroupBy(*aggNode, useParallelism);
        return result;
    }
}

static Result<QueryOutput>
ExecuteAggNoGroupByNoFilter(const QueryDesc &query,
                            bool useAvx,
                            bool useParallelism)
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
                [](int32_t &a, int32_t b) { a += b; },
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

            output.fieldNames.push_back("sum");
            output.values = ExecuteAgg<int64_t>(
                [&](const RowGroup& r, uint8_t *bitmap) {
                    return SumAll(r.columns[colRef.columnIdx],
                                  colRef.Type().get(),
                                  useAvx);
                },
                [](int64_t& a, int64_t b) { a += b; },
                [&](int64_t totalSum) {
                    return Rows({{ ToString(colRef.Type().get(), totalSum) }});
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

static Result<QueryOutput>
ExecuteAggNoGroupByWithFilter(const QueryDesc &query,
                              bool useAvx,
                              bool useParallelism)
{
    // ensure single count(*) aggregate
    int aggregateCount = query.aggregateClauses.size();
    if (aggregateCount != 1)
        return Status::Invalid(aggregateCount, " aggregates not suppprted yet");

    auto filterNode = CreateFilterNode(query.filterClauses, useAvx);
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
            [](int32_t& a, int32_t b) { a += b; },
            [](int32_t a) {
                return Rows({{ std::to_string(a) }});
            },
            *query.tables[0],
            useParallelism
        );
}

static Rows ExecuteGroupBy(const AggregateNode &aggNode,
                           bool useParallelism)
{
    int partitionCount = aggNode.LocalPartitionCount();

    if (!useParallelism)
    {
        std::vector<std::future<LocalAggResultP>> localResults;
        std::promise<LocalAggResultP> promise;
        localResults.push_back(promise.get_future());
        promise.set_value(aggNode.LocalTask([](int){ return true; }));

        return aggNode.GlobalTask(localResults);
    }
    else
    {
        int numThreads = 8;
        std::vector<std::future<LocalAggResultP>> localResults;
        for (int i = 0; i < numThreads; i++)
        {
            localResults.push_back(
                std::async([&](int m) {
                    return aggNode.LocalTask(
                        [&](int idx) {
                            return idx % numThreads == m;
                        });
                }, i));
        }

        return aggNode.GlobalTask(localResults);
    }
}

static Row FieldNames(const std::vector<ColumnDesc> &schema)
{
    Row fieldNames;
    for (const auto & columnDesc: schema)
        fieldNames.push_back(columnDesc.name);
    return fieldNames;
}

};
