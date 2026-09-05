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

std::optional<Result<QueryOutput>> ExecuteScalarFast(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteGroupedCount(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteDistinct(const QueryDesc &, bool, bool);
Result<QueryOutput> ExecuteExternal(const QueryDesc &, bool);
std::optional<Result<QueryOutput>> ExecuteComposite(const QueryDesc &, bool, bool);
Result<QueryOutput> ExecuteModern(const QueryDesc &, bool, bool);
Result<QueryOutput>
ExecuteQuery(const QueryDesc &query, bool useAvx, bool useParallelism)
{
    try {
        // Preserve the vectorized/metadata COUNT fast path; other shapes use typed execution.
        if (query.having.empty() && !query.grouped && query.groupBy.empty() && query.aggregateClauses.size() == 1 &&
            query.aggregateClauses[0].type == AggregateClause::AGGREGATE_COUNT &&
            !query.aggregateClauses[0].expression && !query.limit && !query.offset) {
            if (query.filterClauses.empty()) return ExecuteAggNoGroupByNoFilter(query,useAvx,useParallelism);
            return ExecuteAggNoGroupByWithFilter(query,useAvx,useParallelism);
        }
        if (auto fast = ExecuteScalarFast(query,useAvx,useParallelism)) return std::move(*fast);
        if (auto composite = ExecuteComposite(query,useAvx,useParallelism)) return std::move(*composite);
        if (auto counts = ExecuteGroupedCount(query,useAvx,useParallelism)) return std::move(*counts);
        if (auto distinct = ExecuteDistinct(query,useAvx,useParallelism)) return std::move(*distinct);
        auto result = ExecuteModern(query,useAvx,useParallelism);
        if (!result.ok() && query.allowSpill && result.status().Message().find("Query memory budget exceeded") == 0)
            return ExecuteExternal(query,useAvx);
        return result;
    } catch (const std::exception &e) {
        if (query.allowSpill && std::string(e.what()).find("Query memory budget exceeded") == 0)
            return ExecuteExternal(query,useAvx);
        return Status::Invalid(e.what());
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
            uint64_t count=0;
            for(int group=0;group<columnarTable->RowGroupCount();++group)
                count+=columnarTable->GetRowGroup(group).size;
            output.values={{std::to_string(count)}};

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
    ExecuteAgg<int64_t>(
        [&](const RowGroup& r, uint8_t *bitmap) {
                return filterNode->ExecuteCount(r);
            },
            [](int64_t& a, int64_t b) { a += b; },
            [](int64_t a) {
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
