#include "executor.h"
#include "executor_groupby.h"
#include <functional>

namespace pgaccel
{

static Result<QueryOutput> ExecuteAggNoGroupByNoFilter(
    const QueryDesc &query, bool useAvx, bool useParallelism);
static Result<QueryOutput> ExecuteAggNoGroupByWithFilter(
    const QueryDesc &query, bool useAvx, bool useParallelism);
static FilterNodeP CreateFilterNode(const QueryDesc &query, bool useAvx);
static Rows SingleFilterCount(const QueryDesc &query,
                              const FilterNodeP &filterNode,
                              bool useParallelism);
static Rows ExecuteGroupBy(const QueryDesc &query,
                           const AggregateNodeP &aggNode,
                           bool useParallelism);

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
        QueryOutput result;
        result.fieldNames.push_back("group");
        result.fieldNames.push_back("count");
        result.values = ExecuteGroupBy(
            query,
            std::make_unique<AggregateNode>(
                query.aggregateClauses,
                query.groupBy,
                useAvx),
            useParallelism);
        return result;
    }
}

static FilterNodeP
CreateFilterNode(const QueryDesc &query, bool useAvx)
{
    std::vector<FilterNodeP> filterNodes;

    std::vector<FilterClause> filterClauses = query.filterClauses;
    sort(filterClauses.begin(), filterClauses.end(),
         [](const FilterClause &a, const FilterClause &b) -> bool
         {
            if (a.columnRef != b.columnRef)
                return a.columnRef < b.columnRef;
            return a.op < b.op;
         });

    for (int i = 0; i < filterClauses.size(); i++)
    {
        if (i + 1 < filterClauses.size() &&
            filterClauses[i + 1].columnRef == filterClauses[i].columnRef &&
            (filterClauses[i].op == FilterClause::FILTER_GT ||
             filterClauses[i].op == FilterClause::FILTER_GTE) &&
            (filterClauses[i + 1].op == FilterClause::FILTER_LT ||
             filterClauses[i + 1].op == FilterClause::FILTER_LTE))
        {
            filterNodes.push_back(
                FilterNode::CreateSimpleCompare(
                    filterClauses[i].columnRef,
                    filterClauses[i].value,
                    filterClauses[i].op,
                    filterClauses[i + 1].value,
                    filterClauses[i + 1].op,
                    useAvx
                )
            );

            i++;
        }
        else
        {
            filterNodes.push_back(
                FilterNode::CreateSimpleCompare(
                    filterClauses[i].columnRef,
                    filterClauses[i].value,
                    filterClauses[i].op,
                    "", FilterClause::INVALID,
                    useAvx));
        }
    }

    if (filterNodes.size() == 1)
        return std::move(filterNodes[0]);

    return FilterNode::CreateAndNode(std::move(filterNodes));
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
            output.values = ExecuteAgg<int64_t>(
                [&](const RowGroup& r, uint8_t *bitmap) {
                    return SumAll(r.columns[colRef.columnIdx],
                                  colRef.type,
                                  useAvx);
                },
                [](int64_t& a, int64_t b) { a += b; },
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

static Result<QueryOutput>
ExecuteAggNoGroupByWithFilter(const QueryDesc &query,
                              bool useAvx,
                              bool useParallelism)
{
    // ensure single count(*) aggregate
    int aggregateCount = query.aggregateClauses.size();
    if (aggregateCount != 1)
        return Status::Invalid(aggregateCount, " aggregates not suppprted yet");

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

static Rows
ExecuteGroupBy(const QueryDesc &query,
               const AggregateNodeP &aggNode,
               bool useParallelism)
{
    return
    ExecuteAgg<LocalAggResult>(
        [&](const RowGroup& r, uint8_t *bitmap) {
                return aggNode->ProcessRowGroup(r);
            },
        [&](LocalAggResult& a, LocalAggResult&& b) {
            aggNode->Combine(a, std::move(b));
        },
        [&](const LocalAggResult &a) {
            return aggNode->Finalize(a);
        },
        *query.tables[0],
        useParallelism);
}

};
