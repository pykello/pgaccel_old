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

std::optional<Result<QueryOutput>> ExecuteSortedSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteFilteredSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteScalarFast(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteProjection(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteDictionaryWeighted(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteDenseLength(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteRadixNumeric(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteRadixGroupCount(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecutePostingDistinct(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecutePostingGroupedCount(const QueryDesc &, bool);
std::optional<Result<QueryOutput>> ExecuteGroupedCount(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteDistinct(const QueryDesc &, bool, bool);
Result<QueryOutput> ExecuteExternal(const QueryDesc &, bool);
std::optional<Result<QueryOutput>> ExecuteDenseArithmetic(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteComposite(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteSortedJoinSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteCorrelatedMin(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteCorrelatedSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteCorrelatedAvg(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteJoinDistinct(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteJoinCount(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteExistsCount(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteStarSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteConvergentSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteSeparateSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteIndependentRatio(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecutePairedProfit(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteChainSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteSupplierWait(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecutePrefixAntiJoin(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteDisjunctiveSum(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteJoinSum(const QueryDesc &, bool, bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &, bool, bool);
Result<QueryOutput> ExecuteMaterialized(const QueryDesc &, bool, bool);
std::optional<Result<QueryOutput>> ExecuteGroupedExtreme(const QueryDesc &,bool,bool);
std::optional<Result<QueryOutput>> ExecuteLeftCountHistogram(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteModern(const QueryDesc &, bool, bool);
Result<QueryOutput>
ExecuteQuery(const QueryDesc &query, bool useAvx, bool useParallelism)
{
    if (!query.materializedSources.empty()) {
        if(auto extreme=ExecuteGroupedExtreme(query,useAvx,useParallelism))return std::move(*extreme);
        if(auto histogram=ExecuteLeftCountHistogram(query,useAvx,useParallelism))return std::move(*histogram);
        return ExecuteMaterialized(query, useAvx, useParallelism);
    }
    if (query.tables.size() > 1 || !query.predicates.empty() || !query.membershipQueries.empty() || !query.scalarQueries.empty() ||
        std::any_of(query.aggregateClauses.begin(), query.aggregateClauses.end(), [](const auto &a) {
            return a.type == AggregateClause::AGGREGATE_FORMULA;
        })) {
        if(auto sorted=ExecuteSortedJoinSum(query,useAvx,useParallelism)) return std::move(*sorted);
        if(auto minimum=ExecuteCorrelatedMin(query,useAvx,useParallelism)) return std::move(*minimum);
        if(auto summed=ExecuteCorrelatedSum(query,useAvx,useParallelism)) return std::move(*summed);
        if(auto correlated=ExecuteCorrelatedAvg(query,useAvx,useParallelism)) return std::move(*correlated);
        if(auto exists=ExecuteExistsCount(query,useAvx,useParallelism)) return std::move(*exists);
        if(auto distinct=ExecuteJoinDistinct(query,useAvx,useParallelism)) return std::move(*distinct);
        if(auto counts=ExecuteJoinCount(query,useAvx,useParallelism)) return std::move(*counts);
        if(auto star=ExecuteStarSum(query,useAvx,useParallelism)) return std::move(*star);
        if(auto convergent=ExecuteConvergentSum(query,useAvx,useParallelism)) return std::move(*convergent);
        if(auto separate=ExecuteSeparateSum(query,useAvx,useParallelism)) return std::move(*separate);
        if(auto ratio=ExecuteIndependentRatio(query,useAvx,useParallelism)) return std::move(*ratio);
        if(auto profit=ExecutePairedProfit(query,useAvx,useParallelism)) return std::move(*profit);
        if(auto chain=ExecuteChainSum(query,useAvx,useParallelism)) return std::move(*chain);
        if(auto waiting=ExecuteSupplierWait(query,useAvx,useParallelism)) return std::move(*waiting);
        if(auto prefix=ExecutePrefixAntiJoin(query,useAvx,useParallelism)) return std::move(*prefix);
        if(auto joined=ExecuteJoinSum(query,useAvx,useParallelism)) return std::move(*joined);
        if(auto disjunction=ExecuteDisjunctiveSum(query,useAvx,useParallelism)) return std::move(*disjunction);
        return ExecuteRelational(query, useAvx, useParallelism);
    }
    try {
        // Preserve the vectorized/metadata COUNT fast path; other shapes use typed execution.
        if (query.having.empty() && !query.grouped && query.groupBy.empty() && query.aggregateClauses.size() == 1 &&
            query.aggregateClauses[0].type == AggregateClause::AGGREGATE_COUNT &&
            !query.aggregateClauses[0].expression && !query.limit && !query.offset) {
            if (query.filterClauses.empty()) return ExecuteAggNoGroupByNoFilter(query,useAvx,useParallelism);
            return ExecuteAggNoGroupByWithFilter(query,useAvx,useParallelism);
        }
        if (auto fast = ExecuteScalarFast(query,useAvx,useParallelism)) return std::move(*fast);
        if (auto filtered = ExecuteFilteredSum(query,useAvx,useParallelism)) return std::move(*filtered);
        if (auto projection = ExecuteProjection(query,useAvx,useParallelism)) return std::move(*projection);
        if (auto sorted = ExecuteSortedSum(query,useAvx,useParallelism)) return std::move(*sorted);
        if (auto distinct = ExecutePostingDistinct(query,useAvx,useParallelism)) return std::move(*distinct);
        if (auto counts = ExecutePostingGroupedCount(query,useParallelism)) return std::move(*counts);
        if (auto counts = ExecuteRadixGroupCount(query,useAvx,useParallelism)) return std::move(*counts);
        if (auto weighted = ExecuteDictionaryWeighted(query,useAvx,useParallelism)) return std::move(*weighted);
        if (auto lengths = ExecuteDenseLength(query,useAvx,useParallelism)) return std::move(*lengths);
        if (auto numeric = ExecuteRadixNumeric(query,useAvx,useParallelism)) return std::move(*numeric);
        if (auto dense = ExecuteDenseArithmetic(query,useAvx,useParallelism)) return std::move(*dense);
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
