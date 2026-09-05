#pragma once

#include "column_data.hpp"
#include "types.hpp"
#include "result_type.hpp"
#include "parser.h"
#include <vector>
#include <string>
#include <future>

namespace pgaccel
{

struct Value {
    std::string strValue;
    int64_t int64Value;
};

typedef std::vector<Value> RowX;

struct RowsX {
    std::vector<ColumnDesc> schema;
    std::vector<RowX> rows;
};

typedef std::vector<std::string> Row;
typedef std::vector<Row> Rows;

// nodes

class FilterNodeImpl;
typedef std::unique_ptr<FilterNodeImpl> FilterNodeP;

class FilterNodeImpl {
public:
    virtual ~FilterNodeImpl() = default;
    virtual int ExecuteCount(const RowGroup &rowGroup) const = 0;
    virtual int ExecuteSet(const RowGroup &rowGroup, uint8_t *bitmask) const = 0;
    virtual int ExecuteAnd(const RowGroup &rowGroup, uint8_t *bitmask) const = 0;

    static FilterNodeP CreateSimpleCompare(const ColumnRef &colRef,
                                           const std::string &valueStr,
                                           FilterClause::Op op,
                                           const std::string &fusedValueStr,
                                           FilterClause::Op fusedOp,
                                           bool useAvx);
    static FilterNodeP CreateAndNode(std::vector<FilterNodeP>&& children);

};

struct QueryOutput {
    uint64_t peakMemoryBytes = 0;
    Row fieldNames;
    std::vector<Row> values;
};

Result<QueryOutput> ExecuteQuery(
    const QueryDesc &query,
    bool useAvx,
    bool useParallelism);

int64_t SumAll(const ColumnDataP& columnData,
               const pgaccel::AccelType *type,
               bool useAvx);

FilterNodeP CreateFilterNode(
    const std::vector<FilterClause> &filterClauses,
    bool useAvx);

template<class AccelTy>
int DictIndex(const DictColumnData<AccelTy> &columnData, 
              typename AccelTy::c_type value,
              FilterClause::Op op)
{
    auto lo = std::lower_bound(columnData.dict.begin(), columnData.dict.end(), value);
    auto hi = std::upper_bound(columnData.dict.begin(), columnData.dict.end(), value);
    int l = lo - columnData.dict.begin(), h = hi - columnData.dict.begin();
    switch (op) {
    case FilterClause::FILTER_EQ:
    case FilterClause::FILTER_NE: return lo != columnData.dict.end() && *lo == value ? l : -1;
    case FilterClause::FILTER_GT: return h - 1;
    case FilterClause::FILTER_GTE: return l;
    case FilterClause::FILTER_LT: return l;
    case FilterClause::FILTER_LTE: return h - 1;
    default: return -1;
    }
}

template<typename PartialResult>
Rows
ExecuteAgg(const std::function<PartialResult(const RowGroup&, uint8_t *)> &ProcessRowgroupF,
           const std::function<void(PartialResult&, PartialResult&&)> &CombineF,
           const std::function<Rows(const PartialResult&)> &FinalizeF,
           const ColumnarTable &table,
           bool useParallelism)
{
    if (useParallelism)
    {
        int numThreads = 8;
        int rowGroupCnt = table.RowGroupCount();

        std::vector<std::future<PartialResult>> futureResults;
        for (int i = 0; i < numThreads; i++)
        {
            futureResults.push_back(
                std::async([&](int m) {
                    PartialResult localResult {};
                    uint8_t bitmap[1 << 13];
                    for (int j = 0; j < rowGroupCnt; j++)
                    {
                        if (j % numThreads == m)
                        {
                            const RowGroup &rowGroup = table.GetRowGroup(j);
                            CombineF(localResult, ProcessRowgroupF(rowGroup, bitmap));
                        }
                    }
                    return localResult;
                }, i));
        }

        PartialResult globalResult {};
        for (auto &f: futureResults)
            CombineF(globalResult, std::move(f.get()));

        return FinalizeF(globalResult);
    }
    else
    {
        uint8_t bitmap[1 << 13];
        PartialResult partialResult {};
        int rowGroupCnt = table.RowGroupCount();
        for (int groupIdx = 0; groupIdx < rowGroupCnt; groupIdx++)
        {
            const RowGroup &rowGroup = table.GetRowGroup(groupIdx);
            CombineF(partialResult, ProcessRowgroupF(rowGroup, bitmap));
        }

        return FinalizeF(partialResult);
    }
}

};
