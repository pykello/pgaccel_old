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
    int left = 0, right = columnData.dict.size() - 1;
    int result;
    switch (op)
    {
        case FilterClause::FILTER_LT:
        case FilterClause::FILTER_LTE:
            result = columnData.dict.size();
            break;

        default:
            result = -1;
    }

    while (left <= right) {
        int mid = (left + right) / 2;
        if (columnData.dict[mid] == value)
            return mid;

        if (value < columnData.dict[mid]) {
            right = mid - 1;
            switch (op)
            {
                case FilterClause::FILTER_LT:
                    result = mid;
                    break;
                case FilterClause::FILTER_LTE:
                    result = mid - 1;
                    break;
            }
        } else {
            left = mid + 1;
            switch (op)
            {
                case FilterClause::FILTER_GT:
                    result = mid;
                    break;
                case FilterClause::FILTER_GTE:
                    result = mid + 1;
                    break;
            }
        }
    }

    return result;
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
