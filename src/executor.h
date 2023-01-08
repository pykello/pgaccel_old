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

typedef std::vector<std::string> Row;
typedef std::vector<Row> Rows;

// nodes

enum CompareOp {
    COMPARE_EQ
};

class FilterNode {
public:
    virtual int ExecuteCount(ColumnDataBase *columnData) = 0;
    virtual int ExecuteSet(ColumnDataBase *columnData, uint8_t *bitmask) = 0;
    virtual int ExecuteAnd(ColumnDataBase *columnData, uint8_t *bitmask) = 0;

    static std::unique_ptr<FilterNode> Create(const ColumnDesc &columnDesc,
                                              const std::string &valueStr,
                                              CompareOp op,
                                              bool useAvx);
};

struct QueryOutput {
    Row fieldNames;
    std::vector<Row> values;
};

Result<QueryOutput> ExecuteQuery(
    const QueryDesc &query,
    bool useAvx,
    bool useParallelism);

// sum
int64_t SumAll(const ColumnDataP& columnData,
               const pgaccel::AccelType *type,
               bool useAvx);

template<class AccelTy>
int DictIndex(const DictColumnData<AccelTy> &columnData, 
              typename AccelTy::c_type value)
{
    int left = 0, right = columnData.dict.size() - 1;
    while (left <= right) {
        int mid = (left + right) / 2;
        if (columnData.dict[mid] == value)
            return mid;
        else if (value < columnData.dict[mid]) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return -1;
}

template<typename PartialResult>
Rows
ExecuteAgg(const std::function<PartialResult(const RowGroup&)> &ProcessRowgroupF,
           const std::function<PartialResult(const PartialResult&, const PartialResult&)> &CombineF,
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
                    for (int j = 0; j < rowGroupCnt; j++)
                    {
                        if (j % numThreads == m)
                        {
                            const RowGroup &rowGroup = table.GetRowGroup(j);
                            localResult =
                                CombineF(localResult, ProcessRowgroupF(rowGroup));
                        }
                    }
                    return localResult;
                }, i));
        }

        PartialResult globalResult {};
        for (auto &f: futureResults)
            globalResult = CombineF(globalResult, f.get());

        return FinalizeF(globalResult);
    }
    else
    {
        PartialResult partialResult {};
        int rowGroupCnt = table.RowGroupCount();
        for (int groupIdx = 0; groupIdx < rowGroupCnt; groupIdx++)
        {
            const RowGroup &rowGroup = table.GetRowGroup(groupIdx);
            partialResult = CombineF(partialResult, ProcessRowgroupF(rowGroup));
        }

        return FinalizeF(partialResult);
    }
}

};
