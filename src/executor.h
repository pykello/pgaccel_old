#pragma once

#include "column_data.hpp"
#include "types.hpp"
#include "result_type.hpp"
#include "parser.h"
#include "executor_tasks.hpp"
#include <vector>
#include <string>
#include <future>
#include <memory_resource>

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
    virtual void Prepare(const ColumnarTable &, std::pmr::memory_resource &, int) {}
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
    uint64_t spillBytes = 0;
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
        int workers=std::min(8,std::max(1,table.RowGroupCount()));
        auto partials=RunTasks(workers,[&](int worker) {
            PartialResult local{};uint8_t bitmap[RowGroupSize/8];
            for(int group=worker;group<table.RowGroupCount();group+=workers)
                CombineF(local,ProcessRowgroupF(table.GetRowGroup(group),bitmap));
            return local;
        });
        PartialResult global{};
        for(auto &partial:partials)CombineF(global,std::move(partial));
        return FinalizeF(global);
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
