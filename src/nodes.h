#pragma once

#include "column_data.hpp"
#include "columnar_table.h"
#include "executor_groupby.h"

#include <string>

namespace pgaccel
{

class Node {
public:
    enum Type {
        SCAN_NODE,
        EXTEND_NODE,
        FILTER_NODE,
        AGGREGATE_NODE
    };

    virtual Type GetType() const = 0;
    virtual std::vector<ColumnDesc> Schema() const = 0;
};

struct ExecutionParams {
    bool useAvx = true;
};

class PartitionedNode: public Node {
public:
    virtual std::unique_ptr<RowGroup> Execute(int partition) const = 0;
    virtual int PartitionCount() const = 0;
};

typedef std::unique_ptr<Node> NodeP;
typedef std::unique_ptr<PartitionedNode> PartitionedNodeP;


/*
 * ScanNode scans a subset of columns of a columnar table.
 */
class ScanNode: public PartitionedNode {
public:
    ScanNode(ColumnarTable *table,
             const std::vector<std::string> &selectedColumnNames);

    virtual Type GetType() const {
        return SCAN_NODE;
    }

    virtual std::unique_ptr<RowGroup> Execute(int partition) const;
    virtual int PartitionCount() const;
    virtual std::vector<ColumnDesc> Schema() const;

private:
    ColumnarTable *table;
    std::vector<int> selectedColumnIndexes;
    std::vector<ColumnDesc> schema;
};

/*
 * ExtendNode extends the child node's output by some calculated
 * columns.
 */
class ExtendNode: public PartitionedNode {
public:
    ExtendNode(PartitionedNodeP child);

    virtual Type GetType() const {
        return EXTEND_NODE;
    }

    virtual std::unique_ptr<RowGroup> Execute(int partition) const;
    virtual int PartitionCount() const;
    virtual std::vector<ColumnDesc> Schema() const;
};

/*
 * FilterNode
 */

class FilterNode: public PartitionedNode {
public:
    FilterNode(PartitionedNodeP child,
               const std::vector<FilterClause> filterClauses,
               const ExecutionParams &params);

    virtual Type GetType() const {
        return FILTER_NODE;
    }

    virtual std::unique_ptr<RowGroup> Execute(int partition) const;
    virtual int PartitionCount() const;
    virtual std::vector<ColumnDesc> Schema() const;

private:
    PartitionedNodeP child;
    FilterNodeP impl;
};

/*
 * AggregateNode
 */

class AggregateNode: public Node {
public:
    AggregateNode(PartitionedNodeP child,
                  const std::vector<AggregateClause> &aggregateClauses,
                  const std::vector<ColumnRef> &groupBy,
                  const ExecutionParams &params);

    virtual Type GetType() const {
        return AGGREGATE_NODE;
    }

    LocalAggResultP LocalTask(std::function<bool(int)> selectPartitionF) const;
    Rows GlobalTask(std::vector<std::future<LocalAggResultP>> &localResults) const;

    virtual int LocalPartitionCount() const;
    virtual std::vector<ColumnDesc> Schema() const;

private:
    PartitionedNodeP child;
    AggregateNodeImpl impl;
    std::vector<ColumnDesc> schema;
};

};
