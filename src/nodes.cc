#include "nodes.h"
#include "util.h"

namespace pgaccel
{

/*
 * ==================
 * ==== ScanNode ====
 * ==================
 */

ScanNode::ScanNode(ColumnarTable *table,
                   const std::vector<std::string> &selectedColumnNames)
    : table(table)
{
    const auto &tableSchema = table->Schema();
    for (auto columnName: selectedColumnNames)
    {
        std::string columnNameLc = ToLower(columnName);
        for (size_t i = 0; i < tableSchema.size(); i++)
        {
            if (tableSchema[i].name == columnNameLc)
            {
                schema.push_back(tableSchema[i]);
                selectedColumnIndexes.push_back(i);
                break;
            }
        }
    }
}

std::optional<RowGroup>
ScanNode::Execute(int partition) const
{
    const auto &tableRowGroup = table->GetRowGroup(partition);

    RowGroup resultRowGroup;
    for (auto columnIdx: selectedColumnIndexes)
        resultRowGroup.columns.push_back(tableRowGroup.columns[columnIdx]);
    resultRowGroup.size = tableRowGroup.size;

    return resultRowGroup;
}

std::vector<ColumnDesc>
ScanNode::Schema() const
{
    return schema;
}

int
ScanNode::PartitionCount() const
{
    return table->RowGroupCount();
}

/*
 * ====================
 * ==== FilterNode ====
 * ====================
 */

FilterNode::FilterNode(PartitionedNodeP child,
                       const std::vector<FilterClause> filterClauses)
    : child(std::move(child))
{
    // todo
}

std::optional<RowGroup>
FilterNode::Execute(int partition) const
{
    return child->Execute(partition);
}

int
FilterNode::PartitionCount() const
{
    return child->PartitionCount();
}

std::vector<ColumnDesc>
FilterNode::Schema() const
{
    return child->Schema();
}

/*
 * =======================
 * ==== AggregateNode ====
 * =======================
 */

AggregateNode::AggregateNode(PartitionedNodeP child,
                             const std::vector<AggregateClause> &aggregateClauses,
                             const std::vector<ColumnRef> &groupBy)
    : child(std::move(child)),
      impl(aggregateClauses, groupBy, nullptr, true)
{
    for (const auto &fieldName: impl.FieldNames())
        schema.push_back({
            fieldName,
            std::make_shared<StringType>(),
            ColumnDataBase::RAW_COLUMN_DATA
        }); 
}

LocalAggResult
AggregateNode::LocalTask(std::function<bool(int)> selectPartitionF)
{
    LocalAggResult result;
    int partitionCount = LocalPartitionCount();
    for (int i = 0; i < partitionCount; i++)
        if (selectPartitionF(i))
        {
            auto childRowGroup = child->Execute(i);
            if (childRowGroup.has_value())
                impl.Combine(result, impl.ProcessRowGroup(*childRowGroup));
        }
    return result;
}

Rows
AggregateNode::GlobalTask(std::vector<std::future<LocalAggResultP>> localResults)
{
    LocalAggResult result;
    for (auto &localResult: localResults)
        impl.Combine(result, std::move(*(localResult.get())));

    return impl.Finalize(result);
}

int
AggregateNode::LocalPartitionCount() const
{
    return child->PartitionCount();
}

std::vector<ColumnDesc>
AggregateNode::Schema() const
{
    return schema;
}

};
