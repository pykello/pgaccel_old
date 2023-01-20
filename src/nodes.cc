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
            if (ToLower(tableSchema[i].name) == columnNameLc)
            {
                schema.push_back(tableSchema[i]);
                selectedColumnIndexes.push_back(i);
                break;
            }
        }
    }
}

std::unique_ptr<RowGroup>
ScanNode::Execute(int partition) const
{
    const auto &tableRowGroup = table->GetRowGroup(partition);

    auto resultRowGroup = std::make_unique<RowGroup>();
    for (auto columnIdx: selectedColumnIndexes)
        resultRowGroup->columns.push_back(tableRowGroup.columns[columnIdx]);
    resultRowGroup->size = tableRowGroup.size;

    return std::move(resultRowGroup);
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
                       const std::vector<FilterClause> filterClauses,
                       const ExecutionParams &params)
    : child(std::move(child)),
      impl(CreateFilterNode(filterClauses, params.useAvx))
{
}

std::unique_ptr<RowGroup>
FilterNode::Execute(int partition) const
{
    auto result = child->Execute(partition);
    if (impl)
    {
        result->selectionBitmap =
            std::make_unique<std::array<uint8_t, BITMAP_SIZE>>();
        int x = impl->ExecuteSet(*result, result->selectionBitmap->data());
    }
    return std::move(result);
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
                             const std::vector<ColumnRef> &groupBy,
                             const ExecutionParams &params)
    : child(std::move(child)),
      impl(aggregateClauses, groupBy, nullptr, params.useAvx)
{
    for (const auto &fieldName: impl.FieldNames())
        schema.push_back({
            fieldName,
            std::make_shared<StringType>(),
            ColumnDataBase::RAW_COLUMN_DATA
        }); 
}

LocalAggResultP
AggregateNode::LocalTask(std::function<bool(int)> selectPartitionF) const
{
    LocalAggResultP result = std::make_unique<LocalAggResult>();
    int partitionCount = LocalPartitionCount();
    for (int i = 0; i < partitionCount; i++)
        if (selectPartitionF(i))
        {
            auto childRowGroup = child->Execute(i);
            uint8_t *selectionBitmap = nullptr;
            if (childRowGroup->selectionBitmap)
                selectionBitmap = childRowGroup->selectionBitmap->data();
            impl.Combine(
                *result,
                impl.ProcessRowGroup(*childRowGroup, selectionBitmap));
        }
    return std::move(result);
}

Rows
AggregateNode::GlobalTask(std::vector<std::future<LocalAggResultP>> &localResults) const
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
