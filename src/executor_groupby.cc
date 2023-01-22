#include "executor_groupby.h"
#include "util.h"

namespace pgaccel
{

AggregateNodeImpl::AggregateNodeImpl(
    const std::vector<AggregateClause> &aggregateClauses,
    const std::vector<ColumnRef> &groupBy,
    FilterNodeP &&filterNode,
    bool useAvx)
        : filterNode(std::move(filterNode)),
          useAvx(useAvx)
{
    for(const auto &aggClause: aggregateClauses)
    {
        switch (aggClause.type)
        {
            case AggregateClause::AGGREGATE_COUNT:
                aggregators.push_back(
                    std::make_unique<CountAgg>(useAvx));
                break;

            case AggregateClause::AGGREGATE_SUM:
                aggregators.push_back(
                    std::make_unique<SumAgg>(*aggClause.columnRef, useAvx));
                break;
            
            default:
                break;
        }

        if (aggClause.type != AggregateClause::AGGREGATE_PROJECT)
        {
            projection.push_back(groupBy.size() + aggregators.size() - 1);
            fieldNames.push_back(aggClause.ToString());
        }
        else
        {
            for (int i = 0; i < groupBy.size(); i++)
                if (groupBy[i].columnIdx == aggClause.columnRef->columnIdx)
                {
                    projection.push_back(i);
                    break;
                }
            fieldNames.push_back(aggClause.columnRef->Name());
        }
    }

    this->groupBy = groupBy;
}

LocalAggResult
AggregateNodeImpl::ProcessRowGroup(const RowGroup &rowGroup, uint8_t *selectionBitmap) const
{
    LocalAggResult localResult;

    ColumnDataGroups groups;
    int col = groupBy[0].columnIdx;
    auto dictData = static_cast<DictColumnDataBase *>(rowGroup.columns[col].get());
    groups.groupCount = dictData->dictSize();

    uint8_t bitmap[1<<13];
    if (filterNode)
    {
        filterNode->ExecuteSet(rowGroup, bitmap);
        selectionBitmap = bitmap;
    }

    dictData->to_16(groups.groups);

    std::vector<bool> groupVisited(groups.groupCount, false);
    int setGroups = 0;
    for (int i = 0; i < rowGroup.size && setGroups < groups.groupCount; i++)
        if (selectionBitmap == nullptr ||
            IsBitSet(selectionBitmap[i >> 3], (i & 7)))
        {
            if (!groupVisited[groups.groups[i]])
            {
                groupVisited[groups.groups[i]] = true;
                setGroups++;
            }
        }

    for (const auto &agg: aggregators) {
        auto localAggResult = agg->LocalAggregate(rowGroup, groups, selectionBitmap);
        for (int i = 0; i < groups.groupCount; i++)
            if (groupVisited[i])
                localResult.groupAggStates[{ dictData->label(i) }].push_back(
                    std::move(localAggResult[i]));
    }

    return localResult;
}

void
AggregateNodeImpl::Combine(LocalAggResult &left, LocalAggResult &&right) const
{
    for (auto &group: right.groupAggStates)
    {
        Row label = group.first;
        bool first = left.groupAggStates.count(label) == 0;
        if (first)
        {
            left.groupAggStates[label] = std::move(group.second);
            continue;
        }

        for (int i = 0; i < aggregators.size(); i++)
        {
            aggregators[i]->Combine(
                left.groupAggStates[label][i].get(),
                group.second[i].get()
            );
        }
    }
}

Rows
AggregateNodeImpl::Finalize(const LocalAggResult &localResult) const
{
    Rows result;
    for (const auto &group: localResult.groupAggStates)
    {
        Row row = group.first;
        for (int i = 0; i < aggregators.size(); i++)
        {
            row.push_back(
                aggregators[i]->Finalize(group.second[i].get()));
        }

        Row projectedRow;
        for (auto idx: projection)
            projectedRow.push_back(row[idx]);

        result.push_back(projectedRow);
    }

    return result;
}

Row
AggregateNodeImpl::FieldNames() const
{
    return fieldNames;
}

AggStateVec
CountAgg::LocalAggregate(const RowGroup& rowGroup,
                         const ColumnDataGroups& groups,
                         uint8_t *bitmap)
{

    std::vector<int> countsPerGroup(groups.groupCount, 0);

    if (bitmap) {
        for (int i = 0; i < rowGroup.size; i++)
            if (bitmap[i >> 3] & (1 << (i & 7)))
                countsPerGroup[groups.groups[i]]++;
    } else {
        for (int i = 0; i < rowGroup.size; i++)
            countsPerGroup[groups.groups[i]]++;
    }

    AggStateVec result;
    for (int i = 0; i < groups.groupCount; i++)
        result.push_back(std::make_unique<CountAggState>(countsPerGroup[i]));

    return result;
}

void
CountAgg::Combine(AggState *state1, const AggState *state2)
{
    auto countState1 = static_cast<CountAggState *>(state1);
    auto countState2 = static_cast<const CountAggState *>(state2);

    countState1->value += countState2->value;
}

std::string
CountAgg::Finalize(const AggState *state)
{
    auto countState = static_cast<const CountAggState *>(state);
    return std::to_string(countState->value);
}

template<class storageType, bool hasBitmap>
void
CalculateRawDataSum(
    uint8_t *data,
    int size,
    uint8_t *bitmap,
    std::vector<int64_t> &sumsPerGroup,
    const uint16_t *groups)
{
    auto values = (storageType *) data;
    for (int i = 0; i < size; i++)
        if constexpr(hasBitmap)
        {
            if (IsBitSet(bitmap, i))
                sumsPerGroup[groups[i]] += values[i];
        }
        else
        {
            sumsPerGroup[groups[i]] += values[i];
        }
}

template<class AccelTy, bool hasBitmap>
void
CalculateRawDataSum(
    RawColumnData<AccelTy> *columnData,
    uint8_t *bitmap,
    std::vector<int64_t> &sumsPerGroup,
    const uint16_t *groups)
{
    switch (columnData->bytesPerValue)
    {
        #define CalculateRawDataSum_DISPATCH(width, storageType) \
            case width: \
                return CalculateRawDataSum<storageType, hasBitmap>( \
                        columnData->values, columnData->size, bitmap, sumsPerGroup, groups);
        CalculateRawDataSum_DISPATCH(1, int8_t);
        CalculateRawDataSum_DISPATCH(2, int16_t);
        CalculateRawDataSum_DISPATCH(4, int32_t);
        CalculateRawDataSum_DISPATCH(8, int64_t);
    }
}

template<bool hasBitmap>
void
CalculateRawDataSum(
    ColumnDataBase *columnData,
    uint8_t *bitmap,
    std::vector<int64_t> &sumsPerGroup,
    const uint16_t *groups,
    AccelType *type)
{
    DISPATCH_RAW_TYPE(
        type->type_num(),
        return CalculateRawDataSum<AccelTy COMMA hasBitmap>(
                    (RawColumnData<AccelTy> *) columnData,
                    bitmap,
                    sumsPerGroup,
                    groups));
}

AggStateVec
SumAgg::LocalAggregate(const RowGroup& rowGroup,
                       const ColumnDataGroups& groups,
                       uint8_t *bitmap)
{
    std::vector<int64_t> sumsPerGroup(groups.groupCount, 0);

    auto dataType = this->columnRef.Type().get();
    auto columnData = rowGroup.columns[this->columnRef.columnIdx].get();

    switch (columnData->type)
    {
        case ColumnDataBase::DICT_COLUMN_DATA:
            // not supported yet
            break;

        case ColumnDataBase::RAW_COLUMN_DATA:
            if (bitmap == NULL)
                CalculateRawDataSum<false>(
                    columnData, bitmap, sumsPerGroup, groups.groups, dataType);
            else
                CalculateRawDataSum<true>(
                    columnData, bitmap, sumsPerGroup, groups.groups, dataType);
            break;
    }

    AggStateVec result;
    for (int i = 0; i < groups.groupCount; i++)
        result.push_back(
            std::make_unique<SumAggState>(sumsPerGroup[i], columnRef.Type()));

    return result;
}

void
SumAgg::Combine(AggState *state1, const AggState *state2)
{
    auto sumState1 = static_cast<SumAggState *>(state1);
    auto sumState2 = static_cast<const SumAggState *>(state2);

    sumState1->value += sumState2->value;
}

std::string
SumAgg::Finalize(const AggState *state)
{
    auto sumState = static_cast<const SumAggState *>(state);
    return ToString(sumState->valueType.get(), sumState->value);
}

};
