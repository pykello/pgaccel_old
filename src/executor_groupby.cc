#include "executor_groupby.h"
#include "util.h"

namespace pgaccel
{

AggregateNodeImpl::AggregateNodeImpl(
    const std::vector<AggregateClause> &aggregateClauses,
    const std::vector<ColumnRef> &groupBy,
    FilterNodeP &&filterNode,
    bool useAvx): filterNode(std::move(filterNode))
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
    
    for (auto label: dictData->labels()) {
        groups.labels.push_back({ label });
    }

    uint8_t bitmap[1<<13];
    if (filterNode)
    {
        filterNode->ExecuteSet(rowGroup, bitmap);
        selectionBitmap = bitmap;
    }

    dictData->to_16(groups.groups);

    std::vector<bool> groupVisited(groups.labels.size(), false);
    int setGroups = 0;
    for (int i = 0; i < rowGroup.size && setGroups < groups.labels.size(); i++)
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
        for (int i = 0; i < groups.labels.size(); i++)
            if (groupVisited[i])
                localResult.groupAggStates[{ groups.labels[i] }].push_back(
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
    std::vector<int> countsPerGroup(groups.labels.size(), 0);

    if (bitmap) {
        for (int i = 0; i < rowGroup.size; i++)
            if (bitmap[i >> 3] & (1 << (i & 7)))
                countsPerGroup[groups.groups[i]]++;
    } else {
        for (int i = 0; i < rowGroup.size; i++)
            countsPerGroup[groups.groups[i]]++;
    }

    AggStateVec result;
    for (int i = 0; i < groups.labels.size(); i++)
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

AggStateVec
SumAgg::LocalAggregate(const RowGroup& rowGroup,
                       const ColumnDataGroups& groups,
                       uint8_t *bitmap)
{
    std::vector<int> sumsPerGroup(groups.labels.size(), 0);
    for (int i = 0; i < rowGroup.size; i++)
        sumsPerGroup[groups.groups[i]] += 5; // todo

    AggStateVec result;
    for (int i = 0; i < groups.labels.size(); i++)
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
