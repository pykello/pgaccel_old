#pragma once
#include "executor_state.hpp"
namespace pgaccel
{
namespace
{
template <class T>
void SumGroups(const RawColumnDataBase *data, const DictColumnDataBase *dict, const RowGroup &rg,
               const uint8_t *selection, std::vector<Wide> &sums)
{
    auto values = reinterpret_cast<const T *>(data->values);
    // A row-group total of signed 64-bit inputs fits in 80 bits.
    if (dict->bytesPerValue() == 1)
        Selected(rg, selection, [&](int r) { sums[dict->values[r]] += values[r]; });
    else
    {
        auto codes = reinterpret_cast<const uint16_t *>(dict->values);
        Selected(rg, selection, [&](int r) { sums[codes[r]] += values[r]; });
    }
}
bool DenseAggregate(const QueryDesc &q, const RowGroup &rg, const uint8_t *selection,
                    const DictColumnDataBase *dict, Groups &groups, Budget &budget)
{
    for (const auto &a : q.aggregateClauses)
    {
        if (a.type == AggregateClause::AGGREGATE_PROJECT ||
            a.type == AggregateClause::AGGREGATE_COUNT)
            continue;
        if ((a.type != AggregateClause::AGGREGATE_SUM &&
             a.type != AggregateClause::AGGREGATE_AVG) ||
            !a.columnRef ||
            rg.columns[a.columnRef->columnIdx]->type != ColumnDataBase::RAW_COLUMN_DATA)
            return false;
    }
    Scratch scratch(budget, uint64_t(dict->dictSize()) * 48);
    std::vector<uint64_t> counts(dict->dictSize(), 0);
    std::vector<int> visited;
    auto visit = [&](int code)
    {
        if (!counts[code]++)
            visited.push_back(code);
    };
    if (dict->bytesPerValue() == 1)
        Selected(rg, selection, [&](int r) { visit(dict->values[r]); });
    else
    {
        auto codes = reinterpret_cast<const uint16_t *>(dict->values);
        Selected(rg, selection, [&](int r) { visit(codes[r]); });
    }
    std::vector<States *> states(dict->dictSize(), nullptr);
    for (int code : visited)
        states[code] = &Get(groups, Label(dict, code, q.groupBy[0].Type()->type_num()),
                            q.aggregateClauses.size(), budget);
    for (size_t i = 0; i < q.aggregateClauses.size(); ++i)
    {
        const auto &a = q.aggregateClauses[i];
        if (a.type == AggregateClause::AGGREGATE_PROJECT)
            continue;
        if (a.type == AggregateClause::AGGREGATE_COUNT)
        {
            for (int code : visited)
                (*states[code])[i].count += counts[code];
        }
        else
        {
            auto data =
                static_cast<const RawColumnDataBase *>(rg.columns[a.columnRef->columnIdx].get());
            std::vector<Wide> sums(dict->dictSize(), 0);
            switch (data->bytesPerValue)
            {
            case 1:
                SumGroups<int8_t>(data, dict, rg, selection, sums);
                break;
            case 2:
                SumGroups<int16_t>(data, dict, rg, selection, sums);
                break;
            case 4:
                SumGroups<int32_t>(data, dict, rg, selection, sums);
                break;
            case 8:
                SumGroups<int64_t>(data, dict, rg, selection, sums);
                break;
            }
            for (int code : visited)
            {
                auto &s = (*states[code])[i];
                s.sum = Add(s.sum, sums[code]);
                s.count += counts[code];
            }
        }
    }
    return true;
}
} // namespace
} // namespace pgaccel
