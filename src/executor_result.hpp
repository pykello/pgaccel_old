#pragma once
#include "executor_state.hpp"
namespace pgaccel
{
namespace
{
class QueryResults
{
    const QueryDesc &q;
    Budget &budget;
    size_t n;
    size_t visible;
    uint64_t capacity;
    QueryOutput output;
    std::vector<FinalRow> rows;
    bool compare(const FinalRow &a, const FinalRow &b) const
    {
        for (const auto &o : q.orderBy)
        {
            const auto &l = a[o.output], &r = b[o.output];
            if (l.null != r.null)
                return l.null; // SQL default NULLS FIRST ascending.
            if (l.null)
                continue;
            bool eq = l.average ? l.mean == r.mean : l.value == r.value;
            if (eq)
                continue;
            bool less = l.average ? l.mean < r.mean : Less(l.value, r.value);
            return o.descending ? !less : less;
        }
        // Stable deterministic tie-breaks; ties remain SQL-equivalent.
        for (size_t i = 0; i < a.size(); ++i)
            if (!(a[i].value == b[i].value))
                return Less(a[i].value, b[i].value);
        return false;
    }

  public:
    QueryResults(const QueryDesc &query, Budget &memory)
        : q(query), budget(memory), n(q.aggregateClauses.size()), visible(q.outputColumnCount.value_or(n)),
          capacity(q.limit ? *q.limit + q.offset : UINT64_MAX)
    {
        for (size_t i=0;i<visible;++i)
            output.fieldNames.push_back(q.aggregateClauses[i].ToString());
    }
    size_t size() const { return rows.size(); }
    void add(FinalRow row)
    {
        if (q.limit && !*q.limit)
            return;
        if (!q.limit || rows.size() < capacity)
        {
            budget.charge(64 + n * sizeof(FinalCell));
            rows.push_back(std::move(row));
            if (q.limit)
                std::push_heap(rows.begin(), rows.end(),
                               [&](const auto &a, const auto &b) { return compare(a, b); });
        }
        else if (compare(row, rows.front()))
        {
            std::pop_heap(rows.begin(), rows.end(),
                          [&](const auto &a, const auto &b) { return compare(a, b); });
            rows.back() = std::move(row);
            std::push_heap(rows.begin(), rows.end(),
                           [&](const auto &a, const auto &b) { return compare(a, b); });
        }
    }
    QueryOutput finish()
    {
        size_t keep = q.limit ? std::min<uint64_t>(capacity, rows.size()) : rows.size();
        if (!q.orderBy.empty() || q.grouped)
        {
            if (keep < rows.size())
                std::partial_sort(rows.begin(), rows.begin() + keep, rows.end(),
                                  [&](const auto &a, const auto &b) { return compare(a, b); });
            else
                std::sort(rows.begin(), rows.end(),
                          [&](const auto &a, const auto &b) { return compare(a, b); });
        }
        rows.resize(keep);
        for (size_t index = std::min<uint64_t>(q.offset, rows.size()); index < rows.size(); ++index)
        {
            const auto &r = rows[index];
            Row row;
            for (size_t column=0;column<visible;++column)
            {
                const auto &v=r[column];
                budget.charge(v.value.string ? v.value.text.size() + 32 : 128);
                row.push_back(Format(v));
            }
            output.values.push_back(std::move(row));
        }
        output.peakMemoryBytes = budget.peak.load();
        return std::move(output);
    }
};
} // namespace
} // namespace pgaccel
