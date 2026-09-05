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
    template<class Left,class Right> bool compareValues(Left a,Right b) const
    {
        for (const auto &o : q.orderBy)
        {
            const auto &l = a(o.output), &r = b(o.output);
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
        for (size_t i = 0; i < n; ++i) {
            const auto &l=a(i),&r=b(i);
            if (!(l.value==r.value))return Less(l.value,r.value);
        }
        return false;
    }

    bool compare(const FinalRow &a,const FinalRow &b) const {
        return compareValues([&](size_t i)->const FinalCell &{return a[i];},
                             [&](size_t i)->const FinalCell &{return b[i];});
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
    const FinalCell *limitBoundary(size_t column) const {
        return q.limit && capacity && rows.size() == capacity ? &rows.front()[column] : nullptr;
    }
    std::vector<FinalRow> takeCandidates() { return std::move(rows); }
    template<class Get> void addLazy(Get get)
    {
        if(q.limit && !*q.limit)return;
        if(q.limit && rows.size()>=capacity &&
            !compareValues(get,[&](size_t i)->const FinalCell &{return rows.front()[i];}))return;
        FinalRow row;row.reserve(n);
        for(size_t i=0;i<n;++i)row.push_back(get(i));
        add(std::move(row));
    }
    void add(FinalRow row)
    {
        for (const auto &h:q.having)
        {
            if (row[h.output].null || h.nullBound) return;
            int rowScale = row[h.output].type ? Scale(row[h.output].type) : 0;
            int scale = std::max(rowScale, h.scale);
            auto value = Multiply(row[h.output].value.number, Power(scale - rowScale));
            auto bound = Multiply(h.value, Power(scale - h.scale));
            bool keep=false;
            switch(h.op) {
            case FilterClause::FILTER_EQ:keep=value==bound;break;
            case FilterClause::FILTER_NE:keep=value!=bound;break;
            case FilterClause::FILTER_GT:keep=value>bound;break;
            case FilterClause::FILTER_GTE:keep=value>=bound;break;
            case FilterClause::FILTER_LT:keep=value<bound;break;
            case FilterClause::FILTER_LTE:keep=value<=bound;break;
            default:break;
            }
            if(!keep)return;
        }
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
