#include "executor_value.hpp"
namespace pgaccel
{
// Reuse column statistics and one vector sum for equivalent affine sums.
std::optional<Result<QueryOutput>> ExecuteScalarFast(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.having.empty() || q.grouped || !q.groupBy.empty() || !q.filterClauses.empty() ||
        q.outputColumnCount.value_or(q.aggregateClauses.size())!=q.aggregateClauses.size())
        return {};
    struct Binding
    {
        int column = -1;
        Wide offset = 0;
    };
    std::vector<Binding> bindings;
    std::vector<ColumnRef> columns;
    std::vector<bool> needSum;
    for (const auto &a : q.aggregateClauses)
    {
        if (a.type == AggregateClause::AGGREGATE_PROJECT ||
            a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT)
            return {};
        Binding b;
        if (a.expression)
        {
            auto e = a.expression;
            if (a.type == AggregateClause::AGGREGATE_SUM && e->kind == Expression::ADD &&
                e->left->kind == Expression::COLUMN && e->right->kind == Expression::CONSTANT &&
                e->right->type->type_num() == INT64_TYPE && Scale(e->type.get()) == 0)
            {
                b.offset = ParseDecimal(0, e->right->literal);
                e = e->left;
            }
            if (e->kind != Expression::COLUMN)
                return {};
            auto it = std::find(columns.begin(), columns.end(), *e->column);
            b.column = it - columns.begin();
            if (it == columns.end())
            {
                columns.push_back(*e->column);
                needSum.push_back(false);
            }
            needSum[b.column] = needSum[b.column] || a.type == AggregateClause::AGGREGATE_SUM ||
                                a.type == AggregateClause::AGGREGATE_AVG;
        }
        bindings.push_back(b);
    }
    struct Stats
    {
        uint64_t count = 0;
        Wide sum = 0;
        Cell minimum, maximum;
    };
    auto table = q.tables[0];
    int threads = parallel ? std::min(8, std::max(1, table->RowGroupCount())) : 1;
    auto task = [&](int thread)
    {
        std::vector<Stats> stats(columns.size() + 1);
        for (int g = thread; g < table->RowGroupCount(); g += threads)
        {
            const auto &rg = table->GetRowGroup(g);
            stats.back().count += rg.size;
            if (!rg.size)
                continue;
            for (size_t i = 0; i < columns.size(); ++i)
            {
                auto &s = stats[i];
                const auto &col = columns[i];
                auto data = rg.columns[col.columnIdx].get();
                Cell lo, hi;
                Wide sum = 0;
                if (data->type == ColumnDataBase::DICT_COLUMN_DATA)
                {
                    auto d = static_cast<const DictColumnDataBase *>(data);
                    lo = Label(d, 0, col.Type()->type_num());
                    hi = Label(d, d->dictSize() - 1, col.Type()->type_num());
                }
                else
                {
                    if (col.Type()->type_num() == INT32_TYPE)
                    {
                        auto d = static_cast<const RawColumnData<Int32Type> *>(data);
                        lo.number = d->minValue;
                        hi.number = d->maxValue;
                    }
                    else if (col.Type()->type_num() == DECIMAL_TYPE)
                    {
                        auto d = static_cast<const RawColumnData<DecimalType> *>(data);
                        lo.number = d->minValue;
                        hi.number = d->maxValue;
                    }
                    else
                    {
                        auto d = static_cast<const RawColumnData<Int64Type> *>(data);
                        lo.number = d->minValue;
                        hi.number = d->maxValue;
                    }
                    if (needSum[i])
                    {
                        if (lo.number * rg.size >= INT64_MIN && hi.number * rg.size <= INT64_MAX)
                            sum = SumAll(rg.columns[col.columnIdx], col.Type().get(), avx);
                        else
                            for (int r = 0; r < rg.size; ++r)
                                sum = Add(sum, Read(rg, col, r).number);
                    }
                }
                if (!s.count || Less(lo, s.minimum))
                    s.minimum = lo;
                if (!s.count || Less(s.maximum, hi))
                    s.maximum = hi;
                s.count += rg.size;
                s.sum = Add(s.sum, sum);
            }
        }
        return stats;
    };
    std::vector<Stats> stats(columns.size() + 1);
    if (threads == 1)
        stats = task(0);
    else
    {
        std::vector<std::future<std::vector<Stats>>> futures;
        for (int t = 0; t < threads; ++t)
            futures.push_back(std::async(std::launch::async, task, t));
        for (auto &f : futures)
        {
            auto local = f.get();
            for (size_t i = 0; i < stats.size(); ++i)
            {
                auto &a = stats[i], &b = local[i];
                if (b.count && (!a.count || Less(b.minimum, a.minimum)))
                    a.minimum = b.minimum;
                if (b.count && (!a.count || Less(a.maximum, b.maximum)))
                    a.maximum = b.maximum;
                a.count += b.count;
                a.sum = Add(a.sum, b.sum);
            }
        }
    }
    QueryOutput output;
    Row row;
    for (size_t i = 0; i < bindings.size(); ++i)
    {
        const auto &a = q.aggregateClauses[i];
        const auto &b = bindings[i];
        const auto &s = b.column < 0 ? stats.back() : stats[b.column];
        auto type = a.expression ? a.expression->type.get() : nullptr;
        output.fieldNames.push_back(a.ToString());
        if (a.type == AggregateClause::AGGREGATE_COUNT)
            row.push_back(std::to_string(s.count));
        else if (!s.count)
            row.push_back("NULL");
        else if (a.type == AggregateClause::AGGREGATE_SUM)
            row.push_back(Number(Add(s.sum, Multiply(b.offset, s.count)), Scale(type)));
        else if (a.type == AggregateClause::AGGREGATE_AVG)
        {
            std::ostringstream text;
            text << std::setprecision(17)
                 << (long double)s.sum / s.count / (long double)Power(Scale(type));
            row.push_back(text.str());
        }
        else
        {
            const auto &v = a.type == AggregateClause::AGGREGATE_MIN ? s.minimum : s.maximum;
            row.push_back(v.string ? std::string(v.text) : ToString(type, int64_t(v.number)));
        }
    }
    if ((!q.limit || *q.limit) && !q.offset)
        output.values.push_back(std::move(row));
    return Result<QueryOutput>(std::move(output));
}
} // namespace pgaccel
