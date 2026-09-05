#include "executor_result.hpp"
#include "executor_group_key.hpp"
#include "column_index.hpp"
#include <array>

namespace pgaccel {
std::optional<Result<QueryOutput>> ExecutePostingDistinct(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.grouped || q.groupExpressions.empty()) return {};
    const AggregateClause *distinct = nullptr;
    for (const auto &a : q.aggregateClauses) {
        if (a.type == AggregateClause::AGGREGATE_PROJECT) continue;
        if (distinct || a.type != AggregateClause::AGGREGATE_COUNT_DISTINCT ||
            !a.expression || a.expression->kind != Expression::COLUMN) return {};
        distinct = &a;
    }
    if (!distinct) return {};
    auto index = q.tables[0]->Dictionary(distinct->expression->column->columnIdx);
    if (!index || !index->HasDensePostings()) return {};
    Budget budget(q.memoryBudgetBytes);
    GroupKeys keys(q, &budget);
    // Dense counters are appropriate only for a bounded complete key domain.
    if (!keys.packed || keys.packed->bits() > 24) return {};
    const auto &table = *q.tables[0];
    size_t domain = size_t(1) << keys.packed->bits();
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    uint64_t rows = index->RowOffset(table.RowGroupCount());
    std::pmr::vector<uint32_t> groups(rows, 0, &budget);
    std::pmr::vector<uint64_t> starts(&budget);
    for (int g = 0; g <= table.RowGroupCount(); ++g) starts.push_back(index->RowOffset(g));
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    auto candidateValues = RunTasks(workers, [&](int worker) {
        std::pmr::vector<uint64_t> candidates(filter ? (index->size() + 63) / 64 : 0, 0, &budget);
        for (int g = worker; g < table.RowGroupCount(); g += workers) {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count) continue;
            keys.prepare(g, count == rg.size ? nullptr : bitmap);
            Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
                groups[starts[g] + row] = uint32_t(keys.make(g, row).hash) + 1;
                if (filter) {
                    uint32_t id = index->Codes(g)[row];
                    candidates[id / 64] |= uint64_t(1) << (id % 64);
                }
            });
        }
        return candidates;
    });
    struct Counter { uint32_t count = 0, row = 0; };
    Scratch metadata(budget, (workers * 2 + 1) * sizeof(std::pmr::vector<Counter>));
    auto partials = RunTasks(workers, [&](int worker) {
        std::pmr::vector<Counter> counts(domain, Counter{}, &budget);
        std::pmr::vector<uint64_t> overflow(&budget);
        size_t begin = index->size() * worker / workers, end = index->size() * (worker + 1) / workers;
        auto visit = [&](size_t id) {
            auto postings = index->PostingRows(id);
            std::array<uint64_t, 16> small;
            uint64_t *values;
            size_t n = 0;
            if (postings.size() <= small.size()) values = small.data();
            else { overflow.resize(postings.size()); values = overflow.data(); }
            for (uint32_t row : postings) {
                uint32_t group = groups[row];
                if (group) values[n++] = (uint64_t(group) << 32) | row;
            }
            std::sort(values, values + n);
            uint32_t previous = 0;
            for (size_t i = 0; i < n; ++i) {
                uint32_t group = values[i] >> 32;
                if (group != previous) {
                    auto &counter = counts[group - 1];
                    ++counter.count;
                    counter.row = uint32_t(values[i]);
                    previous = group;
                }
            }
        };
        if (filter) {
            size_t words = (index->size() + 63) / 64;
            for (size_t word = words * worker / workers; word < words * (worker + 1) / workers; ++word) {
                uint64_t candidates = 0;
                for (const auto &local : candidateValues) candidates |= local[word];
                while (candidates) {
                    visit(word * 64 + std::countr_zero(candidates));
                    candidates &= candidates - 1;
                }
            }
        } else for (size_t id = begin; id < end; ++id) visit(id);
        return counts;
    });
    auto candidates = RunTasks(workers, [&](int worker) {
        QueryDesc local = q; local.offset = 0;
        if (q.limit) local.limit = *q.limit + q.offset;
        QueryResults result(local, budget);
        size_t begin = domain * worker / workers, end = domain * (worker + 1) / workers;
        for (size_t group = begin; group < end; ++group) {
            uint64_t count = 0; uint32_t position = 0;
            for (const auto &partial : partials) {
                count += partial[group].count;
                if (partial[group].count) position = partial[group].row;
            }
            if (!count) continue;
            result.addLazy([&](size_t output) {
                const auto &a = q.aggregateClauses[output];
                if (a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT) return FinalCell{Cell{Wide(count)}};
                int g = std::upper_bound(starts.begin(), starts.end(), position) - starts.begin() - 1;
                return FinalCell{Evaluate(*a.expression, table.GetRowGroup(g), position - starts[g]), a.expression->type.get()};
            });
        }
        return result.takeCandidates();
    });
    QueryResults result(q, budget);
    for (auto &worker : candidates) for (auto &row : worker) result.add(std::move(row));
    return result.finish();
}
} // namespace pgaccel
