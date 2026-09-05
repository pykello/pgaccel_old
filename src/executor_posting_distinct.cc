#include "executor_result.hpp"
#include "executor_group_key.hpp"
#include "column_index.hpp"
#include <array>

namespace pgaccel {
std::optional<Result<QueryOutput>> ExecutePostingDistinct(const QueryDesc &q, bool avx, bool parallel)
{
    if (!q.grouped || q.groupExpressions.empty()) return {};
    const AggregateClause *distinct = nullptr;
    std::vector<const AggregateClause *> numeric;
    for (const auto &a : q.aggregateClauses) {
        if (a.type == AggregateClause::AGGREGATE_PROJECT) continue;
        if (a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT) {
            if (distinct || !a.expression || a.expression->kind != Expression::COLUMN) return {};
            distinct = &a;
        } else if ((a.type == AggregateClause::AGGREGATE_COUNT &&
                    (!a.expression || a.expression->kind == Expression::COLUMN)) ||
                   ((a.type == AggregateClause::AGGREGATE_SUM || a.type == AggregateClause::AGGREGATE_AVG) &&
                    a.expression && a.expression->kind == Expression::COLUMN &&
                    a.expression->type->type_num() != STRING_TYPE)) numeric.push_back(&a);
        else return {};
    }
    if (!distinct) return {};
    auto index = q.tables[0]->Dictionary(distinct->expression->column->columnIdx);
    if (!index || !index->HasDensePostings()) return {};
    Budget budget(q.memoryBudgetBytes);
    GroupKeys keys(q, &budget);
    // Dense counters are appropriate only for a bounded complete key domain.
    if (!keys.packed || keys.packed->bits() > 24) return {};
    if (!numeric.empty() && keys.packed->bits() > 16) return {};
    const auto &table = *q.tables[0];
    size_t domain = size_t(1) << keys.packed->bits();
    int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
    uint64_t rows = index->RowOffset(table.RowGroupCount());
    std::pmr::vector<uint32_t> groups(rows, 0, &budget);
    std::pmr::vector<uint64_t> starts(&budget);
    for (int g = 0; g <= table.RowGroupCount(); ++g) starts.push_back(index->RowOffset(g));
    auto filter = q.filterClauses.empty() ? FilterNodeP{} : CreateFilterNode(q.filterClauses, avx);
    struct Input {
        std::pmr::vector<uint64_t> candidates, counts;
        std::pmr::vector<Wide> sums;
        explicit Input(Budget &memory) : candidates(&memory), counts(&memory), sums(&memory) {}
    };
    auto inputs = RunTasks(workers, [&](int worker) {
        Input input(budget);
        if (filter) input.candidates.resize((index->size() + 63) / 64, 0);
        if (!numeric.empty()) {
            input.counts.resize(domain, 0);
            input.sums.resize(domain * numeric.size(), 0);
        }
        for (int g = worker; g < table.RowGroupCount(); g += workers) {
            const auto &rg = table.GetRowGroup(g);
            uint8_t bitmap[RowGroupSize / 8];
            int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
            if (!count) continue;
            keys.prepare(g, count == rg.size ? nullptr : bitmap);
            Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
                uint32_t group = uint32_t(keys.make(g, row).hash);
                groups[starts[g] + row] = group + 1;
                if (!numeric.empty()) {
                    ++input.counts[group];
                    for (size_t i = 0; i < numeric.size(); ++i)
                        if (numeric[i]->type != AggregateClause::AGGREGATE_COUNT) {
                            Wide &sum = input.sums[i * domain + group];
                            sum = Add(sum, Read(rg, *numeric[i]->expression->column, row).number);
                        }
                }
                if (filter) {
                    uint32_t id = index->Codes(g)[row];
                    input.candidates[id / 64] |= uint64_t(1) << (id % 64);
                }
            });
        }
        return input;
    });
    struct Counter { uint32_t count = 0, row = 0; };
    Scratch metadata(budget, (workers * 2 + 1) * sizeof(std::pmr::vector<Counter>));
    auto partials = RunTasks(workers, [&](int worker) {
        std::pmr::vector<Counter> counts(domain, Counter{}, &budget);
        size_t begin = index->size() * worker / workers, end = index->size() * (worker + 1) / workers;
        auto visit = [&](size_t id) {
            std::array<uint32_t, 8> small;
            size_t used = 0;
            IntegerSet overflow(budget);
            for (uint32_t row : index->PostingRows(id)) {
                uint32_t group = groups[row];
                if (!group) continue;
                size_t slot = 0;
                while (slot < used && small[slot] != group) ++slot;
                if (slot < used) continue;
                if (used < small.size()) small[used++] = group;
                else if (!overflow.insert(group)) continue;
                auto &counter = counts[group - 1];
                ++counter.count;
                counter.row = row;
            }
        };
        if (filter) {
            size_t words = (index->size() + 63) / 64;
            for (size_t word = words * worker / workers; word < words * (worker + 1) / workers; ++word) {
                uint64_t candidates = 0;
                for (const auto &input : inputs) candidates |= input.candidates[word];
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
                if (a.type != AggregateClause::AGGREGATE_PROJECT) {
                    size_t slot = std::find(numeric.begin(), numeric.end(), &a) - numeric.begin();
                    uint64_t rows = 0; Wide sum = 0;
                    for (const auto &input : inputs) {
                        rows += input.counts[group];
                        sum = Add(sum, input.sums[slot * domain + group]);
                    }
                    if (a.type == AggregateClause::AGGREGATE_COUNT) return FinalCell{Cell{Wide(rows)}};
                    FinalCell cell{Cell{sum}, a.expression->type.get()};
                    if (a.type == AggregateClause::AGGREGATE_AVG) {
                        cell.average = true;
                        cell.mean = (long double)sum / rows / (long double)Power(Scale(cell.type));
                    }
                    return cell;
                }
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
