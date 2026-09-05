#include "executor_group_key.hpp"
#include "executor_result.hpp"
namespace pgaccel {
namespace {
struct NumericState {
  uint64_t count = 0;
  Wide sum = 0;
};
using NumericStates = std::pmr::vector<NumericState>;
template <class Values>
Result<QueryOutput> Composite(const QueryDesc &q, bool avx, bool parallel) {
  Budget budget(q.memoryBudgetBytes);
  ScopedValueResource valuesMemory(budget);
  GroupKeys keys{q};
  using Map = std::pmr::unordered_map<GroupKey, Values, GroupHash, GroupEqual>;
  auto map = [&] { return Map(0, GroupHash{}, GroupEqual{&keys}, &budget); };
  QueryDesc aggregates = q;
  aggregates.aggregateClauses.clear();
  std::vector<int> indexes;
  for (const auto &a : q.aggregateClauses) {
    indexes.push_back(a.type == AggregateClause::AGGREGATE_PROJECT
                          ? -1
                          : int(aggregates.aggregateClauses.size()));
    if (indexes.back() >= 0)
      aggregates.aggregateClauses.push_back(a);
  }
  auto initial = [&]() -> Values {
    if constexpr (std::is_same_v<Values, uint64_t>)
      return 0;
    else
      return Values(aggregates.aggregateClauses.size(), &budget);
  };
  auto &table = *q.tables[0];
  int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
  auto launch = workers == 1 ? std::launch::deferred : std::launch::async;
  auto filter = q.filterClauses.empty()
                    ? FilterNodeP{}
                    : CreateFilterNode(q.filterClauses, avx);
  using Partitions = std::vector<Map>;
  auto scan = [&](int worker) {
    ScopedValueResource valuesMemory(budget);
    Partitions maps;
    for (int i = 0; i < workers; ++i)
      maps.push_back(map());
    for (int g = worker; g < table.RowGroupCount(); g += workers) {
      const auto &rg = table.GetRowGroup(g);
      uint8_t bitmap[RowGroupSize / 8];
      int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
      if (!count)
        continue;
      Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
        auto key = keys.make(g, row);
        auto &groups = maps[key.hash % workers];
        auto found = groups.find(key);
        if (found == groups.end())
          found = groups.emplace(key, initial()).first;
        auto &values = found->second;
        if constexpr (std::is_same_v<Values, uint64_t>)
          ++values;
        else
          for (size_t i = 0; i < values.size(); ++i) {
            const auto &a = aggregates.aggregateClauses[i];
            if constexpr (std::is_same_v<Values, NumericStates>) {
              ++values[i].count;
              if (a.type != AggregateClause::AGGREGATE_COUNT)
                values[i].sum =
                    Add(values[i].sum, Evaluate(*a.expression, rg, row).number);
            } else
              Update(values[i], a, rg, row, budget);
          }
      });
    }
    return maps;
  };
  std::vector<std::future<Partitions>> scans;
  for (int i = 0; i < workers; ++i)
    scans.push_back(std::async(launch, scan, i));
  std::vector<Partitions> partials;
  for (auto &scan : scans)
    partials.push_back(scan.get());
  auto finalize = [&](Map &groups) {
    QueryDesc local = q;
    local.offset = 0;
    if (q.limit)
      local.limit = *q.limit + q.offset;
    QueryResults results(local, budget);
    for (const auto &[key, values] : groups) {
      FinalRow row;
      for (size_t i = 0; i < q.aggregateClauses.size(); ++i) {
        const auto &a = q.aggregateClauses[i];
        FinalCell cell;
        cell.type = a.expression ? a.expression->type.get() : nullptr;
        if (indexes[i] < 0)
          cell.value =
              Evaluate(*a.expression, table.GetRowGroup(key.group), key.row);
        else {
          State state;
          if constexpr (std::is_same_v<Values, uint64_t>)
            state.count = values;
          else if constexpr (std::is_same_v<Values, NumericStates>) {
            state.count = values[indexes[i]].count;
            state.sum = values[indexes[i]].sum;
          } else
            state = values[indexes[i]];
          switch (a.type) {
          case AggregateClause::AGGREGATE_COUNT:
            cell.type = nullptr;
            cell.value.number = state.count;
            break;
          case AggregateClause::AGGREGATE_COUNT_DISTINCT:
            cell.type = nullptr;
            cell.value.number = state.distinct ? state.distinct->size() : 0;
            break;
          case AggregateClause::AGGREGATE_SUM:
            cell.value.number = state.sum;
            cell.null = !state.count;
            break;
          case AggregateClause::AGGREGATE_AVG:
            cell.average = true;
            cell.null = !state.count;
            if (state.count)
              cell.mean = (long double)state.sum / state.count /
                          (long double)Power(Scale(cell.type));
            break;
          default:
            cell.value = state.extreme;
            cell.null = !state.count;
            break;
          }
        }
        row.push_back(cell);
      }
      results.add(std::move(row));
    }
    return results.takeCandidates();
  };
  auto merge = [&](int owner) {
    ScopedValueResource valuesMemory(budget);
    auto groups = map();
    for (auto &worker : partials) {
      auto &source = worker[owner];
      for (auto it = source.begin(); it != source.end();) {
        auto found = groups.find(it->first);
        if (found == groups.end())
          groups.insert(source.extract(it++));
        else {
          auto &to = found->second, &from = it->second;
          if constexpr (std::is_same_v<Values, uint64_t>)
            to += from;
          else if constexpr (std::is_same_v<Values, NumericStates>) {
            for (size_t i = 0; i < to.size(); ++i) {
              to[i].count += from[i].count;
              to[i].sum = Add(to[i].sum, from[i].sum);
            }
          } else
            Merge(to, from, aggregates, budget);
          it = source.erase(it);
        }
      }
    }
    return finalize(groups);
  };
  std::vector<std::future<std::vector<FinalRow>>> merges;
  for (int i = 0; i < workers; ++i)
    merges.push_back(std::async(launch, merge, i));
  QueryResults results(q, budget);
  for (auto &merge : merges) {
    for (auto &row : merge.get())
      results.add(std::move(row));
  }
  return results.finish();
}
} // namespace
std::optional<Result<QueryOutput>> ExecuteComposite(const QueryDesc &q,
                                                    bool avx, bool parallel) {
  if (q.groupExpressions.empty() || !q.groupBy.empty())
    return {};
  bool count = true, numeric = true;
  for (const auto &a : q.aggregateClauses) {
    if (a.type == AggregateClause::AGGREGATE_PROJECT)
      continue;
    count &= a.type == AggregateClause::AGGREGATE_COUNT;
    numeric &= a.type == AggregateClause::AGGREGATE_COUNT ||
               a.type == AggregateClause::AGGREGATE_SUM ||
               a.type == AggregateClause::AGGREGATE_AVG;
  }
  if (count)
    return Composite<uint64_t>(q, avx, parallel);
  if (numeric)
    return Composite<NumericStates>(q, avx, parallel);
  return Composite<States>(q, avx, parallel);
}
} // namespace pgaccel
