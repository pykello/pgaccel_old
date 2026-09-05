#include "executor_group_key.hpp"
#include "executor_result.hpp"
#include "executor_flat_map.hpp"
#include <array>
#include <concepts>
#include <map>
#include "column_index.hpp"
namespace pgaccel {
namespace {
struct NumericState {
  uint64_t count = 0;
  Wide sum = 0;
};
using NumericStates = std::pmr::vector<NumericState>;
template <class Values>
constexpr bool NumericValues = requires(Values values) {
  { values[0] } -> std::same_as<NumericState &>;
};
template <class Values>
Result<QueryOutput> Composite(const QueryDesc &q, bool avx, bool parallel) {
  Budget budget(q.memoryBudgetBytes);
  ScopedValueResource valuesMemory(budget);
  GroupKeys keys{q, &budget};
  using Map = FlatMap<GroupKey, Values, GroupHash, GroupEqual>;
  auto map = [&] { return Map(budget, GroupHash{&keys}, GroupEqual{&keys}); };
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
    if constexpr (std::is_same_v<Values, InlineIntegerSet>)
      return Values(budget);
    else if constexpr (std::is_same_v<Values, uint64_t>)
      return 0;
    else if constexpr (std::is_same_v<Values, NumericStates> ||
                       std::is_same_v<Values, States>)
      return Values(aggregates.aggregateClauses.size(), &budget);
    else
      return {};
  };
  auto update = [&](Values &values, const RowGroup &rg, int row) {
        if constexpr (std::is_same_v<Values, InlineIntegerSet>)
          values.insert(uint64_t(Evaluate(*aggregates.aggregateClauses[0].expression, rg, row).number));
        else if constexpr (std::is_same_v<Values, uint64_t>)
          ++values;
        else
          for (size_t i = 0; i < values.size(); ++i) {
            const auto &a = aggregates.aggregateClauses[i];
            if constexpr (NumericValues<Values>) {
              ++values[i].count;
              if (a.type != AggregateClause::AGGREGATE_COUNT)
                values[i].sum =
                    Add(values[i].sum, Evaluate(*a.expression, rg, row).number);
            } else
              Update(values[i], a, rg, row, budget);
          }
  };
  // With no aggregate ordering or HAVING, the first K keys are sufficient.
  // Require the visible key prefix to use the same ordering as the group key,
  // so owner and final candidate selection never choose a partially kept key.
  bool bounded = q.limit && *q.limit && q.orderBy.empty() && q.having.empty() &&
                 q.aggregateClauses.size() >= q.groupExpressions.size();
  for (size_t i = 0; i < q.groupExpressions.size() && bounded; ++i) {
    const auto &key = q.groupExpressions[i];
    const auto &output = q.aggregateClauses[i];
    bounded = key->kind == Expression::COLUMN &&
              output.type == AggregateClause::AGGREGATE_PROJECT && output.columnRef &&
              *output.columnRef == *key->column;
  }
  for (const auto &aggregate : q.aggregateClauses)
    if (aggregate.expression && aggregate.expression->kind != Expression::COLUMN &&
        aggregate.expression->kind != Expression::CONSTANT)
      bounded = false;
  const uint64_t capacity = q.limit ? *q.limit + q.offset : 0;
  const GlobalDictionary *singletons = nullptr;
  bool safeSingletons = !bounded;
  for (const auto &expression : q.groupExpressions)
    safeSingletons &= expression->kind == Expression::COLUMN;
  for (const auto &a : q.aggregateClauses)
    if (a.type != AggregateClause::AGGREGATE_PROJECT && a.type != AggregateClause::AGGREGATE_COUNT)
      safeSingletons &= a.expression && a.expression->kind == Expression::COLUMN;
  if (safeSingletons)
    for (const auto &expression : q.groupExpressions)
      if (auto index = q.tables[0]->Dictionary(expression->column->columnIdx);
          index && index->HasSparsePostings()) {
        singletons = index;
        break;
      }
  auto singletonCell = [&](const RowGroup &rg, int row, size_t output) {
    const auto &a = q.aggregateClauses[output];
    if (a.type == AggregateClause::AGGREGATE_COUNT || a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT)
      return FinalCell{Cell{1}};
    FinalCell cell{Evaluate(*a.expression, rg, row), a.expression->type.get()};
    if (a.type == AggregateClause::AGGREGATE_AVG) {
      cell.average = true;
      cell.mean = (long double)cell.value.number / (long double)Power(Scale(cell.type));
    }
    return cell;
  };
  auto &table = *q.tables[0];
  int workers = parallel ? std::min(8, std::max(1, table.RowGroupCount())) : 1;
  auto filter = q.filterClauses.empty()
                    ? FilterNodeP{}
                    : CreateFilterNode(q.filterClauses, avx);
    if (filter) filter->Prepare(table, budget, workers);
  using Partitions = std::vector<Map>;
  struct Partial { Partitions maps; std::vector<FinalRow> singletons; };
  auto scan = [&](int worker) {
    ScopedValueResource valuesMemory(budget);
    Partitions maps;
    QueryDesc singletonQuery = q;
    singletonQuery.offset = 0;
    if (q.limit) singletonQuery.limit = *q.limit + q.offset;
    QueryResults singletonResults(singletonQuery, budget);
    for (int i = 0; i < workers; ++i)
      maps.push_back(map());
    auto less = [&](const GroupKey &a, const GroupKey &b) { return keys.less(a, b); };
    std::pmr::map<GroupKey, Values, decltype(less)> firstKeys(less, &budget);
    for (int g = worker; g < table.RowGroupCount(); g += workers) {
      const auto &rg = table.GetRowGroup(g);
      uint8_t bitmap[RowGroupSize / 8];
      int count = filter ? filter->ExecuteSet(rg, bitmap) : rg.size;
      if (!count)
        continue;
      if (bounded && firstKeys.size() == capacity) {
        const auto &expression = q.groupExpressions[0];
        const auto &last = firstKeys.rbegin()->first;
        auto bound = Format(FinalCell{keys.value(last, expression), expression->type.get()});
        auto upper = FilterNodeImpl::CreateSimpleCompare(*expression->column, bound,
                         FilterClause::FILTER_LTE, "", FilterClause::INVALID, avx);
        count = count == rg.size ? upper->ExecuteSet(rg, bitmap) : upper->ExecuteAnd(rg, bitmap);
        if (!count)
          continue;
      }
      keys.prepare(g, count == rg.size ? nullptr : bitmap);
      const DictColumnDataBase *dictionary = nullptr;
      if (!bounded && q.groupBy.size() == 1) {
        auto data = rg.columns[q.groupBy[0].columnIdx].get();
        if (data->type == ColumnDataBase::DICT_COLUMN_DATA)
          dictionary = static_cast<const DictColumnDataBase *>(data);
      }
      Scratch cacheMemory(budget, dictionary ? uint64_t(dictionary->dictSize()) * sizeof(uint64_t) : 0);
      std::vector<uint64_t> cache(dictionary ? dictionary->dictSize() : 0, UINT64_MAX);
      auto repeated = singletons ? singletons->RepeatedRows() : std::span<const uint32_t>{};
      uint64_t rowBase = singletons ? singletons->RowOffset(g) : 0;
      auto exceptional = std::lower_bound(repeated.begin(), repeated.end(), rowBase);
      auto exceptionalEnd = std::lower_bound(exceptional, repeated.end(), rowBase + rg.size);
      Selected(rg, count == rg.size ? nullptr : bitmap, [&](int row) {
        if (singletons) {
          while (exceptional != exceptionalEnd && *exceptional < rowBase + row) ++exceptional;
          if (exceptional == exceptionalEnd || *exceptional != rowBase + row) {
            singletonResults.addLazy([&](size_t output) { return singletonCell(rg, row, output); });
            return;
          }
        }
        if (bounded) {
          GroupKey key{0, uint32_t(g), uint32_t(row)};
          if (firstKeys.size() == capacity && keys.less(firstKeys.rbegin()->first, key))
            return;
          auto found = firstKeys.find(key);
          if (found == firstKeys.end()) {
            if (firstKeys.size() == capacity)
              firstKeys.erase(std::prev(firstKeys.end()));
            found = firstKeys.emplace(key, initial()).first;
          }
          update(found->second, rg, row);
          return;
        }
        int code = dictionary ? Code(dictionary, row) : 0;
        uint64_t location = dictionary ? cache[code] : UINT64_MAX;
        if (location == UINT64_MAX) {
          auto key = keys.make(g, row);
          uint32_t owner = (GroupHash{&keys}(key) >> 32) % workers;
          auto [index, added] = maps[owner].get(key, initial);
          location = (uint64_t(owner) << 32) | index;
          if (dictionary) cache[code] = location;
        }
        auto &values = maps[location >> 32].at(uint32_t(location)).second;
        update(values, rg, row);
      });
    }
    for (auto &[representative, values] : firstKeys) {
      auto key = keys.make(representative.group, representative.row);
      maps[(GroupHash{&keys}(key) >> 32) % workers].get(key, [&] { return std::move(values); });
    }
    return Partial{std::move(maps), singletonResults.takeCandidates()};
  };
  auto partials=RunTasks(workers,scan);
  auto finalize = [&](Map &groups) {
    QueryDesc local = q;
    local.offset = 0;
    if (q.limit)
      local.limit = *q.limit + q.offset;
    QueryResults results(local, budget);
    for (uint32_t index = 0; index < groups.size(); ++index) {
      const auto &[key, values] = groups.at(index);
      auto get = [&](size_t i) {
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
          else if constexpr (NumericValues<Values>) {
            state.count = values[indexes[i]].count;
            state.sum = values[indexes[i]].sum;
          } else if constexpr (!std::is_same_v<Values, InlineIntegerSet>)
            state = values[indexes[i]];
          switch (a.type) {
          case AggregateClause::AGGREGATE_COUNT:
            cell.type = nullptr;
            cell.value.number = state.count;
            break;
          case AggregateClause::AGGREGATE_COUNT_DISTINCT:
            cell.type = nullptr;
            if constexpr (std::is_same_v<Values, InlineIntegerSet>)
              cell.value.number = values.size();
            else
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
        return cell;
      };
      results.addLazy(get);
    }
    return results.takeCandidates();
  };
  auto merge = [&](int owner) {
    ScopedValueResource valuesMemory(budget);
    auto groups = map();
    for (auto &worker : partials) {
      auto &source = worker.maps[owner];
      source.forEach([&](const GroupKey &key, Values &from) {
        auto [index, added] = groups.get(key, [&] { return std::move(from); });
        if (!added) {
          auto &to = groups.at(index).second;
          if constexpr (std::is_same_v<Values, InlineIntegerSet>)
            from.forEach([&](uint64_t value) { to.insert(value); });
          else if constexpr (std::is_same_v<Values, uint64_t>)
            to += from;
          else if constexpr (NumericValues<Values>) {
            for (size_t i = 0; i < to.size(); ++i) {
              to[i].count += from[i].count;
              to[i].sum = Add(to[i].sum, from[i].sum);
            }
          } else
            Merge(to, from, aggregates, budget);
        }
      });
      source.release();
    }
    return finalize(groups);
  };
  auto merges=RunTasks(workers,merge);
  QueryResults results(q, budget);
  for (auto &worker : partials)
    for (auto &row : worker.singletons) results.add(std::move(row));
  for (auto &merge : merges) {
    for (auto &row : merge)
      results.add(std::move(row));
  }
  return results.finish();
}
} // namespace
std::optional<Result<QueryOutput>> ExecuteComposite(const QueryDesc &q,
                                                    bool avx, bool parallel) {
  if (q.groupExpressions.empty())
    return {};
  const AggregateClause *onlyAggregate = nullptr;
  size_t aggregateCount = 0;
  for (const auto &a : q.aggregateClauses)
    if (a.type != AggregateClause::AGGREGATE_PROJECT) {
      onlyAggregate = &a;
      ++aggregateCount;
    }
  bool inlineDistinct = q.groupBy.size() == 1;
  if (inlineDistinct && q.groupBy[0].Type()->type_num() == STRING_TYPE) {
    // Small dictionaries already amortize general state over many rows. The
    // larger inline payload only pays off when per-group allocation dominates.
    size_t maximumLabels = 0;
    for (int g = 0; g < q.tables[0]->RowGroupCount(); ++g) {
      auto data = q.tables[0]->GetRowGroup(g).columns[q.groupBy[0].columnIdx].get();
      if (data->type == ColumnDataBase::DICT_COLUMN_DATA)
        maximumLabels = std::max(maximumLabels,
            size_t(static_cast<const DictColumnDataBase *>(data)->dictSize()));
    }
    inlineDistinct = maximumLabels > 4096;
  }
  if (inlineDistinct && aggregateCount == 1 &&
      onlyAggregate->type == AggregateClause::AGGREGATE_COUNT_DISTINCT &&
      onlyAggregate->expression && onlyAggregate->expression->kind == Expression::COLUMN &&
      onlyAggregate->expression->type->type_num() != STRING_TYPE)
    return Composite<InlineIntegerSet>(q, avx, parallel);
  if (!q.groupBy.empty())
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
  if (numeric) {
    size_t aggregates = 0;
    for (const auto &a : q.aggregateClauses)
      aggregates += a.type != AggregateClause::AGGREGATE_PROJECT;
    switch (aggregates) {
    case 1: return Composite<std::array<NumericState, 1>>(q, avx, parallel);
    case 2: return Composite<std::array<NumericState, 2>>(q, avx, parallel);
    case 3: return Composite<std::array<NumericState, 3>>(q, avx, parallel);
    case 4: return Composite<std::array<NumericState, 4>>(q, avx, parallel);
    }
    return Composite<NumericStates>(q, avx, parallel);
  }
  return Composite<States>(q, avx, parallel);
}
} // namespace pgaccel
