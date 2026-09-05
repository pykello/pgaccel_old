#pragma once
#include "executor_state.hpp"
#include "executor_dictionary_expression.hpp"
#include "executor_packed_key.hpp"
namespace pgaccel {
namespace {
// A representative resident row avoids copying string payloads into each key.
// Equality compares typed logical values, never row-group dictionary codes.
struct GroupKey {
  uint64_t hash;
  uint32_t group, row;
};
struct GroupKeys {
  const QueryDesc &q;
  std::vector<std::shared_ptr<Expression>> identity;
  bool scalar = false;
  std::pmr::vector<DictionaryExpression> cached;
  std::optional<PackedGroupKey> packed;
  explicit GroupKeys(const QueryDesc &query, Budget *budget = nullptr)
      : q(query), cached(budget ? budget : std::pmr::get_default_resource()) {
    for (const auto &expression : q.groupExpressions) {
      bool redundant = false;
      if ((expression->kind == Expression::ADD || expression->kind == Expression::SUBTRACT) &&
          expression->left->kind == Expression::COLUMN &&
          expression->right->kind == Expression::CONSTANT &&
          expression->right->type->type_num() == INT64_TYPE &&
          (expression->left->type->type_num() == INT32_TYPE ||
           expression->left->type->type_num() == INT64_TYPE)) {
        // A stored signed integer plus/minus an Int64 constant always fits the
        // checked 128-bit expression domain. An explicit base key determines it.
        for (const auto &base : q.groupExpressions)
          redundant |= base->kind == Expression::COLUMN &&
                       *base->column == *expression->left->column;
      }
      if (!redundant)
        identity.push_back(expression);
    }
    scalar = identity.size() == 1 && identity[0]->kind == Expression::COLUMN &&
             identity[0]->type->type_num() != STRING_TYPE;
    if (budget) {
      packed.emplace(q, identity, *budget);
      if (!packed->valid() && !packed->enableConditional(q, identity, *budget)) packed.reset();
    }
    if (budget)
      for (const auto &expression : identity)
        if (auto source = DictionaryExpressionSource(*expression))
          cached.emplace_back(q, expression, *source, *budget);
  }
  void prepare(int group, const uint8_t *selection) {
    for (auto &expression : cached) expression.prepare(group, selection);
  }
  Cell value(const GroupKey &key,
             const std::shared_ptr<Expression> &expression) const {
    for (const auto &candidate : cached)
      if (candidate.expression.get() == expression.get())
        return candidate.value(key.group, key.row);
    return Evaluate(*expression, q.tables[0]->GetRowGroup(key.group), key.row);
  }
  GroupKey make(int group, int row) const {
    GroupKey key{0, uint32_t(group), uint32_t(row)};
    if (packed) {
      key.hash = packed->word(group, row);
      return key;
    }
    if (scalar) {
      // A stored numeric column fits 64 bits. Retain its exact identity in the
      // existing key word so collision checks do not reread resident rows.
      key.hash = uint64_t(value(key, identity[0]).number);
      return key;
    }
    for (const auto &column : identity)
      key.hash ^= Hash{}(value(key, column)) + 0x9e3779b97f4a7c15ULL +
                  (key.hash << 6) + (key.hash >> 2);
    return key;
  }
  bool equal(const GroupKey &a, const GroupKey &b) const {
    for (const auto &column : identity)
      if (!(value(a, column) == value(b, column)))
        return false;
    return true;
  }
  bool less(const GroupKey &a, const GroupKey &b) const {
    for (const auto &column : q.groupExpressions) {
      auto left = value(a, column), right = value(b, column);
      if (!(left == right))
        return Less(left, right);
    }
    return false;
  }
};
struct GroupHash {
  const GroupKeys *keys;
  size_t operator()(const GroupKey &key) const {
    return (keys->scalar || keys->packed) ? IntegerHash{}(key.hash) : key.hash;
  }
};
struct GroupEqual {
  const GroupKeys *keys;
  bool operator()(const GroupKey &a, const GroupKey &b) const {
    return a.hash == b.hash && (keys->scalar || keys->packed || keys->equal(a, b));
  }
};
} // namespace
} // namespace pgaccel
