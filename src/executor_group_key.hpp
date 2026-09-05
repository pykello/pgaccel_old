#pragma once
#include "executor_state.hpp"
namespace pgaccel {
namespace {
// A representative resident row avoids copying string payloads into each key.
// Equality compares typed logical values, never row-group dictionary codes.
struct GroupKey {
  uint64_t hash;
  uint32_t group, row;
};
struct GroupHash {
  size_t operator()(const GroupKey &key) const { return key.hash; }
};
struct GroupKeys {
  const QueryDesc &q;
  std::vector<std::shared_ptr<Expression>> identity;
  explicit GroupKeys(const QueryDesc &query) : q(query) {
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
  }
  Cell value(const GroupKey &key,
             const std::shared_ptr<Expression> &expression) const {
    return Evaluate(*expression, q.tables[0]->GetRowGroup(key.group), key.row);
  }
  GroupKey make(int group, int row) const {
    GroupKey key{0, uint32_t(group), uint32_t(row)};
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
struct GroupEqual {
  const GroupKeys *keys;
  bool operator()(const GroupKey &a, const GroupKey &b) const {
    return a.hash == b.hash && keys->equal(a, b);
  }
};
} // namespace
} // namespace pgaccel
