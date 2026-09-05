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
  Cell value(const GroupKey &key, const ColumnRef &column) const {
    return Read(q.tables[0]->GetRowGroup(key.group), column, key.row);
  }
  GroupKey make(int group, int row) const {
    GroupKey key{0, uint32_t(group), uint32_t(row)};
    for (const auto &column : q.groupBy)
      key.hash ^= Hash{}(value(key, column)) + 0x9e3779b97f4a7c15ULL +
                  (key.hash << 6) + (key.hash >> 2);
    return key;
  }
  bool equal(const GroupKey &a, const GroupKey &b) const {
    for (const auto &column : q.groupBy)
      if (!(value(a, column) == value(b, column)))
        return false;
    return true;
  }
  bool less(const GroupKey &a, const GroupKey &b) const {
    for (const auto &column : q.groupBy) {
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
