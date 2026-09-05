#include "executor_value.hpp"
#include <re2/re2.h>
namespace pgaccel {
namespace {
class ValuePredicate : public FilterNodeImpl {
  FilterClause f;
  std::unique_ptr<RE2> pattern;
  std::string needle;
  bool substring = false, negate = false;
  std::unordered_set<Cell, Hash> members;
  bool matches(Cell value) const {
    bool yes =
        pattern
            ? (substring ? value.text.find(needle) != std::string_view::npos
                         : RE2::FullMatch(re2::StringPiece(value.text.data(),
                                                           value.text.size()),
                                          *pattern))
            : members.contains(value);
    return negate ? !yes : yes;
  }
  int execute(const RowGroup &rg, uint8_t *bitmap, bool intersect) const {
    auto data = rg.columns[f.columnRef.columnIdx].get();
    const DictColumnDataBase *dict =
        data->type == ColumnDataBase::DICT_COLUMN_DATA
            ? static_cast<const DictColumnDataBase *>(data)
            : nullptr;
    std::vector<uint8_t> labels;
    if (dict) {
      labels.resize(dict->dictSize());
      for (int i = 0; i < dict->dictSize(); ++i)
        labels[i] = matches(Label(dict, i, f.columnRef.Type()->type_num()));
    }
    if (!intersect)
      memset(bitmap, 0, (rg.size + 7) / 8);
    int count = 0;
    for (int row = 0; row < rg.size; ++row) {
      bool selected = !intersect || (bitmap[row / 8] & (1u << (row % 8)));
      bool yes = selected && (dict ? labels[Code(dict, row)]
                                   : matches(Read(rg, f.columnRef, row)));
      if (yes) {
        bitmap[row / 8] |= 1u << (row % 8);
        ++count;
      } else
        bitmap[row / 8] &= ~(1u << (row % 8));
    }
    return count;
  }

public:
  explicit ValuePredicate(const FilterClause &clause) : f(clause) {
    negate = f.op == FilterClause::FILTER_NOT_LIKE ||
             f.op == FilterClause::FILTER_NOT_IN;
    if (f.op == FilterClause::FILTER_LIKE ||
        f.op == FilterClause::FILTER_NOT_LIKE) {
      std::string regex;
      for (size_t i = 0; i < f.value.size(); ++i) {
        char c = f.value[i];
        if (c == '%')
          regex += ".*";
        else if (c == '_')
          regex += '.';
        else {
          if (c == '\\' && i + 1 < f.value.size() &&
              (f.value[i + 1] == '%' || f.value[i + 1] == '_' ||
               f.value[i + 1] == '\\'))
            c = f.value[++i];
          regex += RE2::QuoteMeta(std::string(1, c));
        }
      }
      RE2::Options options;
      options.set_dot_nl(true);
      options.set_log_errors(false);
      pattern = std::make_unique<RE2>(regex, options);
      if (!pattern->ok())
        throw std::invalid_argument("Invalid UTF-8 LIKE pattern");
      if (f.value.size() >= 2 && f.value.front() == '%' &&
          f.value.back() == '%') {
        needle = f.value.substr(1, f.value.size() - 2);
        substring = needle.find_first_of("%_\\") == std::string::npos;
      }
    } else {
      for (const auto &literal : f.values) {
        Cell value;
        auto type = f.columnRef.Type()->type_num();
        if (type == STRING_TYPE)
          value = {0, literal, true};
        else if (type == DATE_TYPE)
          value.number = ParseDate(literal);
        else if (type == TIMESTAMP_TYPE)
          value.number = ParseTimestamp(literal);
        else
          value.number = ParseDecimal(Scale(f.columnRef.Type().get()), literal);
        members.insert(value);
      }
    }
  }
  int ExecuteCount(const RowGroup &rg) const override {
    uint8_t bitmap[RowGroupSize / 8];
    return execute(rg, bitmap, false);
  }
  int ExecuteSet(const RowGroup &rg, uint8_t *bitmap) const override {
    return execute(rg, bitmap, false);
  }
  int ExecuteAnd(const RowGroup &rg, uint8_t *bitmap) const override {
    return execute(rg, bitmap, true);
  }
};
} // namespace
FilterNodeP CreateValuePredicate(const FilterClause &f) {
  return std::make_unique<ValuePredicate>(f);
}
} // namespace pgaccel
