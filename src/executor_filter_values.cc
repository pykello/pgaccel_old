#include "executor_value.hpp"
#include <re2/re2.h>
#include <array>
#include <immintrin.h>
#include "column_index.hpp"
namespace pgaccel {
namespace {
class ValuePredicate : public FilterNodeImpl {
  FilterClause f;
  std::unique_ptr<RE2> pattern;
  std::string needle;
  bool substring = false, negate = false, avx;
  StringSignature required;
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
    if (dict) {
      // At most 65,536 local labels: the complete membership map fits in 8 KiB.
      std::array<uint32_t, RowGroupSize / 32> labels{};
      auto signatures = substring && needle.size() >= 3
          ? data->localStringSignatures : std::span<const StringSignature>{};
      for (int i = 0; i < dict->dictSize(); ++i) {
        bool yes = !signatures.empty() && !signatures[i].mayContain(required)
            ? negate : matches(Label(dict, i, f.columnRef.Type()->type_num()));
        if (yes) labels[i / 32] |= uint32_t(1) << (i % 32);
      }
      auto apply = [&]<class CodeType>() {
        auto codes = reinterpret_cast<const CodeType *>(dict->values);
        int count = 0, base = 0;
        if (avx) {
          for (; base + 16 <= rg.size; base += 16) {
            uint16_t previous = UINT16_MAX;
            if (intersect) {
              memcpy(&previous, bitmap + base / 8, sizeof(previous));
              if (!previous) continue;
            }
            __m512i ids;
            if constexpr (sizeof(CodeType) == 1)
              ids = _mm512_cvtepu8_epi32(_mm_loadu_si128(reinterpret_cast<const __m128i *>(codes + base)));
            else
              ids = _mm512_cvtepu16_epi32(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(codes + base)));
            auto words = _mm512_i32gather_epi32(_mm512_srli_epi32(ids, 5), labels.data(), 4);
            auto shifts = _mm512_and_si512(ids, _mm512_set1_epi32(31));
            uint16_t selected = _mm512_test_epi32_mask(_mm512_srlv_epi32(words, shifts), _mm512_set1_epi32(1));
            selected &= previous;
            memcpy(bitmap + base / 8, &selected, sizeof(selected));
            count += __builtin_popcount(selected);
          }
        }
        for (; base < rg.size; base += 64) {
          int n = std::min(64, rg.size - base);
          uint64_t selected = n == 64 ? UINT64_MAX : (uint64_t(1) << n) - 1;
          if (intersect) {
            uint64_t previous = 0;
            memcpy(&previous, bitmap + base / 8, (n + 7) / 8);
            selected &= previous;
          }
          uint64_t result = 0;
          while (selected) {
            int bit = __builtin_ctzll(selected);
            uint32_t code = codes[base + bit];
            result |= uint64_t((labels[code / 32] >> (code % 32)) & 1) << bit;
            selected &= selected - 1;
          }
          memcpy(bitmap + base / 8, &result, (n + 7) / 8);
          count += __builtin_popcountll(result);
        }
        return count;
      };
      return dict->bytesPerValue() == 1 ? apply.template operator()<uint8_t>()
                                       : apply.template operator()<uint16_t>();
    }
    if (!intersect)
      memset(bitmap, 0, (rg.size + 7) / 8);
    int count = 0;
    for (int row = 0; row < rg.size; ++row) {
      bool selected = !intersect || (bitmap[row / 8] & (1u << (row % 8)));
      bool yes = selected && matches(Read(rg, f.columnRef, row));
      if (yes) {
        bitmap[row / 8] |= 1u << (row % 8);
        ++count;
      } else
        bitmap[row / 8] &= ~(1u << (row % 8));
    }
    return count;
  }

public:
  explicit ValuePredicate(const FilterClause &clause, bool useAvx) : f(clause), avx(useAvx) {
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
        if (substring) required = StringSignature::Build(needle);
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
FilterNodeP CreateValuePredicate(const FilterClause &f, bool avx) {
  return std::make_unique<ValuePredicate>(f, avx);
}
} // namespace pgaccel
