#pragma once
#include "columnar_table.h"
#include <span>
#include <string_view>

namespace pgaccel {
// Immutable, lossless alternate column representation. Codes refer to local
// dictionary labels for dictionary stripes and to rows for raw stripes.
class GlobalDictionary {
    struct Impl;
    std::unique_ptr<Impl> impl;
    explicit GlobalDictionary(uint64_t memoryLimit);
  public:
    ~GlobalDictionary();
    GlobalDictionary(const GlobalDictionary &) = delete;
    static Result<std::shared_ptr<const GlobalDictionary>> Build(
        const ColumnarTable &table, int column, uint64_t memoryLimit);
    std::span<const uint32_t> Codes(int group) const;
    std::span<const int64_t> Numbers() const;
    std::span<const std::string_view> Strings() const;
    uint32_t Find(int64_t value) const;
    uint32_t Find(std::string_view value) const;
    size_t size() const;
    uint64_t bytes() const;
    uint64_t peakBytes() const;
};
} // namespace pgaccel
