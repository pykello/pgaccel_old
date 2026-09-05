#include "column_index.hpp"
#include "column_intern.hpp"
#include "executor_state.hpp"
#include <chrono>

namespace pgaccel {
struct GlobalDictionary::Impl {
    Budget memory;
    ColumnIntern<int64_t, IntegerHash> numbers;
    ColumnIntern<std::string_view, std::hash<std::string_view>> strings;
    std::pmr::vector<std::pmr::vector<uint32_t>> codes;
    std::pmr::vector<uint64_t> rowOffsets;
    std::pmr::vector<uint32_t> repeatBoundaries, repeatedRows;
    std::pmr::vector<AdditionalPosting> additionalRows;
    bool string = false, sparsePostings = false;
    explicit Impl(uint64_t limit) : memory(limit), numbers(memory), strings(memory), codes(&memory),
        rowOffsets(&memory), repeatBoundaries(&memory), repeatedRows(&memory), additionalRows(&memory) {
        memory.charge(sizeof(Impl) + sizeof(GlobalDictionary));
    }
};
GlobalDictionary::GlobalDictionary(uint64_t limit) : impl(std::make_unique<Impl>(limit)) {}
GlobalDictionary::~GlobalDictionary() = default;
std::span<const uint32_t> GlobalDictionary::Codes(int group) const { return impl->codes[group]; }
std::span<const int64_t> GlobalDictionary::Numbers() const { return impl->numbers.keys(); }
std::span<const std::string_view> GlobalDictionary::Strings() const { return impl->strings.keys(); }
uint32_t GlobalDictionary::Find(int64_t value) const { return impl->numbers.find(value); }
uint32_t GlobalDictionary::Find(std::string_view value) const { return impl->strings.find(value); }
size_t GlobalDictionary::size() const { return impl->string ? Strings().size() : Numbers().size(); }
uint64_t GlobalDictionary::bytes() const { return impl->memory.used.load(); }
uint64_t GlobalDictionary::peakBytes() const { return impl->memory.peak.load(); }

bool GlobalDictionary::HasSparsePostings() const { return impl->sparsePostings; }
uint64_t GlobalDictionary::RowOffset(int group) const { return impl->rowOffsets[group]; }
uint32_t GlobalDictionary::FirstRow(uint32_t id) const {
    if (!HasSparsePostings() || id >= size()) throw std::out_of_range("Invalid sparse posting identity");
    // IDs are assigned in first-occurrence row order. Each earlier repeat
    // shifts subsequent first-row positions by one; equal boundaries count.
    auto end = std::upper_bound(impl->repeatBoundaries.begin(), impl->repeatBoundaries.end(), id);
    return uint64_t(id) + (end - impl->repeatBoundaries.begin());
}
std::span<const AdditionalPosting> GlobalDictionary::AdditionalRows(uint32_t id) const {
    const auto &rows = impl->additionalRows;
    auto first = std::lower_bound(rows.begin(), rows.end(), id,
        [](const AdditionalPosting &row, uint32_t value) { return row.id < value; });
    auto end = std::upper_bound(first, rows.end(), id,
        [](uint32_t value, const AdditionalPosting &row) { return value < row.id; });
    return {first, end};
}
std::span<const uint32_t> GlobalDictionary::RepeatedRows() const { return impl->repeatedRows; }

Result<std::shared_ptr<const GlobalDictionary>> GlobalDictionary::Build(
    const ColumnarTable &table, int column, uint64_t limit) {
    try {
        if (column < 0 || column >= table.ColumnCount()) return Status::Invalid("Invalid dictionary column");
        auto result = std::shared_ptr<GlobalDictionary>(new GlobalDictionary(limit));
        auto &index = *result->impl;
        index.string = table.Schema()[column].type->type_num() == STRING_TYPE;
        index.codes.resize(table.RowGroupCount());
        uint64_t rows = 0;
        bool rawOnly = true;
        for (int g = 0; g < table.RowGroupCount(); ++g) {
            const auto &rg = table.GetRowGroup(g);
            auto data = rg.columns[column].get();
            index.rowOffsets.push_back(rows);
            rows += rg.size;
            auto &codes = index.codes[g];
            if (data->type == ColumnDataBase::DICT_COLUMN_DATA) {
                rawOnly = false;
                auto dictionary = static_cast<const DictColumnDataBase *>(data);
                codes.resize(dictionary->dictSize());
                // Native validation and import guarantee that every label is used.
                for (int code = 0; code < dictionary->dictSize(); ++code) {
                    if (index.string)
                        codes[code] = index.strings.intern(
                            static_cast<const DictColumnData<StringType> *>(data)->dict[code]);
                    else
                        codes[code] = index.numbers.intern(
                            static_cast<const DictColumnData<DateType> *>(data)->dict[code]);
                }
            } else {
                if (index.string) return Status::Invalid("Invalid raw string dictionary input");
                auto raw = static_cast<const RawColumnDataBase *>(data);
                codes.resize(rg.size);
                auto encode = [&]<class T>() {
                    auto values = reinterpret_cast<const T *>(raw->values);
                    for (int row = 0; row < rg.size; ++row) codes[row] = index.numbers.intern(values[row]);
                };
                switch (raw->bytesPerValue) {
                case 1: encode.template operator()<int8_t>(); break;
                case 2: encode.template operator()<int16_t>(); break;
                case 4: encode.template operator()<int32_t>(); break;
                case 8: encode.template operator()<int64_t>(); break;
                default: return Status::Invalid("Invalid dictionary input width");
                }
            }
        }
        index.rowOffsets.push_back(rows);
        if (rawOnly && rows && rows <= UINT32_MAX && rows - result->size() <= result->size() / 1024) {
            index.sparsePostings = true;
            uint32_t next = 0;
            for (int g = 0; g < table.RowGroupCount(); ++g)
                for (size_t row = 0; row < index.codes[g].size(); ++row) {
                    uint32_t id = index.codes[g][row];
                    if (id == next) ++next;
                    else {
                        index.repeatBoundaries.push_back(next);
                        index.additionalRows.push_back({id, uint32_t(index.rowOffsets[g] + row)});
                    }
                }
            std::sort(index.additionalRows.begin(), index.additionalRows.end(),
                [](const auto &a, const auto &b) { return a.id < b.id || (a.id == b.id && a.row < b.row); });
            uint32_t previous = UINT32_MAX;
            for (const auto &repeat : index.additionalRows) {
                if (repeat.id != previous) index.repeatedRows.push_back(result->FirstRow(repeat.id));
                index.repeatedRows.push_back(repeat.row);
                previous = repeat.id;
            }
            std::sort(index.repeatedRows.begin(), index.repeatedRows.end());
        }
        return std::shared_ptr<const GlobalDictionary>(std::move(result));
    } catch (const std::exception &error) {
        return Status::Invalid(std::string("Dictionary construction failed: ") + error.what());
    }
}

Result<DictionaryBuildStats> ColumnarTable::BuildDictionaries(uint64_t limit) {
    using Clock = std::chrono::steady_clock;
    DictionaryBuildStats stats;
    dictionaries_.resize(ColumnCount());
    for (const auto &index : dictionaries_) if (index) stats.bytes += index->bytes();
    stats.peakBytes = stats.bytes;
    if (stats.bytes > limit) return Status::Invalid("Dictionary memory limit below existing indexes");
    for (int column = 0; column < ColumnCount(); ++column) {
        if (dictionaries_[column]) continue;
        // Dictionary stripes benefit from shared logical IDs. For raw data,
        // avoid adding four-byte mappings to columns already stored in 1–2 bytes.
        bool candidate = false;
        for (const auto &rg : row_groups_) {
            auto data = rg.columns[column].get();
            candidate |= data->type == ColumnDataBase::DICT_COLUMN_DATA ||
                static_cast<const RawColumnDataBase *>(data)->bytesPerValue >= 4;
            if (candidate) break;
        }
        if (!candidate) continue;
        auto start = Clock::now();
        auto built = GlobalDictionary::Build(*this, column, limit - stats.bytes);
        if (!built.ok()) return built.status();
        auto index = std::move(built).ValueUnsafe();
        stats.peakBytes = std::max(stats.peakBytes, stats.bytes + index->peakBytes());
        stats.bytes += index->bytes();
        stats.columns.push_back({column, index->size(), index->bytes(), index->peakBytes(),
            std::chrono::duration<double, std::milli>(Clock::now() - start).count()});
        dictionaries_[column] = std::move(index);
    }
    return stats;
}
} // namespace pgaccel
