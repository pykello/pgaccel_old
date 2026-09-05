#pragma once

#include <string>
#include <vector>
#include <optional>
#include <ostream>
#include "types.hpp"
#include "column_data.hpp"
#include "result_type.hpp"

namespace pgaccel {

const int BITMAP_SIZE = 1 << 13;

struct ColumnDesc {
    std::string name;
    std::shared_ptr<AccelType> type;
    ColumnDataBase::Type layout;
};

struct RowGroup {
    std::vector<ColumnDataP> columns;
    int size = 0;
    std::unique_ptr<std::array<uint8_t, BITMAP_SIZE>> selectionBitmap;
    int selectedSize = 0;
};

class GlobalDictionary;
struct DictionaryColumnStats {
    int column;
    uint64_t values, bytes, peakBytes;
    double buildMs;
};
struct DictionaryBuildStats {
    uint64_t bytes = 0, peakBytes = 0;
    std::vector<DictionaryColumnStats> columns;
};
class ColumnarTable;
typedef std::unique_ptr<ColumnarTable> ColumnarTableP;

class ColumnarTable {
public:
    const std::vector<ColumnDesc> & Schema() const
    {
        return schema_;
    }

    const std::string Name() const
    {
        return name_;
    }

    std::optional<int> ColumnIndex(const std::string& name) const;

    const RowGroup &GetRowGroup(int idx) const {
        return row_groups_[idx];
    }

    int RowGroupCount() const {
        return row_groups_.size();
    }

    int ColumnCount() const {
        return schema_.size();
    }

    const GlobalDictionary *Dictionary(int column) const {
        return dictionaries_.empty() ? nullptr : dictionaries_[column].get();
    }
    // Explicit construction before concurrent readers; no SQL is consulted.
    Result<DictionaryBuildStats> BuildDictionaries(uint64_t memoryLimit);

    Result<bool> Save(const std::string &path);
    Result<bool> Save(std::ostream& metadataStream,
                      std::ostream& dataStream) const;

    static ColumnarTableP ImportParquet(
        const std::string &tableName,
        const std::string &path,
        std::optional<std::set<std::string>> fields = std::nullopt,
        const std::map<std::string,std::string> &declaredTypes = {});

    static Result<ColumnarTableP> Load(
        const std::string &tableName,
        const std::string &path,
        std::optional<std::set<std::string>> fields = std::nullopt);

    static Result<ColumnarTableP> Load(
        const std::string &tableName,
        std::istream& metadataStream,
        std::istream& dataStream,
        std::optional<std::set<std::string>> fields = std::nullopt);


private:
    ColumnarTable() {}

    std::vector<ColumnDesc> schema_;
    std::vector<RowGroup> row_groups_;
    std::string name_;
    std::vector<std::shared_ptr<const GlobalDictionary>> dictionaries_;
};

};
