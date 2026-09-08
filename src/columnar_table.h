#pragma once

#include <string>
#include <vector>
#include <optional>
#include <ostream>
#include <functional>
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

struct DirectScanStats {
    uint64_t rows = 0, groups = 0, bytesRead = 0, readCalls = 0;
    uint64_t ioBufferBytes = 0;
};
using ScanConsumer = std::function<Result<bool>(const std::vector<ColumnDesc> &, const RowGroup &)>;

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

    // Synchronous projected scan. Each callback borrows one owned stripe;
    // retaining its column shared_ptrs deliberately extends their lifetime.
    // A false callback value stops early; an error aborts the scan.
    static Result<DirectScanStats> ScanDirect(const std::string &path,
        const std::set<std::string> &fields, const ScanConsumer &consumer);

    Result<bool> Save(const std::string &path, bool aligned = false);
    Result<bool> Save(std::ostream& metadataStream,
                      std::ostream& dataStream, bool aligned = false) const;

    // Execution-owned numeric intermediates; an empty input binds a schema.
    static ColumnarTableP NumericIntermediate(
        const std::string &name, const std::vector<ColumnDesc> &schema,
        const std::vector<std::vector<std::string>> &rows);

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
    static Result<ColumnarTableP> LoadImpl(const std::string &tableName,
        std::istream &metadata, std::istream &data,
        std::optional<std::set<std::string>> fields,
        const std::string *directPath = nullptr,
        const ScanConsumer &consumer = {}, DirectScanStats *stats = nullptr);

    std::vector<ColumnDesc> schema_;
    std::vector<RowGroup> row_groups_;
    std::string name_;
    std::vector<std::shared_ptr<const GlobalDictionary>> dictionaries_;
};

};
