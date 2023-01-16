#pragma once

#include <string>
#include <vector>
#include <optional>
#include <ostream>
#include "types.hpp"
#include "column_data.hpp"
#include "result_type.hpp"

namespace pgaccel {

struct ColumnDesc {
    std::string name;
    std::unique_ptr<AccelType> type;
    ColumnDataBase::Type layout;
};

struct RowGroup {
    std::vector<ColumnDataP> columns;
    int size;
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

    Result<bool> Save(const std::string &path);
    Result<bool> Save(std::ostream& metadataStream,
                      std::ostream& dataStream) const;

    static ColumnarTableP ImportParquet(
        const std::string &tableName,
        const std::string &path,
        std::optional<std::set<std::string>> fields = std::nullopt);

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
};

};
