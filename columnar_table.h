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
};

class ColumnarTable;
typedef std::unique_ptr<ColumnarTable> ColumnarTableP;

class ColumnarTable {
public:
    const std::vector<ColumnDesc> & Schema() const
    {
        return schema_;
    }

    const std::vector<ColumnDataP>& ColumnData(int idx) const
    {
        return column_data_vecs_[idx];
    }

    const std::string Name() const
    {
        return name_;
    }

    std::optional<int> ColumnIndex(const std::string& name) const;

    Result<bool> Save(const std::string &path);
    Result<bool> Save(std::ostream& metadataStream,
                      std::ostream& dataStream) const;

    static ColumnarTableP ImportParquet(
        const std::string &tableName,
        const std::string &path,
        std::optional<std::set<std::string>> fields = {});

    static Result<ColumnarTableP> Load(
        const std::string &tableName,
        const std::string &path,
        std::optional<std::set<std::string>> fields = {});
    static Result<ColumnarTableP> Load(
        const std::string &tableName,
        std::istream& metadataStream,
        std::istream& dataStream,
        std::optional<std::set<std::string>> fields);


private:
    ColumnarTable() {}

    std::vector<ColumnDesc> schema_;
    std::vector<std::vector<ColumnDataP>> column_data_vecs_;
    std::string name_;
};

};
