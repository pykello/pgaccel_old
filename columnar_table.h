#pragma once

#include <string>
#include <vector>
#include <optional>
#include "types.hpp"
#include "column_data.hpp"

namespace pgaccel {

struct ColumnDesc {
    std::string name;
    std::unique_ptr<AccelType> type;
};

class ColumnarTable {
public:
    const std::vector<ColumnDesc> & Schema() const
    {
        return schema_;
    }

    const std::vector<ColumnDataP>& ColumnData(int idx) const
    {
        return column_data_[idx];
    }

    std::optional<int> ColumnIndex(const std::string& name) const;

    static std::optional<ColumnarTable> ImportParquet(
        const std::string &path,
        std::optional<std::set<std::string>> fields = {});

private:
    ColumnarTable() {}

    std::vector<ColumnDesc> schema_;
    std::vector<std::vector<ColumnDataP>> column_data_;
};

};
