#include "columnar_table.h"
#include "executor_result_number.hpp"

namespace pgaccel {
ColumnarTableP ColumnarTable::NumericIntermediate(
    const std::string &name, const std::vector<ColumnDesc> &schema,
    const std::vector<std::vector<std::string>> &rows) {
    auto table = ColumnarTableP(new ColumnarTable());
    table->name_ = name; table->schema_ = schema;
    for (const auto &column : schema)
        if (column.type->type_num() != INT64_TYPE && column.type->type_num() != DECIMAL_TYPE)
            throw std::invalid_argument("Intermediate columns require Int64 or Decimal");
    for (const auto &row : rows)
        if (row.size() != schema.size()) throw std::invalid_argument("Intermediate row width mismatch");
    for (size_t start = 0; start < rows.size(); start += RowGroupSize) {
        RowGroup group; group.size = std::min<size_t>(RowGroupSize, rows.size() - start);
        for (size_t c = 0; c < schema.size(); ++c) {
            auto make = [&]<class T>() -> ColumnDataP {
                auto data = std::make_shared<RawColumnData<T>>();
                data->type = ColumnDataBase::RAW_COLUMN_DATA;
                data->size = group.size; data->bytesPerValue = 8;
                data->values = (uint8_t *)AllocateAligned(512, size_t(group.size) * 8);
                for (int r = 0; r < group.size; ++r) {
                    Wide value = ResultNumber(rows[start + r][c], Scale(schema[c].type.get()));
                    if (value < INT64_MIN || value > INT64_MAX)
                        throw std::overflow_error("Intermediate numeric value exceeds 64 bits");
                    ((int64_t *)data->values)[r] = int64_t(value);
                    if (!r) data->minValue = data->maxValue = int64_t(value);
                    else { data->minValue = std::min(data->minValue, int64_t(value));
                           data->maxValue = std::max(data->maxValue, int64_t(value)); }
                }
                return data;
            };
            group.columns.push_back(schema[c].type->type_num() == DECIMAL_TYPE ?
                make.template operator()<DecimalType>() : make.template operator()<Int64Type>());
        }
        table->row_groups_.push_back(std::move(group));
    }
    return table;
}
}
