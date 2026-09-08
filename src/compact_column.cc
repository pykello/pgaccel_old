#include "compact_column.hpp"
#include <cstring>
#include <limits>
namespace pgaccel {
namespace {
template<class Logical>
ColumnDataP CompactRaw(const ColumnDataBase &source, const std::vector<uint16_t> &rows) {
    const auto &input = static_cast<const RawColumnData<Logical> &>(source);
    auto output = std::make_shared<RawColumnData<Logical>>();
    output->type = source.type; output->size = rows.size();
    output->bytesPerValue = input.bytesPerValue;
    output->values = static_cast<uint8_t *>(AllocateAligned(512, rows.size()*input.bytesPerValue));
    int64_t low = INT64_MAX, high = INT64_MIN;
    auto copy = [&]<class T>() {
        auto from = reinterpret_cast<const T *>(input.values);
        auto to = reinterpret_cast<T *>(output->values);
        for (size_t i=0; i<rows.size(); ++i) {
            int64_t value = from[rows[i]]; to[i] = value;
            low = std::min(low, value); high = std::max(high, value);
        }
    };
    switch (input.bytesPerValue) {
    case 1: copy.template operator()<int8_t>(); break;
    case 2: copy.template operator()<int16_t>(); break;
    case 4: copy.template operator()<int32_t>(); break;
    case 8: copy.template operator()<int64_t>(); break;
    default: throw std::invalid_argument("Invalid compact raw width");
    }
    output->minValue = rows.empty() ? 0 : low;
    output->maxValue = rows.empty() ? 0 : high;
    return output;
}
template<class Logical>
ColumnDataP CompactDictionary(const ColumnDataBase &source, const std::vector<uint16_t> &rows) {
    const auto &input = static_cast<const DictColumnData<Logical> &>(source);
    auto output = std::make_shared<DictColumnData<Logical>>();
    output->type = source.type; output->size = rows.size(); output->valueType = input.valueType;
    std::vector<int> remap(input.dict.size(), -1);
    auto code = [&](uint16_t row) { return input.bytesPerValue()==1 ? input.values[row]
        : reinterpret_cast<const uint16_t *>(input.values)[row]; };
    for (auto row: rows) {
        auto id = code(row);
        if (size_t(id)>=remap.size()) throw std::invalid_argument("Invalid compact dictionary code");
        remap[id] = 0;
    }
    for (size_t i=0; i<remap.size(); ++i) if (remap[i]==0) {
        remap[i] = output->dict.size(); output->dict.push_back(input.dict[i]);
    }
    output->values = static_cast<uint8_t *>(AllocateAligned(512, rows.size()*output->bytesPerValue()));
    for (size_t i=0; i<rows.size(); ++i) {
        auto id = remap[code(rows[i])];
        if (output->bytesPerValue()==1) output->values[i] = id;
        else reinterpret_cast<uint16_t *>(output->values)[i] = id;
    }
    return output;
}
}
Result<ColumnDataP> CompactNativeColumn(const ColumnDataBase &source,
    const AccelType &logical, const std::vector<uint16_t> &rows) {
    try {
        for (size_t i=0; i<rows.size(); ++i)
            if (rows[i]>=source.size || (i && rows[i]<=rows[i-1]))
                return Status::Invalid("Compaction needs ordered unique valid rows");
        if (source.type==ColumnDataBase::DICT_COLUMN_DATA) {
            switch (logical.type_num()) {
            case STRING_TYPE: return CompactDictionary<StringType>(source, rows);
            case DATE_TYPE: return CompactDictionary<DateType>(source, rows);
            default: return Status::Invalid("Unsupported compact dictionary type");
            }
        }
        if (source.type!=ColumnDataBase::RAW_COLUMN_DATA)
            return Status::Invalid("Unsupported compact layout");
        switch (logical.type_num()) {
        case INT32_TYPE: return CompactRaw<Int32Type>(source, rows);
        case INT64_TYPE: case TIMESTAMP_TYPE: return CompactRaw<Int64Type>(source, rows);
        case DECIMAL_TYPE: return CompactRaw<DecimalType>(source, rows);
        default: return Status::Invalid("Unsupported compact raw type");
        }
    } catch (const std::exception &e) { return Status::Invalid(e.what()); }
}
}
