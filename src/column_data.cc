#include "column_data.hpp"

namespace pgaccel
{

template<>
Result<bool>
DictColumnData<StringType>::SaveValue(std::ostream &out,
                                      const std::string &value) const
{
    int len = value.length();
    out.write((char *) &len, sizeof(len));
    out.write(value.c_str(), len);
    return true;
}

template<class AccelTy>
static void
ReadValues(std::istream &in, int count, typename AccelTy::c_type *out)
{
    in.read((char *) out, count * sizeof(typename AccelTy::c_type));
}

template<>
void
ReadValues<StringType>(std::istream &in, int count, std::string *out)
{
    for (int i = 0; i < count; i++)
    {
        int size = 0;
        in.read((char *) &size, sizeof(size));
        if (!in || size < 0 || size > (64 << 20)) throw std::invalid_argument("Invalid native string length");
        out[i].resize(size);
        in.read(out[i].data(),size);
        if (!in) throw std::invalid_argument("Truncated native string");
    }
}

template<class AccelTy>
static Result<ColumnDataP>
LoadDictColumnData(std::istream &in)
{
    int dictSize = 0;
    auto result = std::make_shared<DictColumnData<AccelTy>>();
    result->type = ColumnDataBase::DICT_COLUMN_DATA;
    result->valueType = std::make_unique<AccelTy>();
    in.read((char *) &dictSize, sizeof(dictSize));
    if (!in || dictSize < 0 || dictSize > RowGroupSize) return Status::Invalid("Invalid dictionary size");
    for (int i = 0; i < dictSize; i++)
    {
        typename AccelTy::c_type value;
        ReadValues<AccelTy>(in, 1, &value);
        result->dict.push_back(value);
    }

    in.read((char *) &result->size, sizeof(result->size));
    if (!in || result->size < 0 || result->size > RowGroupSize || (result->size && !dictSize))
        return Status::Invalid("Invalid dictionary row count");
    if (!std::is_sorted(result->dict.begin(),result->dict.end()) ||
        std::adjacent_find(result->dict.begin(),result->dict.end()) != result->dict.end())
        return Status::Invalid("Dictionary must be sorted and unique");
    int bytesPerValue = (dictSize < 256) ? 1 : 2;
    result->values = (uint8_t *) AllocateAligned(512, bytesPerValue * result->size);
    in.read((char *) result->values, bytesPerValue * result->size);

    if (!in) return Status::Invalid("Truncated column data");
    std::vector<bool> used(dictSize,false);
    for (int i = 0; i < result->size; ++i) {
        int code = bytesPerValue == 1 ? result->values[i] : ((uint16_t*)result->values)[i];
        if (code >= dictSize) return Status::Invalid("Dictionary code out of bounds");
        used[code] = true;
    }
    if (std::find(used.begin(),used.end(),false) != used.end())
        return Status::Invalid("Native dictionary contains unused values");
    ColumnDataP resultCasted = std::move(result);
    return resultCasted;
}

static Result<ColumnDataP>
LoadDictColumnData(std::istream &in, AccelType *dataType)
{
    switch (dataType->type_num())
    {
        case TypeNum::INT32_TYPE:
            return LoadDictColumnData<Int32Type>(in);
        case TypeNum::INT64_TYPE:
            return LoadDictColumnData<Int64Type>(in);
        case TypeNum::STRING_TYPE:
            return LoadDictColumnData<StringType>(in);
        case TypeNum::DATE_TYPE:
            return LoadDictColumnData<DateType>(in);
        case TypeNum::DECIMAL_TYPE:
            return LoadDictColumnData<DecimalType>(in);
    }

    return Status::Invalid("Invalid type for DictColumnData: ", dataType->type_num());
}

template<class AccelTy>
static Result<ColumnDataP>
LoadRawColumnData(std::istream &in)
{
    auto result = std::make_shared<RawColumnData<AccelTy>>();
    result->type = ColumnDataBase::RAW_COLUMN_DATA;

    in.read((char *) &result->size, sizeof (result->size));
    in.read((char *) &result->bytesPerValue, sizeof (result->bytesPerValue));
    in.read((char *) &result->minValue, sizeof (result->minValue));
    in.read((char *) &result->maxValue, sizeof (result->maxValue));

    if (!in || result->size < 0 || result->size > RowGroupSize ||
        (result->bytesPerValue != 1 && result->bytesPerValue != 2 && result->bytesPerValue != 4 && result->bytesPerValue != 8) ||
        result->minValue > result->maxValue)
        return Status::Invalid("Invalid raw column header");
    result->values =
        (uint8_t *) AllocateAligned(512, result->bytesPerValue * result->size);
    in.read((char *) result->values, result->bytesPerValue * result->size);
    if (!in) return Status::Invalid("Truncated raw column");
    // Range pruning trusts these bounds; validate them against stored values.
    int64_t actualMin = INT64_MAX, actualMax = INT64_MIN;
    for (int row = 0; row < result->size; ++row) {
        int64_t value = 0;
        switch (result->bytesPerValue) {
        case 1: value = ((int8_t*)result->values)[row]; break;
        case 2: value = ((int16_t*)result->values)[row]; break;
        case 4: value = ((int32_t*)result->values)[row]; break;
        case 8: value = ((int64_t*)result->values)[row]; break;
        }
        actualMin = std::min(actualMin,value); actualMax = std::max(actualMax,value);
        if (value < result->minValue || value > result->maxValue)
            return Status::Invalid("Native value outside declared bounds");
    }

    if (result->size && (actualMin != result->minValue || actualMax != result->maxValue))
        return Status::Invalid("Native bounds do not match stored values");
    ColumnDataP resultCasted = std::move(result);
    return resultCasted;
}

static Result<ColumnDataP>
LoadRawColumnData(std::istream &in, AccelType *dataType)
{
    switch (dataType->type_num())
    {
        case TypeNum::INT32_TYPE:
            return LoadRawColumnData<Int32Type>(in);
        case TypeNum::TIMESTAMP_TYPE:
        case TypeNum::INT64_TYPE:
            return LoadRawColumnData<Int64Type>(in);
        case TypeNum::DATE_TYPE:
            return LoadRawColumnData<DateType>(in);
        case TypeNum::DECIMAL_TYPE:
            return LoadRawColumnData<DecimalType>(in);
    }

    return Status::Invalid("Invalid type for RawColumnDate: ", dataType->type_num());
}

Result<ColumnDataP> ColumnDataBase::Load(std::istream &in, AccelType *dataType)
{
    ColumnDataBase::Type type = ColumnDataBase::Type(-1);
    in.read((char *) &type, sizeof(type));

    if (!in) return Status::Invalid("Truncated column header");
    bool dictionary = dataType->type_num() == STRING_TYPE || dataType->type_num() == DATE_TYPE;
    if (type != (dictionary ? DICT_COLUMN_DATA : RAW_COLUMN_DATA))
        return Status::Invalid("Unsupported native layout for logical type");
    switch (type)
    {
        case ColumnDataBase::DICT_COLUMN_DATA:
            return LoadDictColumnData(in, dataType);
        case ColumnDataBase::RAW_COLUMN_DATA:
            return LoadRawColumnData(in, dataType);
        default:
            return Status::Invalid("Unknown column data type: ", type);
    }
}

void
DictColumnDataBase::to_16(uint16_t *out)
{
    switch (bytesPerValue())
    {
        case 1:
        {
            for (int i = 0; i < size; i++)
                out[i] = values[i];
            break;
        }
        case 2:
        {
            memcpy(out, values, 2 * size);
            break;
        }
    }
}

};
