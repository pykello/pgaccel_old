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
        int size;
        in.read((char *) &size, sizeof(size));
        for (int j = 0; j < size; j++)
        {
            char c;
            in.read((char *) &c, sizeof(c));
            out[i].push_back(c);
        }
    }
}

template<class AccelTy>
static Result<ColumnDataP>
LoadDictColumnData(std::istream &in)
{
    int dictSize;
    auto result = std::make_unique<DictColumnData<AccelTy>>();
    result->type = ColumnDataBase::DICT_COLUMN_DATA;
    in.read((char *) &dictSize, sizeof(dictSize));
    for (int i = 0; i < dictSize; i++)
    {
        typename AccelTy::c_type value;
        ReadValues<AccelTy>(in, 1, &value);
        result->dict.push_back(value);
    }

    in.read((char *) &result->size, sizeof(result->size));
    int bytesPerValue = (dictSize < 256) ? 1 : 2;
    result->values = (uint8_t *) aligned_alloc(512, bytesPerValue * result->size);
    in.read((char *) result->values, bytesPerValue * result->size);

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
    auto result = std::make_unique<RawColumnData<AccelTy>>();
    result->type = ColumnDataBase::RAW_COLUMN_DATA;

    in.read((char *) &result->size, sizeof (result->size));
    in.read((char *) &result->bytesPerValue, sizeof (result->bytesPerValue));
    in.read((char *) &result->minValue, sizeof (result->minValue));
    in.read((char *) &result->maxValue, sizeof (result->maxValue));

    result->values =
        (uint8_t *) aligned_alloc(512, result->bytesPerValue * result->size);
    in.read((char *) result->values, result->bytesPerValue * result->size);

    ColumnDataP resultCasted = std::move(result);
    return resultCasted;}

static Result<ColumnDataP>
LoadRawColumnData(std::istream &in, AccelType *dataType)
{
    switch (dataType->type_num())
    {
        case TypeNum::INT32_TYPE:
            return LoadRawColumnData<Int32Type>(in);
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
    ColumnDataBase::Type type;
    in.read((char *) &type, sizeof(type));

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
