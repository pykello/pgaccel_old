#include "column_data.hpp"
#include <limits>

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

static bool SkipNativePadding(std::istream &in) {
    auto position = in.tellg();
    if (position < 0) return false;
    size_t padding = (512 - uint64_t(position) % 512) % 512;
    char bytes[512];
    in.read(bytes, padding);
    return bool(in) && std::all_of(bytes, bytes + padding, [](char c) { return c == 0; });
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
    // One binary dictionary read needs one stream guard, rather than two
    // sentries per label. Keep short-read and exception state handling.
    std::istream::sentry guard(in,true);
    if (!guard) throw std::invalid_argument("Truncated native string dictionary");
    auto *buffer=in.rdbuf();
    auto readExact=[&](char *destination,std::streamsize size) {
        std::streamsize got;
        try { got=buffer->sgetn(destination,size); }
        catch (...) {
            auto error=std::current_exception();
            try { in.setstate(std::ios::badbit); } catch (...) {}
            if (in.exceptions() & std::ios::badbit) std::rethrow_exception(error);
            return false;
        }
        if (got!=size) {
            in.setstate(std::ios::eofbit|std::ios::failbit);
            return false;
        }
        return true;
    };
    for (int i = 0; i < count; i++) {
        int size = 0;
        if (!readExact(reinterpret_cast<char*>(&size),sizeof(size)) || size<0 || size>(64<<20))
            throw std::invalid_argument("Invalid native string length");
        out[i].resize(size);
        if (!readExact(out[i].data(),size))
            throw std::invalid_argument("Truncated native string");
    }
}

template<class AccelTy>
static Result<ColumnDataP>
LoadDictColumnData(std::istream &in, bool aligned, std::vector<std::string> *scratch = nullptr)
{
    int dictSize = 0;
    auto result = std::make_shared<DictColumnData<AccelTy>>();
    result->type = ColumnDataBase::DICT_COLUMN_DATA;
    result->valueType = std::make_unique<AccelTy>();
    in.read((char *) &dictSize, sizeof(dictSize));
    if (!in || dictSize < 0 || dictSize > RowGroupSize) return Status::Invalid("Invalid dictionary size");
    // The validated count fixes storage before decoding. Construct strings
    // directly in their final slots, avoiding temporary heap allocations and
    // copies; numeric dictionaries can be read as one contiguous block.
    if constexpr(std::is_same_v<AccelTy,StringType>)
        if (scratch) result->dict.swap(*scratch);
    result->dict.resize(dictSize);
    ReadValues<AccelTy>(in, dictSize, result->dict.data());

    in.read((char *) &result->size, sizeof(result->size));
    if (!in || result->size < 0 || result->size > RowGroupSize || (result->size && !dictSize))
        return Status::Invalid("Invalid dictionary row count");
    if (!std::is_sorted(result->dict.begin(),result->dict.end()) ||
        std::adjacent_find(result->dict.begin(),result->dict.end()) != result->dict.end())
        return Status::Invalid("Dictionary must be sorted and unique");
    int bytesPerValue = (dictSize < 256) ? 1 : 2;
    result->values = (uint8_t *) AllocateAligned(512, bytesPerValue * result->size);
    if (aligned && !SkipNativePadding(in)) return Status::Invalid("Invalid native padding");
    in.read((char *) result->values, bytesPerValue * result->size);
    if (aligned && !SkipNativePadding(in)) return Status::Invalid("Invalid native padding");

    if (!in) return Status::Invalid("Truncated column data");
    // Byte stores avoid a dependent read/modify/write of a packed bit for
    // every code. This scratch array is bounded by one stripe dictionary.
    std::vector<uint8_t> used(dictSize,0);
    for (int i = 0; i < result->size; ++i) {
        int code = bytesPerValue == 1 ? result->values[i] : ((uint16_t*)result->values)[i];
        if (code >= dictSize) return Status::Invalid("Dictionary code out of bounds");
        used[code] = 1;
    }
    if (std::find(used.begin(),used.end(),uint8_t(0)) != used.end())
        return Status::Invalid("Native dictionary contains unused values");
    ColumnDataP resultCasted = std::move(result);
    return resultCasted;
}

static Result<ColumnDataP>
LoadDictColumnData(std::istream &in, AccelType *dataType, bool aligned, std::vector<std::string> *scratch)
{
    switch (dataType->type_num())
    {
        case TypeNum::INT32_TYPE:
            return LoadDictColumnData<Int32Type>(in, aligned);
        case TypeNum::INT64_TYPE:
            return LoadDictColumnData<Int64Type>(in, aligned);
        case TypeNum::STRING_TYPE:
            return LoadDictColumnData<StringType>(in, aligned, scratch);
        case TypeNum::DATE_TYPE:
            return LoadDictColumnData<DateType>(in, aligned);
        case TypeNum::DECIMAL_TYPE:
            return LoadDictColumnData<DecimalType>(in, aligned);
    }

    return Status::Invalid("Invalid type for DictColumnData: ", dataType->type_num());
}

// Keep the reduction at the stored width so the compiler can vectorize it.
// Dispatch once per stripe, rather than branching on width for every value.
template<class Stored>
static std::pair<int64_t,int64_t> NativeBounds(const uint8_t *bytes, int count) {
    auto values = reinterpret_cast<const Stored *>(bytes);
    Stored minimum = std::numeric_limits<Stored>::max();
    Stored maximum = std::numeric_limits<Stored>::lowest();
    for (int i = 0; i < count; ++i) {
        minimum = std::min(minimum, values[i]);
        maximum = std::max(maximum, values[i]);
    }
    return {minimum, maximum};
}

template<class AccelTy>
static Result<ColumnDataP>
LoadRawColumnData(std::istream &in, bool aligned)
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
    if (aligned && !SkipNativePadding(in)) return Status::Invalid("Invalid native padding");
    in.read((char *) result->values, result->bytesPerValue * result->size);
    if (aligned && !SkipNativePadding(in)) return Status::Invalid("Invalid native padding");
    if (!in) return Status::Invalid("Truncated raw column");
    // Range pruning trusts these bounds; validate them against stored values.
    std::pair<int64_t,int64_t> bounds;
    switch (result->bytesPerValue) {
    case 1: bounds = NativeBounds<int8_t>(result->values, result->size); break;
    case 2: bounds = NativeBounds<int16_t>(result->values, result->size); break;
    case 4: bounds = NativeBounds<int32_t>(result->values, result->size); break;
    case 8: bounds = NativeBounds<int64_t>(result->values, result->size); break;
    }
    auto [actualMin, actualMax] = bounds;
    if (result->size && (actualMin < result->minValue || actualMax > result->maxValue))
        return Status::Invalid("Native value outside declared bounds");

    if (result->size && (actualMin != result->minValue || actualMax != result->maxValue))
        return Status::Invalid("Native bounds do not match stored values");
    ColumnDataP resultCasted = std::move(result);
    return resultCasted;
}

static Result<ColumnDataP>
LoadRawColumnData(std::istream &in, AccelType *dataType, bool aligned)
{
    switch (dataType->type_num())
    {
        case TypeNum::INT32_TYPE:
            return LoadRawColumnData<Int32Type>(in, aligned);
        case TypeNum::TIMESTAMP_TYPE:
        case TypeNum::INT64_TYPE:
            return LoadRawColumnData<Int64Type>(in, aligned);
        case TypeNum::DATE_TYPE:
            return LoadRawColumnData<DateType>(in, aligned);
        case TypeNum::DECIMAL_TYPE:
            return LoadRawColumnData<DecimalType>(in, aligned);
    }

    return Status::Invalid("Invalid type for RawColumnDate: ", dataType->type_num());
}

static Result<ColumnDataP> LoadNativeColumn(std::istream &in, AccelType *dataType, bool aligned,
                                          std::vector<std::string> *scratch)
{
    ColumnDataBase::Type type = ColumnDataBase::Type(-1);
    in.read((char *) &type, sizeof(type));

    if (!in) return Status::Invalid("Truncated column header");
    bool dictionary = dataType->type_num() == STRING_TYPE || dataType->type_num() == DATE_TYPE;
    if (type != (dictionary ? ColumnDataBase::DICT_COLUMN_DATA : ColumnDataBase::RAW_COLUMN_DATA))
        return Status::Invalid("Unsupported native layout for logical type");
    switch (type)
    {
        case ColumnDataBase::DICT_COLUMN_DATA:
            return LoadDictColumnData(in, dataType, aligned, scratch);
        case ColumnDataBase::RAW_COLUMN_DATA:
            return LoadRawColumnData(in, dataType, aligned);
        default:
            return Status::Invalid("Unknown column data type: ", type);
    }
}

Result<ColumnDataP> ColumnDataBase::Load(std::istream &in, AccelType *type, bool aligned) {
    return LoadNativeColumn(in,type,aligned,nullptr);
}
Result<ColumnDataP> ColumnDataBase::LoadReusingStrings(std::istream &in, AccelType *type,
                                                    std::vector<std::string> &scratch, bool aligned) {
    return LoadNativeColumn(in,type,aligned,&scratch);
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
