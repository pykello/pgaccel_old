#include <parquet/types.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <arrow/api.h>

template <parquet::Type::type TYPE>
struct pgaccel_type_traits {};

template <>
struct pgaccel_type_traits<parquet::Type::BYTE_ARRAY> {
    using dict_type = std::string;
    static std::string convert(parquet::ByteArray &value) {
        return std::string((char *) value.ptr, value.len);
    }
};
