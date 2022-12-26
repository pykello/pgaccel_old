
#pragma once

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
    static bool lessThan(parquet::ByteArray &a, parquet::ByteArray &b) {
        if (a.ptr == b.ptr)
            return false;
        int i;
        for (i = 0; i < a.len && i < b.len; i++)
            if (a.ptr[i] != b.ptr[i])
                return a.ptr[i] < b.ptr[i];
        return i == a.len && i != b.len;
    }
};

#define NUMERIC_PGACCEL_TYPE_TRAITS(PhyTy) \
    template <> \
    struct pgaccel_type_traits<PhyTy::type_num> { \
        using dict_type = int32_t; \
        static int32_t convert(PhyTy::c_type value) { \
            return value; \
        } \
    };

NUMERIC_PGACCEL_TYPE_TRAITS(parquet::Int32Type)
NUMERIC_PGACCEL_TYPE_TRAITS(parquet::Int64Type)
