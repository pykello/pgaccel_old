#include <cstdint>
#include <string>
#include <parquet/types.h>

namespace pgaccel {

enum TypeNum {
    STRING_TYPE,
    INT32_TYPE,
    INT64_TYPE,
    DECIMAL_TYPE,
    DATE_TYPE
};

struct StringType {
    using c_type = std::string;
    static const TypeNum type_num = STRING_TYPE;

    static c_type FromParquet(const parquet::ByteArray &value) {
        return std::string((char *) value.ptr, value.len);
    }
};

struct Int32Type {
    using c_type = int32_t;
    static const TypeNum type_num = INT32_TYPE;

    static c_type FromParquet(int32_t value) {
        return value;
    }
};

struct Int64Type {
    using c_type = int64_t;
    static const TypeNum type_num = INT64_TYPE;

    static c_type FromParquet(int64_t value) {
        return value;
    }
};

struct DecimalType {
    using c_type = int64_t;
    static const TypeNum type_num = DECIMAL_TYPE;

    static c_type FromParquet(int64_t value) {
        return value;
    }
    
    // non-static
    int scale;
};

struct DateType {
    using c_type = int32_t;
    static const TypeNum type_num = DATE_TYPE;

    static c_type FromParquet(int32_t value) {
        return value;
    }
};

};
