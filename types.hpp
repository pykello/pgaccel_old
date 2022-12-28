#pragma once

#include <cstdint>
#include <string>
#include <parquet/types.h>

namespace pgaccel {

enum TypeNum {
    STRING_TYPE,
    INT32_TYPE,
    INT64_TYPE,
    DECIMAL_TYPE,
    DATE_TYPE,
    INVALID_TYPE
};

struct AccelType {
    virtual TypeNum type_num() const {
        return INVALID_TYPE;
    }
};

struct StringType: public AccelType {
    using c_type = std::string;

    virtual TypeNum type_num() const {
        return STRING_TYPE;
    }

    static c_type FromParquet(const parquet::ByteArray &value) {
        return std::string((char *) value.ptr, value.len);
    }
};

struct Int32Type: public AccelType {
    using c_type = int32_t;

    virtual TypeNum type_num() const {
        return INT32_TYPE;
    }

    static c_type FromParquet(int32_t value) {
        return value;
    }
};

struct Int64Type: public AccelType {
    using c_type = int64_t;

    virtual TypeNum type_num() const {
        return INT64_TYPE;
    }

    static c_type FromParquet(int64_t value) {
        return value;
    }
};

struct DecimalType: public AccelType {
    using c_type = int64_t;

    virtual TypeNum type_num() const {
        return DECIMAL_TYPE;
    }

    static c_type FromParquet(int64_t value) {
        return value;
    }
    
    // fields
    int scale;
};

struct DateType: public AccelType {
    using c_type = int32_t;

    TypeNum type_num() const {
        return DATE_TYPE;
    }

    static c_type FromParquet(int32_t value) {
        return value;
    }
};

};
