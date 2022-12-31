#pragma once

#include <cstdint>
#include <string>
#include <parquet/types.h>
#include <ostream>

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

    virtual std::string ToString() const {
        return "INVALID";
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

    virtual std::string ToString() const {
        return "String";
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

    virtual std::string ToString() const {
        return "Int32";
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

    virtual std::string ToString() const {
        return "Int64";
    }
};

struct DecimalType: public AccelType {
    using c_type = int64_t;

    virtual TypeNum type_num() const {
        return DECIMAL_TYPE;
    }

    virtual std::string ToString() const {
        char s[100];
        sprintf(s, "Decimal(%d)", scale);
        return s;
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

    virtual std::string ToString() const {
        return "Date";
    }
};

int64_t ParseDecimal(int scale, const std::string &valueStr);
int32_t ParseDate(const std::string &s);

};
