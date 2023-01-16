#pragma once

#include <cstdint>
#include <string>
#include <parquet/types.h>
#include <ostream>
#include <istream>

namespace pgaccel {

enum TypeNum {
    STRING_TYPE,
    INT32_TYPE,
    INT64_TYPE,
    DECIMAL_TYPE,
    DATE_TYPE,
    INVALID_TYPE
};

int64_t ParseDecimal(int scale, const std::string &valueStr);
int32_t ParseDate(const std::string &s);

struct StringType;
struct Int32Type;
struct Int64Type;
struct DecimalType;
struct DateType;

struct AccelType {
    virtual TypeNum type_num() const {
        return INVALID_TYPE;
    }

    StringType *asStringType() {
        return reinterpret_cast<StringType *>(this);
    }

    DateType *asDateType() {
        return reinterpret_cast<DateType *>(this);
    }

    DecimalType *asDecimalType() {
        return reinterpret_cast<DecimalType *>(this);
    }

    Int32Type *asInt32Type() {
        return reinterpret_cast<Int32Type *>(this);
    }

    Int64Type *asInt64Type() {
        return reinterpret_cast<Int64Type *>(this);
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

    c_type Parse(const std::string &str) const {
        return str;
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

    c_type Parse(const std::string &str) const {
        return std::stoi(str);
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

    c_type Parse(const std::string &str) const {
        return std::stoll(str);
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

    c_type Parse(const std::string &str) const {
        return ParseDecimal(scale, str);
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

    c_type Parse(const std::string &str) const {
        return ParseDate(str);
    }

    static c_type FromParquet(int32_t value) {
        return value;
    }

    virtual std::string ToString() const {
        return "Date";
    }
};

std::string ToString(const AccelType *type, int64_t value);
std::string ToString(const AccelType *type, const std::string &value);

};
