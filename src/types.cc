#include "types.hpp"
#include <ctime>
#include <string>

namespace pgaccel {

static uint64_t
pow(uint64_t p, int q) {
    uint64_t result = 1;
    for (int i = 0; i < q; i++)
        result *= p;
    return result;
}

int64_t
ParseDecimal(int scale, const std::string &valueStr)
{
    uint32_t whole, decimal;
    size_t pos = valueStr.find(".");
    if (pos == std::string::npos) {
        whole = std::stoul(valueStr);
        decimal = 0;
    } else {
        std::string wholeStr = valueStr.substr(0, pos);
        std::string decimalStr = valueStr.substr(pos + 1);
        // if decimal part has more digits than scale, trim
        if (decimalStr.length() > scale) {
            decimalStr = decimalStr.substr(0, scale);
        }

        // pad decimal part until its length is equal to scale
        while (decimalStr.length() < scale) {
            decimalStr += "0";
        }

        whole = std::stoul(wholeStr);
        decimal = std::stoul(decimalStr);
    }

    return whole * pow(10, scale) + decimal;
}

int32_t
ParseDate(const std::string &s)
{
    auto year = s.substr(0, 4);
    auto month = s.substr(5, 2);
    auto day = s.substr(8);
    std::tm tm{};
    tm.tm_year = std::stol(year) - 1900;
    tm.tm_mon = std::stol(month) - 1;
    tm.tm_mday = std::stol(day);
    tm.tm_hour = 10;
    tm.tm_min = 15;
    std::time_t t = std::mktime(&tm); 
    return t/(60*60*24);
}

std::string
ToString(const AccelType *type, int64_t value)
{
    switch (type->type_num())
    {
        case DECIMAL_TYPE:
        {
            auto decimalType = static_cast<const DecimalType *>(type);
            int64_t x = pow(10, decimalType->scale);
            int64_t decimal = value % x;
            int64_t whole = value / x;
            std::string decimalStr = std::to_string(decimal);
            while (decimalStr.length() < decimalType->scale)
                decimalStr = std::string("0") + decimalStr;
            return std::to_string(whole) + "." + decimalStr;
        }

        case DATE_TYPE:
        {
            int64_t seconds = value * 60 * 60 * 24;
            auto time = localtime(&seconds);
            char result[50];
            sprintf(result, "%d-%02d-%02d",
                    time->tm_year + 1900,
                    time->tm_mon + 1,
                    time->tm_mday);
            return result;
        }

        default:
            return std::to_string(value);
    }
}

std::string
ToString(const AccelType *type, const std::string &value)
{
    return value;
}


};
