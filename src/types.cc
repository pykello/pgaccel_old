#include "types.hpp"
#include <chrono>
#include <limits>
#include <stdexcept>

namespace pgaccel {
int64_t ParseDecimal(int scale, const std::string &s) {
    if (scale < 0 || scale > 18 || s.empty()) throw std::invalid_argument("Invalid decimal");
    size_t i = 0; bool negative = s[0] == '-';
    if (s[0] == '-' || s[0] == '+') ++i;
    __int128 value = 0; int fractional = -1; bool digit = false;
    for (; i < s.size(); ++i) {
        if (s[i] == '.' && fractional == -1) { fractional = 0; continue; }
        if (s[i] < '0' || s[i] > '9') throw std::invalid_argument("Invalid decimal");
        digit = true;
        if (fractional >= scale) {
            if (s[i] != '0') throw std::invalid_argument("Decimal literal loses precision");
            continue;
        }
        value = value * 10 + s[i] - '0';
        if (value > (__int128(1) << 63)) throw std::out_of_range("Decimal overflow");
        if (fractional >= 0) ++fractional;
    }
    if (!digit) throw std::invalid_argument("Invalid decimal");
    for (int n = std::max(0, fractional); n < scale; ++n) {
        value *= 10;
        if (value > (__int128(1) << 63)) throw std::out_of_range("Decimal overflow");
    }
    if (negative) value = -value;
    if (value > INT64_MAX || value < INT64_MIN) throw std::out_of_range("Decimal overflow");
    return int64_t(value);
}
int32_t ParseDate(const std::string &s) {
    using namespace std::chrono;
    if (s.size() != 10 || s[4] != '-' || s[7] != '-') throw std::invalid_argument("Expected YYYY-MM-DD");
    auto part = [&](int start, int n) {
        int v = 0;
        for (int i = start; i < start + n; ++i) {
            if (s[i] < '0' || s[i] > '9') throw std::invalid_argument("Invalid date");
            v = v * 10 + s[i] - '0';
        }
        return v;
    };
    year_month_day date{year(part(0,4)), month(part(5,2)), day(part(8,2))};
    if (!date.ok()) throw std::invalid_argument("Invalid calendar date");
    return sys_days(date).time_since_epoch().count();
}
std::string ToString(const AccelType *type, int64_t value) {
    if (type->type_num() == DECIMAL_TYPE) {
        int scale = static_cast<const DecimalType*>(type)->scale;
        uint64_t magnitude = value < 0 ? uint64_t(-(value + 1)) + 1 : uint64_t(value);
        std::string s = std::to_string(magnitude);
        if (scale) {
            if (s.size() <= scale) s.insert(0, scale + 1 - s.size(), '0');
            s.insert(s.size() - scale, 1, '.');
        }
        return (value < 0 ? "-" : "") + s;
    }
    if (type->type_num() == DATE_TYPE) {
        using namespace std::chrono;
        year_month_day date{sys_days{days{value}}};
        char s[32];
        snprintf(s, sizeof(s), "%04d-%02u-%02u", int(date.year()), unsigned(date.month()), unsigned(date.day()));
        return s;
    }
    return std::to_string(value);
}
std::string ToString(const AccelType *, const std::string &value) { return value; }
}
