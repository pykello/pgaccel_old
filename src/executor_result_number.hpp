#pragma once
#include "executor_value.hpp"
#include <charconv>
namespace pgaccel {
namespace {
// Parse typed internal query output without narrowing SUM's 128-bit value.
// AVG output may use scientific notation; round once to its declared scale.
Wide ResultNumber(std::string_view text, int scale) {
    if (text.empty() || scale < 0 || scale > 18) throw std::invalid_argument("Invalid result number");
    size_t at = 0; bool negative = text[0] == '-';
    if (text[0] == '-' || text[0] == '+') ++at;
    __uint128_t magnitude = 0, maximum = __uint128_t(1) << 127;
    bool dot = false, digit = false; int fractional = 0;
    while (at < text.size() && text[at] != 'e' && text[at] != 'E') {
        char c = text[at++];
        if (c == '.' && !dot) { dot = true; continue; }
        if (c < '0' || c > '9') throw std::invalid_argument("Invalid result number");
        digit = true;
        if (magnitude > (maximum - (c - '0')) / 10) throw std::overflow_error("Result number overflow");
        magnitude = magnitude * 10 + c - '0';
        if (dot) ++fractional;
    }
    if (!digit) throw std::invalid_argument("Invalid result number");
    int exponent = 0;
    if (at < text.size()) {
        ++at;
        if (at < text.size() && text[at] == '+') ++at;
        auto result = std::from_chars(text.data() + at, text.data() + text.size(), exponent);
        if (result.ec != std::errc{} || result.ptr != text.data() + text.size() || exponent < -1000 || exponent > 1000)
            throw std::invalid_argument("Invalid result exponent");
    }
    int shift = scale + exponent - fractional;
    if (shift < 0) {
        if (shift < -38) magnitude = 0;
        else {
            __uint128_t divisor = Power(-shift);
            auto remainder = magnitude % divisor;
            magnitude = magnitude / divisor + (remainder >= divisor / 2);
        }
    } else {
        if (magnitude && shift > 38) throw std::overflow_error("Result number overflow");
        while (magnitude && shift-- > 0) {
            if (magnitude > maximum / 10) throw std::overflow_error("Result number overflow");
            magnitude *= 10;
        }
    }
    if (magnitude > maximum || (!negative && magnitude == maximum)) throw std::overflow_error("Result number overflow");
    return negative && magnitude ? -Wide(magnitude - 1) - 1 : Wide(magnitude);
}
}
}
