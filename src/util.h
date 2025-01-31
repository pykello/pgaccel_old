#pragma once

#include <functional>
#include <string>
#include <vector>

namespace pgaccel {

std::string ToLower(const std::string& s);
std::vector<std::string> Split(const std::string &s,
                               const std::function<bool(char)> &is_delimiter);
uint64_t MeasureDurationMs(const std::function<void()> &body);

inline bool IsBitSet(uint8_t v, int idx)
{
    return v & (1 << idx);
}

inline bool IsBitSet(uint8_t *v, int idx)
{
    return IsBitSet(v[idx >> 3], idx & 7);
}

};
