#pragma once

#include <functional>
#include <string>
#include <vector>

namespace pgaccel {

std::string ToLower(const std::string& s);
std::vector<std::string> Split(const std::string &s,
                               const std::function<bool(char)> &is_delimiter);
uint64_t MeasureDurationMs(const std::function<void()> &body);

};
