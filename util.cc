#include "util.h"

#include <chrono>
#include <cstdlib>
using namespace std::chrono;

namespace pgaccel {

std::string ToLower(const std::string& s)
{
    std::string result;
    for (auto ch: s)
        result.push_back(tolower(ch));
    return result;
}

std::vector<std::string>
Split(const std::string &s, const std::function<bool(char)> &is_delimiter)
{
    std::vector<std::string> result;
    std::string current;
    for (int i = 0; i < s.length(); i++) {
        if (is_delimiter(s[i])) {
            if (current.length())
                result.push_back(current);
            current = "";
        } else {
            current += s[i];
        }
    }
    if (current.length())
        result.push_back(current);
    return result;
}

uint64_t MeasureDurationMs(const std::function<void()> &body)
{
    auto start = high_resolution_clock::now();

    body();

    auto stop = high_resolution_clock::now();
    auto duration  = duration_cast<microseconds>(stop - start);
    return duration.count() / 1000;
}

};
