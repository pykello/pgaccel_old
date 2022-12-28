#include "util.h"

#include <cstdlib>

namespace pgaccel {

std::string ToLower(const std::string& s)
{
    std::string result;
    for (auto ch: s)
        result.push_back(tolower(ch));
    return result;
}

};
