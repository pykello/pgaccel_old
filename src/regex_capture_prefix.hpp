#pragma once
#include <re2/re2.h>
#include <cstring>
#include <memory>
#include <string_view>

namespace pgaccel {
// For ^P.*$ with dot_nl enabled, the suffix accepts every remaining ASCII
// byte. Matching P alone preserves its leftmost/greedy capture choices. Only
// whole-input replacement by one capture is eligible; non-ASCII falls back.
inline std::shared_ptr<const re2::RE2> CompileCapturePrefix(
    std::string_view pattern, std::string_view replacement)
{
    if (replacement.size() != 2 || replacement[0] != '\\' ||
        replacement[1] < '1' || replacement[1] > '9' ||
        pattern.size() < 4 || pattern.front() != '^' || !pattern.ends_with(".*$")) return {};
    auto prefix = pattern.substr(0, pattern.size() - 3);
    int depth = 0;
    bool characterClass = false;
    for (size_t i = 0; i < prefix.size(); ++i) {
        char c = prefix[i];
        if (c == '\\') {
            if (++i == prefix.size() || prefix[i] == 'Q' || prefix[i] == 'E') return {};
            continue;
        }
        if (characterClass) {
            // Nested POSIX classes and initial literal ] use the normal path.
            if (c == '[') return {};
            if (c == ']') characterClass = false;
            continue;
        }
        if (c == '[') {
            size_t first = i + 1;
            if (first < prefix.size() && prefix[first] == '^') ++first;
            if (first == prefix.size() || prefix[first] == ']') return {};
            characterClass = true;
        } else if (c == '(') {
            if (i + 1 < prefix.size() && prefix[i + 1] == '?') {
                if (i + 2 >= prefix.size() || prefix[i + 2] != ':') return {};
                i += 2;
            }
            ++depth;
        } else if (c == ')') {
            if (--depth < 0) return {};
        } else if (c == '|' && depth == 0) return {};
    }
    if (depth || characterClass) return {};
    re2::RE2::Options options;
    options.set_log_errors(false); options.set_dot_nl(true);
    auto result = std::make_shared<re2::RE2>(std::string(prefix), options);
    if (!result->ok() || result->NumberOfCapturingGroups() < replacement[1] - '0') return {};
    return result;
}

inline bool IsAscii(std::string_view text)
{
    size_t i = 0;
    for (; i + 32 <= text.size(); i += 32) {
        uint64_t words[4];
        std::memcpy(words, text.data() + i, sizeof(words));
        if ((words[0] | words[1] | words[2] | words[3]) & 0x8080808080808080ULL) return false;
    }
    for (; i < text.size(); ++i) if (static_cast<unsigned char>(text[i]) >= 128) return false;
    return true;
}
} // namespace pgaccel
