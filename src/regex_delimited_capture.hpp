#pragma once
#include <array>
#include <cstring>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace pgaccel {
// Original finite-prefix plan for ^L([^D]+)D.*$, where L contains only
// ASCII literals and greedy optional literals and D is one ASCII delimiter.
// Prefix alternatives retain regex priority. Everything else uses RE2.
class DelimitedCapture {
    std::vector<std::string> prefixes;
    char delimiter;
    static std::optional<char> literal(std::string_view pattern, size_t &position) {
        if (position == pattern.size()) return {};
        unsigned char c = pattern[position++];
        if (c >= 128) return {};
        if (c == '\\') {
            if (position == pattern.size()) return {};
            c = pattern[position++];
            if (c >= 128 || std::string_view("\\.^$|?*+()[]{}-/").find(char(c)) == std::string_view::npos) return {};
            return char(c);
        }
        if (std::string_view(".^$|?*+()[]{}").find(char(c)) != std::string_view::npos) return {};
        return char(c);
    }
  public:
    static std::shared_ptr<const DelimitedCapture> Compile(std::string_view pattern, std::string_view rewrite) {
        if (rewrite != "\\1" || pattern.empty() || pattern.front() != '^') return {};
        auto plan = std::make_shared<DelimitedCapture>();
        plan->prefixes.emplace_back();
        size_t position = 1;
        while (position < pattern.size() && !pattern.substr(position).starts_with("([^")) {
            std::string token;
            if (pattern.substr(position).starts_with("(?:")) {
                position += 3;
                while (position < pattern.size() && pattern[position] != ')') {
                    auto c = literal(pattern, position); if (!c) return {};
                    token.push_back(*c);
                }
                if (position == pattern.size() || token.empty()) return {};
                ++position;
            } else {
                auto c = literal(pattern, position); if (!c) return {};
                token.push_back(*c);
            }
            bool optional = position < pattern.size() && pattern[position] == '?';
            if (optional) ++position;
            if (optional && plan->prefixes.size() >= 16) return {};
            std::vector<std::string> expanded;
            expanded.reserve(plan->prefixes.size() * (optional ? 2 : 1));
            for (const auto &prefix : plan->prefixes) {
                expanded.push_back(prefix + token);
                if (optional) expanded.push_back(prefix);
            }
            plan->prefixes = std::move(expanded);
        }
        if (!pattern.substr(position).starts_with("([^")) return {};
        position += 3;
        auto delimiter = literal(pattern, position);
        if (!delimiter || !pattern.substr(position).starts_with("]+)")) return {};
        position += 3;
        auto separator = literal(pattern, position);
        if (!separator || *separator != *delimiter || pattern.substr(position) != ".*$") return {};
        plan->delimiter = *delimiter;
        return plan;
    }
    std::optional<std::string_view> match(std::string_view input) const {
        for (const auto &prefix : prefixes) {
            if (!input.starts_with(prefix)) continue;
            size_t end = input.find(delimiter, prefix.size());
            if (end != std::string_view::npos && end > prefix.size())
                return input.substr(prefix.size(), end - prefix.size());
        }
        return {};
    }
};

inline bool IsValidUtf8(std::string_view input) {
    size_t i = 0;
    auto byte = [&](size_t position) { return static_cast<unsigned char>(input[position]); };
    while (i < input.size()) {
        if (i + 32 <= input.size()) {
            uint64_t words[4]; std::memcpy(words, input.data() + i, sizeof(words));
            if (!((words[0] | words[1] | words[2] | words[3]) & 0x8080808080808080ULL)) {
                i += 32; continue;
            }
        }
        unsigned lead = byte(i++);
        if (lead < 128) continue;
        unsigned remaining, low = 0x80, high = 0xbf;
        if (lead >= 0xc2 && lead <= 0xdf) remaining = 1;
        else if (lead >= 0xe0 && lead <= 0xef) {
            remaining = 2;
            if (lead == 0xe0) low = 0xa0;
            if (lead == 0xed) high = 0x9f;
        } else if (lead >= 0xf0 && lead <= 0xf4) {
            remaining = 3;
            if (lead == 0xf0) low = 0x90;
            if (lead == 0xf4) high = 0x8f;
        } else return false;
        if (remaining > input.size() - i || byte(i) < low || byte(i) > high) return false;
        ++i;
        while (--remaining) { if (byte(i) < 0x80 || byte(i) > 0xbf) return false; ++i; }
    }
    return true;
}
} // namespace pgaccel
