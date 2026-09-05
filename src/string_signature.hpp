#pragma once
#include <array>
#include <cstdint>
#include <string_view>

namespace pgaccel {
// Original conservative trigram signature. A missing bit proves absence;
// matching bits are only candidates and always require an exact string check.
struct StringSignature {
    std::array<uint64_t, 2> words{};
    bool mayContain(const StringSignature &required) const {
        return (words[0] & required.words[0]) == required.words[0] &&
               (words[1] & required.words[1]) == required.words[1];
    }
    static StringSignature Build(std::string_view text) {
        StringSignature result;
        uint32_t gram = 0;
        size_t seen = 0;
        for (unsigned char byte : text) {
            gram = ((gram << 8) | byte) & 0xffffffu;
            if (++seen >= 3) {
                unsigned bit = (gram * 0x9e3779b1u) >> 25;
                result.words[bit >> 6] |= uint64_t(1) << (bit & 63);
            }
        }
        return result;
    }
};
} // namespace pgaccel
