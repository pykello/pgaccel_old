// Original benchmark implementation; no third-party code or dependencies.
// Build: c++ -O3 -std=c++20 -mavx512f -mavx512bw bitmap_probe.cc -o bitmap_probe
#include <immintrin.h>
#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <stdexcept>
#include <vector>

using Clock = std::chrono::steady_clock;
enum Op { EQ, NE, LT, LE, GT, GE, RANGE };
constexpr const char *names[] = {"eq", "ne", "lt", "le", "gt", "ge", "range"};
struct Stripe {
    size_t rows, words;
    unsigned bits;
    std::vector<uint16_t> values;
    std::vector<uint8_t> narrow;
    std::vector<uint64_t> planes;
    Stripe(size_t n, unsigned cardinality, bool clustered, uint64_t seed)
        : rows(n), words(((n + 511) / 512) * 8), bits(std::bit_width(cardinality - 1)),
          values(words * 64), narrow(cardinality <= 256 ? words * 64 : 0),
          planes(words * bits) {
        std::mt19937_64 random(seed);
        for (size_t i = 0; i < rows; ++i)
            values[i] = clustered ? i * cardinality / rows : random() % cardinality;
        if (!narrow.empty())
            for (size_t i = 0; i < rows; ++i) narrow[i] = values[i];
    }
    void build() {
        std::fill(planes.begin(), planes.end(), 0);
        for (size_t i = 0; i < words * 64; i += 32) {
            auto value = _mm512_loadu_si512(values.data() + i);
            for (unsigned b = 0; b < bits; ++b) {
                uint64_t mask = _mm512_test_epi16_mask(value, _mm512_set1_epi16(1u << b));
                planes[b * words + i / 64] |= mask << (i % 64);
            }
        }
    }
};
void tail(const Stripe &s, uint64_t *out) {
    size_t used = (s.rows + 63) / 64;
    if (s.rows % 64) out[used - 1] &= (uint64_t(1) << (s.rows % 64)) - 1;
    std::fill(out + used, out + s.words, 0);
}
bool predicate(uint16_t x, Op op, int a, int b) {
    switch (op) {
    case EQ: return x == a;
    case NE: return x != a;
    case LT: return x < a;
    case LE: return x <= a;
    case GT: return x > a;
    case GE: return x >= a;
    case RANGE: return x >= a && x <= b;
    }
    return false;
}
// Compare against an unsigned code. Handle endpoints before narrowing.
template <bool byte> uint64_t compare(__m512i v, int t, bool equal) {
    constexpr int maximum = byte ? 255 : 65535;
    constexpr uint64_t all = byte ? UINT64_MAX : UINT32_MAX;
    if (t < 0) return 0;
    if (t > maximum) return equal ? 0 : all;
    if constexpr (byte) {
        auto k = _mm512_set1_epi8(t);
        return equal ? _mm512_cmpeq_epu8_mask(v, k) : _mm512_cmple_epu8_mask(v, k);
    } else {
        auto k = _mm512_set1_epi16(t);
        return equal ? _mm512_cmpeq_epu16_mask(v, k) : _mm512_cmple_epu16_mask(v, k);
    }
}
template <bool byte, Op op> void vectorFilter(const Stripe &s, int a, int b, uint64_t *out) {
    constexpr size_t step = byte ? 64 : 32;
    for (size_t i = 0; i < s.words * 64; i += step) {
        auto v = byte ? _mm512_loadu_si512(s.narrow.data() + i)
                      : _mm512_loadu_si512(s.values.data() + i);
        uint64_t mask = 0;
        switch (op) {
        case EQ: mask = compare<byte>(v, a, true); break;
        case NE: mask = ~compare<byte>(v, a, true); break;
        case LT: mask = compare<byte>(v, a - 1, false); break;
        case LE: mask = compare<byte>(v, a, false); break;
        case GT: mask = ~compare<byte>(v, a, false); break;
        case GE: mask = ~compare<byte>(v, a - 1, false); break;
        case RANGE: mask = ~compare<byte>(v, a - 1, false) & compare<byte>(v, b, false); break;
        }
        if constexpr (!byte) mask &= UINT32_MAX;
        memcpy(reinterpret_cast<uint8_t *>(out) + i / 8, &mask, step / 8);
    }
    tail(s, out);
}
__m512i lessEqual(const Stripe &s, size_t word, int threshold) {
    auto all = _mm512_set1_epi64(-1);
    if (threshold < 0) return _mm512_setzero_si512();
    if (threshold >= (1 << s.bits) - 1) return all;
    auto result = all;
    // Low-bit suffixes of ones impose no constraint, so start above them.
    unsigned first = std::countr_one(unsigned(threshold));
    for (unsigned b = first; b < s.bits; ++b) {
        auto plane = _mm512_loadu_si512(s.planes.data() + b * s.words + word);
        auto zero = _mm512_xor_si512(plane, all);
        result = (threshold & (1 << b)) ? _mm512_or_si512(zero, result)
                                      : _mm512_and_si512(zero, result);
    }
    return result;
}
__m512i equalTo(const Stripe &s, size_t word, int threshold) {
    auto all = _mm512_set1_epi64(-1);
    if (threshold < 0 || threshold >= (1 << s.bits)) return _mm512_setzero_si512();
    auto result = all;
    for (unsigned b = 0; b < s.bits; ++b) {
        auto plane = _mm512_loadu_si512(s.planes.data() + b * s.words + word);
        result = _mm512_and_si512(result, threshold & (1 << b) ? plane : _mm512_xor_si512(plane, all));
    }
    return result;
}
template <Op op> void bitmapFilter(const Stripe &s, int a, int b, uint64_t *out) {
    auto all = _mm512_set1_epi64(-1);
    for (size_t word = 0; word < s.words; word += 8) {
        __m512i mask;
        switch (op) {
        case EQ: mask = equalTo(s, word, a); break;
        case NE: mask = _mm512_xor_si512(equalTo(s, word, a), all); break;
        case LT: mask = lessEqual(s, word, a - 1); break;
        case LE: mask = lessEqual(s, word, a); break;
        case GT: mask = _mm512_xor_si512(lessEqual(s, word, a), all); break;
        case GE: mask = _mm512_xor_si512(lessEqual(s, word, a - 1), all); break;
        case RANGE: mask = _mm512_andnot_si512(lessEqual(s, word, a - 1), lessEqual(s, word, b)); break;
        }
        _mm512_storeu_si512(out + word, mask);
    }
    tail(s, out);
}
template <Op op> void filter(const Stripe &s, int a, int b, uint64_t *out, bool bitmap) {
    if (bitmap) bitmapFilter<op>(s, a, b, out);
    else if (s.narrow.empty()) vectorFilter<false, op>(s, a, b, out);
    else vectorFilter<true, op>(s, a, b, out);
}
void dispatch(const Stripe &s, Op op, int a, int b, uint64_t *out, bool bitmap) {
    switch (op) {
    case EQ: return filter<EQ>(s, a, b, out, bitmap);
    case NE: return filter<NE>(s, a, b, out, bitmap);
    case LT: return filter<LT>(s, a, b, out, bitmap);
    case LE: return filter<LE>(s, a, b, out, bitmap);
    case GT: return filter<GT>(s, a, b, out, bitmap);
    case GE: return filter<GE>(s, a, b, out, bitmap);
    case RANGE: return filter<RANGE>(s, a, b, out, bitmap);
    }
}
void validate(const Stripe &s) {
    std::vector<uint64_t> actual(s.words), expected(s.words), vector(s.words);
    std::mt19937 random(42);
    std::vector<int> thresholds{-1, 0, 1, 7, 127, 128, 255, 256, 32767, 65535, 65536};
    for (int i = 0; i < 30; ++i) thresholds.push_back(random() % (1 << s.bits));
    for (Op op : {EQ, NE, LT, LE, GT, GE, RANGE}) for (int t : thresholds) {
        std::fill(expected.begin(), expected.end(), 0);
        for (size_t i = 0; i < s.rows; ++i)
            if (predicate(s.values[i], op, t, t + 13)) expected[i / 64] |= uint64_t(1) << (i % 64);
        dispatch(s, op, t, t + 13, actual.data(), true);
        dispatch(s, op, t, t + 13, vector.data(), false);
        if (expected != actual || expected != vector) throw std::runtime_error("bitmap oracle mismatch");
    }
}
template <class F> double median(F f) {
    for (int i = 0; i < 5; ++i) f();
    std::array<double, 7> times;
    for (auto &time : times) {
        auto start = Clock::now();
        for (int repeat = 0; repeat < 8; ++repeat) f();
        time = std::chrono::duration<double, std::micro>(Clock::now() - start).count() / 8;
    }
    std::sort(times.begin(), times.end());
    return times[3];
}
int main() {
    // Explicit full and partial stripes, both encoding widths and orderings.
    for (unsigned cardinality : {16u, 256u, 65536u})
        for (bool clustered : {false, true})
            for (size_t n : {size_t(1), size_t(63), size_t(513), size_t(65535), size_t(65536)}) {
                Stripe s(n, cardinality, clustered, 71); s.build(); validate(s);
            }
    std::cout << "cardinality,clustered,stripes,operator,threshold,forward_bytes,index_bytes,build_ms,vector_us,bitmap_us\n";
    for (unsigned cardinality : {16u, 256u, 65536u}) for (bool clustered : {false, true})
        for (size_t count : {size_t(1), size_t(16), size_t(256)}) {
            std::vector<Stripe> stripes;
            for (size_t i = 0; i < count; ++i) stripes.emplace_back(65536, cardinality, clustered, i + 13);
            auto start = Clock::now();
            for (auto &s : stripes) s.build();
            double build = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
            std::vector<uint64_t> output(count * stripes[0].words);
            for (Op op : {EQ, NE, LT, LE, GT, GE, RANGE})
                for (int threshold : {int(cardinality / 2 - 1), int(cardinality * 0.371)}) {
                    auto run = [&](bool bitmap) {
                        for (size_t i = 0; i < count; ++i) {
                            auto &s = stripes[i]; auto dest = output.data() + i * s.words;
                            dispatch(s, op, threshold, threshold + cardinality / 8, dest, bitmap);
                        }
                        asm volatile("" : : "g"(output.data()) : "memory");
                    };
                    double vector = median([&] { run(false); }), bitmap = median([&] { run(true); });
                    std::cout << cardinality << ',' << clustered << ',' << count << ',' << names[op] << ',' << threshold << ','
                              << count * 65536 * (cardinality <= 256 ? 1 : 2) << ','
                              << count * stripes[0].planes.size() * 8 << ',' << build << ',' << vector << ',' << bitmap << '\n';
                }
        }
}
