#pragma once

#include <immintrin.h>
#include <cctype>
#include <cstdint>

namespace pgaccel
{

template<int REGW, int N, bool sign>
struct AvxTraits {};

template<>
struct AvxTraits<512, 8, true> {
    using register_type = __m512i;
    using atom_type = int8_t;
    using mask_type = __mmask64;

    inline static register_type set1(atom_type v) {
        return _mm512_set1_epi8(v);
    }

    inline static mask_type compare(register_type a, register_type b, int op)
    {
        return _mm512_cmp_epi8_mask(a, b, op);
    }

    inline static mask_type mask_compare(mask_type mask,
                                         register_type a,
                                         register_type b,
                                         int op)
    {
        return _mm512_mask_cmp_epi8_mask(mask, a, b, op);
    }
};

template<>
struct AvxTraits<512, 8, false> {
    using register_type = __m512i;
    using atom_type = uint8_t;
    using mask_type = __mmask64;

    inline static register_type set1(atom_type v) {
        return _mm512_set1_epi8(v);
    }

    inline static mask_type compare(register_type a, register_type b, int op)
    {
        return _mm512_cmp_epu8_mask(a, b, op);
    }

    inline static mask_type mask_compare(mask_type mask,
                                         register_type a,
                                         register_type b,
                                         int op)
    {
        return _mm512_mask_cmp_epu8_mask(mask, a, b, op);
    }
};

template<>
struct AvxTraits<512, 16, true> {
    using register_type = __m512i;
    using atom_type = int16_t;
    using mask_type = __mmask32;

    inline static register_type set1(atom_type v) {
        return _mm512_set1_epi16(v);
    }

    inline static mask_type compare(register_type a, register_type b, int op)
    {
        return _mm512_cmp_epi16_mask(a, b, op);
    }

    inline static mask_type mask_compare(mask_type mask,
                                         register_type a,
                                         register_type b,
                                         int op)
    {
        return _mm512_mask_cmp_epi16_mask(mask, a, b, op);
    }
};

template<>
struct AvxTraits<512, 16, false> {
    using register_type = __m512i;
    using atom_type = uint16_t;
    using mask_type = __mmask32;

    inline static register_type set1(atom_type v) {
        return _mm512_set1_epi16(v);
    }

    inline static mask_type compare(register_type a, register_type b, int op)
    {
        return _mm512_cmp_epu16_mask(a, b, op);
    }

    inline static mask_type mask_compare(mask_type mask,
                                         register_type a,
                                         register_type b,
                                         int op)
    {
        return _mm512_mask_cmp_epu16_mask(mask, a, b, op);
    }
};

template<>
struct AvxTraits<512, 32, true> {
    using register_type = __m512i;
    using atom_type = int32_t;
    using mask_type = __mmask16;

    inline static register_type set1(atom_type v) {
        return _mm512_set1_epi32(v);
    }

    inline static mask_type compare(register_type a, register_type b, int op)
    {
        return _mm512_cmp_epi32_mask(a, b, op);
    }

    inline static mask_type mask_compare(mask_type mask,
                                         register_type a,
                                         register_type b,
                                         int op)
    {
        return _mm512_mask_cmp_epi32_mask(mask, a, b, op);
    }
};

template<>
struct AvxTraits<512, 32, false> {
    using register_type = __m512i;
    using atom_type = uint32_t;
    using mask_type = __mmask16;

    inline static register_type set1(atom_type v) {
        return _mm512_set1_epi32(v);
    }

    inline static mask_type compare(register_type a, register_type b, int op)
    {
        return _mm512_cmp_epu32_mask(a, b, op);
    }

    inline static mask_type mask_compare(mask_type mask,
                                         register_type a,
                                         register_type b,
                                         int op)
    {
        return _mm512_mask_cmp_epu32_mask(mask, a, b, op);
    }
};

template<>
struct AvxTraits<512, 64, true> {
    using register_type = __m512i;
    using atom_type = int64_t;
    using mask_type = __mmask8;

    inline static register_type set1(atom_type v) {
        return _mm512_set1_epi64(v);
    }

    inline static mask_type compare(register_type a, register_type b, int op)
    {
        return _mm512_cmp_epi64_mask(a, b, op);
    }

    inline static mask_type mask_compare(mask_type mask,
                                         register_type a,
                                         register_type b,
                                         int op)
    {
        return _mm512_mask_cmp_epi64_mask(mask, a, b, op);
    }
};

template<>
struct AvxTraits<512, 64, false> {
    using register_type = __m512i;
    using atom_type = uint64_t;
    using mask_type = __mmask8;

    inline static register_type set1(atom_type v) {
        return _mm512_set1_epi64(v);
    }

    inline static mask_type compare(register_type a, register_type b, int op)
    {
        return _mm512_cmp_epu64_mask(a, b, op);
    }

    inline static mask_type mask_compare(mask_type mask,
                                         register_type a,
                                         register_type b,
                                         int op)
    {
        return _mm512_mask_cmp_epu64_mask(mask, a, b, op);
    }
};

};
