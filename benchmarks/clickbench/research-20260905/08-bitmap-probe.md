# Experiment 9: bit-sliced bitmap comparison — keep probe, defer integration

The user proposed compressed range bitmaps instead of integer vectors. This
experiment tests the simplest bounded-space starting point: one row-aligned
bitmap per code bit. It does not yet implement a compressed tree.

A stripe of 65,536 rows with 65,536 labels needs 128 KiB of 16-bit codes.
An uncompressed cumulative bitmap per label would need roughly 512 MiB.
Sixteen bit planes instead need 128 KiB; retaining the forward vector for
projection doubles the code storage. Four planes for 16 labels need 32 KiB,
compared with the existing 64 KiB byte codes. Label storage is additional in
both cases. The probe reports forward and index bytes independently.

For x <= t, process low bits first, initially admitting every row. At a zero
threshold bit, intersect with the complement of that bit plane. At a one
threshold bit, union with the complement. An initial suffix of threshold ones
requires no reads. Thus x <= 32767 in a 16-bit domain reads just the highest
plane. EQ intersects matching planes; complements and shifted thresholds
provide the other comparisons, and two bounds provide an inclusive range.
The implementation uses contiguous AVX-512 Boolean operations, not gathers.

## Method and correctness

Original standalone C++20 implementation in `benchmarks/clickbench/bitmap_probe.cc`;
no new dependency. Compile with GCC 13.3, `-O3 -mavx512f -mavx512bw`.
The scalar oracle covers all six comparisons and ranges, negative and
out-of-domain thresholds, 30 seeded random thresholds, both random and
clustered values, and stripe sizes 1, 63, 513, 65,535, and 65,536.
Cardinalities are 16, 256, and 65,536. Every check passes.

Time whole scans over 1, 16, and 256 full stripes on one thread. The baseline
uses byte codes through 256 labels and 16-bit codes above that, matching the
engine's storage widths. Both paths dispatch operators outside the inner loop
and write the same bitmap. Report the median of seven measurements, each
averaging eight scans after five warmups. Construction is timed separately.
No other benchmark engine or compiler ran during these measurements.

The initial prototype performed extra runtime dispatch and read-modify-write
output work in its vector baseline. Its raw CSV is retained as
`bitmap-probe-initial-runtime-dispatch.csv` but superseded by the corrected
`bitmap-probe.csv`; only the corrected comparison below supports decisions.

## Corrected results

Random data, 256 stripes (16,777,216 rows); microseconds per complete scan:

| Labels | Predicate | Threshold | Vector us | Bitmaps us | Speedup |
| ---: | --- | ---: | ---: | ---: | ---: |
| 16 | eq | 7 | 765.852 | 407.747 | 1.88× |
| 16 | le | 7 | 820.584 | 190.972 | 4.30× |
| 16 | le | 5 | 772.455 | 352.948 | 2.19× |
| 16 | range | 7 | 770.062 | 446.656 | 1.72× |
| 16 | range | 5 | 769.830 | 435.921 | 1.77× |
| 256 | eq | 127 | 760.296 | 748.053 | 1.02× |
| 256 | le | 127 | 763.333 | 188.841 | 4.04× |
| 256 | le | 94 | 760.172 | 750.854 | 1.01× |
| 256 | range | 127 | 773.798 | 783.596 | 0.99× |
| 256 | range | 94 | 773.066 | 828.071 | 0.93× |
| 65536 | eq | 32767 | 1817.380 | 1733.700 | 1.05× |
| 65536 | le | 32767 | 1903.760 | 193.657 | 9.83× |
| 65536 | le | 24313 | 1937.100 | 1733.490 | 1.12× |
| 65536 | range | 32767 | 2139.650 | 1836.260 | 1.17× |
| 65536 | range | 24313 | 2060.020 | 2141.170 | 0.96× |

Index construction for these 256-stripe random inputs takes 5.62 ms at 16
labels, 10.10 ms at 256 labels, and 37.80 ms at 65,536 labels. The corresponding
index sizes are 8, 16, and 32 MiB. These are construction measurements, not
query latency; they must be included in an index amortization decision.

## Decision and next test

Keep the reproducible probe. Defer adding a default index to the engine.
Aligned one-sided ranges can win substantially, but an arbitrary high-cardinality
threshold improves only modestly, and some two-sided ranges regress. The
result supports testing low-cardinality or frequently reused aligned ranges;
it does not support replacing every integer vector or claiming a ClickBench
win. Ordinary full-month date bounds already benefit from stripe metadata,
so an index must beat that existing shortcut in an engine-level comparison.

A compressed-tree follow-up should measure node/container metadata, construction,
selectivity, and row-position remapping separately. It should first establish
an advantage over these contiguous bit planes before introducing gathers.

## References and license boundary

Stockinger and Wu separate bitmap encoding, compression, and binning, and
explain why range encoding reduces bitmap reads for one-sided predicates:
[Bitmap Indices for Data Warehouses](https://sdm.lbl.gov/~kewu/ps/LBNL-59952.pdf).
[Chan and Ioannidis, 1998](https://www.vldb.org/dblp/db/conf/sigmod/ChanI98.html)
is a foundational encoding reference.
[RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap#range-bitmaps)
provides a contemporary range-bitmap implementation and declares Apache-2.0.
None of its code is included in this probe. No proprietary or copyleft
implementation was imported. These references are research provenance,
not a claim that this prototype reproduces their implementations or results.
