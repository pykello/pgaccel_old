# Q8: conditional ratios through compact join payloads

The new path at `0de993a` handles a year-grouped conditional SUM divided by
an ordinary SUM over independent, proved-unique dimension chains. It does
not recognize TPC-H table names, constants or saved results. Q8 is the first
benchmark consumer. The general relational executor remains the fallback.

## Before

The general path joins eight logical tables, evaluates expressions against
bound rows and maintains general aggregate states. The original SF100
comparison measured PGAccel at 118.216 seconds, DuckDB at 2.057 seconds and
ClickHouse at 5.176 seconds. Those are historical warm minima, not a fresh
comparison with the new binary. The revised goal is a warm-median speedup
of at least 2x against both engines at SF10 and SF100.

## After

1. Orient integer equality joins toward keys whose uniqueness is proved
   from the loaded data. Require independent dimension chains that consume
   every logical table; aliases of nation remain separate logical nodes.
2. Apply local filters before propagating payloads toward lineitem. A
   successful part join carries 1. The orders chain carries an order-year
   code only after its customer, nation and region joins survive. The
   supplier chain carries the exact identity of its nation row.
3. Translate each date dictionary's actual values to year codes once per
   stripe. Dictionary code 42 in one stripe need not mean the same date as
   code 42 elsewhere. Reserve code zero for rejected joins and bound the
   observed year span to 64 years.
4. Use one, two or four bytes for each payload according to its proved
   maximum. Sparse key domains use membership bits and rank prefixes plus
   a separately narrowed payload array, avoiding an eight-byte row value.
5. Probe the most selective fact-side membership first, using bounded
   AVX-512 bitmap probes where supported. Check remaining joins only for
   survivors and gather their arithmetic operands.
6. Accumulate ordinary and conditional sums in private year arrays. Merge
   exact wide-integer sums and use the existing formula evaluator for
   decimal scaling, division, output ordering, limit and offset.

For example, an eligible 1995 order can carry code 4 when the observed date
range begins in 1992. A lineitem with that order, an eligible part and a
supplier in Brazil contributes to both sums in year group 4. The same row
with a supplier in another nation contributes only to the denominator.
Missing joins contribute to neither sum. Nation labels are evaluated from
actual values; row identities remain exact until aggregation.

## Applicability and correctness

The optimized path rejects duplicate dimension keys, shared join chains,
cross-arm predicates and unsupported expressions. Unsupported shapes use
the general executor, preserving multiplicities. The year group must come
from a different chain from the conditional dimension. The CASE false
branch must be exactly zero; its true expression and the ordinary SUM must
use the fact table only. Allocation is charged to the query budget.

The 240-test native suite passes. Focused regression cases compare output
with the general relational executor in scalar/vector and serial/parallel
modes. They cover sparse shuffled order keys, missing foreign keys,
duplicate-key fallback, aliases, negative dates, different date
representations, filters, conditional labels, empty results and slices.
All 17 focused ASan/UBSan tests pass. All22 SF1 answers also match on the
frozen module. Fresh comparisons are recorded in [Q8 results](Q8.md).

This is an original implementation using the project's existing licensed
components. No external join implementation was copied. The bounded-key
proofs, narrow integer payloads, bitmap ranks and late arithmetic gathers
extend the Q5/Q7 implementation documented in this directory.
