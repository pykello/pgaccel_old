# Q9: profit aggregation through bounded repeated-key joins

The implementation is at `c2321f0`; both benchmark gates pass; see [Q9 results](Q9.md).
The revised acceptance gate is 2x faster than both DuckDB and ClickHouse
at SF10 and SF100 with validated answers and full client timing.

Q9 needs the exact (part, supplier) join to partsupp and a cost operand from
that table. A single unique-key payload is insufficient: the part key can
have several supplier rows and duplicate matching pairs must preserve SQL
multiplicity. It also groups by supplier nation and order year.

The implemented path is:

1. Prove part, supplier, order and nation key uniqueness using existing
   actual-data checks. Recognize the two equality classes connecting the
   fact to partsupp, not any table or column spelling.
2. Prove partsupp consists of fixed-size repeated part-key runs using the
   existing RepeatedKeyIndex. The fanout is inferred from the data and
   verified for every row; do not assume four rows from the TPC-H schema.
   Unsupported ordering, fanout or stripe boundaries retain fallback.
3. Apply part filters once. Carry selected-part identities, order-year codes
   and supplier-nation identities to the fact scan. Q8's year mapping and
   bounded payload support are reusable.
4. For a surviving fact row, compare its supplier key against every entry
   in the proved bounded part run. Preserve all matching entries, including
   duplicate pairs. Evaluate partsupp filters before contributing.
5. Gather fact arithmetic once, then account for each exact matching cost
   row with the existing decimal scale rules. Accumulate wide sums by
   nation value and order year in private worker arrays.

This first implementation probes the existing bounded runs directly. If
random traffic dominates, compare copying selected supplier/cost runs into
a compact query-local layout. Retain the copy only if its construction and
cleanup are included and complete-query timings improve. Reject any
unproved assumption about supplier-key formulas or generated key patterns.

Reference regressions cover duplicate pairs, missing pairs, negative and
sparse keys, shuffled stripes, extra cost-table filters, differing nation
labels, year boundaries and decimal scaling. They confirm fallback when the run proof fails and compare exact output
against the general executor in scalar/vector and serial/parallel modes.
All 244 native tests and 10 focused ASan/UBSan tests pass. All22 SF1
answers match on the frozen module.

For duplicate matching pairs, the executor computes each row's revenue
minus its cost-times-quantity separately, using checked wide arithmetic
and the existing decimal scales. It does not combine cost rows first,
which could introduce different intermediate overflow behavior.

The repeated-key proof infers a power-of-two fanout up to 16, verifies
every stored key and accepts shuffled stripes only when their verified
ranges form the required sequence. No generated-key formula is assumed.
This is an original extension of existing project components; no external
join code or newly licensed dependency was introduced.
