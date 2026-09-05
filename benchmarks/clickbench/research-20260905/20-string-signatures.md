# Experiment 20: conservative trigram string signatures

Keep this as an experimental indexed path. Each global string label gets
a 128-bit signature of its byte trigrams. A missing required bit proves
that a substring is absent. A possible match always receives the original
exact byte comparison. NOT LIKE reverses only the exact logical result.
Patterns with internal wildcards or escapes and needles shorter than three
bytes keep their existing evaluation.

For '%google%', the necessary grams are goo, oog, ogl, and gle.
Hash collisions can admit extra candidates, but cannot suppress a true
match. This is an original one-hash trigram signature implementation.
The classic small approximate filter followed by an exact secondary test
is research context: [Bloom, 1970](https://www.cs.princeton.edu/courses/archive/spr05/cos598E/bib/p422-bloom.pdf).
No code from that publication or an external library is included.

## Full-data subset

Engine 26f3db7. Eight threads, original SQL, minimum client latency of trials
two and three. The preceding membership run is unindexed, so the difference
includes auxiliary dictionary access and signature storage.

| Query | Membership unindexed | Signature indexed | ClickHouse |
| ---: | ---: | ---: | ---: |
| 21 | 212.544 | 161.782 | 336.286 |
| 22 | 217.762 | 168.190 | 96.774 |
| 23 | 343.636 | 270.440 | 537.647 |
| 24 | 216.128 | 163.205 | 96.147 |

Q22 and Q24 remain losses. Q21 and Q23 improve existing wins. No new
overall score is inferred by mixing these selected measurements.

The complete indexed load costs 47.235 seconds for native
loading plus 119.491 seconds for index construction.
Retained auxiliary indexes total 19,380,466,164 bytes
(18.049 GiB), versus 17.123 GiB before signatures.
Loaded peak RSS is 55,823,642,624 bytes.
These costs are in packed-signatures.meta.json and are not query latency.

All 27 combined-run answers validate. The native suite has 67 passing tests.
Signature tests enumerate actual substrings of seeded arbitrary byte strings
and check LIKE/NOT LIKE with embedded NULs, Unicode, case differences, short
needles, and both AVX modes. Exact verification remains mandatory.
