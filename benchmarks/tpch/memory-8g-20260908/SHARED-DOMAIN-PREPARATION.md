# Shared join domains during direct preparation

The latest complete sweep still has Q11 slightly above its saved warm time.
Its parent and scalar subquery share the same fact, joins and filters. Existing
preparation rejected all scalar scopes, so it could not remove fact rows that
cannot join to the filtered dimension domain. The executor itself already
re-evaluates those joins and the scalar total on each call.

## Stage 1: retained scope-equivalence proof

`3a31c4e` adds DirectSharedJoinScopes in `src/direct_scope_filters.hpp`.
It recursively requires the same ordered bound table list, literal filter
clauses and direct equality predicates in every scalar scope. The predicate
check accepts exact literal filters and bound column equality; unsupported
expressions decline this optional preparation. Correlated scalar keys,
outer/existence joins, membership subqueries and materialized sources also
decline. A differing filter or unfiltered use of the same table prevents
shared pruning.

After proof, `src/direct_query.hpp` clears scalar descriptors only in the
preparation copy, enabling the existing necessary membership calculation.
The original parsed query and its scalar queries remain intact. Execution
rebinds and executes the original SQL. Dimensions retain their full key
sequences; only the chosen fact input can lose rows. The existing unique-use
check for the fact table still applies.

All 285 tests, 22 SF1 answers and Python integration pass. The new test checks
both answers and prepared row counts for identical, differing and missing
child filters. [SF1 evidence](auto-projection-sf1-shared-scope-gate.json).
This stage alone does not provide a measured Q11 speedup.

## Stage 2 design: propagation through different dimension keys

Before this stage, PrepareDirectMemberships followed only equality classes. It could follow
f.k=b.k=d.k, but could not carry a nation filter through supplier.nation_key to
supplier.supplier_key and then to partsupp.supplier_key. Q11 needs the latter.

Implement bounded recursive necessary domains while preserving the original
query's duplicate expansion and all predicates. When forming allowed source
keys, intersect the source's literal filters with memberships derived from
other dimensions. Exclude previously visited table aliases to terminate
cycles, and cap traversal depth so complex joins cannot cause unbounded
preparation work. Unsupported or over-budget branches must decline optional
pruning, not drop rows or fail a previously supported query.

Share the existing bitmap byte allowance across recursion. Child constraints
consume that allowance while the parent bitmap is built; return their charge
when those temporary constraints are destroyed. Do not divide the root cap
unconditionally: large direct domains that already fit, such as order-key
bitmaps, must keep their previous allowance. Avoid a separate full allowance
per recursion level, which could multiply peak scratch memory.

Test different-key chains, duplicate dimension keys, no matching dimensions,
cycles, tight bitmap budgets, and a scalar child with a different domain.
Dimensions must remain intact. Then validate all SF1 answers and measure Q11
at SF100 with first preparation, warm execution, total memory and answer
checks. Check other affected preparation paths before a full capped sweep.


## Stage 2 retained implementation and evidence

`32e7afe` implements the design above with three traversal levels and a visited
alias path. Before collecting a source's allowed keys, it derives necessary
memberships on that source's other join columns. These constraints intersect
its literal filters. The source table stays intact; the original query still
expands duplicate joins and checks every predicate.

A single remaining-byte counter covers all live membership bitmaps. Recursive
results consume it until their parent finishes using them; an RAII charge
release handles successful and declined paths. A parent that cannot fit simply
declines that optional bitmap. The test checks both surviving bitmap bytes and
restored remaining allowance at 0, 8, 16 and 64-byte limits, including a cycle.
This is a scratch-bitmap allowance, not a replacement for the total cgroup cap.

All 286 tests, all 22 SF1 answers and Python integration pass. Tests include
same and differing scalar scopes, different-key chains, duplicate dimensions,
no qualifying dimension rows, cycles and byte limits. Dimension row counts
remain intact. The SF100 measurements below each pass nine typed answers in
fresh 8 GiB zero-swap processes.

| Query | Warm median ms | First call s | Total peak GiB |
|---|---:|---:|---:|
| Q11 | 8.844 | 1.068 | 0.098 |
| Q3 | 342.229 | 11.331 | 2.386 |
| Q5 | 261.495 | 11.380 | 2.067 |

Q11 now has substantial margin below the saved 30.496 ms, rather than a
fractional improvement susceptible to ordinary run variation. Q3 and Q5 also
remain below their saved times, though their individual warm medians are
slower than the preceding complete sweep. Keep both sets of evidence. The
new module still needs the complete workload and transitions checked together.
Warm calls reuse SQL-specific prepared input; these results do not establish
changing-query execution at the old fully resident latency.

Evidence: [Q11](native-direct-chain-domain-q11-sf100.json),
[Q3](native-direct-chain-domain-q3-sf100.json),
[Q5](native-direct-chain-domain-q5-sf100.json),
[SF1 gate](auto-projection-sf1-chain-domain-gate.json).
