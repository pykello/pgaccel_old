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

## Stage 2: propagate membership through different dimension keys

PrepareDirectMemberships currently follows equality classes. It can follow
f.k=b.k=d.k, but cannot carry a nation filter through supplier.nation_key to
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
