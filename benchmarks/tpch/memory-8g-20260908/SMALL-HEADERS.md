# Small direct buffers for schema identity reads

Commit `61edd0b` adds a Header mode to DirectInput. It owns one aligned
4 KiB buffer and issues no speculative reads. Default Payload mode keeps
two 8 MiB buffers and the retained one-block read-ahead implementation.
`ColumnarTable::LoadSchema` and the identity-reading stream in `ScanDirect`
use Header mode. Projected column streams retain Payload mode.

The prior header stream fetched an 8 MiB block and could read another even
though snapshot identity validation consumes only 24 bytes. Source-group
and column-offset validation then uses seek positions, not payload reads.
Header mode still checks the descriptor, snapshot length, read result and
logical seek bounds. It remains O_DIRECT and has no buffered fallback.
Unsupported filesystem/device alignment still fails explicitly.

`DirectInput::bufferBytes()` reports the instance's owned allocation. Scan
statistics now charge 4 KiB for the identity reader plus 16 MiB per projected
payload stream. Actual cgroup memory also includes allocations, thread
stacks, query state and file-cache charge; static buffer counts alone do
not establish compliance with the total memory target.

All 291 native tests, Python integration and all 22 typed SF1 answers pass.
New checks cover a 24-byte header read fetching only 4 KiB, boundary seeks,
short tails, and rejection of corrupt/truncated snapshot identities.

| Query/build | First call s | Warm median ms | Total peak GiB | Recorded load GiB |
|---|---:|---:|---:|---:|
| Q1 control | 8.626 | 419.640 | 6.969 | 6.914 |
| Q1 small header | 8.818 | 419.500 | 6.953 | 6.898 |
| Q2 control | 1.783 | 67.997 | 1.350 | 1.276 |
| Q2 small header | 1.475 | 67.271 | 1.350 | 1.260 |
| Q13 control | 28.727 | 549.078 | 1.811 | 8.829 |
| Q13 small header | 27.682 | 547.945 | 1.848 | 8.814 |

Each run executes nine calls in a fresh process with MemoryMax=8G and
MemorySwapMax=0. Q2 has a matching fresh control run after the candidate;
Q1 and Q13 use the preceding read-ahead controls. These are individual runs,
not confidence intervals. Q2 first-call latency improves about 17.3%, Q13
about 3.6%; Q1 is about 2.2% slower in the preserved sample. Keep the smaller
header footprint and Q2 benefit without claiming a universal latency gain.

All 27 candidate SF100 calls and nine Q2 control calls pass their typed
oracle. The record's load bytes cover the existing preparation metric, not
device-wide traffic or all constructor schema reads. Warm calls reuse the
prepared statement-specific input; their medians are kept as measured.

Evidence: [Q1](native-direct-small-header-q1-sf100.json),
[Q2](native-direct-small-header-q2-sf100.json),
[Q13](native-direct-small-header-q13-sf100.json),
[Q2 control](native-direct-small-header-control-q2-sf100.json),
[SF1 gate](auto-projection-sf1-small-header-gate.json).

This follow-up does not replace the frozen
[complete read-ahead sweep](TRANSITION-READ-AHEAD.md). Matching resident
latency for changing SQL in 8 GiB remains unproven. The query's initial
schema discovery still happens on each new SQL statement; it now avoids
large speculative payload transfers for those identity reads.
