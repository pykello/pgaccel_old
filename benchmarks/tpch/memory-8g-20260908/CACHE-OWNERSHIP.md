# Bounded original-block cache foundation

This checkpoint adds snapshot identities and cache ownership. **The cache is
not connected to direct scans or query execution.** It changes neither the
measured SQL preparation boundary nor the latest SF100 performance results.

## Identity

`src/native_snapshot.hpp` defines a validated snapshot UUID plus an opened-file
stamp: device, inode, length, modification time and change time, including
nanoseconds. `DirectInput` captures the stamp with `fstat` on its own descriptor.
`ColumnarTable::LoadImpl` publishes it only after matching the versioned payload
magic and UUID against the metadata. Legacy files and generic buffered streams
have no cache identity.

`src/native_block_cache.hpp` keys blocks by this identity, physical column start,
stripe number and logical type number. A missing identity refuses admission and
lookup. A replacement snapshot or changed stamp produces different keys; old
entries remain charged until eviction. The key names original source blocks,
not selected rows, predicate representatives or any query result.

This assumes snapshots remain immutable during a query. Capturing a stamp does
not synchronize concurrent writers. Scan integration must check the identity of
any separately opened payload descriptor before using it with a header-derived
key. It must also preserve column layout and stripe boundaries. The existing
same-SQL prepared-table reuse does not reopen files; this foundation does not
add automatic refresh to that path.

## Ownership and admission

`NativeBlockCache` uses a list for LRU order and an ordered map for lookup. A hit
moves an entry to the front; admission evicts from the back until the entry fits.
The byte charge includes `NativeColumnCharge`, both lookup nodes, and explicit
allocation allowances. An over-budget or unsupported block is refused before
mutating cache contents. Shrinking the limit evicts immediately. Clearing the
cache releases all cache-held references.

The loader must supply validated, original owned blocks. The charge helper
checks supported ownership/layout, not source provenance. A repeated key keeps
the already-admitted block rather than replacing it with caller-provided data.
Lookup returns a shared pointer to const data. Eviction releases cache ownership;
a query holding another pointer keeps that block alive. Consequently the cache
charge is neither total process memory nor an RSS upper bound. Cache metadata,
active query state, allocator retention and I/O buffers must still fit the
external 8 GiB limit together. The cache is serialized by its caller, not
internally synchronized.

The proposed prototype budget remains 512 MiB. The preceding
[COLUMN-ADMISSION.md](COLUMN-ADMISSION.md) measured 479.175 MiB of native block
charges for the 17 small reused columns, before these entry charges. This
checkpoint does not remeasure the combined cache and query process.

## Validation

- All 298 native tests pass.
- Python native integration passes.
- All 22 SF1 queries match their typed reference answers in
  [auto-projection-sf1-cache-identity-gate.json](auto-projection-sf1-cache-identity-gate.json).
- New tests cover stable identity, timestamp changes, rewritten snapshots,
  rejected corrupt headers, uncached legacy files, byte limits, LRU order,
  repeated keys and query-held references surviving eviction.

There is no new SF100 timing sweep: this cache is not used by that execution
path. [TRANSITION-STRINGS.md](TRANSITION-STRINGS.md) remains the latest complete
performance and memory comparison.

## Next integration

Supply original blocks before row selection and string/date transformation.
Admit small source columns initially so large fact scans cannot evict every
reused dimension. Cached blocks must remain immutable even when an existing
mutable table API receives data derived from them.

Variable-length native stripes make partial hits nontrivial: skipping a cached
stripe also requires its validated ending byte offset to locate the next miss.
Either retain that offset with each block or reuse only complete cached columns.
Do not assume stripe numbers imply fixed byte offsets. Preserve early-stop and
malformed-input behavior, and distinguish cache hits from physical direct-read
bytes in measurements. Check original row alignment across different SQL texts,
then run the full transition sequence under the 8 GiB cgroup.

Input still uses O_DIRECT and bounded asynchronous read-ahead with two 8 MiB
buffers per active payload stream. io_uring is not implemented. Reducing repeated
input and dictionary construction is the current measured opportunity; changing
I/O submission needs its own end-to-end comparison before being retained.
