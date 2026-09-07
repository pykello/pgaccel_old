# Proposed native file format

Date: 2026-09-07. This is a design proposal, not an implemented format.

Evolve the native snapshot into an immutable, independently readable
column-chunk format while preserving the compact layouts that make PGAccel
fast. This supports the residency and index work in the
[staged improvement plan](20260907-staged-improvements.md).

## Current layout and motivation

The current format records column offsets and supports selecting columns.
However, it loads every stripe of each selected column, reconstructs string
dictionaries as `std::string` objects, and ties serialization to the native ABI.
These are the first things to change.

| Area | Suggested design | Benefit |
|---|---|---|
| File structure | One self-contained file per immutable table segment, with a footer directory | Atomic publication and direct access |
| Addressing | Offset, stored length and decoded length for every column/stripe buffer | Load individual stripes without scanning preceding data |
| Strings | Contiguous byte buffer plus offsets | Avoid per-string allocations; permit borrowed views |
| Integers | Preserve narrow arrays; add optional frame-of-reference and bit packing | Smaller storage without forcing every scan through decompression |
| Dictionaries | Keep local dictionaries and explicit code widths | Preserve existing fast filtering/grouping |
| Compression | Independently compressed buffers, including an uncompressed option | Choose between direct access and lower I/O |
| Metadata | Logical types, encoding versions, row counts, statistics and checksums | Portable interpretation and safe pruning |
| Indexes | Optional versioned sidecars tied to exact snapshot identity | Rebuild indexes without rewriting base data |

## Possible layout

```text
Header: magic, version, feature flags, segment identity

Column A
  Stripe 0: validity | dictionary offsets | dictionary bytes | codes
  Stripe 1: ...
Column B
  Stripe 0: validity | narrow numeric values
  Stripe 1: ...

Footer
  schema
  stripe row ranges
  buffer offsets, lengths, encodings, checksums
  per-stripe statistics
  optional index references

Trailer: footer length, magic
```

Atomic publication still requires a writer protocol: finish and validate a
new immutable file before publishing it. The format alone does not make a
partially written file safe to expose.

## Design choices

### 1. Keep 64K execution stripes initially

Separate execution stripe size from compression-block size and file size.
Experiment with those independently instead of changing all three together.

### 2. Make strings directly addressable

Use 32-bit offsets within bounded chunks, with explicit size limits. Dictionary
labels become views into owned or pinned byte buffers. This could reduce
allocation overhead; its actual memory savings need measurement.

### 3. Support direct scanning and decoded caching

Align uncompressed buffers for SIMD and define safe tail handling. Compressed
buffers decode into a bounded cache. Memory mapping alone will not bound
residency, and compressed data cannot be scanned as if already decoded.

Ownership must keep buffers pinned while queries retain string views or other
references into them. Eviction cannot invalidate active references.

### 4. Persist structural properties carefully

Sortedness, uniqueness and repeated-key structure could avoid repeated join
proofs. Bind them to the exact data and validate their provenance before
trusting them. A checksum detects corruption; it does not prove that a
uniqueness claim was computed correctly.

### 5. Reserve validity bitmaps now

Keep dictionary code zero available for an actual value. Designing the
physical representation for NULLs does not require enabling nullable SQL
immediately. SQL support still needs end-to-end semantic work.

## References

Arrow's offsets/buffers, alignment and random-access IPC design are useful
references. Parquet's metadata directory provides another model for locating
independently readable data. Borrow those principles while preserving
PGAccel's execution-oriented encodings.

- [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html)
- [Parquet metadata](https://parquet.apache.org/docs/file-format/metadata/)

## Implementation order

1. Portable headers and a chunk directory.
2. Contiguous string storage.
3. Lazy reads with pinned ownership.
4. Optional compression.
5. Persistent indexes and structural proofs.

The first version should retain today's numeric/code encodings. That isolates
the benefit of lower allocation overhead and selective residency before adding
decoding complexity. Preserve old snapshot readability through an explicit
compatibility reader or conversion tool, rather than silently reinterpreting
old files.
