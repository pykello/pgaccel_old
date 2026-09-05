# Additional dependency license record

## Abseil C++

- Used for: flat exact numeric hash sets, with pgaccel's query allocator.
- License: Apache-2.0.
- Upstream: https://github.com/abseil/abseil-cpp
- License: https://github.com/abseil/abseil-cpp/blob/master/LICENSE
- Installed package: `libabsl-dev`, version `20220623.1-3.1ubuntu3.2`.
- Local license verified: `/usr/share/doc/libabsl-dev/copyright`.
- Ubuntu installation: `sudo apt-get install libabsl-dev`.
- Build integration: system CMake package `absl`, target `absl::flat_hash_set`.

No upstream source has been vendored or relicensed. The installed package
retains its copyright and license notices. The dependency is permissively
licensed and does not require licensing pgaccel under a copyleft license.
The API invalidates references on rehash, so it is used only where no cached
pointer into its slots is retained.

## Decision

Rejected after the allocator-exception test aborted. The CMake dependency was
removed; this is a historical license review, not an active requirement.
See `01-abseil-rejected.md`.

## Retained hash primitive: SplitMix64 finalizer

IntegerHash uses the standard SplitMix64 final mixing steps, rather than a
newly invented mixing function. The [author's implementation](https://prng.di.unimi.it/splitmix64.c)
credits Sebastiano Vigna (2015), dedicates the software to the public domain,
and explicitly permits use, copying, modification, and distribution for any
purpose. This license notice was checked directly during the final audit.
No external library is linked for this primitive.

The surrounding exact sets, flat maps, indexes, group layouts, and query
executors are implemented in this repository. No GPL/AGPL or restricted
source-available algorithm dependency was added. Abseil remains a rejected,
removed build dependency; its installed development package is historical.
