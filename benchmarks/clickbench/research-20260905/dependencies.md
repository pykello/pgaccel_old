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
