# Research references and license decisions

## Hash-table layout and aggregation

- [Abseil Swiss Tables design](https://abseil.io/about/design/swisstables):
  compact control bytes filter candidate keys before payload access. Candidate
  experiment: flat exact numeric sets instead of allocated general Cell nodes.
- [Abseil container guide](https://abseil.io/docs/cpp/guides/container): flat
  containers invalidate pointers on rehash, so pgaccel's code-to-state pointer
  caches cannot be changed to flat maps without addressing that invariant.
- [Parallel grouped aggregation in DuckDB](https://duckdb.org/2022/03/07/aggregate-hashtable):
  separate lookup metadata from stable payload blocks; partition complete groups
  for independent owner merges. Candidate experiments: compact state and
  partitioning input before building high-cardinality state.
- [Cache-Efficient Top-k Aggregation](https://www.vldb.org/pvldb/vol17/p644-siddiqui.pdf):
  candidate reading for bounded partitioned aggregation. No code has been copied.

## Bitmap proposal

- [Chan and Ioannidis, Bitmap Index Design and Evaluation](https://www.vldb.org/dblp/db/conf/sigmod/ChanI98.html):
  range and component encoding motivate the space/query tradeoff experiment.
- [Roaring RangeBitmap](https://github.com/RoaringBitmap/RoaringBitmap/blob/master/roaringbitmap/src/main/java/org/roaringbitmap/RangeBitmap.java):
  reference implementation for range selection over bit slices.
- [Claude and Navarro, The Wavelet Matrix](https://users.dcc.uchile.cl/~gnavarro/ps/spire12.4.pdf):
  compressed tree alternative; relative positions complicate intersection with
  pgaccel's original-row selection masks.
- [Intel gather/scatter guide](https://builders.intel.com/docs/networkbuilders/intel-avx-512-permuting-data-within-and-between-avx-registers-technology-guide-1668169807.pdf):
  gather/scatter handles indexed memory accesses; contiguous Boolean bitmap
  operations are the initial implementation target.

## License review

No newly incorporated dependency at this checkpoint. Original experiment code
will remain in this repository. Merely reading a paper or describing an
algorithm does not mean its authors' implementation has been incorporated.

Abseil is an eligible dependency: upstream [LICENSE](https://github.com/abseil/abseil-cpp/blob/master/LICENSE)
and the installed `/usr/share/doc/libabsl-dev/copyright` declare Apache-2.0.
If used, retain the system dependency and document its package version and
installation. The existing header and CMake package are already installed.

`ankerl::unordered_dense` is another candidate whose upstream
[LICENSE](https://github.com/martinus/unordered_dense/blob/main/LICENSE) declares
MIT. It has not been added. GPL/AGPL or source-available restricted dependencies
will not be incorporated into these experiments.
