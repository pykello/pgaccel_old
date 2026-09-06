# Q15 grouped-total iterations

Warm client medians exclude only trial 1; every answer matches DuckDB.

| Checkpoint | Warm median ms | Warm min ms | Warm max ms | Query peak MiB |
|---|---:|---:|---:|---:|
| 537c8aa | 1731.706 | 1718.763 | 1923.232 | 191.832 |
| 918e691 | 11.764 | 11.581 | 12.765 | 7.737 |

[Design and staged changes](Q15-DESIGN.md). The frozen candidate passes
all 193 native tests, eight focused ASan/UBSan tests and all 22 SF1 answer
comparisons. Fresh 15-trial SF10 confirmation is required before SF100;
the nine-trial iteration alone is not a promotion result.
