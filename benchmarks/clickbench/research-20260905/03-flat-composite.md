# Experiment 4: flat composite lookup — keep

Replace allocated hash nodes with an original flat slot array containing a
32-bit hash tag and payload index. Payloads are contiguous and indexes survive
growth. Equality still compares complete logical grouping keys; collisions
never change answers. Owner selection uses high hash bits to avoid constraining
the low bits used by the local power-of-two lookup table.

All 37 native tests and all 30 full-data trial answers pass. Forced collisions
and allocation failure are tested. Raw tied-output differences retain their
independent order-refinement checks. No new dependency is used.

| Query | Before warm ms | Flat warm ms | Speedup | CH warm ms |
| --- | ---: | ---: | ---: | ---: |
| Q12 | 1227.440 | 1340.775 | 0.92× | 232.250 |
| Q15 | 3296.802 | 807.977 | 4.08× | 585.294 |
| Q17 | 13863.109 | 2734.386 | 5.07× | 1420.392 |
| Q18 | 14170.699 | 2840.081 | 4.99× | 456.745 |
| Q19 | 35776.802 | 5867.807 | 6.10× | 2366.156 |
| Q29 | 43213.475 | 40600.693 | 1.06× | 1951.951 |
| Q31 | 4064.579 | 794.700 | 5.11× | 315.143 |
| Q32 | 7461.196 | 1254.123 | 5.95× | 423.013 |
| Q33 | 61299.439 | 11215.244 | 5.47× | 2772.555 |
| Q36 | 9973.960 | 6600.445 | 1.51× | 317.880 |

Q31–Q33 compare against the immediately preceding inline-state experiment.
Other rows compare against the preserved pre-research baseline. Q12 is a
regression in this run (a general distinct-state query); Q29 barely changes
because expression evaluation dominates. The broad high-cardinality gains
justify keeping the table while continuing to address those cases. None of
these measured queries yet beats its ClickHouse baseline.

Raw evidence: `../docs/clickbench/research-20260905/flat-composite.jsonl`
and `flat-composite-gate.json`, with binary identity in the metadata sidecar.
