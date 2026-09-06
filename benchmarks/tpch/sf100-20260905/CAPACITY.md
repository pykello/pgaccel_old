# Q20 capacity recheck — in progress

The baseline rejected Q20 at the 64 GiB query-allocation limit. The separate
recheck uses the same frozen binary, source SQL, native data, and 192 GiB
service cap, with an 80 GiB query allowance and three trials. It begins only
after all baseline query engines exit. Its timings and memory will be
reported separately; the baseline failure remains visible.

Raw evidence: `q20-capacity-80g.jsonl`, `q20-capacity-80g.log`,
`q20-capacity-resources.jsonl`, and `q20-capacity-phase.json` in this folder.
