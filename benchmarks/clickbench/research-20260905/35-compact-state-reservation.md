# Experiment 35: compact numeric state and bounded input reservation

Keep the smaller numeric state and bounded reservation. A checked 128-bit
sum can be stored through two 64-bit words aligned to eight bytes.
Bit-casting preserves its complete representation and arithmetic. For two
numeric inputs, the shared-count state shrinks from 48 to 40 bytes.
The earlier subset did not demonstrate a clear latency improvement from
this padding change alone; its concrete benefit is smaller hash payloads.

Filtered radix input previously grew its vector repeatedly. Reserve bounded
capacity once, without constructing unused records, then charge the entire
reservation to the query budget. The measured version caps this at half
the total budget; the reviewed follow-up deducts existing planning allocations
before computing that cap, protecting small-budget selective queries.

## Full-data result

Original queries, full data, eight threads, minimum client milliseconds
of trials two and three. Q31 isolates the reservation follow-up relative
to the preceding compact-state run; Q40 also gains conditional layouts.

| Query | Before reservation | After reservation | Fresh ClickHouse | New peak allocation GiB |
| ---: | ---: | ---: | ---: | ---: |
| 17 | 1013.774 | 992.885 | 1364.565 | 2.980 |
| 31 | 293.588 | 247.470 | 292.493 | 3.373 |
| 40 | 188.018 | 44.706 | 118.877 | 1.501 |
| 41 | 8.676 | 8.564 | 25.487 | 1.492 |
| 42 | 12.899 | 11.212 | 27.087 | 1.492 |

Q31 becomes a clear win. It reserves more memory to avoid input copying;
that is a real allocation tradeoff, even though the engine does not
initialize unused capacity. The reported peak includes reserved bytes.
Q41 and Q42 remain wins but also show this reservation cost.

The measured engine is da05fe6. All 21 answers pass validation and all
83 native tests pass. The subsequent remaining-budget correction adds
a highly selective query that must fit in 32 KiB without spilling.

Evidence: conditional-reserve.jsonl, its .meta.json, and its gate.
This is original code with no added dependency. The complete workload
must be rerun before claiming the 39-query objective.
