# Q13 string preparation profile

On small-header commit `5d8f644`, the instrumented SF100 Q13 first call takes
28.622 seconds. Orders preparation takes 28.020 seconds: selection is 0.032,
compaction 0.165 and predicate transformation 5.126 seconds. String dictionary
loading totals 15.007 seconds, including 13.205 seconds decoding 149459270
dictionary entries and 0.233 seconds validating dictionary codes.

These are nested elapsed measurements, not independent CPU totals. Decoder
time includes its allocations and any read waits. The remainder of table
preparation includes other column loading, allocation/destruction and scan
bookkeeping; this profile does not separately attribute those costs.

All nine SF100 answers pass under the 8 GiB cap with zero permitted swap.
Warm median is 549.640 ms and live total peak 1.859 GiB. Temporary timing
instrumentation is absent from production source and the installed module.
The [patch](string-preparation-profile.patch) and
[evidence](native-direct-string-prep-profile-q13-sf100.json) preserve the
profile's frozen module hash and raw answer hash.

The next experiment reuses released string dictionary slots between stripes.
It leaves the decoder and predicate evaluator intact, avoiding a new streamed
predicate implementation before measuring simpler allocation reuse. A consumer
that retains a column must prevent reuse of that column's labels.
