# Native dictionary validation during direct preparation

A temporary loader profile on dense-retention commit `3a395d0` attributes
5.536 seconds of SF100 Q1 preparation to dictionary loading. Code-range
and dictionary-coverage validation account for 2.887 seconds of that total;
dictionary value decoding accounts for 0.088 seconds. These are nested
elapsed measurements, not additive independent phases. Instrumentation
runs once per stripe and is absent from production source.

The instrumented first call takes 12.450 seconds, with a 416.670 ms warm
median. All nine answers pass under the 8 GiB cap with zero permitted swap.
This identifies CPU validation as a useful next target; it does not measure
how much asynchronous input could overlap the remaining loading work.

Reproduction artifacts: [instrumentation](native-validation-profile.patch)
and [profile evidence](native-direct-native-validation-profile-q1-sf100.json).
The evidence records the frozen module hash and raw result hash.
