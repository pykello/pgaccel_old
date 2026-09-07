# Final all-query reproduction

The frozen native module, source revision, SHA256 and validation evidence
are recorded in [CHECKPOINT.json](CHECKPOINT.json). The same module is used
for every query at both scales. Initial data loading is outside warm query
timings but remains in the load metadata and service memory measurements.

Run from `/home/hadi/projects/pgaccel_old`. Copy the three `run-final-all1*.py`
scripts to `/tmp` before invoking `python3 /tmp/run-final-all1.py`, since the
driver references the scale scripts there. These scripts retain the exact
local paths, databases and service units used for this attempt. Preserve
existing evidence or use a new attempt name before rerunning.

The driver runs engines sequentially, validates all trials, and checks
warm medians after excluding only trial one. The original driver requires 4x for 16 queries
and 2x for Q7/Q8/Q9/Q10/Q21/Q22. It checks the module hash across scales.
SF100 services have a 192 GiB cgroup cap with swap disabled. Do not run
builds or tests during any timed engine phase.

After both scales finish, run `report-final-all1.py` from the repository
root to generate the Markdown and compact JSON reports. The script reads
the original frozen checkpoint path and raw JSONL files. The raw logs and
SF100 command/resource records live in the parent directory. Included test
logs cover the final module's native suite, focused sanitizers and Python
integration checks; earlier focused regression logs remain in each query's
reproduction folder.

## Actual completion and revised threshold

The original driver stopped after SF10 at the old 4x gate. On 2026-09-07
the user accepted 3x for those 16 queries and requested no rework. We kept
the complete SF10 run and invoked only `run-final-all1-sf100.py` to finish.
The original driver is preserved as historical evidence; the current
`report-final-all1.py` checks the revised 3x/2x targets and validates all
trials and the module hash at both scales.

Generate timing tables with `report-final-all1.py`, then append existing
memory samples with `add-final-memory.py`. Neither report generator runs
queries. `finish-final-docs.py` records final status and interpretation;
its README edits are intended as a one-time publication step.
