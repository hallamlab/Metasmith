# profiling

Where an unpinned solve of `metagenomics_from_paired_reads` spends its time and memory. The finding
is `data/metasmith/plans/08-where-the-unpinned-solve-goes.md`; this is the apparatus.

**`perf` does not work on this host** — `perf_event_paranoid` is 4 — and there is no `cargo`, so the
engine cannot be profiled directly. The engine's wall-clock and RSS are measured on the binary; the
call counts come from the Python solver, which answers the same `solve_by_mcts` and is a line-for-line
port. Read its counts, not its times: cProfile roughly doubles this workload and inflates
high-call-count functions.

    dump_wire.py <arm> <out.json>     encode one arm of duplicate_work.py to the wire, once
    ladder.sh <arm> <iter> <refine>   run the binary on it and report wall, peak RSS, plan size
    cprofile_arm.py <arm> <refine>    the same arm on the python solver, under cProfile
    branching2.py <req> <rep> <name>  what one refiner iteration enumerates, per step
    objective.py <req> <rep> <name>   score_node's objective on a returned plan, from the wire alone
    refine_matters.py                 every shipped template at max_refine 256 vs 0, by fingerprint
    refine_matters_gen.py             the same over CORPUS + STRESS_CORPUS

The arms come from `../duplicate_work.py`: `shipped`, `unpinned`, and the partial pins between them.

**CAUTION** `branching2.py`'s slot cross-product under-predicts what the generator actually
enumerates — 335 against a measured 110,866 for the unpinned plan. The measurement is the authority;
where the extra candidates come from is not settled, and anyone repairing the refiner needs to know.
