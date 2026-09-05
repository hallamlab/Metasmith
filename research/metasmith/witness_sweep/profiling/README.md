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
    refine_matters.py                 every shipped template at max_refine 256 vs 0, seed 42 only
    refine_matters_gen.py             the same over CORPUS + STRESS_CORPUS
    refine_seeds.py                   the same over four seeds -- the one that found the answer
    refine_earns.py                   where the refiner earns its budget, and how much it needs
    refine_budget.py                  are budgets of 4/8/16 distinguishable from 256? (55 pairs)
    relation_probe.py                 why each candidate is rejected, and how many are really admissible

The arms come from `../duplicate_work.py`: `shipped`, `unpinned`, and the partial pins between them.

**CAUTION** Do not answer "does the refiner do anything" at one seed. `refine_matters.py` compares
256 against 0 at seed 42, finds no difference on 22 cases, and is *wrong* as a general claim:
`isolate_assembly_from_long_reads` goes 13 steps to 11 at seed 7 and to 9 at seed 99. Use
`refine_seeds.py`.

**CAUTION** `branching2.py`'s slot cross-product under-predicts what the generator actually
enumerates — 335 against a measured 110,866 for the unpinned plan. The measurement is the authority;
where the extra candidates come from is not settled, and anyone repairing the refiner needs to know.

**The rejections have a measured cause, and it is not the one this directory first recorded.**
`relation_probe.py` attributes every rejection on all eleven templates and both metagenomics arms to
one thing: `validate_node` decides ancestry over the step graph, a branched given application carries
`used == {}`, so every anchor bound to a given fails. No other cause appears. Under the relation the
specification uses, 40 of the unpinned workflow's 110,866 candidates are admissible and 1 of the
shipped workflow's 755, and the +209 and +207 improvements on `fosmid_inserts_from_pooled_reads` and
`ecspr_survey_from_pooled_reads` come back.
