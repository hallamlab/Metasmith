# metasmith solver (rust engine) vs. pyperplan on PlanBench Blocksworld

Measured 2026-08-19. The coverage claim is `check_plan`-validated and holds independently of the machine; the wall-time columns do not.

Instances: PlanBench's own generated Blocksworld set (`karthikv792/LLMs-Planning`, `plan-bench/instances/blocksworld/`), grounded with pyperplan's parser, fully state-graph-expanded, and packaged as pure-state metasmith `SolverProblem`s (see `MANIFEST.md`, `graph_expand.py`). Only the Rust solver engine is exercised here -- the Python fallback is not part of this comparison.

## blocksworld_3 (3 blocks, n=100, 0 errored)

| solver | coverage | mean plan length | mean wall time (s) |
|---|---|---|---|
| pyperplan | 100% | 5.0 | 0.0014 |
| metasmith (rust) | 100% | 7.0 | 0.0058 |

## blocksworld_4 (4 blocks, n=20, 0 errored)

| solver | coverage | mean plan length | mean wall time (s) |
|---|---|---|---|
| pyperplan | 100% | 7.3 | 0.0052 |
| metasmith (rust) | 100% | 9.3 | 0.0141 |

## Verdict

Published bar (PlanBench, Fast Downward on its full Blocksworld set): ~100% coverage. This run: pyperplan 100%/100% (3-block/4-block), metasmith-rust 100%/100% (all metasmith numbers are `check_plan`-validated, not just "the search returned").

pyperplan runs in-process while the Rust engine round-trips through a subprocess per solve, so the wall-time columns are not perfectly apples-to-apples -- read them as orders of magnitude, not to the millisecond.
