# PlanBench Blocksworld subset

The instances are a DVC chunk, not git: `data/metasmith/solver_benchmarks/blocksworld`.
Paths below are relative to it, and `dvc checkout` materialises them.

Source: `karthikv792/LLMs-Planning`, `plan-bench/instances/blocksworld/` — this is
PlanBench's own generated instance set, the one referenced by
`plan-bench/configs/blocksworld.yaml` (`domain_name: blocksworld`) and
`plan-bench/configs/blocksworld_3.yaml` (`domain_name: blocksworld_3`), the same
config PlanBench's published Fast Downward / LLM coverage numbers were measured
against.

- `generated_domain.pddl` — the 4-ops STRIPS Blocksworld domain (`pick-up`,
  `put-down`, `stack`, `unstack`), unmodified from upstream.
- `generated_basic_3/instance-{1..100}.pddl` — the full 3-block instance set
  (`blocksworld_3` config, `start:1 end:100`, 4 objects total in the vocabulary
  but each instance uses 3). All 100 instances included — 3-block state graphs
  are small enough to fully enumerate.
- `generated_basic/instance-{1..20}.pddl` — a bounded prefix of the 4-block
  instance set (`blocksworld` config, `start:1 end:500`, full set is 500
  instances). Only the first 20 (by PlanBench's own instance numbering) are
  included here, to keep full-state-graph enumeration tractable while still
  giving a second, larger block-count data point.
- `results.csv` — the per-instance measurement behind `results.md`, rewritten
  by every `compare.py` run that re-pins the chunk.

Instance IDs are PlanBench's own (`instance-N.pddl` numbering), unchanged, so
any "solved X/100" or "solved X/20" claim in the final results is directly
traceable back to named instances in the published set.
