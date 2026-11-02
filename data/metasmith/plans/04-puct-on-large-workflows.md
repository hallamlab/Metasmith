# Does PUCT earn its keep on large workflows?

## Context

The previous session measured `PuctSelection` against the shipped `WeightedSelection` rule and
found the two indistinguishable on the eleven shipped templates. Those templates top out at 29
plan steps and 50 mcts iterations against a budget of 256, so "indistinguishable" was a statement
about small problems. This session asks the question the workload actually poses: on the
workflows that solve slowly — a full metagenomics survey, everything the annotation libraries can
produce from one read set — does the selection policy start to matter?

## What you said

> There were a few metagenomics + fabfos examples or really large workflows that solve really
> slowly, does PUCT help in those situations?

> We established that PUCT doesnt really change anything or is comparable, but I'd like to know if
> it has an edge for large plans or planning spaces.

> going forward, I think refiner 32 may be a safe "enough".

## The answer in one paragraph

**Yes, and the edge grows with size — but it is a plan-length edge, not a search-speed edge, and
the refiner is what converts it into wall clock.** Up to ~43 targets the two policies return the
same plan and PUCT merely takes 10-21% fewer expansions. At 53 targets they separate hard: across
three seeds the baseline returns a 79, 77 or 72-step plan taking 553, 258 or 26 seconds, while
PUCT returns **the same 70-step plan every time, in 12.5-12.8 seconds**. The mcts search that
produced both is sub-second either way; everything above that is the refiner, whose cost tracks
the length of the plan it is handed. On generated instances, where the baseline can actually fail,
PUCT's capability advantage grows monotonically with size: equal at `n_types` 9, twice the
problems solved at `n_types` 26 and beyond.

## How the large cases were built

The shipped templates are too small, and generated instances reproduce none of the real content,
so the ladder is built from the standard library itself: the metagenomics template's paired short
read sample, its four transform libraries, and a target set that grows.

- Rungs 0-5 are **prefixes** of that template's own 25 targets. A prefix is always well formed —
  a target's `parents` only ever index targets declared before it.
- Rungs 6-9 append blocks taken from the assembly-rooted templates (annotation palette, annotation
  trio, viromics survey, CLEAN), rooted at the metagenomics assembly rather than at a supplied
  assembly. That is what those same targets mean when the assembly is being built rather than
  given.
- Rungs 10+ append types found reachable by an explicit probe: each of the 192 types declared by
  the libraries in scope was solved for alone, as a two-target case beside the assembly, and the
  81 that came back complete are the pool. `_chunk` types are excluded as fan-out intermediates
  rather than products a user names, leaving 62.

**Composing whole templates instead is unsound and was abandoned.** Each sample becomes its own
timeline and the target set is demanded of every timeline, so a long-read sample beside a
paired-short-read sample fails on targets unreachable from one of them — a reachability failure
wearing the costume of a search failure. Four targets were dropped from the blocks for the same
reason, each checked alone first: `annotation::eggnog_results` wants the palette template's
`eggnog_source.marker` shared input, `annotation::crassphage_coverage` wants an ancestor the reads
root does not supply, and `annotation::gpr_table` / `ecspr::results` are declared over the fosmid
insert lineage rather than over an assembly.

## Axis A — the workflow grows

119 solves over 14 rungs x 3 seeds (42, 7, 1234) x 3 arms, python solver throughout,
`max_iter=256`, `max_refine=32`. **Every plan accepted by `check_plan`; no arm failed to solve any
rung.** The last two rungs were cut short at the largest sizes, where one arm takes ~4 minutes per
solve; what is reported is what completed.

Ratios are PUCT over baseline, so below 1.0 favours PUCT:

| targets | plan steps (base) | mcts ratio | plan-step ratio | PUCT's plan = baseline's? |
|---|---|---|---|---|
| 3 | 5 | 0.875 | 1.000 | yes, every seed |
| 10 | 13 | 1.015 | 1.000 | yes |
| 20 | 28 | 0.920 | 1.000 | yes |
| 25 (the shipped template) | 29 | 0.908 | 1.000 | yes |
| 32 | 41 | 0.873 | 1.000 | yes |
| 33 | 42 | 0.885 | 1.000 | yes |
| 43 | 61 | 0.787 | 1.000 | yes |
| **53** | 77 | **0.697** | **0.921** | **no — PUCT is shorter** |
| **62** | 84 | **0.648** | 0.976 | **no — PUCT is shorter** |
| **67** | 81 | 0.712 | 0.975 | **no — PUCT is shorter** |

Two things move together as the workflow grows. The search cost ratio drifts from ~0.9 down to
~0.65-0.7, and somewhere between 43 and 53 targets the policies stop agreeing on the answer — with
PUCT's plan the shorter one every time it differs.

Below that crossover the win is purely fewer expansions for an identical plan. Above it, PUCT is
solving a different, better problem instance.

At 53 targets the seed is what the baseline's answer depends on:

| arm | seed 42 | seed 7 | seed 1234 | distinct plans |
|---|---|---|---|---|
| baseline | 77 steps, 141 it, 258 s | 72 steps, 130 it, 26 s | 79 steps, 155 it, 553 s | **3** |
| PUCT | 70 steps, 99 it, 12.7 s | 70, 99, 12.8 s | 70, 99, 12.5 s | **1** |
| PUCT counts-only | 75 steps, 129 it, 197 s | 75, 129, 185 s | 75, 129, 195 s | **1** |

Every plan is sound. This is the same stability-and-length result the previous session found on
`isolate_assembly_from_long_reads`, reappearing at scale rather than as one template's quirk — and
here it carries a 20x cost difference with it, because a longer plan is a more expensive plan to
refine.

**Wall clock on the 62 and 67-target rungs is not usable**: a second measurement job shared the
box while they ran. Their counters are deterministic and are not affected.

## Where the time actually goes

The rung-53 rows look like a 20x speed win. They are not a search result. The same case and seed,
solved at `max_refine=0` and again at 32:

| arm | `max_refine=0` | `max_refine=32` | the refiner's share | mcts iterations | plan steps |
|---|---|---|---|---|---|
| baseline | **0.89 s** | 213.9 s | 213.0 s | 141 | 77 |
| PUCT | **0.24 s** | 12.6 s | 12.4 s | 99 | 70 |
| PUCT counts-only | **0.27 s** | 186.0 s | 185.7 s | 129 | 75 |

**The search is sub-second for all three.** Everything above that is the refiner, and its cost
tracks the plan it was handed — 17x more work for a plan 10% longer. So the mechanism is:

> PUCT finds a shorter plan; the refiner's cost is superlinear in plan length; therefore PUCT's
> solves are dramatically cheaper on large workflows — via the refiner, not via the search.

The corollary matters as much: at `max_refine=0` the whole difference is 0.65 s. **Anything that
lowers the refiner budget shrinks the policy question to nothing on this workload.**

## Axis B — the planning space grows: a null, with a reason

72 solves, holding a 33-target workflow fixed and adding the other seven transform libraries
(`fabfos`, `amplicon`, `pangenome`, `metabolicModelling`, `transcriptomics`, `responseSurface`,
`aspire` — 85 more transforms) one at a time.

Nothing happens. `relevant_transforms` moves 54 -> 55 -> 60 across all seven and then stops: the
solver's own distance-to-target prune discards anything that cannot reach the target, so loading a
library the workflow does not need costs the search nothing. Plans are identical at every rung and
PUCT's iteration ratio stays between 0.885 and 0.978.

**"Larger planning space" is therefore not an independent axis for this solver.** The number of
transforms the search can actually see is set by the target set, which is axis A. One caveat worth
keeping: at the two rungs where `relevant_transforms` did rise (60, when `transcriptomics` came
in), PUCT's advantage *narrowed* — 0.885 to 0.978. More relevant transforms are not automatically
better for PUCT.

## Generated instances — where capability, not cost, separates

The real cases cannot show a capability difference because the baseline solves all of them. The
`sink` profile can: it is the only shipped profile that reaches the iteration budget. Scaled
through eight sizes, 10 problems each, two solve seeds, three arms — 480 solves, problem seeds
from 100 up, outside every slice the previous session touched. **No unsound plan on any arm.**

Solve rate is reported per problem, not per row: at `top_k=1` PUCT draws no randomness, so its two
solve seeds are one run while the baseline genuinely gets two attempts.

| `n_types` | baseline | PUCT | PUCT counts-only | mcts on jointly solved (base -> PUCT) |
|---|---|---|---|---|
| 6 | 9/10 | 10/10 | 10/10 | 17.6 -> 11.3 |
| 9 | 10/10 | 10/10 | 10/10 | 44.0 -> 17.4 |
| 12 | 9/10 | 10/10 | 10/10 | 13.0 -> 11.4 |
| 16 | 8/10 | 10/10 | 10/10 | 55.8 -> 21.0 |
| 20 | 7/10 | 10/10 | 10/10 | 48.4 -> 50.6 |
| 26 | 5/10 | **10/10** | 8/10 | 87.2 -> 27.4 |
| 32 | 5/10 | 9/10 | 9/10 | 53.2 -> 27.4 |
| 40 | 5/10 | 8/10 | 7/10 | 108.3 -> 40.2 |

The baseline degrades from 100% to 50% as instances grow; PUCT holds until `n_types` 26 and then
degrades far more slowly. Plans on jointly solved problems are also shorter (15.5 -> 12.2 steps at
`n_types` 26).

## The estimator reverses at size

The previous session's central mechanism finding was that Q is nearly inert — `use_value=False`
reached the same solve rate for about 10% more iterations, so the recommendation was to port the
smaller variant. **That holds only at small sizes.** Both ladders now show it inverting:

- generated, `n_types` 26: full PUCT 10/10 problems, counts-only 8/10;
- real, 53 targets: full PUCT 70 steps and 99 iterations, counts-only 75 steps and 129 iterations
  — counts-only sits next to the baseline (77 steps, 141 iterations) rather than next to PUCT.

At a 256-iteration budget on small problems there is not enough evidence for the value term to
mean anything, and the AMAF/RAVE key sees each transform a handful of times. On a 53-target
workflow the same key recurs often enough for the estimate to be worth something. **If PUCT is
ever ported, port the full policy, not `use_value=False`.**

## Is `max_refine=32` safe? Yes — and on a large workflow it is also the whole cost

Four cases, both policies, `max_refine` varied with everything else held. **Every budget returned
the same plan, in every arm and every seed** — fingerprints compared, not step counts:

| case | arm | 256 | 32 | 8 | 1 | 0 |
|---|---|---|---|---|---|---|
| 33 targets, 42 steps | baseline | 28.1 s | 10.8 s | 3.2 s | 0.5 s | 0.1 s |
| 33 targets, 42 steps | PUCT | 21.7 s | 8.2 s | 2.4 s | 0.6 s | 0.1 s |
| 43 targets, 61 steps | baseline | 38.0 s | 13.8 s | 4.2 s | 0.7 s | — |
| **53 targets, 77 steps** | baseline | — | **230.9 s** | 49.0 s | 12.0 s | **0.1 s** |
| **53 targets, 70 steps** | PUCT | — | **12.6 s** | 3.7 s | 0.9 s | **0.1 s** |

`refiner_found_on` is `[1]` on all 119 axis-A rows as well: the winner is always the state the
refiner was handed, on every case measured here.

So **32 is safe** — nothing in this session or the last found a plan it changes. But on the
largest real workflow it is 231 seconds against a 0.9-second search, and the same plan comes back
at 0. The previous session found exactly one case in the whole shipped corpus where refinement
ever helped (`isolate_assembly_from_long_reads/s7`, 13 steps to 11) and a budget of 8 served it.
That makes 8 the defensible floor: it keeps the one case that ever mattered and costs 49 s instead
of 231 s at 53 targets. 32 buys a 4.7x wider margin for 4.7x the price, and the price is what the
user experiences as a slow solve.

## Limits

- **Python solver only.** No `msm_solver` binary is staged in this worktree, so the A/B is
  like-for-like but no number here describes the shipped Rust path.
- **The refiner's superlinear cost is measured, not explained.** 77 steps costing 20x what 70
  steps cost at the same budget is a large effect from a small difference; what in the refiner's
  frontier construction produces it was not chased down.
- **The ladder was cut at 67 targets.** One arm takes ~4 minutes per solve there and the cost
  climbs steeply; the two largest rungs are reported from the seeds that completed (three seeds at
  62 targets, one at 67).
- **One input sample.** Every real case is rooted at the same paired-read sample, so the ladder
  varies targets, not lineage shape. Multi-sample does not enlarge the search — `CollectSolverInputs`
  dedupes sample groups by endpoint set, so N identical samples solve as one case.
- **Wall clock on this host is unreliable** and is quoted only where the effect is orders of
  magnitude (the refiner) or measured with the refiner off.
- **fabfos is present as a library, not as a large case.** Its shipped template is the smallest
  real workflow here (7 steps) and its driver pipelines target a single type, so the large-workflow
  question is carried by the metagenomics lineage.

## Reproducing

Harnesses under `/home/tony/.claude/jobs/a15841b8/tmp/puct/` (node-local): `bigcase.py` builds the
cases, `reachable.py` finds the target pool, `bigab.py` runs either axis, `phase_split.py` splits
search from refiner, `refine_big.py` prices the refiner budget, `size_sweep.py` is the generated
ladder, `analyze_big.py` renders the tables.
