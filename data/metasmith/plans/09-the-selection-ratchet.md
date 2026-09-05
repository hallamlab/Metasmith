# The selection ratchet: PUCT on the engine, and the trade it took two rounds to break

## Context

`docs/metasmith/solver-spec.md` licensed this. The plan witness is proved to agree with the written
specification, so a selection change can move every plan without moving what "correct" means, and
plan-fingerprint parity is suspended until PUCT lands. Two earlier reports measured `PuctSelection`
and did not adopt it. Both were Python-only results on corpora that could not see a selection
change, and this host runs the Rust engine.

Three agents worked in parallel worktrees for two rounds against one harness,
`research/metasmith/solver_ratchet/`. Every number below is that harness's, on 81 frozen wire
payloads, with the default path byte-identical throughout.

## The answer in one paragraph

**PUCT solves every case in the corpus and is 3.25x faster on the real workflows, and the mechanism
on the metagenomics arms is that it stops generating duplicate work.** The shipped rule solves 57 of
81; the head solves 81. Real-workflow plan length falls from 700 steps to 693 and never rises above
it in any accepted round. Serial interleaved timing over the seven real payloads is 1.290 s against
0.397 s. The default stays `weighted`: none of this ships until someone decides it should.

## What moved

| | shipped rule | PUCT base | round 1 | round 2 head |
|---|---|---|---|---|
| solved | 57/81 | 76/81 | 78/81 | **81/81** |
| real-workflow steps | 700 | 696 | 693 | **693** |
| mcts iterations | 3175 | 2188 | 2206 | 2213 |

Serial wall clock, three interleaved reps, `max_refine=8`:

| payload | shipped rule | head | speedup |
|---|---|---|---|
| unpinned | 0.493 s | 0.080 s | **6.16x** |
| orfs_only | 0.310 s | 0.057 s | **5.44x** |
| ladder-68 | 0.243 s | 0.073 s | 3.33x |
| bins_only | 0.127 s | 0.070 s | 1.81x |
| ladder-53 | 0.070 s | 0.063 s | 1.11x |
| shipped | 0.017 s | 0.017 s | 1.00x |
| ladder-43 | 0.030 s | 0.037 s | 0.81x |
| **total** | **1.290 s** | **0.397 s** | **3.25x** |

`ladder-43` is slower. It is the only real payload that regresses and it is 7 ms.

## PUCT does not merge the duplicate. It never creates it.

On the `unpinned` metagenomics arm, transforms applied more than once:

    shipped rule   36 steps, 50 its   t70x2  t46x2  t64x2  t62x4  t76x3
    head + PUCT    33 steps, 43 its   t70x2         t62x3  t76x3

`.awm/context.md` records this case as structurally beyond the refiner: `expand_node` yields
`base + [appl]`, so every candidate has exactly as many steps as its parent, no move deletes a step,
and the remedy was assigned to the author -- a target-level anchor on every target that could
diverge, which is why `metagenomics_from_paired_reads` pins sixteen of its twenty-five. A better
search does not need the anchor to avoid the duplicate. The anchor is still the only thing that
*guarantees* it.

## The trade, and why it took a structural fact to break

Round 1 found one pattern in every arm. Every route to more solved cases -- `epsilon_milli`,
`top_k > 1`, `temperature`, dropping the refiner's second prior channel -- is the same lever, more
randomness in the draw. Each reaches the hard generated cases and each lengthens real plans. The
best capability result of round 1 solved 80 of 81 and cost 11 metagenomics steps.

**A solved-count-first ranking calls that a win. It is not one**, and the ranking was changed
mid-run to say so: a real-workflow step regression is a LOSS however many generated cases it buys.

`w2` broke the trade because it is not a weaker randomisation, it is a different fact. A transform
whose product has the **same type** as one of its own requirements re-enables itself forever, and
that is readable off the transform's signature before the search starts rather than off the
frontier's population after the runaway has grown. The flag is identically 0 on all 17 real payloads
and 1-5 on every generated one, so a policy reading it cannot move a real plan at any weight. 0.9
through 1.2 all reach 81/81 at the same real-plan length.

**CAUTION** The type-equality test is deliberate. A product that is a strict superset also
re-enables its transform, but it adds a property each time, so the state signature moves and the
search terminates on its own. That is an enrichment step and the metagenomics workflow has exactly
one. Measured separately as `w2_super`: inert at every weight to 20, byte-identical to the head.

## Two contradictions the parallel arms settled

**Reports 02 and 04 disagreed about the value estimator, and both were right about their own
regime.** `use_value=false` is step-identical and iteration-flat on `ladder-3..32` and costs
+9/+23/+30/+47/+36 iterations and +5/+6 steps on `ladder-33..68`. The crossover is near 33 targets,
not 50.

**The estimator earns its cost as first-play urgency, not as a ranking.** `progress_of` divides by
`requires.len() + 1`, so one satisfied requirement is 1/69 on the widest rung and every observed key
collapses to ~0 after a single observation. Q is a novelty signal. Scaling the reward into a real
ranking was measured and is worse -- x5 costs three solves, x69 costs five -- while raising `fpu`
above every attainable reward and decaying stale evidence both widen the same gap and both win.

## What did not work, and is worth not re-trying

- **A negative reward for a no-progress expansion** holds real steps and cuts 11 iterations but
  reaches no unsolved case. On `sink26-103`, 249 of 256 observations already carry reward 0, so a
  penalty has nothing to differentiate.
- **Concentrating the frontier** -- `dup`, `dup_lin`, `cap` -- reaches none of the hard cases. They
  read the frontier's population after the runaway has grown and cannot tell it from a chain that
  honestly offers the same transform three times.
- **Boosting a flooded key** (`widen`) reaches one case and costs 13 real-workflow steps, the same
  tax randomisation pays. Gating it to the refiner is a byte-exact no-op, because an 8-iteration
  budget never puts two slots of one key in a frontier. **Every lever of this kind lives in the mcts
  phase and nowhere else.**
- `reward_mode="absolute"` is worse than its Python docstring predicts. It makes `sink40-106` time
  out.

All survive as default-inert knobs. The next round needs the losing directions reachable.

## Correctness

- 81 of 81 plans witness-accepted, adjudicated by the reference binary rather than the candidate's.
- Default path byte-identical to the pre-change engine on all 81 payloads, every round.
- `pytest tests/metasmith/solver`: 252 passed, 224 skipped, 1 xfailed.
- Witness sweep under `MSM_SOLVER_POLICY=puct`: templates 11/11, fabfos 3/3, aspire 7/7 accepted,
  208 decoys caught, 0 missed, 0 reference disagreements. PUCT shortens the eleven shipped templates
  from 159 steps to 156.
- The shipping profile reproduces the harness's fast profile byte for byte on all 81 payloads.

## What adopting this would cost

Making PUCT the default moves every plan. `tests/metasmith/solver/fingerprints.json` and
`SOLVER_RNG_VERSION` move together, and the Python `solve_by_mcts` has to move with them bit for
bit or `test_engine_differential` goes red. That is the work this report does not do.

**CAUTION** The Python `PuctSelection` and this Rust policy are not the same implementation.
`progress_of` here counts satisfied target requirements through `dep_is_a` over the production map,
where Python walks endpoints with `IsA`. The divergence is near-moot at this scale -- zeroing the
reward term costs one solve -- but it is a real difference and a differential test will find it.

## Reproducing

`research/metasmith/solver_ratchet/README.md`. The per-run record of everything all three agents
tried is `research/metasmith/solver_ratchet/results/findings.md`.
