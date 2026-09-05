<!-- Run report. The plan as approved, the corrections the evidence forced, and the
     run log. Copied here from ~/.claude/plans/, which is node-local and unversioned. -->

# The witness, held against every real workflow this repository can reach

## Context

The last session proved the plan witness: `check` returns exactly the truth value of the written
specification, for every problem and every plan. That proof is about the *checker*. It says nothing
about whether the checker has ever been pointed at the work this project actually does.

Until this session it had not been. The witness suite adjudicated eight generated problems and three
stress variants and nothing else — not one shipped template, not one research driver, not one
real-library solve — and no plan built from a real workflow had ever been deliberately broken to see
whether the witness noticed. This session closes that in both directions, and then answers one
question about the refiner: whether an underspecified workflow that runs the same tool twice is a
problem, and whether the lineage specification is the remedy.

## What you said

> read previous journal. perform sanity check.

> gather every real solved plan and driver you can (search deep please). verify that the current rust
> solver solves it (useing mock transforms if you have to).

> verify that the witness indicates valid where appropriate. corrupt the solutions -> verify that the
> witness indicates invalid solves.

> sometimes, underspecified plan will duplicate work (2 copies of prodigal, each serviing a differnt
> set of transforms). I think the refiner is meant to fix this, but the witness may not find it.
> that's ok if we can then add a lineage specification to fix it. see if you can reproduce this case
> and verify that it is not a problem

> ignore kbase

## The result in one paragraph

**168 real cases, 166 accepted by the proved witness, and 1,533 corrupted copies of them every one
rejected by the clause it was built to break.** The two cases that were not accepted are both correct
outcomes rather than failures: one driver is deliberately unsolvable, and one library test's search
runs out of frontier and returns a plan with no target step, which the gate refuses by design. The
Rust witness and the independent Python statement of the same specification disagreed on nothing —
not on an accepted plan, and not on any of the 1,533 mutations. The duplicated-work case reproduces
exactly as described, the witness accepts it and is right to, the refiner cannot fix it and never
could, and a target lineage pin removes it.

## The sanity check

| check | result |
|---|---|
| engine identity | `msm_solver` 0.1.0, wire 2, rng 1, capabilities `rng, solve, check` |
| backend under test | `Backend("solve")` and `Backend("check")` both `rust`, asserted inside every case |
| solver suite, including the `--python-solver` axis, before | **472 passed, 1 xfailed** in 385 s — identical to what the last session left |
| the same suite, after | **475 passed, 1 xfailed** in 381 s — the three added by `test_plan_witness_real.py`, nothing else moved |
| `docker/solver_witness/dev.sh --lean-check` | **PASS**; `check_spec` and `check_correct` on `[propext, Classical.choice, Quot.sound]` and nothing else |
| `tests/metasmith/solver/fingerprints.json` | byte-identical to `HEAD`, sha `26cfb6f0` |
| `SOLVER_RNG_VERSION` | 1 on both sides, untouched |

## The corpus

Every arm solves on the Rust engine, is adjudicated by `msm_solver check` on the exact bytes the
engine returned, is cross-checked against `metasmith.testing.solver_spec.check_spec`, and then has
each of the ten decoys applied to a reply the witness has just accepted.

| arm | cases | outcome | seconds |
|---|---|---|---|
| shipped GUI templates | 11 | 11 accepted | 4.1 |
| fabfos's shipped pipelines | 3 | 3 accepted | 0.6 |
| the ASPIRE amplicon driver, all seven cases | 7 | 7 accepted | 2.6 |
| the viromics driver, carried in from `viromics/make-template` | 1 | 1 accepted, 55 steps | 0.8 |
| distinct plans solved by the real-library test corpus | 25 | 24 accepted, 1 incomplete | 7.4 |
| PlanBench Blocksworld | 120 | 120 accepted | 8.3 |
| `dl_embeddings_from_orfs`, the BLOCKED driver | 1 | did not solve, as expected | 0.0 |
| **total** | **168** | **166 accepted, 1 incomplete, 1 unsolved** | **23.7** |

The largest real case is the viromics driver at 55 steps; the largest shipped one is
`metagenomics_from_paired_reads` at 31. **Step counts in this section are the solver's own** --
`len(solution.dependency_plan)`, which carries the synthetic given step and the target step on top of
the executable ones, so it runs two above the `len(plan.steps)` a template prints. The
duplicated-work section below counts executable steps, because that is the number a duplicate costs
you.

**The two non-accepted cases are both the specification working.** `dl_embeddings_from_orfs` is
listed BLOCKED in `build_templates.py` because the only producer of `sequences::orfs_shard` is
disabled; it drops all six of its targets and never reaches the witness, which is a different outcome
from a plan being refused and is reported as one. The incomplete case is a one-step reply the witness
refuses on `target` with `complete=False` — the search's frontier ran out, and the engine gate's
contract is `complete → sound` rather than `sound`, so refusing that would be refusing an honest
"no answer". It is the first time that path has been exercised by a real workflow rather than by a
regression fixture.

## What the accepted column does and does not prove

Worth stating plainly, because it is easy to over-read. `msm_solver solve` runs the witness itself
before it emits: `main.rs` gates on `complete -> sound` and returns an error rather than a plan when
the audit fails. So for a complete plan, "the witness accepts what the engine returned" is true by
construction — a plan that failed would never have reached the sweep as a plan at all. The accepted
column is therefore evidence of something slightly different, and still worth having: **on 166 real
workflows the search never produced a complete plan that its own gate refused.** A case where it did
would have surfaced here as a solve error, and none did.

The two independent claims are elsewhere. `check_spec` — a second statement of the same
specification, in another language, over the raw JSON rather than the crate's structs — agreed with
the engine on every accepted plan and on every mutation, which is the only thing that could catch a
clause the port states differently from the reference. And the decoys are the only evidence in this
report that the witness *rejects* anything at all.

## The decoys

One mutation per clause, each derived from a reply the witness had just accepted, so a rejection
cannot be blamed on the plan merely being malformed. A decoy caught only by some *other* clause is a
failure: it proves nothing about the clause it was built for.

| clause | caught | missed | no case could host it |
|---|---|---|---|
| conformance | 166 | 0 | 0 |
| derived | 46 | 0 | 120 |
| emission | 166 | 0 | 0 |
| givens | 166 | 0 | 0 |
| indexed | 166 | 0 | 0 |
| provenance | 166 | 0 | 0 |
| schedulable | 166 | 0 | 0 |
| shape | 166 | 0 | 0 |
| target | 166 | 0 | 0 |
| uniqueProducer | 159 | 0 | 7 |

**1,533 corrupted plans, zero missed.** Ten clauses against 166 accepted plans is 1,660 attempts, of
which 127 no case could host. Two entries in that last column are worth reading rather than skipping.

`uniqueProducer` cannot be hosted by a three-step plan: the decoy makes a *second* step emit an
endpoint the first already emits, and `ecspr_results_from_gpr_table` and `fabfos/ecspr` have no
second producing step. Every other real case hosts it.

`derived` cannot be hosted by any Blocksworld instance, and the reason is a property of that domain
rather than of the witness. The decoy adds one parent the step did not confer, choosing an index
below the endpoint's own so that it trips `derived` and not `indexed`. A Blocksworld plan is a chain
of states in which endpoint *e* already declares every index below it as an ancestor, so there is no
spare index to add. The clause is exercised 46 times on this project's own workflows.

**The engine's witness and the Python reference agreed on all 168 accepted plans and all 1,533
mutations**, including on *which* clause fired. Those are two implementations written from the same
document in two languages, and their disagreement is the only thing that would catch a clause the
port states differently from the reference.

## The duplicated-work case

### It reproduces, and the shape is exactly the one described

Three targets against the standard library — `sequences::orfs`, `annotation::kofamscan_results`,
`annotation::diamond_uniref50_results` — with no lineage anchor between them:

```
15 steps.  prodigal ×2, chunkOrfsForAnnotation ×2

  prodigal  order=8   on sequences::megahit_assembly   serves chunkOrfsForAnnotation, merge_diamond_uniref50
  prodigal  order=9   on sequences::spades_assembly    serves chunkOrfsForAnnotation, merge_kofamscan
```

Two copies of prodigal, each serving a different set of transforms. `spades_assembly` and
`megahit_assembly` both satisfy `sequences::assembly`, and with nothing tying the two annotation
targets to one another the planner is free to answer them from different assemblers — so it runs both
legs.

### The witness accepts it, and that is correct

`msm_solver check` returns ok, `check_spec` returns ok, and `check_plan` returns ok. The specification
says in as many words that it claims nothing about optimality; its only cardinality clauses are one
producer per *endpoint* and exactly one target step. Two prodigal applications bound to two different
assemblies are two legitimate steps of a sound plan. The witness not finding this is the witness being
right.

### The refiner cannot fix it, and never could

`max_refine=0` and `max_refine=256` return the identical plan fingerprint, and the reason is
structural rather than a tuning failure. `expand_node` yields `base + [appl]` where `base` is the plan
minus one step and `appl` is that same transform re-applied with a different binding, so **every
candidate has exactly as many steps as its parent**. There is no move that deletes a step and none
that collapses two onto one. A merged plan is not in the search space the refiner explores.

The objective could not see it either, even if the move existed. `score_node`'s entropy term counts
only endpoints reached through a *declared* lineage anchor, and `prodigal` declares none on its
products — so duplicating it moves the objective by zero.

### The lineage specification removes it

The same three targets with the two annotation targets anchored to the first:

```
TARGETS = [
    "sequences::orfs",
    {"type": "annotation::kofamscan_results",      "parents": [0]},
    {"type": "annotation::diamond_uniref50_results","parents": [0]},
]
```

```
12 steps.  no duplicated transform at all.  dropped_targets: []   witness: accepted
```

Three steps saved, both assemblers collapse to one, and the plan is still accepted.

### The narrowest pin: every target that could diverge needs one

Pinning only `kofamscan_results` and leaving `diamond_uniref50_results` free gives back the 15-step
plan with both prodigals. **One pin is not enough.** The anchor is a statement about one target's
binding, so a second unanchored target is still free to be answered from anywhere. That is why
`metagenomics_from_paired_reads` pins sixteen of its twenty-five targets rather than one, and it is
the practical rule for a workflow author: pin every target that shares an ambiguous ancestor, not the
first one.

### Why this is not a test

The obvious next move is to pin the fifteen-step shape in the suite, and it would be the wrong one.
"Two prodigal steps appear" is a statement about which plan *this* search returns from an ambiguous
target set, not about whether the ambiguity exists. The next task on this scope is the PUCT port,
which moves every plan by design — a policy that happened to answer both annotation targets from one
assembler would fail that test while changing nothing about the finding. The reproduction lives in
`research/metasmith/witness_sweep/duplicate_work.py`, where re-running it is one command and a
different answer is a result rather than a red suite.

### What that means for the question asked

It is not a problem, in the precise sense that matters: the duplication is expressible, detectable and
removable by the workflow author, using a surface that already exists and is already used for exactly
this in the shipped library. What is *not* true is that the refiner would ever have done it — that
belief should not survive this report.

## What the session overturned

**"The refiner is meant to fix this."** It is not implemented to. Nothing in the repository said so
before, and `.awm/context.md` now does.

**The unpinned full template is no longer a 6 GB, 190-second solve; it is not a solve at all here.**
The comment above `metagenomics_from_paired_reads`'s targets records that cost, from the
Python-solver era. On the Rust engine the same unpinned target set reached 8 GB and was killed by the
cap, and at a 24 GB cap it was still climbing past 12 GB after most of an hour without returning, at
which point it was stopped. The pinned template solves in 1.8 s. So the number in that comment is
stale, and the lesson it draws is understated: pinning is not a tidiness preference, it is the
difference between a solve and no answer. That is also why the reproduction in this report is the
fifteen-step `minimal` arm rather than the full one -- a case nobody can re-run is not a
reproduction.

## Deviations

- **The duplicated-work reproduction is a three-target subset, not the full unpinned template.** The
  plan named the full one. It does not terminate here, which is itself reported above; the subset
  produces the same shape -- two `prodigal` steps serving disjoint consumer sets -- in fifteen steps
  and five seconds.
- **"Fully unpinned" is not expressible for that template.** Three `taxonomy::checkm_stats` targets
  are distinguishable only by the binner each descends from, and `TargetBuilder` refuses a second
  target of one type with identical parents. The `unpinned` arm therefore strips only the pins to
  target 0 -- which is exactly the ambiguity under study.
- **The real-library corpus is 25 distinct plans, not the ~60 solves the plan estimated.** Most of
  that directory's `GenerateWorkflow` calls sit inside `@pytest.mark.slow` classes that stage and run
  Nextflow. The sweep ran `-m "not slow"` and de-duplicated by plan fingerprint, since several tests
  solve the same workflow and a duplicate row would inflate every count in the decoy matrix without
  adjudicating anything new.
- **An arm was added that the plan did not name.** ASPIRE's seven cases: plan-only, no cluster, and
  seven real workflows for one library load.
- **The compaction tasks were not run.** T4 and T7 are closed as deferred; context stayed well inside
  the window and the `reflection` tool is not available in this harness.
- **One pre-existing failure was observed and left alone.**
  `tests/metasmith_libraries/test_logistics_workflow.py::test_can_plan_sra_download_workflow` fails on
  `no transform library declares [sequences::reads]`. It is unrelated to anything here -- no Python or
  Rust under `src/metasmith` was changed -- and it is outside the solver suite.

## Followups

- **The KBase library is refused by the witness with clause `indexed`, and the cause is in the
  encoder.** Reproduced live on the current staged binary from
  `research/kbase/reports/witness_indexed_repro.json`: 27 of that library's 326 transforms declare two
  structurally identical input slots. `_Interner.node()` in `solver_wire.py` interns a `Dependency` by
  signature, so the two slots become one wire id and `Indexed`'s "no transform names one requirement
  twice" is violated before any plan exists. `Application.used` is a dict keyed the same way, so the
  Python solver silently binds only one of the two rather than refusing. The witness is right and the
  representation is the defect. Left alone this session at your instruction.
- **`duplicate_work.py` solves each case twice.** `describe` needs the in-memory plan and `adjudicate`
  re-solves through `solve_and_check` to get the wire. Harmless at a second a case, and the whole cost
  on the unpinned arm.
- **`same_slots` bit-width truncation** remains as the last session left it.
