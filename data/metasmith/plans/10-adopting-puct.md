<!-- Adoption report. What shipping the ratchet's winner actually cost, and what it revealed. -->

# Adopting PUCT, and retiring the solver it was measured against

## What shipped

PUCT is the solver's selection rule, unconditionally. The weighted rule, the
`MSM_SOLVER_POLICY` switch and the entire Python solver are gone. `SOLVER_RNG_VERSION` is 3,
plan fingerprints are re-pinned, and the production gate now takes its verdict from the proved
function rather than from the crate beside it.

Report 09 measured the winner. This is what it cost to ship, and the three things that only became
visible once it did.

## The interlock was the price, and deleting Python paid it

Report 09 named adoption's cost precisely: *"`tests/metasmith/solver/fingerprints.json` and
`SOLVER_RNG_VERSION` move together, and the Python `solve_by_mcts` has to move with them bit for
bit or `test_engine_differential` goes red."* That is a PUCT port to Python, reconciling two
`progress_of` implementations that were already known to differ — the Rust one counts satisfied
target requirements through `dep_is_a` over the production map, the Python one walks endpoints
with `IsA`.

None of that was done, because the Python solver was deleted first. The order is the whole trick:
the differential tests are what made adoption expensive, and they die with the implementation they
compare against.

The deletion is a split rather than a delete. `solver.py` was 1,550 lines holding two things under
one name — the data model that about 150 sites import, and the search. It is 326 lines now:
`Node`, `Dependency`, `Endpoint`, `Transform`, `Application`, `Solution`, `_canonicalise_givens`
and `solve_by_mcts` as the entrypoint. `solver_policy.py` and `solver_bound.py` went as files.
`solver_rng.py` and `solver_math.py` stayed, and are no longer the Python search's: they are the
executable statement of the RNG stream that `rng-trace` holds the binary against, which is the
only cross-language differential left and the only thing that would catch `rand_chacha` changing
underneath the port.

Four test files went whole. Where a property was real and only the counterparty was Python, it is
asserted about the engine instead — the distance table still has to give two transforms sharing a
key their own distance, read now out of `msm_solver describe`; producers still come back in rank
order; the corpus still has to return a plan `check_plan` accepts.

## The production gate was pointed at the wrong function

`msm_solver solve` has adjudicated every complete plan since the witness landed. It called
`solver_witness_audit::audit`.

`audit` is not the proved function. `SolverProof.check_spec` is about `solver_witness::check`;
`audit` re-implements the same judgement as loops that can name a coordinate, and the two are tied
by a single `debug_assert_eq!(v.ok(), w::check(p, q))` — which `[profile.release]` compiles out. So
every shipped binary took its verdict from the half nothing is proved about, and the tie between
them was checked only in builds nobody ships.

Both `cmd_solve` and `cmd_check` now take the verdict from `check` and call `audit` only to say
which clause failed. The cost is one extra pass over a plan whose solve dominates by orders of
magnitude. It is what makes "the witness gates production" literally true of the binary that runs.

The Python backend was the other half of that hole: it emitted no wire, so nothing adjudicated it,
and `_get_solver_class` selected it automatically whenever a binary was missing — with one warning
per process. When DVC checked the engine binaries out mode 444, every plan in every worktree took
that path. Deleting it makes the gated path the only path, and an absent engine now raises with
which of the three things went wrong.

## The search no longer draws

Not intended, and worth knowing.

The weighted rule spent its randomness on a three-way arm choice: two exploit arms and one explore
arm at 5%. PUCT ranks instead, `top_k` is 1, and `pick_top_k` with one candidate calls
`bounded_int(1)`, which consumes nothing. Across 72 corpus and profile cases at four solve seeds,
**the solve seed does not change the plan on any of them**.

That killed a test rather than a feature. `test_the_corpus_spans_both_sides_of_rng_sensitivity`
demanded the corpus keep at least one seed-sensitive case, so a swapped PRNG could not pass
unnoticed. Its premise is gone: no corpus can be seed-sensitive against a search that does not
sample. A PRNG swap is caught by `test_rng_contract.py`, which drives the stream directly through
`rng-trace` and never depended on a plan happening to move. The test now pins the inertness itself,
so putting sampling back onto the selection path is a decision rather than a discovery.

The stream still has to agree — ties still draw — which is why `SOLVER_RNG_VERSION` still exists
and why it went to 3.

## The refiner blow-up was a search problem

The single most consequential thing this uncovered, and it was not what adoption was for.

`sink-6807` is the instance report 08 and the scratch-array work were written about: a 57-step
plan that enumerates 33,062 candidates an iteration, 4.2s and 0.25 GB at a refiner budget of 8,
and roughly **70 GB at 256**. `sink-178` at solve seed 7 cost 3.5s for a *single* refiner
iteration, which is what `tests/metasmith/perf` pinned.

Under PUCT, `sink-6807` is a **four-step plan**. `sink-178` is seven steps and a full budget of 256
finishes in under a hundredth of a second. Every instance that was expensive — 6503, 6807, 8575,
9087, 9375, 9391, 9927, 178, 24 — now settles between four and nine steps and is unmeasurable.

The refiner's cost is candidate count times plan length and that was always understood to be
intrinsic. It is: the mistake was treating the plan length as given. The weighted rule settled on
long plans on exactly these instances, and the refiner then paid for them. Nothing about the
refiner changed here.

The perf test was rewritten around the cause rather than the symptom. It asserts the step counts
those instances settle on, because a selection change that goes back to handing the refiner
57-step plans shows up as a number long before anyone waits out the wall clock.

## What moved

Four of the eight pinned corpus cases changed fingerprint, all at the same step count, all sound.

`sink-9391` was the one instance the weighted rule had stopped reaching at all, pinned by a test
whose own message said to move it back rather than relax it if the search ever found it again. It
does: nine steps, `check_plan(strict=True)` clean. It is in `FORMERLY_UNSOUND` now and that test is
gone.

Nothing else in 1,121 solver, unit and flow tests moved.

## The numbers, on the binary that ships

81 of 81 ratchet payloads solved, against the weighted rule's 57. Real-workflow steps 700 to 693.
All 81 plans witness-accepted. The witness sweep: templates 11/11, fabfos 3/3, aspire 7/7, every
decoy caught, no disagreement with the Python `check_spec`.

Wall clock over nine real payloads, best of four interleaved runs, both binaries built with the
same toolchain and profile:

| payload | weighted | PUCT | | steps |
|---|---|---|---|---|
| unpinned | 0.521s | 0.103s | **5.06×** | 36 → 33 |
| orfs_only | 0.303s | 0.075s | **4.04×** | 35 → 32 |
| ladder-68 | 0.271s | 0.107s | 2.53× | 81 → 81 |
| ladder-62 | 0.226s | 0.098s | 2.31× | 81 → 81 |
| bins_only | 0.148s | 0.101s | 1.47× | 35 → 34 |
| ladder-53 | 0.104s | 0.089s | 1.17× | 72 → 72 |
| shipped | 0.048s | 0.041s | 1.17× | 31 → 31 |
| ladder-43 | 0.056s | 0.065s | 0.86× | 63 → 63 |
| ladder-32 | 0.046s | 0.053s | 0.87× | 43 → 43 |
| **total** | **1.723s** | **0.732s** | **2.35×** | 477 → 470 |

**CAUTION** Report 09's 3.25× was over a different seven-payload subset on a differently loaded
box; the same seven measured today give 2.50×. Neither number is wrong and the mechanism is the
same one — PUCT does not generate the duplicate work `expand_node` is structurally unable to
remove — but quote the payload set with the ratio.

**CAUTION** The same comparison against the *staged cross build* reads 1.78×, and the three
smallest payloads come out slower. That is musl against glibc, not PUCT: the shipping binary is
statically linked and pays roughly 12–15 ms more at startup plus a slower allocator under load. It
applied equally to the binary that preceded it. Compare policies on one toolchain; compare
toolchains separately.

## What this does not close

**The Lean drift guard is still manual.** `docker/solver_witness/dev.sh --lean-check` is the only
thing that adjudicates the Rust witness against the proof, nothing in any build or test path
invokes it, and there is no CI in this repository at all. Making the witness the production gate
raises what that gap is worth. The ratchet harness now hashes both witness crates against
`results/witness-digest.txt` and refuses to report a number if they moved, which is a tripwire for
one workflow rather than a fix.

**The KBase library is refused on `indexed`, and the cause is the encoder.** 27 of its 326
transforms declare two structurally identical input slots, and `_Interner.node` interns a
`Dependency` by signature, so two slots collapse to one wire id before any plan exists. It is not
on this branch and this change did not touch it — the gate that refuses it was already
unconditional — but fixing the interner changes the wire for every problem with duplicate slots
and belongs in its own change.

**One pre-existing red test.** `tests/metasmith_libraries/test_logistics_workflow.py::
test_can_plan_sra_download_workflow` fails identically on `feat/dev`: no transform library
produces or consumes bare `sequences::reads`. It fails in spec validation, before any solve.

**A platform consequence, deliberately taken.** With no Python fallback, a host outside the four
staged targets cannot plan at all rather than planning slowly.
