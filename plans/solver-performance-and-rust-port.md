# Solver: verification harness, performance, and the Rust port

## Context

The `metagenomics_from_paired_reads` template takes ~33.5s to solve. Measurement showed
the cost is almost entirely the plan *refiner* — the second phase of `solve_by_mcts`, which
tries single-edge swaps against a found plan looking for a better arrangement. On all four
shipped templates the refiner **never changes the plan it was given**; on metagenomics it
spends 23 of those 33 seconds proving that.

Underneath sits a correctness problem. An earlier attempt in this same conversation to speed
up the refiner's validity check by memoizing its graph walk was **unsound**, and nothing in
the test suite caught it — the branch it broke never executes anywhere. That gap is the real
finding and it closes first.

The outcome we want: a solver roughly an order of magnitude faster, ported to Rust and
shipped as release-time binaries exactly the way `msm_relay` already is, behind a verification
harness strong enough to trust the port, with the Python solver kept permanently as fallback
and differential-test reference.

**This plan is written to survive a compaction.** Everything needed to execute is below.

## What you said

> Accuracy and current solution parity is essential, so no matter what we do, we must be
> careful. the test coverage is likely not sufficient. there are many edge cases that are
> likely not exercised, like when the edge refinemnt is actually needed, and various wierd
> combinations of lineage constraints + duplicate transform use.

> i dont really have any cases to suggest to improve test coverage of esge cases, ive simply
> optimized the solver and gone no further than what I could figure out from static analysis
> and a lot of thinking over years.

> fair, parity is easier to check, but topological equivalence of results is sufficient.

> for rust, i was thinking we go the msm relay route and just ship 4 binaries that we compile
> at release time.

> the max distance to isnt cached because swapping edges could change the distance, which
> would also require an incremental delta system

> please also include verification runs to test the proposed changes actually improve
> performance, since having a delta and computing hashes is not free, brute forcing and
> leaving some components as is may actually be faster

> another thing about rust, we need to be careful about the string construction for hashes.
> it was easy to do that in python, but I'm sure we can choose better data types to serve as
> keys and hashes and to calculate hashes from

## High-level goals

**G1.** Be able to tell, mechanically and without reading the solver, whether a change to it
broke anything.

**G2.** Close the coverage gaps we know exist — cycles, repeated signatures, and lineage
constraints crossed with duplicate transform use.

**G3.** Make the solver substantially faster, proving each individual change earned its place
rather than assuming it did.

**G4.** Move the search to Rust, shipped as release-time binaries, with Python kept as a
permanent fallback and reference.

**G5.** Never trade accuracy for any of the above.

## Acceptance criteria

1. A topological fingerprint is stable across repeated runs of an unchanged solver on all four
   shipped templates. It must not read `instance_id`.
2. A semantic checker, sharing no code with the solver, verifies for any (problem, plan): every
   step's inputs are given or produced by an earlier step; the step graph is acyclic; every
   requested target is satisfied; every declared per-slot lineage constraint holds.
3. A random problem generator produces instances with controllable cycle density, lineage
   density, product-group branching, duplicate-transform pressure, and signature-collision
   pressure. ≥10,000 generated instances pass the checker against the current Python solver;
   any failures are triaged before the port begins.
4. For instances with ≤6 transforms, an exhaustive oracle confirms the solver finds a plan
   whenever one exists.
5. A test drives `_is_valid`'s `return False # looped` branch and asserts it fired — or, if it
   proves unreachable from `expand_node`, that conclusion is recorded with its evidence.
6. A test asserts correct validation of a state containing two distinct `Application` objects
   sharing a `Signature()`.
7. Every performance change lands with a before/after benchmark on a fixed corpus. Any change
   that does not measurably improve wall time is **reverted**, not kept.
8. `metagenomics_from_paired_reads` solves in <3s via the Python path, topologically equivalent.
9. Rust and Python produce topologically equivalent plans on 100% of the generated corpus given
   the same seed.
10. With no `msm_solver` binary present, metasmith solves exactly as it does today.
11. Release builds all four targets; `RELEASE_PROTOCOL.md` documents it.

## Established measurements — do not re-derive

All on `metagenomics_from_paired_reads` (29 steps) unless noted. Baseline wall time 33.5s.

- `refine_mcts` is >99% of solve. `_is_valid` is called **19,683** times.
- **100% of those 19,683 calls fail the lineage term.** Lineage costs **0.08s** total; the loop
  walk costs **23.1s**.
- Lineage-prefilter reorder measured: **33.4s → 10.0s**, with **identical** structural
  fingerprints on all four templates.
- On all four templates the refiner never changes the plan (`valids` only ever holds the
  initial state). Three of four templates make only **one** `_is_valid` call total.
- Two tests *do* produce a plan-changing refinement: `TestBranching::test_branching_complex_workflow`
  and `TestBranching::test_branching_overlapping_groups`. They are the only evidence the refiner
  is not inert.
- The `return False # looped` branch fires **nowhere**: 0 across 19,686 template calls, 0 across
  all 19 solver tests, 0 across all 10 original scratch scenarios including `loop_1`.
- `TestCircularDependencies::test_loop_handling` never reaches the refiner at all —
  `refine_mcts` is called **0** times for it. Worth understanding on its own.
- Signature-collision states (two distinct `Application` objects, one signature) are real:
  **6 of 19,720** on metagenomics; `simple_2` → 2; `branching_4` → 2.
- `expand_node` yields **193,280** children, of which only **19,683** are unique — ~90% are
  constructed, signature-sorted, then discarded by the `seen` check.
- `Application.Signature()` is called **18.1M** times; `RefinerState.Signature()` 212k times,
  each an O(steps) sort-and-join.
- `_max_distance_to`: **511,758** calls = 26.0 per `score_node`; 22.0 distinct (e,a) pairs;
  **17.8 distinct source endpoints**. Grouping walks by source saves only **31.4%** — real but
  modest, and much smaller than first claimed.
- `max_refine=8` vs `256` gives identical plans on all four templates. This is a *restatement*
  of the refiner being inert on them, not independent evidence. Do not use it to justify
  lowering the cap.

## Established correctness findings — do not re-litigate

- **The path-dependent loop walk is load-bearing.** `_is_valid` asks "does any walk from
  `given_appl` repeat an Application *signature*", which equals cycle detection only if
  signature↔object is a bijection. It is not. Property testing over 40,000 random graphs: 0
  disagreements in 30,000 when signatures were distinct; 122 in 30,000 when they could collide —
  every one being *legacy=looped, memoized=clean*, i.e. the memoized version **misses** cycles.
  **Do not memoize this walk on signature. Do not memoize it on object identity either** — the
  predicate depends on the path's signature multiset, not on reachability.
- `_has_ancestor` (`solver.py:775`) indexes `produced_from[e]` **unguarded**. Running it earlier
  than today can raise `KeyError` on states the original bailed out of first. Wrap the prefilter
  in `try/except KeyError: pass` so it falls through to the unchanged full check — that keeps it
  exact and adds no new crash.
- `_max_distance_to` (`solver.py:873`) uses a `seen` set with a LIFO stack, so it computes
  *depth at first pop*, not true maximum distance. Its `max_d > 0` test (not `>= 0`) makes a
  distance of zero indistinguishable from not-found, returning 1.0. **Both quirks feed the score
  and must be preserved.** Grouping walks by source endpoint *is* exact — the walk depends only
  on the source; the destination is merely an `if n == a` test during traversal, and `seen`
  guarantees one visit per node.
- `_max_distance_to` is **not** cached. The cached thing nearby is `distance_scores`
  (`solver.py:433`, consumed via `max_distance_score` at `:469` and `:996`) — precomputed once
  per solve over `Transform`s for the mcts phase. Different structure, different phase.
- **Latent bug:** `expand_node` (`solver.py:940`) removes the swapped step with
  `[s for s in state.steps if s.Signature() != step.Signature()]` — this drops **both** steps
  when two share a signature. Independent of everything else here; T2's collision test exposes it.
- `instance_id` is **random** for absent/deferred inputs (per `AGENTS.md`, leaf ids fall back to
  a random per-call id). Any fingerprint reading it is nondeterministic — this already produced
  one false "the plans changed" result during investigation.
- Determinism surfaces beyond the RNG: `np.argpartition` tie-breaking at `solver.py:912`, `:969`,
  `:1011`, and `:753`. Introselect's internal order is *not* "first maximum".
- `solve_by_mcts` calls `np.random.seed(seed)` at `solver.py:328` — a **process-global**
  mutation. Relevant to test isolation and to T4.

## Environment and repo rules

- Env is **`msm`** (not `msm_env`). Run as `PYTHONPATH="$PWD/src" mamba run -n msm <cmd>`.
  **Pin** `PYTHONPATH`, do not merely unset it — `metasmith` is not installed into `msm`, and
  an unset var breaks subprocess tests while an ambient one resolves to another checkout.
- **pytest exit codes lie through `mamba run` + a pipe.** Always quote the "N passed" line;
  never trust the status.
- **Never `git checkout <path>` in a metasmith worktree** — unstaged edits from other in-flight
  streams live there and a restore is unrecoverable. Revert via `cp` from an explicit backup.
- Solver tests today: `tests/flow/test_solver.py` (836 lines, 19 tests across `TestBasicSolver`,
  `TestCircularDependencies`, `TestBranching`, `TestGivenLineage`, `TestMultiSampleBinningWorkflow`),
  plus `test_solver_scaling_cyanoverse.py`, `test_branching.py`, `test_lineage_parents.py`,
  `test_plan_hints.py`.
- Working tree is **clean** at `c568a46` on `feat/gui1`. `solver.py` is pristine — the unsound
  rewrite was already reverted.

## Tasks

- **T1** — Verification harness: topological fingerprint, semantic checker, problem generator,
  brute-force oracle, benchmark runner. New `tests/solver/` axis.
- **T2** — Close the coverage gaps; absorb the two scenarios still only in the notebooks.
- **T3** — Land the exact Python wins, each individually A/B gated.
- **T4** — Replace the numpy RNG with a portable PRNG shared by both implementations.
- **T5** — Port the search to Rust; ship as binaries following the `msm_relay` precedent.
- **T6** — Add incrementality inside Rust, kept only where it beats rebuilding.

## Approach by task

### T1 — Verification harness

The gate everything else is measured against. Nothing else starts until it exists.

Create a new **`tests/solver/`** axis and add `("solver", ["fast"])` to `_DIR_MARKERS` in
`tests/conftest.py:28`. That file **fails loudly** on any test file sitting under no axis, so
the entry is mandatory, not optional. Move the existing solver tests there from `tests/flow/`.
This is the right home: `tests/flow/AGENTS.md` opens by stating "**This is NOT solver
correctness**" and that solver tests live separately. Benchmarks go to `tests/perf/`
(marker `slow`, precedent `tests/perf/test_library_scale.py`).

The **topological fingerprint** canonicalizes the plan DAG by colour refinement: seed each
step's colour from its transform key plus the sorted dtype signatures of its slots, then iterate
`colour ← hash(colour, sorted multiset of in-neighbour colours, sorted multiset of out-neighbour
colours)` to a fixed point. Fingerprint = sorted multiset of final colours. Transform keys are
strong labels so this converges in one or two rounds; where two steps keep the same colour, fall
back to explicit matching rather than declaring equality.

The **semantic checker** must not import solver internals — verification is far simpler than
search, which is the entire reason it can adjudicate a plan from an implementation we don't yet
trust. Target: readable in one sitting.

The **problem generator** is the answer to having no edge cases to suggest — we don't need to
know them if we can generate the space they live in. The dials matter more than the volume;
signature-collision pressure and lineage-crossed-with-duplicate-transform pressure are where the
known bugs live.

The **benchmark runner** fixes a corpus (four shipped templates + a stable slice of generated
instances), reports wall time per phase, and diffs fingerprints against a recorded baseline.
Every later task reports through it.

Templates live in the **sibling repo** `/home/tony/agentic_workspace/projects/metasmith-libraries/main`,
reached via the `metasmith_libraries_root` fixture (`tests/flow/conftest.py:61`). Load with
`from metasmith.agents import Template`; `Template.Discover(root)` then `template.spec.Solve()`.

**Gotchas:** a fingerprint touching `instance_id` shows changes that aren't there. A checker that
imports solver internals will agree with the solver's bugs. Generated instances need a solve
timeout — an unlucky draw can produce a search that doesn't terminate quickly. `solve_by_mcts`
seeds numpy globally (`:328`), so tests can leak RNG state into each other.

### T2 — Test coverage

`main/workflow_solver/branching_test.py` is **fully absorbed already** — all ten scenarios
(`trivial`, `simple`, `simple_2`, `loop_1`, `branching_1`–`6`) are present in
`tests/flow/test_solver.py` with the same transform sets, givens and targets; `loop_1` is
verbatim `TestCircularDependencies::test_loop_handling`. **Nothing to deduplicate from it.**

Two things in `main/workflow_solver/solver_test.ipynb` are *not* absorbed and should be ported:
- **cell 2** — a dense reciprocal-cycle web (assembly↔bins, bins↔tax, bins↔contigs, contigs↔ORFs,
  ORFs→annotation): four bidirectional pairs against the current loop test's two, and no lineage
  constraint on the target.
- **cell 5** — parametric *search* scaling, M×N up to 256×256, with two lineage-constrained slots
  on a joining transform. A different axis from `test_solver_scaling_cyanoverse.py`, which scales
  *data* (21k items, one transform).

Then the gaps the measurements exposed: a test that reaches the loop branch and asserts it fired;
a test that asserts a signature-collision state validates correctly (`simple_2` and `branching_4`
already produce collisions with tiny inputs); and pins on the two tests that exercise a
plan-changing refinement, since they're currently the only proof the refiner isn't inert.

**Gotchas:** a test asserting "the solver handled cycles" without asserting *which* branch
rejected them is exactly the test that already exists and already protects nothing — assert the
branch fired. Reaching the loop branch may prove impossible; that is a legitimate result, but it
must be recorded as evidence rather than assumed.

### T3 — Python exact wins

Three changes, landed and benchmarked **separately** so any regression is attributable.

1. **Lineage prefilter** — reorder `_is_valid`'s three independent, side-effect-free predicates
   so the cheapest runs first. Reordering an AND is exact by construction. Measured 33.4s → 10.0s
   with identical fingerprints on all four templates. Implement as a prefilter hoisted inside
   `validate_node` (`solver.py:768`) wrapped in `try/except KeyError: pass`, leaving the existing
   `_is_valid` body untouched below it.
2. **`_max_distance_to` regroup** (`solver.py:873`) — one backward walk per distinct source
   endpoint, answering every destination from the resulting depth map. Exact. Measured saving:
   **31.4%** of walks. This is a genuine revert candidate if the wall-clock doesn't follow.
3. **Child dedup** — move the `seen` check (`solver.py:963`) ahead of `RefinerState` construction
   in `expand_node`. 193,280 → 19,683 constructions.

**Gotchas:** all three preserve the loop walk untouched — see *Established correctness findings*.
Preserve `_max_distance_to`'s depth-at-first-pop and `max_d > 0` quirks. Land T3 **before** T4;
doing it after would make these benchmarks unreadable.

### T4 — RNG contract

Making the PRNG *algorithm* the shared contract — rather than shuttling random numbers across a
process boundary — is what lets differential testing demand both implementations find the *same*
plan, instead of merely both finding *a* plan. **ChaCha8** is ~20 lines of pure Python and is
`rand_chacha` on the Rust side; both `rand_chacha` and `rand_pcg` carry explicit value-stability
guarantees across releases, which `rand::StdRng` explicitly does not.

Export *decisions*, not bits — a weighted choice and a bounded integer — so the two sides cannot
disagree about how weights become an index. Replace the four `np.random` sites (`solver.py:328`,
`:907`, `:913`, `:915`, and the mcts-phase twins at `:1006`, `:1012`, `:1014`) and the
`np.argpartition` tie-breaks (`:753`, `:912`, `:969`, `:1011`).

**This is the one step that legitimately cannot hold fingerprint parity** — a different stream
finds different, equally valid plans. Validate across many seeds by comparing *distributions* of
completeness, step count, and checker verdict; never by comparing a single run.

**Gotchas:** `np.argpartition` tie-breaking is a determinism surface independent of the RNG and
must be ported deliberately or replaced with an explicit rule on both sides. The global
`np.random.seed` at `:328` must go away, not just be wrapped.

### T5 — Rust solver core

**The precedent is already in this repo and it is complete.** `main/relay_agent/` is a Cargo
project (`msm_relay`, edition 2024) cross-compiled to exactly four targets by
`main/relay_agent/build.sh`:

> `x86_64-unknown-linux-musl`, `x86_64-apple-darwin`, `aarch64-unknown-linux-musl`,
> `aarch64-apple-darwin`

Driven by `./dev.sh -brc` (fetch the upstream cross-compile container
`joseluisq/rust-linux-darwin-builder`) then `./dev.sh -br` (build all four), which delegate to
`main/relay_agent/dev.sh -bb` / `-b` (`dev.sh:245-252`). Binaries are baked into the docker image
at `/app/msm_relay.{arch}-{system}` and `bootstrap.py:41-42` copies the right one to
`<deploy_root>/relay/msm_relay`; `constants.py:112` resolves it. `_assert_real_relays`
(`dev.sh:30`) guards against 28-byte stub binaries — a real bug from 0.18.4.

Model `main/solver_engine/` (`msm_solver`) on all of that, **with one important difference**:
the relay runs on the *agent host*, but the solver runs *locally at plan time* in whatever
process is planning (CLI, GUI, notebook). So it cannot rely on the agent-deploy path — it must
also ship inside the pip wheel and conda package. Ship all four binaries as package data and
select at import by `platform.machine()`/`platform.system()`, mirroring `bootstrap.py:41`'s
`{architecture}-{system}` naming. Note that `setup.py` currently ships `gui/static/**` and
nothing else beyond the Python tree, so package data needs a new entry.

The subprocess boundary forces the port's scope, which is good: the binary receives a serialized
problem and returns a step list, so `Transform` / `Endpoint` / `Dependency` stay Python-side as
the public API the standard library and DAG renderer already depend on.

**Data design is where the port earns more than speed.** Intern property strings to `u32` once
and represent a type as a fixed-size bitset, turning `IsA` — `y.properties ⊆ x.properties`, the
hottest primitive in the entire system — into a couple of AND instructions instead of a Python
subset test. Use arena indices for identity. Most importantly, **separate `ApplicationId` from
`ApplicationSig` at the type level**: Python conflates arena identity with the structural
signature behind one string, and that conflation is exactly what made the earlier rewrite unsound
and what makes `expand_node`'s step removal drop both steps on a collision. In Rust the compiler
can stop that.

Presence of the binary means use it; absence means fall back to Python. That gives the
side-by-side switch for free and makes the fallback a real path rather than a flag nobody
exercises.

**Gotchas:** the wire format is a genuine interface and needs a version constant from day one —
this repo has scar tissue from `LIN_PAYLOAD_VERSION` desyncing from its Groovy emitter and
failing every containerized task while the fast suite stayed green. Signature *collisions* in
Rust are semantic, not hash accidents, and must survive the port rather than being hashed away.
The Python fallback must be exercised in CI, not merely present. `-br` failing silently leaves
stub binaries — respect the existing guard and extend it to `msm_solver`.

### T6 — Rust incrementality

A child state is its parent with one application swapped, so `e2appl`, `produced_from`,
`_product2producer`, the production map and the lineage counts are all O(swapped slots) to update
rather than O(state). The state signature becomes O(1) with an invertible commutative combiner.

Distances are the hard one, and this is precisely why they aren't cached today: a swap changes
the producer of some endpoints, so distances change for anything whose backward cone contains a
changed producer. The invalidation set is forward-reachability from the two swapped endpoints.

**This task is explicitly conditional on measurement.** Maintaining a delta is not free, and for
a graph of thirty-odd nodes rebuilding may genuinely win. Each incremental structure lands behind
a benchmark against the rebuild-from-scratch baseline, and anything that doesn't beat it is
dropped. Expect some to be dropped — that is the task working, not failing.

**Gotchas:** the frontier holds thousands of live states expanded **out of order**, so a single
mutable working state with an undo log is not sufficient — this needs persistent/copy-on-write
structures (`im`/`rpds`) or an arena with parent pointers and path replay. Underestimating that
is the main way this turns into a rewrite. Use **wrapping 128-bit addition** for the incremental
signature combiner, **not XOR**: XOR cancels on duplicates, and two identical step signatures is
precisely the collision case we are trying to keep visible.

## Verification

Every task reports through T1's benchmark runner. Concretely:

- **Per-change A/B**: record fingerprint + wall time before, apply one change, record after.
  Fingerprints must match (except T4); wall time must improve or the change is reverted.
- **Corpus gate**: `PYTHONPATH="$PWD/src" mamba run -n msm python -m pytest tests/solver -q`
  after every change — quote the "N passed" line, never the exit status.
- **Template gate**: all four shipped templates solve `ok=True` with topologically equivalent
  plans. Baseline structural fingerprints under the current code:
  `annotation_palette 374145c3cbfd`, `isolate_assembly c149024ce4a6`,
  `metagenomics d7764b141ede`, `pangenome_heatmap 63c604ff6a97` — verified stable across two
  independent runs. (These come from a scratch fingerprint whose exact hashing T1 will replace;
  treat them as evidence that stability is achievable, and re-baseline with T1's canonical form.)
- **Differential gate** (T5 onward): Rust and Python, same seed, topologically equivalent on
  100% of the generated corpus.
- **Fallback gate**: with the binary absent, the full suite passes unchanged.
- **Release gate**: `./dev.sh -brc && ./dev.sh -br` produces four non-stub binaries; the extended
  `_assert_real_relays` check passes.

## Execution protocol

Run under the **autopilot** skill (`~/.claude/skills/autopilot/SKILL.md`). Each of T1–T6 is a
work block. The sequencing rule is that a block finishes and its benchmarks are recorded before
the next begins — the A/B gates are worthless if two changes land between measurements.

**Compact between blocks.** At each block boundary call
`reflection(verb="compact", args={pane: "0:0", followup: "<next task>"})` so the session compacts
at end-of-turn and resumes with fresh context pointed at the next block. Do not carry a block's
investigation history into the next one. **This plan file is the handoff** — revise it in place
as the work teaches us things; a stale plan mid-run is worse than none.

Mirror this file to `plans/solver-performance-and-rust-port.md` in the repo as the durable
artifact (a draft is already there from before plan mode and should be overwritten with this
version), and register it via
`artifact(verb="register", args={project:"metasmith", scope:"gui1", …})`.

**Debrief at the very end.** After T6 closes, run the `debrief` skill — commit, journal via
`scope_post kind=journal`, refresh.

## Callouts

**The refiner may not earn its cost.** It never changes the plan on any of the four shipped
templates. It is not inert in general — two branching tests produce plan-changing refinements —
but on lineage-dense real workflows every single-edge swap breaks a lineage constraint, so
single-swap refinement structurally cannot improve them. Once T1 exists it is worth asking
directly whether the refiner should run at all on such plans, because today it costs 70% of the
solve and returns its input. Flagged, not planned — that call is yours.

**`expand_node`'s signature-based step removal is a latent bug** in the current Python solver,
independent of everything here. T2's collision test is what would expose it.

**Wheel size.** Shipping four binaries as package data grows the wheel and conda package by
roughly 4× one stripped Rust binary. Acceptable, but worth knowing before release.

---

# Progress

## T1 — verification harness — DONE

Shipped:

- `src/metasmith/testing/solver_verification.py` — `plan_fingerprint` (colour
  refinement over the step/endpoint bipartite DAG; slot identity carried on the
  edges), `check_plan` (production, typing, acyclicity, topological order,
  target reached, lineage — sharing no code with the search), `generate_problem`
  + `GeneratorDials`, and two oracles: `forward_closure_solvable` (exact and
  uncapped for lineage-free problems) and `exhaustive_solvable` (lineage
  included, answers `None` at its cap rather than guessing).
- `src/metasmith/testing/solver_bench.py` — `CORPUS` (fast, pinned),
  `STRESS_CORPUS` (slow, mcts-bound), template runner, JSON out, `--baseline`
  diff, `--pin`.
- `tests/solver/` — new axis, registered in `tests/conftest.py::_DIR_MARKERS`.
  `test_solver.py` and `test_solver_scaling_cyanoverse.py` moved here from
  `tests/flow/`; `metasmith_libraries_root` lifted to `tests/conftest.py` since
  three axes now want it.
- `tests/solver/test_verification_harness.py` (48 tests) — the harness's own
  adversarial pass, including a tokenizer-level guard that nothing in it reads
  `instance_id`.
- `tests/solver/test_corpus_pin.py` + `fingerprints.json` — the generated
  corpus pinned; moving it is a deliberate act.
- `tests/perf/test_solver_benchmark.py` — cross-process determinism pin and a
  smoke test on the reporting.
- `tests/unit/test_library_view_ordering.py` — pins the fix below.

`pytest tests/solver -q` → **77 passed in 7.3s**. `tests/perf/test_solver_benchmark.py` → **2 passed**.

### Finding: the template solve path was nondeterministic across processes

The first two-run comparison changed the fingerprint on **all four** shipped
templates with no code change. It was not the fingerprint: inputs, relevant
transforms, and every consumed and produced property set hashed identically
across runs — but one of the five chosen transforms differed each time.

Cause: `DataInstanceLibraryView.Iterate` and
`TransformInstanceLibraryView.IterateTransforms` walked `self._mask`, a
`set[Path]`. `Path.__hash__` is the string hash, which python randomizes per
process. Where several transforms are interchangeable (same input and output
types, different tool) the planner saw them in a different order every run and
picked a different one. Same template, same inputs, two different workflows.

Fixed by sorting both walks. Confirmed: `PYTHONHASHSEED` 0/1/2/3 now agree, and
two independent full benchmark runs agree on all 12 cases.

This is user-visible beyond this work — it means a shipped template did not name
one workflow — and it is worth calling out separately from the performance work.

### Finding: `Solution.complete` is not a completeness flag

`solve_by_mcts` hardcodes `complete=True` on its success return (`solver.py:1350`),
discarding `MctsResult.complete`. An unsatisfiable target yields a partial
3-step plan with `complete=True`, zero refiner calls, and no application of the
target transform. `WorkflowPlan.Generate` guards on
`not result.complete or not result.dependency_plan`, so the first half of that
guard never fires and the whole check rests on the plan being *empty*. A
partial, non-empty plan walks through. `check_plan` catches it ("expected
exactly 1 application of the target transform, found 0"). **Pin in T2.**

### Baseline (post-determinism-fix, `c568a46` + T1)

Two independent processes, identical fingerprints on every case.

| case | fingerprint | seconds |
|---|---|---|
| `annotation_palette_from_assembly` | `032997be12c82041f4556510` | 0.91 |
| `isolate_assembly_from_long_reads` | `5de487d62f5a3cd8fa15c59e` | 0.62 |
| `metagenomics_from_paired_reads` | `ef298da676acd6803b094a48` | **37.2** |
| `pangenome_heatmap_from_assembly` | `3b4e9735bda5714c262ac7e7` | 0.35 |

The pre-fix fingerprints recorded in the plan body above are superseded — the
fix legitimately changed which transform one step uses.

### Note on the corpus: two different bottlenecks

Generated instances are **mcts-bound** — they reach the refiner with one or two
iterations and spend their time in the search. The shipped templates are the
opposite: metagenomics spends >99% of its solve inside `refine_mcts`. A change
that helps only one phase looks free on the other's cases, so T3 must report
both. `STRESS_CORPUS` exists so the mcts side does not depend on the sibling
repo being present.

### Finding: the solver returns unrunnable plans on cyclic transform graphs

Sweep of **10,000** generated problems through `check_plan`
(`$CLAUDE_JOB_DIR/tmp/sweep.py`, 425s):

| outcome | count |
|---|---|
| sound plan | 9,882 |
| **unsound plan** | **59** |
| no plan found (target unreached) | 31 |
| solve exceeded 10s | 28 |

The 31 no-plan cases are *honest*: the exhaustive oracle was run on every one
small enough to answer, and it never contradicted the solver — **0 disagreements**.

The 59 unsound plans all carry the same violation: a step whose input **no step
in the plan produces**. `Solution.complete` is `True` for every one of them.
They land only on profiles whose transform graph carries a cycle — either the
`cycle_density` dial, or a second product group pointing back at a shallower
type. `cyclic` 26/1250, `sink` 20/1250, `pgroups` 13/1250; `plain`, `lineage`,
`dupes`, `multi`, `tiny` are all clean at 1250/1250.

`cyclic-217` verified by hand: transform set includes `{t2},{t3}→{t2}` and
`{t1},{t5}→{t1}`; the returned plan's step 4 reads a `t3` and step 7 reads a
`t2` and a `t3` that nothing writes, and the target's own path runs through
both. The plan cannot execute.

This is the same shape the path-dependent loop rejection exists to catch, and
which measurement showed **never fires** anywhere. Four cases pinned
`xfail(strict=True)` in `tests/solver/test_known_unsound.py` — they will
announce themselves the moment the solver stops returning them.

Also noted: 28 of 10,000 small problems (7 types, ≤6 transforms) took **over 10
seconds** to solve. Search cost is not only a big-library problem.

**This changes T2's shape.** The coverage task was scoped as "write a test that
reaches the loop branch". There is now a corpus of 59 concrete inputs where the
solver demonstrably should have rejected and did not, so T2 starts from
evidence rather than from construction.

### Carried into T2

- `check_plan` has never been run against the **shipped templates** — the bench
  fingerprints them but cannot adjudicate them, because building a
  `SolverProblem` from a template means reconstructing the given/transform/target
  triple the way `WorkflowPlan.Generate` does. Wire that up; if a shipped
  template is unsound the whole picture changes.
- Pin the 59-case unsoundness properly (four are pinned; decide whether the
  right fix is in `_is_valid`, in `prune_steps`, or in refusing to return an
  incomplete plan at all).
- The `Solution.complete` hardcode and `WorkflowPlan.Generate`'s dead guard.
- `expand_node`'s signature-based step removal dropping both steps on a
  collision (from the plan body above; still unexercised).

## T2 — test coverage — DONE

The gaps T1 named are closed, and closing them turned up three defects the
suite could not previously see. One is fixed; two are pinned with their
mechanism, because both live in code T5 has to port and a port that inherits
them silently is worse than one that inherits them knowingly.

### The existing solver suite asserted a constant

18 of the 19 tests in `tests/solver/test_solver.py` ended on `assert
sol.complete`, and `solve_by_mcts` hardcoded `complete=True`. Every solve in
that file now goes through `_solve`, which adjudicates with `check_plan` before
returning. Two tests failed immediately:

- **`test_loop_handling`** — the canonical "solver handles circular
  dependencies without infinite loops" test documented the opposite of its
  name. The search walks the cycle until the iteration budget runs out and
  returns the timeline it happens to be holding: `max_iter=256` gives a 256-step
  plan, `max_iter=4096` gives 4096, and none of them ever apply the target. The
  problem is solvable — `exhaustive_solvable` finds a five-application plan at
  every cap from 5 up — so this is a completeness failure in the search, now
  three tests: the budget bound, the honest verdict, and an `xfail(strict)` on
  the plan the oracle proves exists.
- **`test_mixed_samples_all_have_stats_with_lineage`** — a checker false
  positive, and the only one found. Multi-sample plans merge one timeline's
  endpoints into another's and the representative keeps *one* sample's
  properties, so the synthetic given step legitimately emits a
  `read_length:long` endpoint from the slot that stood for the short-read
  sample. `check_plan` now requires that step to emit something the problem
  actually gave, and notes the merge rather than failing it.

### Fixed: `Solution.complete` said nothing, then said the wrong thing

Hardcoded `True` made `WorkflowPlan.Generate`'s `not result.complete` guard dead
code — the whole check rested on the plan being *empty*, so a partial non-empty
plan became a workflow.

Propagating `MctsResult.complete` instead was wrong in the other direction, and
the flow suite caught it: six `test_binning_dag` tests went red, and the plans
behind them were **sound** — target applied, `check_plan` green. That flag is
set only when every timeline resolves on one pass; a multi-sample search
normally exits by running its frontier down while holding a good merged plan.
The sweep put a number on it: **1250 of 1250** generated multi-given instances
and 1181 of 1182 `sink` instances exit that way.

The discriminator that is actually right is `solved_state is not None` — did the
search ever merge a solved timeline in. Landed there. Re-sweeping 10,000
instances gives a tally identical to T1's except that the 31 unsolved cases now
*report* being unsolved, where they previously claimed completion.

### Pinned: where the 59 unsound plans come from

Traced end to end on `cyclic-217`, and the chain is the same on all four
hand-checked cases:

1. the search hands `refine_mcts` a plan that is **acyclic and fully produced**;
2. the refiner rebinds an input to an endpoint produced by a later step,
   creating a **cycle**, and `validate_node` calls that state **valid**;
3. `rectify` unifies endpoint objects in `get_order` order, which cannot be
   topological on a cyclic graph, so a consumer is rewritten before its producer
   and keeps an endpoint the producer then replaces.

Step 3 is the laundering: the cycle disappears and an unproduced input appears
in its place, so the returned plan is acyclic, looks well-formed, and cannot
run. Nothing downstream can tell it was ever a cycle.

Why step 2 gets through: `_is_valid`'s forward walk starts at the given
application and follows *consumers* of each produced endpoint, so an application
counts as reached the moment **one** of its inputs is available — its other
inputs are never tested for being produced. Only the target's direct inputs get
that test, via `missing`. A cycle off that walk is invisible.

`tests/solver/test_refiner_validity.py` pins each link separately, so a fix at
any one of them announces itself.

### The loop branch is reachable after all

T1 recorded zero hits on `_is_valid`'s `return False # looped` across the four
templates, the whole solver axis, and the original scratch scenarios, leaving it
open whether the branch was dead. It is not: `cyclic-217` drives it **eight
times** in one solve. It is *incomplete*, not dead — which is the more awkward
result, and the one T5 has to carry. The test finds the line by its marker
comment rather than by number.

### Also landed

- **The shipped templates are adjudicated for the first time.** All four are
  sound, and their fingerprints are byte-identical to T1's baseline after every
  change in this task. `WorkflowPlan` now carries `_solver_inputs`, the exact
  triple handed to `solve_by_mcts`, so the checker grades the problem that was
  solved rather than a re-derivation of it; `CollectSolverInputs` is that
  triple's one construction site. An unadjudicable template is reported as a
  failure, not skipped.
- **The two scenarios that only ever lived in the notebook** are ported
  (`test_scratch_scenarios.py`). Together they place the boundary of the cyclic
  failure: the dense reciprocal web — four bidirectional pairs, no target
  lineage — solves to the six-step direct route and is stable across seeds, and
  the M×N join scales search breadth without lengthening the plan. Cycles alone
  are fine and a lineage constraint alone is fine; it is a lineage constraint
  whose satisfaction *requires walking a cycle* that defeats the search.
- **Signature collisions** are pinned as semantics rather than accident, next to
  the `expand_node` expression that drops both members of a colliding pair.

### Gates

| gate | result |
|---|---|
| `tests/solver` | 98 passed, 6 xfailed |
| fast suite | **1456 passed**, 7 skipped, 331 deselected, 9 xfailed |
| templates | 4/4 sound, fingerprints unchanged from T1 |
| 10k sweep | 59 unsound (unchanged), 0 oracle disagreements |

### Carried into T3

- The refiner/`rectify` cycle laundering is **not fixed**, only pinned. It has
  to be settled before T5 ports `_is_valid`, and the cheapest sound fix is
  probably to adjudicate the refiner's winner *after* rectification and fall
  back to the search's plan, which is sound in all 10,000 sweep cases.
- The cyclic-search completeness failure (`test_solver.py`'s `xfail`) is
  untouched and is a search defect, not a refiner one.
- 27 of 10,000 small instances still take over 10 seconds. Search cost is not
  only a big-library problem, and T3's corpus should keep a case like it.

## T3 — Python exact wins — DONE

All three planned changes landed, each benchmarked against the state before it
rather than against the original, so every number below is that change's own
contribution. None of them altered a fingerprint on any of the twelve corpus
cases, and none of them was reverted. Together they take
`metagenomics_from_paired_reads` from **34.84s to 7.61s (−78%)**.

| change | metagenomics | corpus total | fingerprints |
|---|---|---|---|
| baseline | 34.840s | 36.624s | — |
| 1. lineage prefilter | 10.678s (−69.3%) | 12.493s | unchanged |
| 2. `_max_distance_to` regroup | 9.142s (−14.4%) | 11.028s | unchanged |
| 3. dedup before construction | 7.614s (−16.7%) | 9.419s | unchanged |

Each is exact by construction, which is what let them go in without a
soundness argument per change:

1. **Lineage prefilter.** `_is_valid` is an AND of three side-effect-free terms
   run most-expensive-first. Reordering an AND cannot change its value. The
   `try/except KeyError` is not defensive padding — `_has_ancestor` indexes
   `produced_from` unguarded, and reaching it earlier than the original order
   can hit a state the loop walk would have rejected first, so a `KeyError`
   means *the prefilter cannot answer* and must fall through to the full check.
2. **`_max_distance_to` regroup.** The walk depends only on its *source*; the
   destination is a plain equality test during traversal and `seen` guarantees
   one visit per node, so one walk per distinct source answers every
   destination. The depth map reproduces both of the original's quirks — depth
   at first pop rather than true maximum, and `max_d > 0` making zero
   indistinguishable from not-found — because both feed the score.
3. **Dedup before construction.** A state's signature is the sorted join of its
   steps' signatures, so it can be computed without the state. `expand_node`
   now yields `(signature, base, application)` and the caller builds a
   `RefinerState` only on a cache miss: 193,280 constructions become 19,683.
   The per-step base list and its sorted signatures are hoisted out of the
   candidate loop, which is where most of this change's time actually comes
   from. The signature comparison that drops **both** members of a colliding
   pair is preserved verbatim — it is a latent bug, and this was a performance
   change.

### The two bottlenecks are still two bottlenecks

The `STRESS_CORPUS` total moved **+0.3%** — noise. All three wins are
refiner-side, and those instances are mcts-bound. This is the result T1
predicted and the reason both corpora exist: T3 has done nothing whatsoever for
the search phase, and no amount of further refiner work will.

### Where the remaining 7.6s goes

Profiled after all three changes (cProfile inflates the wall to 21.4s; read the
proportions, not the seconds):

| | tottime | cumtime | calls |
|---|---|---|---|
| `_depths_from` | 5.83 | 8.58 | 511,758 |
| `score_node` | 2.05 | 13.58 | 19,683 |
| `Node.__hash__` | 1.83 | 1.83 | **39,310,301** |
| `Application.Signature` | 1.82 | 2.62 | 1,136,388 |
| `refine_mcts` | 0.20 | 18.56 | 1 |

`score_node` is 73% of the refiner and the backward distance walk is 63% of
`score_node`. The memo added in change 2 is per-`score_node`, and it cannot be
otherwise: `_product2producer` is rebuilt for every state, so a depth map from
one state says nothing about the next. Making that memo survive across states
*is* T6, and this profile is the argument for it.

`Application.Signature` fell from **18.1M** calls to 1.14M. What replaced it at
the top is `Node.__hash__` at 39.3M calls — a bare attribute read whose entire
cost is Python call overhead, driven by endpoint set and dict membership in the
distance walk. That is not fixable in Python and it is exactly what T5's
interned-`u32`-and-bitset design removes.

**Acceptance criterion 8 (<3s on the Python path) is not met by T3** — 7.61s is
where exact, local Python changes end. Getting under 3s needs either T6's
cross-state incrementality or the port.

### Gates

| gate | result |
|---|---|
| `tests/solver` | **98 passed**, 6 xfailed in 7.98s |
| fast suite | **1456 passed**, 7 skipped, 331 deselected, 9 xfailed in 302.66s |
| templates | 4/4 sound, all four fingerprints unchanged from T1 |
| `STRESS_CORPUS` | 3/3 sound, fingerprints unchanged, +0.3% |

The six `xfail(strict=True)` cases still fail, which is the point: T3 was
required to change nothing about what the solver decides, and the pinned
defects are the sharpest available evidence that it didn't.

The fast suite itself dropped from 351.7s to 302.7s as a side effect.

### Carried into T4

- Everything carried into T3 is still open: the refiner/`rectify` cycle
  laundering, the cyclic-search completeness failure, and the 27 slow instances.
  T3 was a performance task and deliberately touched none of them.
- The three `np.random` call sites and four `np.argpartition` tie-breaks are
  untouched and are T4's actual subject.
