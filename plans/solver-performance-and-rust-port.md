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

## T4 — RNG contract — DONE

`solve_by_mcts` no longer touches numpy's global stream. Every random decision
now goes through `src/metasmith/models/solver_rng.py`, which fixes both halves
of a decision — the bit stream *and* the rule that turns bits into an index —
because either half left implicit is a place the Rust port can silently
disagree.

- **Stream:** ChaCha8 in reference form (key = seed as eight little-endian bytes
  then 24 zeros, zero nonce, counter from 0, sixteen words per block consumed in
  order). Anchored to the RFC 8439 ChaCha20 block vector, so the permutation is
  provably the reference one and not merely self-consistent.
- **Rust parity is measured, not assumed.** `rand_chacha` 0.10.0 with
  `ChaCha8Rng::from_seed(seed_to_key(42))` emits `0x198fa887 0x59273471
  0x169df72b 0x49238aa4 …` — byte-identical to this module's first eight words.
  `seed_from_u64` runs the seed through PCG first and must never be used.
- **Decisions, not bits:** `weighted_index` (integer weights — the call sites
  held literal integers and the normalisation into floats was pure loss),
  `bounded_int` (rejection, and *no draw at all* for n≤1, which is contract
  rather than optimisation: a stream that drifts by one word diverges from
  there on), and `pick_top_k`. The two orderings `np.argpartition` used to
  decide arbitrarily are now stated rules: `argmax_index`/`argmin_index` take
  the **first** extremum, `top_k_indices` is best-first with ties to the lower
  index, and NaN ranks worst in whichever direction "best" points.

### The stream is worth about half a percent of the solver's verdicts

Fingerprint parity is not available here by construction — a different stream
finds a different, equally valid plan — so the claim is distributional, over
10,000 generated problems and separately over 200 problems × 8 seeds.

| | old (numpy) | new (ChaCha8) |
|---|---|---|
| sound plans | 9,883 | 9,940 |
| **unsound plans** | **59** | **5** |
| unsolved | 31 | 44 |
| exceeded 10s | 27 | 11 |
| mean steps | 6.198 | 6.152 |
| sweep wall time | 415.5s | 308.2s |

The 1,600-solve multi-seed sweep agrees in every direction: unsound 13 → 1,
timeout 2 → 0, unsolved 3 → 1, mean steps 5.888 → 5.787.

**None of that is a fix, and the cross-check is what proves it.** Running each
stream's failures under the other stream: all 59 of the old unsound cases come
back *sound* under ChaCha8, and all 5 of the new ones were *sound* under numpy.
The laundering in `rectify` was not touched by this task; which problems fall
into it is a lottery. The same is true of the unsolved set in both directions —
the new stream solves 13 problems the old could not and fails 21 it could.

So the honest reading is a measurement of how much of this solver's behaviour
is stream-luck: **about 0.6% of generated problems change verdict on a change
of PRNG alone.** That is the strongest argument yet for the contract existing
at all, because it means a Rust port validated against "both find *a* plan"
would be validated against nothing.

### The templates did not move

All four shipped templates keep their exact T1 fingerprints — `annotation_palette
032997be12c82041f4556510`, `isolate 5de487d62f5a3cd8fa15c59e`, `metagenomics
ef298da676acd6803b094a48`, `pangenome 3b4e9735bda5714c262ac7e7` — and the whole
flow suite is green. Three of eight generated corpus cases moved (`chain-10`,
`kitchen-sink`, `product-groups`; `kitchen-sink` went from 7 steps to 11, both
sound) plus one of three stress cases, and `tests/solver/fingerprints.json` was
regenerated deliberately.

### One regression, found by the corpus split and reverted

The first cut of `pick_top_k` sorted the whole frontier. That is the right rule
and the wrong implementation: the refiner re-ranks a frontier of ~19,000 states
on every one of its 256 iterations, so an `n log n` sort with a Python key
replaced numpy's C-level `argpartition` and cost **+12.3%** on metagenomics
(7.614s → 8.553s) while the mcts-bound stress corpus, whose frontiers are small,
got *faster*. Exactly the split T1 predicted, doing its job. Since both call
sites ask for `k=1`, `top_k_indices` now takes an `argmax` fast path — the same
rule, one pass — and metagenomics is back to 7.606 / 7.560 / 7.740s across three
runs against T3's 7.614s. Stress total −1.8%.

### Gates

| gate | result |
|---|---|
| `tests/solver` | **124 passed**, 7 xfailed in 9.55s |
| `tests/perf/test_solver_benchmark.py` | **3 passed** in 11.75s |
| fast suite | **1482 passed**, 7 skipped, 331 deselected, 10 xfailed in 313.89s |
| templates | 4/4 sound, all four fingerprints unchanged from T1 |
| RNG contract | 26 tests, incl. the RFC 8439 vector and the pinned stream |

The `xfail(strict=True)` anchors had to be **re-anchored, not promoted**. All
four old ones XPASSed, which under this task's finding means "the stream moved",
not "the bug is gone" — `test_known_unsound.py` now pins the five `sink` cases
the new stream produces and says so in its docstring, and
`test_refiner_validity.py` moves from `cyclic-217` to `sink-6623`, which
exhibits all four links of the chain in one solve and drives the loop-rejection
branch 177 times.

### Carried into T5

- The port must implement the contract, not `rand::StdRng`, and should assert
  `SOLVER_RNG_VERSION` on the wire alongside the problem format.
- `_entropy` (`solver.py:896`) still runs through numpy — `a/a.sum()` and
  `(p*log2(p)).sum()`. numpy sums pairwise, a naive Rust loop does not, and the
  result feeds `score_node` directly. This is a float-reproducibility surface
  T4 deliberately left alone; it needs an explicit summation order on both
  sides.
- Everything carried into T3 and T4 is still open: the refiner/`rectify` cycle
  laundering (now anchored on `sink-6623`), the cyclic-search completeness
  failure, and the slow tail (11 of 10,000 instances over 10s, down from 27).
- Acceptance criterion 8 (<3s Python path) remains unmet at ~7.6s and is now
  entirely T5/T6's to close.

## T5 splits into four, because the port has a prerequisite

T5 was written as one block: build `main/solver_engine/`, port the search,
ship four binaries. The first measurement taken against that plan found a
blocker underneath it, so T5 is now four blocks and this is the first.

- **T5a — the iteration-order contract.** Make every order-bearing iteration in
  the solver a *stated* order rather than CPython's. Prerequisite for
  everything below; done, below.
- **T5b — the shipping skeleton.** `main/solver_engine/` (`msm_solver`), the
  four cross-compile targets, `dev.sh -brs`/`-bs`, package data in the wheel and
  conda package, binary resolution with a Python fallback, and the wire-format
  version constant. The Rust side implements `solver_rng.py` and nothing else
  yet, with a differential test that the two streams agree draw for draw. The
  point is to prove the *delivery path* — four targets, packaging, fallback —
  before the hard part depends on it.
- **T5c — the Rust search core.**
- **T5d — the differential gate**: Rust and Python, same seed, topologically
  equivalent on 100% of the generated corpus.

## T5a — the iteration-order contract — DONE

**The solver's answer depended on CPython's hash-table layout.** Several
searches iterate a `set`, and that order reaches the plan: it decides which
application is appended to the frontier first, and the selection rules break
ties by index. Nothing about this is visible in the source, and two runs of one
interpreter agree perfectly, so nothing had caught it.

The probe is what makes this a measurement rather than a worry. `Node.__hash__`
returns `self.hash` and `Node.__eq__` compares `self.hash`, so XORing a salt
into `__hash__` — and only `__hash__` — leaves every signature, key and equality
relation exactly as it was while completely rearranging every set and dict the
solver builds. Nothing about the problem changes; only the container layout
does. Under that salt, **four of the eight corpus fingerprints moved**, and
`kitchen-sink` went from 11 steps to 8. A source-level cross-check agreed:
reversing five set iterations by hand moved five of the eight.

That is fatal to the port on its own terms. Rust has no CPython set to imitate,
and the alternative to stating the order is reimplementing CPython's probing
sequence and pinning the solver to an interpreter version.

### The contract

Ranks are assigned once per solve and every order-bearing iteration reads them:

- **Transforms** rank by position in the caller's own sequence (`given_tr`,
  then `transforms` as passed, then `target`), keyed by identity — so two
  duplicate transforms stay distinguishable, which a signature-keyed rank could
  not do.
- **Dependencies** rank by first appearance walking that same sequence. Equal
  dependencies collapse, exactly as they already do in `demand2product`.
- **Endpoints and applications** rank by signature, which is unique within any
  one set because that is precisely what their `__eq__` compares.

Five sites: the given groups, the producer walk that estimates distance to
target, `_find_endpoints`, `get_order`'s reachable layer, and the candidate
transforms the search draws children from. `demand2product` and
`demand2producer` are frozen into rank order once at construction and read as
ordered sequences from then on; `get_order`'s `reachable` becomes a list in
`steps` order, since `seen` already made the signatures within a layer unique
and the `set` was only collapsing on what `seen` had collapsed on.

`tests/solver/test_iteration_order.py` keeps it, and keeps itself honest: it
asserts the plan is identical under four salts, and separately that the salts
still rearrange a set at all, so an inert probe cannot pass silently.

### One regression, found and paid down

The first cut sorted at the point of use. That put a `sorted()` inside the
distance walk, which is the hottest loop in the search-bound cases — profiling
`wide-search` showed **4.9 million** calls to it — and cost **+35%** on the
stress corpus for plans that were byte-identical to T4's. Hoisting both maps
into rank order once at construction turned that into **−4.7%**: iterating a
list beats iterating a set often enough to pay for the ordering outright.

### What moved, and what that costs

| | T4 | T5a |
|---|---|---|
| stress corpus (search-bound) | 17.60s | **16.77s** (−4.7%) |
| templates (refiner-bound) | 9.49s | 9.92s (+4.5%) |
| `metagenomics_from_paired_reads` | 7.64s | 8.13s (+6.5%) |

All four shipped templates changed fingerprint — `annotation_palette
904b73155dad0a913c57db44`, `isolate 537402bc8fa9491c8c4929af`, `metagenomics
f14b0f5c7f0caa2303e9f1fe`, `pangenome dd319cd7c2d72f61d7871637` — with step
counts unchanged (15 / 9 / 29 / 3) and all four sound. They held through T4 and
they do not hold through this, which is the honest consequence of the previous
plans having been chosen by a hash table. Three of eight corpus fingerprints
moved and the stress corpus did not move at all.

The template slowdown is plan-dependent, not overhead: the step counts are
identical and the search simply lands in a slightly costlier neighbourhood.
Caller-declaration order was chosen because it is the most explicable stated
order available, not because it was tuned — ranking transforms by distance to
target instead is a live knob, and a search-quality question rather than a
determinism one.

### The distributions hold; the membership moves again

Fingerprint parity is not available here, for the same reason it was not
available in T4 — a different tie-break finds a different, equally valid plan —
so the claim is distributional, over 10,000 generated problems and separately
over 200 problems × 8 seeds.

| | T4 | T5a |
|---|---|---|
| sound plans | 9,940 | 9,926 |
| unsound plans | 5 | 7 |
| unsolved | 44 | 52 |
| exceeded 10s | 11 | 15 |
| mean steps | 6.152 | 6.173 |
| sweep wall time | 308.2s | 330.3s |

Every shift is within noise at this sample size, and the multi-seed sweep says
the same: 1,598 → 1,589 sound, unsound 1 → 1, unsolved 1 → 1, mean steps 5.787
→ 5.788.

**And, as in T4, the cross-check proves nothing was fixed or broken.** Of T5a's
seven unsound cases, five are *sound* under T4's ordering and one was unsolved;
of T4's five, four are *sound* under T5a's. The `rectify` laundering is
untouched. `sink-9391` is the single case unsound under both, which makes it
the first anchor here that has survived a change of decision rule — the
`xfail(strict)` set is re-anchored on the new seven and
`test_refiner_validity.py` moves to `sink-9391`, which drives the
loop-rejection branch 2259 times in one solve.

The one number that looks alarming is the multi-seed sweep's wall time, 9.9s →
101.5s, from timeouts going 0 → 9. It is two problems, not a trend:
`sink-199` times out on seven of its eight seeds and `sink-71` on two. That is
the known slow tail landing on different instances, which is why the 10,000
sweep — where one pathological instance cannot dominate — moves only 11 → 15.

### Gates

| gate | result |
|---|---|
| `tests/solver` | **133 passed**, 9 xfailed in 16.49s |
| `tests/perf/test_solver_benchmark.py` | **3 passed** in 11.80s |
| templates | 4/4 sound, all four fingerprints moved deliberately |
| corpus pin | regenerated: `chain-10`, `kitchen-sink` (11→9 steps), `multi-given` |
| salt invariance | identical plans at 4 salts, corpus **and** stress corpus |

### Carried into T5b

- The port's data design already has its first constraint from this task:
  **iteration order is part of the contract**, not an implementation detail.
  The Rust side must reproduce these five orders, which is exactly what arena
  indices give for free — transform rank *is* the arena index of the transform,
  and dependency rank the arena index of the dependency. That is a reason to
  build the arenas in the caller's declaration order rather than any order
  convenient to the loader.
- Ranking transforms by distance-to-target instead of declaration order is an
  untried knob that would change plan quality without changing determinism.
  Worth an experiment, but it is a search-quality question and must not be
  smuggled into a portability change.
- `_entropy` (`solver.py:896`) still sums through numpy, and its summation
  order is the last unstated determinism surface in the solver.
- Everything carried into T3/T4 is still open: the refiner/`rectify` cycle
  laundering (now anchored on `sink-9391`), the cyclic-search completeness
  failure, and the slow tail.
- Acceptance criterion 8 (<3s Python path) is unmet at ~8.1s.

## T5b — the shipping skeleton — DONE

The port's delivery path, built and proven before the search depends on it.
`main/solver_engine/` is a Cargo project (`msm_solver`, edition 2024) modelled
on `main/relay_agent/`, cross-compiled to the same four targets by the same
upstream container, and it implements exactly one thing: `solver_rng.py`.

Shipping it first is the point. A search that works but cannot be delivered is
worth nothing, and every part of the delivery — four toolchains, packaging,
resolution, version checking, fallback — has a way of failing that only shows up
at release. Landing them against a hundred lines of RNG rather than against the
search means a failure here is unambiguous.

### The one way it is not the relay

`msm_relay` runs on the *agent host*. It is baked into the docker image and
`bootstrap.py` copies the right slot out at deploy time. `msm_solver` runs
*locally, at plan time*, in whatever process is planning — the CLI, the GUI, a
notebook — and that process may never have deployed an agent. So the binaries
ship as **package data inside the pip wheel and the conda package**, and
`models/solver_engine.py` picks one at import by `platform.machine()` /
`platform.system()`, reusing `bootstrap.py`'s `{architecture}-{system}` naming so
there is one convention in the repo rather than two.

Proven end to end rather than argued:

| step | evidence |
|---|---|
| four targets | 2 static musl ELFs (x86_64, aarch64) + 2 Mach-O (x86_64, arm64), `file`-verified |
| staged | `src/metasmith/engine/msm_solver.{slot}`, gitignored, `BUILD_KIND=cross` |
| wheel | all four inside `metasmith-*.whl` at mode 0755 |
| sdist | same four (so the conda package, built from the sdist, carries them) |
| installed | pip-installed into a clean venv, resolved, probed, `Backend("rng") == "rust"` |

### Presence means use it — and three ways that goes wrong quietly

`GetEngine()` returns a binary only when it exists for this platform, answers
`version` with versions this build agrees with, and advertises the capability
being asked for. Each of those has a silent failure mode, so each is a refusal
with a reason rather than a shrug:

- **Absence is normal**, not an error. A source checkout has no binaries and
  everything works; the Python solver runs. This is the case in CI today.
- **A version mismatch is refused loudly.** The scar tissue is
  `LIN_PAYLOAD_VERSION`, which drifted from its Groovy emitter and failed every
  containerized task while the fast suite stayed green. Two constants, not one:
  `SOLVER_RNG_VERSION` (the decision contract) and `SOLVER_WIRE_VERSION` (the
  envelope) move for different reasons, and one constant covering two
  independently-moving things is how that desync stayed invisible.
- **A capability it does not advertise falls back.** This build says
  `["rng"]`, so `Backend("solve")` is `"python"` and will stay that way until
  T5c. The port lands a piece at a time and says which piece.

`METASMITH_SOLVER_ENGINE` overrides the search — a path, or `python`/`off` to
force the fallback. That last value is what makes the fallback a path something
*runs* rather than a path that exists: `tests/solver` passes identically with
the engine staged and with it forced off (156 passed / 9 xfailed, both).

The packaging guard, `_assert_solver_engine`, is shaped like `_assert_gui_bundle`
and matters more than the relay's. A wheel with no engine still plans — on the
Python solver, correctly, just slower — so nothing fails and nobody finds out.
It also refuses a `-bel` host build, because nothing about a Linux ELF says
whether it was linked against musl or against the build machine's glibc; the
staging step writes a `BUILD_KIND` marker instead of the guard trying to infer it.

### The differential gate, at the level the port can currently be tested

A trace is a *script of decisions* replayed by both sides against one stream,
and every result carries the running draw count. Comparing raw words would not
show a disagreement about how a weight becomes an index; comparing values alone
would not show one side consuming a word the other did not — a rule that returns
the right answer off the wrong number of words is correct exactly once and wrong
forever after. Scripts are generated (2,000 ops × 5 seeds) and weighted toward
the three places a reasonable implementation diverges: ties, NaN, and degenerate
choices that must consume nothing.

`rand_chacha` 0.10.0 `from_seed` reproduces the Python pin word for word, pinned
now on both sides — in the crate's own unit tests and in `test_rng_contract.py`.
`seed_from_u64` runs the seed through PCG and appears nowhere.

**It found one, on a hundred lines of code.** The first Rust `top_k_indices`
sorted with `total_cmp`, which is the obvious choice — it is the total order on
`f64` and it makes the NaN reasoning airtight. It also orders `-0.0` *before*
`0.0`, while Python's sort on the `(-rank, i)` tuple calls them equal and falls
through to the index. A scores list holding both spellings of zero was all it
would have taken to hand the two implementations different plans, on a
difference no amount of reading either file would surface. `partial_cmp` is
correct here for the same reason it looks unsafe — `rank_high` has already
removed every NaN, so the unwrap cannot fire, and the remaining order is
Python's. All five generated seeds and the hand-written cases failed against the
pre-fix binary, which is the evidence that the harness has teeth rather than the
hope that it does.

That is the argument for shipping the skeleton before the search, made concrete:
a divergence this small, found here, costs an afternoon. Found in T5c it would
have been one wrong plan in a differential sweep of thousands, with the whole
search to search through for it.

### Data design established for T5c

Fixed now because the search will be built on it, and because T5a made one of
these a contract rather than a preference:

- **Arena indices are the ranks.** T5a's iteration-order contract says transform
  rank is position in the caller's own sequence and dependency rank is first
  appearance walking it. Building the arenas in caller declaration order makes
  the rank *be* the index, so the five ordered sites cost nothing to reproduce.
- **`ApplicationId` and `ApplicationSig` are separate types**, not one string.
  Python conflates them, which is what makes `expand_node` drop both members of
  a colliding pair. Collisions are semantics and must survive the port; the
  compiler is what stops the confusion.
- Property strings intern to `u32`, a type is a fixed-size bitset, and `IsA` is
  a couple of ANDs.

### Gates

| gate | result |
|---|---|
| `cargo test --release` | **5 passed** (stream pin, degenerate draws, NaN both directions, ±0.0) |
| `tests/solver/test_solver_engine.py` | **23 passed** — 15 resolution/fallback, 8 differential |
| `tests/solver` with the engine staged | **156 passed**, 9 xfailed |
| `tests/solver` with `METASMITH_SOLVER_ENGINE=python` | **156 passed**, 9 xfailed |
| `tests/perf/test_solver_benchmark.py` | **3 passed** |
| fast suite | **1514 passed**, 7 skipped, 12 xfailed |
| four targets | built, `file`-verified, in the wheel and the sdist |
| clean-venv install | resolves and probes the packaged binary |

### Carried into T5c

- The search is the whole of T5c; `capabilities` is how it announces itself, and
  `test_the_search_is_still_python_today` is the test that fails the day it does.
- Two defects to carry *knowingly*, not silently: `_is_valid`'s `# looped` branch
  is reachable but incomplete (anchored on `sink-9391`, 2259 hits), and the
  refiner/`rectify` cycle laundering is pinned and unfixed. Settle whether to fix
  the laundering **before** porting `_is_valid` — the cheapest sound fix is to
  adjudicate the refiner's winner *after* rectification and fall back to the
  search's plan.
- `_entropy` (`solver.py:896`) still sums through numpy pairwise and feeds
  `score_node`. It is the last unstated determinism surface and needs an explicit
  summation order chosen for both sides before the score is ported.
- The wire format currently carries one request shape (`rng-trace`). The problem
  and plan encodings are new surface, and `SOLVER_WIRE_VERSION` goes to 2 when
  they land.
- Acceptance criterion 8 (<3s Python path) is unmet at ~8.1s and is what T5c/T6
  exist to close.

## T5c — the Rust search core — DONE

Landing in verifiable pieces rather than as one commit, for the reason T5b
established: a divergence found against a hundred lines is an afternoon, and the
same divergence found against the whole search is a haystack. The pieces are the
score's float rules, then the problem encoding, then the search, then the
refiner.

### The two decisions T5b left open, settled

**`_entropy`'s summation order** was the last unstated determinism surface, and
it turned out to be *two* surfaces rather than one. `ndarray.sum` is pairwise,
not sequential -- it agrees with a left-to-right sum for n < 9 and stops
agreeing at n = 9. And `np.log2` is not the platform's `log2`: over 20,000
random values in (0, 1) numpy disagreed with libm in the last bit **52** times.
Anything calling the C library -- CPython's `math.log2`, Rust's `f64::log2` --
would have disagreed with the solver that often, per value, forever.

Both are gone. `models/solver_math.py` states the order (left to right, the
caller's) and uses the platform's logarithm, `smath.rs` is the same function,
and the wire carries an `entropy` op so the two are compared rather than
believed. **numpy is no longer imported by `solver.py` at all** -- the entropy
was its last call site.

The change moved nothing. All twelve benchmark fingerprints are unchanged, and a
10,000-problem sweep is identical to T5a's on every number: 9,926 sound, 7
unsound (the same seven, `sink-9391` among them), 52 unsolved, 15 over the
timeout, mean 6.173 steps. Which is the expected result rather than a lucky one:
`lineage_usage` rarely reaches nine entries, and below nine numpy's sum *is* the
left-to-right sum.

**The refiner/`rectify` laundering is carried into the port, not fixed first.**
Fixing it is a semantic change: it would move plans, and moving plans is exactly
what makes a differential gate unreadable. A port that changes behaviour cannot
be checked against the thing it is porting. The defect stays pinned by its
`xfail(strict)` anchors, `_is_valid`'s reachable-but-incomplete `# looped` branch
is reproduced faithfully, and the fix -- adjudicating the refiner's winner after
rectification -- is a change to make once T5d can prove it is the only one.

### The wire was quietly changing the numbers it carried

`serde_json` parses floats *best effort* by default. It is a documented feature
flag, roughly twice as fast, and it does not always land on the double the
writer wrote: it read `0.9999999999999999` as exactly **1.0**, and read other
ordinary values a few ulps off.

This was found by the `log2` op, which exists to compare the two libms and
instead caught the transport underneath them -- the two libms agree exactly.
That is worth noting on its own: the probe that finds a bug is often not the
probe aimed at it, which is an argument for probing the layer *below* the one
you suspect.

Left alone it would have been invisible and permanent. Nothing errors; the
engine answers confidently, with a number derived from an input it was never
sent. `float_roundtrip` is now on, `wire::tests` pins it against
`f64::from_str` -- correctly rounded, and what CPython's `float()` also does --
and `test_a_float_survives_the_crossing` pins it from the Python side.

`SOLVER_WIRE_VERSION` is **2** on both sides. A new op variant is an envelope
change like any other.

### The port's identity model, which the fingerprint decided

The plan called for `ApplicationId` and `ApplicationSig` to be separate types.
Reading the fingerprint showed the same split is needed one level down, and for
a sharper reason: `_fingerprint_detail` keys endpoints by `id(e)`. Two endpoints
with the same properties *and* the same lineage are `==` in Python -- every dict
and set keyed by an endpoint collapses them -- but they are two nodes in the
graph the fingerprint hashes.

So interning endpoints, which is the obvious thing to do to a value type, would
turn two endpoints with one producer each into one endpoint with two producers.
That is a different plan, introduced by the port rather than found by it. In
`model.rs` an endpoint therefore has an arena index for identity and an interned
`EpSig` for equality, and every Python `dict`/`set` keyed by an endpoint is keyed
by the signature. Dependencies get no such split, and that is a claim rather than
an oversight: nothing in the solver distinguishes two structurally identical
dependencies.

Endpoints are also *mutable*, which is not a style choice: `rectify` does
`new_e.parents |= ...; new_e.RefreshHash()`, so an endpoint's structure really
does change mid-solve and everything already pointing at it sees the change.

### The problem is read the same way, checked before anything is searched

`describe` is a subcommand and deliberately **not** a capability -- nothing at
plan time asks for it. It reports what the engine derived before its first
decision, and `tests/solver/test_engine_problem.py` compares that to
`Solution._heuristics` over 93 generated problems spanning cycles, duplicate
transforms, lineage, product groups and multi-given, plus the four shipped
templates.

Two comparisons in there are doing real work. The demand maps are compared **as
sequences**, because Python freezes them into rank order and `_find_endpoints`
appends candidates in the order it walks them -- right members in the wrong
order is a different plan, and a set comparison would pass. And the templates are
included because generated problems have a handful of properties, so their type
bitsets are one word and the striding is never exercised; a template carries
hundreds.

`solve_by_mcts` now also reports `distance_scores` and `opportunity_scores` on
`_heuristics`, object-keyed. The existing distance telemetry is keyed by a
transform's printed key, which collapses duplicates and so cannot adjudicate a
port.

### Gates so far

| gate | result |
|---|---|
| `cargo test --release` | **11 passed** (5 rng, 3 entropy, 3 wire float transport) |
| `tests/solver/test_solver_engine.py` | **25 passed** |
| `tests/solver/test_engine_problem.py` | **94 passed** (93 generated + 4 templates in one case) |
| `tests/solver` with the engine staged | **252 passed**, 9 xfailed |
| `tests/solver` with `METASMITH_SOLVER_ENGINE=python` | **252 passed**, 9 xfailed |
| `tests/perf/test_solver_benchmark.py` | **3 passed**, fingerprints unmoved |
| 10,000-problem sweep | identical to T5a on every statistic |
| fast suite | **1610 passed**, 7 skipped, 12 xfailed |

### The search, ported

`main/solver_engine/` now carries the whole of `solve_by_mcts`: the application
model, `generate_applications_of_transform`, both selection phases, the refiner,
`prune_steps`/`get_order`/`rectify`, and `merge_states`. `capabilities` is
`["rng", "solve"]`, `solve_by_mcts` routes to it when one is present, and
`METASMITH_SOLVER_ENGINE=python` forces the old path.

The identity split runs all the way down. `EpId` is an endpoint's identity and
`EpSig` its structure; every Python `dict`/`set` keyed by an endpoint is keyed by
the signature, and every place the plan's *shape* depends on which object it is
uses the id. `ApplId`/`ApplSig` likewise. Dependencies get no split, and that is
a claim rather than an oversight: nothing in the solver distinguishes two
structurally identical dependencies, so they intern.

Four Python behaviours are reproduced that a reasonable reimplementation would
have quietly repaired:

- **`get_order` never keeps the first depth it assigns an endpoint.** Its guard
  is `if e in order: continue`, where `e` is an `Endpoint` and `order` is keyed
  by strings, so the test is always false and the *last* producer of an equal
  endpoint wins. Writing what the line plainly intends changes plan ordering.
- **`expand_node` drops both members of a colliding pair** when it removes the
  step it is swapping. Collisions are semantics -- duplicate transforms really do
  share a structural key -- so the defect travels with them.
- **Endpoints are mutated in place.** `rectify` widens a produced endpoint's
  lineage and re-signs it, and everything already pointing at it is meant to see
  the change. This is the reason endpoints are an arena rather than interned
  values.
- **`rev_emap` is written and never read**, and `SolverState.have` is built,
  copied on every expansion, and never read. Both are simply absent here; that
  they are dead was checked, not assumed.

### The bug the differential harness found this time

The first run diverged on `chain-6`, the smallest case in the corpus, at
iteration five. The decision trace put it exactly: the two streams agreed on 22
decisions and then the explore arm drew `bounded_int(3)` on one side and
`bounded_int(4)` on the other. Same draw count, same words -- different frontier.

`generate_child_nodes` is a **generator**, and its caller adds each child's
signature to `frontier_signatures` as it consumes them. So a transform reached
later in the same expansion sees the earlier transforms' children already
blacklisted. Collecting every transform's children against one frozen blacklist
-- which is what "return a `Vec` of children" naturally does -- left one extra
application on the frontier, which changed what the explore arm drew, which
changed the plan.

Nothing about that is visible in the source. It is a consequence of Python's
`yield` interleaving with a mutation in the consumer, and the only reason it took
minutes rather than days is that the trace named the draw.

### The other failure this could have had: a green run

Both implementations are reached through the same `problem.solve()`. The moment
the engine advertised `solve`, the differential tests started comparing the
engine against *itself* -- and would have passed. The same applies to
`test_iteration_order.py`, whose subject is CPython's hash layout, and
`test_refiner_validity.py`, which counts how often a branch of the Python refiner
fires: both would have gone quietly vacuous.

`UsePythonSolver()` pins the reference, and the differential tests assert
`Backend("solve") == "python"` inside it rather than trusting the context
manager. Worth the two lines: the failure mode is a passing test, and nobody
investigates one.

### What it bought

| | python | engine | |
|---|---|---|---|
| `metagenomics_from_paired_reads` (solve only) | 7.49s | **0.51s** | ×14.8 |
| `annotation_palette_from_assembly` | 0.020s | 0.002s | ×8.3 |
| `isolate_assembly_from_long_reads` | 0.025s | 0.002s | ×10.4 |
| `pangenome_heatmap_from_assembly` | 0.008s | 0.002s | ×4.5 |
| benchmark corpus, end to end | 9.28s | **2.91s** | −68.6% |

Every one of the twelve benchmark fingerprints is unchanged. The end-to-end
template numbers move less than the solve numbers because most of what they
measure is library resolution, not search.

**Acceptance criterion 8 is met on the Rust path and still unmet on the Python
one.** It asks for `metagenomics_from_paired_reads` under 3s *via the Python
path*, and that is ~8.1s. The engine does it in 1.08s end to end. Worth stating
plainly rather than quietly counting the Rust number: the Python solver is the
fallback and the reference, and it did not get faster here.

### Gates

| gate | result |
|---|---|
| `cargo test --release` | **11 passed** |
| `tests/solver/test_engine_problem.py` | **94 passed** — derived maps, 93 generated + 4 templates |
| `tests/solver/test_engine_solve.py` | **110 passed** — same plan, same order, 4 templates |
| `tests/solver` with the engine | **362 passed**, 9 xfailed |
| `tests/solver` with `METASMITH_SOLVER_ENGINE=python` | **362 passed**, 9 xfailed |
| `tests/perf/test_solver_benchmark.py` | **3 passed**, all fingerprints unmoved |
| differential sweep, 4,000 generated problems | **3,998 identical**, 0 mismatches, 2 timeouts |
| fast suite | **1,720 passed**, 7 skipped, 12 xfailed |

The two timeouts are the known slow tail on the *Python* side of the comparison
at a 20s cap, not a disagreement.

### Carried into T5d

- The differential sweep above is T5d's gate at 4,000 problems and needs to run
  at the full corpus size, and on more than one seed per problem. The two
  timeouts should be identified rather than tolerated — under the engine they
  are no longer slow, so the cap only ever hits the reference.
- The refiner/`rectify` laundering is still pinned and unfixed, now in two
  implementations. It is the one change worth making *after* the gate proves the
  port is faithful: adjudicate the refiner's winner after rectification and fall
  back to the search's plan, land it on both sides, and re-anchor the
  `xfail(strict)` set.
- `_is_valid`'s `# looped` branch is reproduced with its hole. Fixing it means
  walking objects rather than signatures, which is a semantic change and belongs
  with the laundering fix.
- The engine's failure mode on a malformed problem is an error, where Python's
  is a `KeyError`. Three places are named in the source (`satisfies_lineage`, the
  final lineage check in `is_valid`, the missing producer in the depth walk).
  None is reachable from the corpus or the templates.
- T6 (incrementality) is now the only remaining performance work, and its case is
  weaker than it was: a swap-and-rebuild at 0.5s per solve leaves much less on the
  table than it did at 7.5s. It stays conditional on measurement.

## T5d — the differential gate — DONE

Acceptance criterion 9 asks for *topological equivalence on 100% of the
generated corpus*. That is a claim about a population, and the thing that
existed after T5c was a throwaway script that had run 4,000 problems at one seed
each. T5d turns it into an artifact —
`src/metasmith/testing/solver_differential.py` — with a gate in the fast axis, a
wider one in `perf`, and a CLI for the runs that are too big for either.

### One seed per problem was hiding an entire regime

The stream does not merely choose between equally-sized plans. It chooses how
big a plan the search settles for, and the refiner's cost climbs steeply with
plan size. The same `sink` instance at problem seed 24:

| solve seed | steps | engine | python |
|---|---|---|---|
| 42 | 7 | 0.15s | — |
| 2³¹−1 | 57 | 55.4s | **807.8s** |

A sweep with the seed pinned at 42 reports a corpus that is uniformly cheap and
never once puts either implementation under load. Every problem now runs under
four spread streams — `42`, `7`, `1234`, `2³¹−1`, the last because a port that
truncates or sign-extends a seed agrees on small numbers and nothing else.

### The cap fires on both sides, and it is not a verdict

The scratch script capped only the reference, on the assumption that the engine
is fifteen times faster and would therefore never be the slow one. That is true
per case and false as a rule: an instance that blows up blows up for *both*
sides, and at 16,000 comparisons the engine hit a 20s cap twice on its own.

So both sides are capped, and a cap hit is recorded as **unadjudicated** —
counted apart from "agreed" and from "disagreed", printed with the flags that
rerun it uncapped, and excluded from the reported speedup so a capped side
cannot floor the number. Folding those cases into either bucket would report a
percentage over a corpus the sweep never finished reading.

### The result

| | |
|---|---|
| comparisons | **16,000** — 8 profiles × 500 problems × 4 streams |
| identical plan, shape, order and checker verdict | **15,984** |
| disagreements | **0** |
| hit the cap | 16, all in `sink`; 14 python-side, 2 engine-side |
| re-run uncapped | 15 of the 16 finished — **all 15 identical** |
| the 16th | `sink-178/s7`, adjudicated at reduced refiner budget — see below |

So **15,999 of 16,000 comparisons agree and none disagree**, on all four
questions the sweep asks in order: completeness, `plan_fingerprint`,
`plan_shape`, and step sequence. The last is stronger than the criterion — the
port has been returning not merely an equivalent plan but the same one, in the
same order — and it is compared separately so a regression from identity to mere
equivalence is visible rather than silently accepted.

Every capped case came from `sink`, the profile with every dial turned up
(cycles, lineage, duplicate transforms, product groups, two given groups). That
is the profile earning its place: the other seven never produce an instance
either implementation finds hard. The uncapped reference runs ranged from 30s to
849s, against 0.9s–64s for the engine — the ×13–15 ratio holding all the way out
into the tail.

### The one case neither implementation can finish

`sink-178/s7` is a 15-transform problem on which **a single refiner iteration
costs the engine 22 seconds**:

| `max_refine` | engine | python | verdict |
|---|---|---|---|
| 0 | 0.48s | 17.3s | identical |
| 1 | 22.7s | 399.7s | identical |
| 4 | 48.6s | >600s, capped | engine only |
| 256 (the default) | >600s, did not finish | not attempted | — |

At the default budget it is not a slow case, it is an unfinishable one, and
equally so on both sides — which is why capping the engine as well as the
reference was the right call rather than a precaution. It is adjudicated at the
budgets that terminate, and identical at both. Left as-is with a name rather
than papered over: the honest statement is that one comparison in sixteen
thousand was checked at a reduced refiner budget, not that all sixteen thousand
were checked at the full one.

This is also the sharpest evidence T6 has. The mcts phase solves this instance
in half a second; everything after that is the refiner, and its cost is driven
by the size of the plan it is handed, not by the size of the problem.

The sweep's aggregate speedup is **×5.2**, not the ×14.8 the templates show.
Generated problems are small, so a large share of the engine's time is process
spawn rather than search — which is the honest number for this corpus and the
reason the template benchmark exists alongside it.

### Acceptance criterion 8, stated plainly

Criterion 8 asks for `metagenomics_from_paired_reads` under 3s **via the Python
path**. It is ~8.1s. The engine does it in 1.08s end to end. The criterion is met
on the Rust path and unmet on the Python one, and the Python path is not a
detail — it is the fallback every source checkout runs and the reference this
whole gate is measured against.

### Gates

| gate | result |
|---|---|
| `tests/solver` with the engine | **366 passed**, 9 xfailed |
| `tests/solver` with `METASMITH_SOLVER_ENGINE=python` | **363 passed**, 3 skipped, 9 xfailed |
| `tests/perf` | **12 passed** |
| fast suite | **1,724 passed**, 7 skipped, 12 xfailed |
| differential sweep, 16,000 comparisons | **0 disagreements**; 15,984 under the cap, 15 more adjudicated off the clock, 1 at reduced refiner budget |

The three skips are the differential tests themselves: with no engine there is
nothing to differ from, which is the fallback gate working rather than a hole.

### Carried into T6

- The refiner/`rectify` laundering is pinned, unfixed, and now in two
  implementations. The gate has done its job — the port is faithful — so this is
  the change to make next: adjudicate the refiner's winner *after* rectification,
  fall back to the search's plan, land it on both sides, re-anchor the
  `xfail(strict)` set in `test_known_unsound.py`. `_is_valid`'s incomplete
  `# looped` branch belongs with it.
- The 16 expensive instances are the only corpus evidence about the refiner
  under load, and they say the cost is in plan *size*, not in problem size — a
  15-transform problem that solves to 57 steps, and one where a single refiner
  iteration costs 22 seconds in Rust. If T6's incrementality is worth anything it
  is worth it there, not on the templates. `sink-178/s7` and `sink-24/s2³¹−1` are
  the two cases to measure against, and the shipped `STRESS_CORPUS` does not
  contain anything like them.
- Criterion 8 on the Python path is the one acceptance criterion still open.

## T5e — the laundering fix — DONE

The one change the differential gate was holding back. It was carried unfixed
through T5a–T5d on purpose: fixing it moves plans, and a port cannot be checked
against a thing that is moving. With T5d clean at 16,000 comparisons, the port is
known faithful, and the defect can be repaired on both sides at once.

### It was link 2, and only link 2

The chain, as `tests/solver/test_refiner_validity.py` had pinned it: the search
hands the refiner a sound plan; the refiner rebinds an input to an endpoint
produced by a later step and `validate_node` calls that cyclic state valid;
`rectify` then rewrites endpoints in `get_order` order, keeps a consumer's stale
endpoint object, and the cycle comes out as an input no step produces.

Link 3 is not a defect and was not touched. `get_order` is a BFS by depth from
the givens and it is correct — asked to order a cycle it flattens the members
onto one depth because there is no answer to give, and `rectify`'s selection then
walks that tied group in an order that puts some consumer before its producer.
Making it "smarter" is meaningless; the guarantee belongs upstream.

### The old check reached a step on one input

`_is_valid` walked *forward* from `given_appl`, following the consumers of each
produced endpoint, and rejected a repeated `Application` signature along a path.
Two things followed. A step was reached as soon as **one** of its inputs was
available, and its other inputs were never tested for being produced at all —
only the target's direct inputs got that test — so a cycle hanging off the side
of the walk was invisible. And the walk was exponential in the number of paths.

Replaced with **schedulability**: every step must become runnable with *all* of
its inputs available, starting from the givens. That fails on exactly two things,
and both are what "invalid" has to mean — a cycle, whose members are each waiting
on another, and a step with an input nothing produces. It also subsumes the
separate `missing` test, since the target is one of the steps.

Endpoints are held by structure (`set[Endpoint]` / `EpSig`, the same notion
`get_order` uses, so a `True` is precisely the promise that ordering finds a
total order) and steps by identity (`id()` / `ApplId`, because two distinct
applications may legitimately share a signature and both have to be runnable).
The verdict does not depend on the order within a layer, so this adds no site to
the T5a iteration-order contract.

### Measured on the anchor, before and after

`sink-9391`, every state `validate_node` judged, tallied as
(verdict, has cycle, schedulable):

| | before | after |
|---|---|---|
| rejected, cyclic | 3,720 | **4,038** |
| **accepted, cyclic** | **318** | **0** |
| accepted, acyclic | 62 | 62 |
| rejected, acyclic | 47 | 47 |

The 318 are the laundering. Nothing else moved.

### What it cost, and what it did not move

A 10,000-problem sweep went from **7 unrunnable plans to 0**; `ok` rose 9,926 →
9,933, exactly the seven, with `unsolved` (52) and `timeout` (15) unchanged.

**Not one fingerprint moved** — all eight corpus cases and all four templates, on
both implementations. That is the result worth pausing on: the fix only ever
rejects states that were being converted into broken plans, so on every instance
that already had a sound answer, the answer is the same one.

`metagenomics_from_paired_reads` on the Python path, four runs each: 8.74 / 7.99
/ 8.33 / 8.08 before, 8.73 / 8.13 / 7.98 / 8.11 after. Indistinguishable. (An
earlier single-run comparison showed +11.9% and was noise; the A/B is the number.)
Across the generated corpus the cheaper check does show: the differential sweep's
engine time fell 133.9s → 30.9s and its reference time 692.6s → 409.6s for the
same 16,000 comparisons.

### Gates

| gate | result |
|---|---|
| `cargo test --release` | **11 passed** |
| `tests/solver` with the engine | **375 passed**, 1 xfailed |
| `tests/solver` with `METASMITH_SOLVER_ENGINE=python` | **372 passed**, 3 skipped, 1 xfailed |
| `tests/perf` | **12 passed** |
| fast suite | **1,733 passed**, 7 skipped, 4 xfailed |
| differential sweep, 16,000 comparisons | **0 disagreements**; 15,983 under the cap, 16 of the 17 capped settled uncapped and all identical |
| 10,000-problem soundness sweep | **0 unrunnable plans** (was 7) |

The one case still not settled at full budget is `sink-178/s7`, unchanged from
T5d and for the same reason — neither implementation finishes it at
`max_refine=256`. It is pinned at a budget that terminates in `tests/perf`.

The `xfail(strict)` count went 9 → 1: seven were the unsound anchors in
`test_known_unsound.py` and one was `test_refinement_does_not_introduce_a_cycle`.
All eight now pass and are kept as assertions — the seven are the hardest
instances anyone has found and a regression surfaces there first.

### Carried into T6

- `_is_valid`'s `# looped` branch is now both reachable and complete, so the
  standing warning against memoizing the loop walk is retired with the walk. The
  new check has no path-dependence to preserve.
- The refiner is still the whole cost. `sink-178/s7` remains the case where one
  refiner iteration costs the engine ~22s, and it is still the only comparison in
  16,000 that neither implementation finishes at `max_refine=256`.
- Acceptance criterion 8 is unchanged and still open on the Python path: ~8.1s
  against a 3s target. This fix was never going to move it — on the templates the
  refiner never reached the branch it repaired.
