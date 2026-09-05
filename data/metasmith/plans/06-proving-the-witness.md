<!-- Run report. The plan as approved, the corrections the evidence forced, and the
     run log. Copied here from ~/.claude/plans/, which is node-local and unversioned. -->

# Prove the witness, so the solver can be replaced

## Context

The solver decides which bioinformatics tools to chain together, and its answer is now adjudicated
by a small Rust predicate — the plan witness — that the engine runs on every plan before emitting
one. That predicate is the trusted computing base, and it is trusted because somebody read it, not
because anything proved it agrees with the written specification.

This plan closes that gap: prove `check_spec`, the equation saying the extracted checker returns
exactly the truth value of the specification, for every problem and every plan. Once it holds, the
search underneath is free — any plan a new search returns is certified by a proved checker, which
is precisely what makes the next piece of work, translating the solver to PUCT, safe to do.

The obligation has been stated and type-checked against the extracted code since the last session.
Only the proof is missing.

## What you said

> we need to do T11

> objective remains to produce a lean certified rust witness to validate each solution of a solver

> this will allow us to then freely change the implementation of the underlying solver with some
> guarentee of correctness

> specifically, the next task (not this session) will be to translate the solver to PUCT

> produce the working witness. use subagents to preserve your context since this is a long and
> complex job

> do whatever is simplest, but correct

## The obligation

One equation, already on disk and already well-typed against the extracted checker:

> `check_spec (p : types.Problem) (q : types.Plan) : solver_witness.check p q = Result.ok (decide (Valid p q))`

It is stated as an equation rather than a biconditional deliberately: the equation carries
totality, soundness and completeness together, where `check p q = ok true ↔ Valid p q` would be
satisfied vacuously by a checker that rejects everything. `check_correct` — the principal's
`∀S. Certified(S) == Valid(S)` — follows from it in one line and is already written.

## What this session measured, and what it overturns

The last session recorded that the proof needs a re-extraction with `-decreases-clauses`, because a
`partial_fixpoint` gives only Scott induction and so cannot assert termination. Half of that is
right. The other half is wrong, and the difference removes an entire phase of work.

- **The `-decreases-clauses` extraction does not compile, and cannot be made to on this pin.** Three
  independent failures: all 73 of its generated tactic tokens are dotted, which Lean cannot resolve
  inside a quotation (reproduced minimally — 30 dotted names fail, 30 undotted pass); it emits
  `partial_fixpoint` on the same definition as `termination_by`, which is a hard parse error; and it
  lowers every conditional to a dependent `if h:` inside a `do` block, which Lean 4.31.0 rejects
  outright (reproduced in five lines, while the default extraction compiles in the same project).
- **It is not needed.** A `partial_fixpoint` definition still carries `eq_def` and `eq_1` — the full
  unfolding equation. Termination does not have to be a property of the definition; it can be a
  theorem about it, proved by ordinary well-founded induction on `bound − index` in the proof, over
  the default extraction that already builds clean and that `Spec.lean` is already stated over.
- A spike on `bits.eq_loop` confirmed the skeleton: unfold with `eq_def`, `dsimp only`, `split`, base
  case by `scalar_tac`, recursive branch to the induction hypothesis at a smaller measure.

Two further corrections to what the record says. `dev.sh --lean-check` pipes `lake build` into
`tail`, so it reports success over a failing build — the same defect the extraction gate was fixed
for. And the `schedulable` gotcha carried forward from the last plan is not real: the Rust says
`pi < cj` and the Lean says `pi.2 < cj.2`. They agree, and both forbid a step consuming its own
output. Nothing to decide there.

## Issues

- **I1.** `check_spec` is stated with `sorry`, so nothing relates the shipped checker to the
  specification, and the trusted base is trusted only by reading.
- **I2.** The recorded route to the proof targets an extraction that cannot compile, so following
  the plan as written would spend a session on a dead end.
- **I3.** `--lean-check` cannot fail. A broken Lean build exits 0, and its `sorry` gate reads one
  file, so a proof tree could be entirely `sorry` and the command would still report a pass.
- **I4.** There is one place for Lean proofs to live and it is the file holding the specification,
  which does not divide into units a subagent can own.
- **I5.** The `ancestors` table and `ancestorB` are different algorithms, so the two clauses that
  use lineage cannot be proved without relating them.

## High-level goals

- **G1.** Establish, once, how to reason about an extracted loop, so every later proof repeats a
  recipe rather than rediscovering one.
- **G2.** Prove each of the ten clauses does exactly what the specification says that clause means.
- **G3.** Settle the one place where the checker and the specification compute the same relation by
  different means.
- **G4.** Make the toolchain incapable of reporting a pass over an unproved or broken tree.
- **G5.** Discharge the obligation itself, so the checker's agreement with the specification is a
  theorem.
- **G6.** Leave the record saying what is now true, including where the previous route was wrong.

## Acceptance criteria

- `docker/solver_witness/dev.sh --lean-check` builds the whole Lean tree and **exits non-zero** on
  any build error, on any `sorry`, and on any `axiom` naming a crate function — verified by
  deliberately breaking each of the three and watching it fail.
- `check_spec` and `check_correct` are proved. `grep -r sorry src/solver_witness/lean/` returns
  nothing, and no proof rests on an added axiom.
- Every one of the ten clause lemmas is stated as an equation against `decide` of its `ValidC`
  conjunct, and each is proved against the function `check` actually calls — for `UniqueProducer`
  that is `cl_unique_producer`'s own scan, not the pointwise `unique_producer_at` the audit crate
  uses.
- The ancestry bridge is proved: the dense single-pass table and the specification's recursive
  relation decide the same thing, under `WellIndexed`.
- **No Rust and no Python changes.** `tests/metasmith/solver/fingerprints.json` is byte-identical
  and `SOLVER_RNG_VERSION` is untouched. If a fingerprint moves, something was done that this plan
  did not sanction.
- The suite is green on the Rust engine — `Backend("solve") == "rust"` asserted — at no worse than
  the 472 passed / 1 xfailed the last session left.
- `docs/metasmith/solver-spec.md`, `docker/solver_witness/README.md` and the `-xd` block in
  `docker/solver_witness/dev.sh` no longer assert the superseded route.

## Tasks

1. **T1.** Make the Lean gate capable of failing (G4).
2. **T2.** Lay out the proof tree and teach the toolchain to carry it (G4, G1).
3. **T3.** Settle the loop-lemma recipe on one loop and write it down (G1).
4. **T4.** Compact.
5. **T5.** Prove the accessor and bit-set lemmas (G1).
6. **T6.** Prove the four small ancestry-free clauses: Target, UniqueProducer, Schedulable,
   Provenance (G2).
7. **T7.** Compact.
8. **T8.** Prove Derived and Shape (G2).
9. **T9.** Prove Givens (G2).
10. **T10.** Compact.
11. **T11.** Prove Indexed (G2).
12. **T12.** Compact.
13. **T13.** Prove the ancestry bridge (G3).
14. **T14.** Prove Conformance and Emission (G2, G3).
15. **T15.** Compact.
16. **T16.** Prove `check_spec` and `check_correct` (G5).
17. **T17.** Gate the build and run the full suite (G4, G5).
18. **T18.** Write the run report and update the documents (G6).
19. **T19.** Debrief.

## How the subagents are used

The proof work divides cleanly and that is what makes delegation safe here. Each clause is one Lean
file that nothing else imports, so a subagent owns a file outright and cannot break a sibling.

The brief every proof subagent gets is the same: the clause name, the Rust entry function and its
loop-bearing helpers with line numbers, the `ValidC` conjunct it must reach, the inventory of shared
lemmas already proved, the recipe from T3, and the build command. The contract is that it returns
only when its file compiles with no `sorry` and no added `axiom`, and that it may not touch
`Spec.lean`, the extraction, or any file it does not own. A subagent that cannot close a proof
returns the goal state it is stuck on rather than a weakened statement — a clause lemma restated to
be provable is the failure mode this whole exercise exists to prevent.

Builds serialize on lake's own lock in one project directory, so subagents may run in parallel and
will simply queue on the build. Two at a time is the working assumption; the box is already loaded.

## Approach by task

### T1. Make the Lean gate capable of failing

`--lean-check` pipes `lake build` into `tail`, so the pipeline's status is `tail`'s and a failing
build exits 0. Capture the build status, fail on it, and extend the `sorry` scan from the single
specification file to every Lean source the project builds, with the same treatment for an `axiom`
naming a crate function that the extraction gate already applies.

`Gotchas:` the container body runs under `set -e`, which is exactly why the pipeline hides the
failure rather than aborting — fixing this means capturing a status, not adding `pipefail` and
hoping. Prove all three failure modes actually fail before trusting the gate; a gate believed rather
than tested is how the previous extraction shipped full of holes.

### T2. Lay out the proof tree and teach the toolchain to carry it

`src/solver_witness/lean/` holds one file. Add a `Proof/` subtree beside `Spec.lean`: a shared
lemma module, and one module per clause. `--lean-check` currently copies three named files into the
project; make it copy the tree, which the lakefile's submodule glob already picks up.

`Gotchas:` the lakefile has no root module and depends on `globs` for that — do not add one. Keep
`Spec.lean` free of proofs: it is the normative statement and a proof landing in it is how a
specification starts being shaped by what was easy to prove.

### T3. Settle the loop-lemma recipe on one loop

The one genuine unknown left. Aeneas's spec lemmas are Hoare triples (`Usize.add_spec : … → x + y ⦃
z => ↑z = ↑x + ↑y ⦄`) and its `step` tactic — `progress` is deprecated under that name — consumes
them, but `check_spec` is a plain equation and `step` refused the `∃ r, f = ok r` form the spike
tried. Settle which of the two shapes the loop lemmas are stated in, get one loop closed end to end,
and write the recipe into the shared module as a worked comment.

`bits.eq_loop` is the loop to use: the spike already has it half-proved, and it needs the
side-condition `len a ≤ len b`, which is representative — most loops here carry one.

`Gotchas:` if triples compose but do not convert cleanly to the raw equation, the conversion belongs
here, once, not rediscovered per clause. Do not settle this by reading; the two builds it takes to
try both shapes are cheaper than a wrong choice repeated ten times.

### T5. Prove the accessor and bit-set lemmas

`access.rs` and `bits.rs` are the leaves every clause bottoms out in. Two families: each accessor
against the corresponding `Spec.lean` projection (`nodeProps`, `epParents`, `nNodes` …), **including
the out-of-range case**, since the accessors are total and return zero or an empty reading where the
specification's `getD` returns the empty node; and each bit-set operation (`zeros`, `set`, `get`,
`union`, `eq`, `of_ids`) against list membership, `SameSet` and `IsA`.

`Gotchas:` `bits::union` writes `a[i]` while iterating `a.len()`, so it needs a length-preservation
invariant that later lemmas will assume. `bits::subset` is unreachable from any clause — do not
spend time on it. The accessors' total-on-out-of-range behaviour is load-bearing for the
specification not being vacuous, and it is the detail most likely to be assumed rather than proved.

### T6. Prove the four small ancestry-free clauses

Only `cl_conformance` and `cl_emission` take the ancestry table; the other eight are independent of
it, which is what makes this ordering possible. Take the four cheapest first: **Target** (1 loop,
no shared helpers, the Lean side a single `filter … |>.length = 1`), **UniqueProducer** (4),
**Schedulable** (5) and **Provenance** (6). `emits` is shared by the last three, so prove it once in
the shared module.

`Gotchas:` `cl_unique_producer` does **not** call `unique_producer_at` — it reimplements the
judgement as an O(n) `seen` scan, and only a `debug_assert_eq!` in the audit crate ties the two. The
lemma must be about the scan, because the scan is what `check` runs. `cl_provenance` tries
`is_given` before the emitter search while the Lean conjunct lists the disjuncts the other way
round; that is a commutation, not a difference.

### T8. Prove Derived and Shape

Derived (8 loops) needs `confers` and the `bits` equality lemmas; Shape (11) needs `used_slots`,
`produced_slots`, `declared_slots`, `groups_match`, `no_repeat` and `same_slots` — the last routed
through `bits::of_ids`/`zeros`/`eq`, all proved in T5.

`Gotchas:` `no_repeat`'s inner loop starts at `i + 1`, so the measure is fine but the correspondence
to Lean's `List.Nodup` is over pairs rather than over a decreasing prefix — expect that to want its
own bridging lemma rather than to fall out. Several of these loops are bounded by a nested
projection like `q.steps[si].produced[gi].len()`, so the measure mentions a projection that must
itself be shown in range.

### T9. Prove Givens

15 loops, and the clause is only partly pointwise: `givens_at` covers the per-pair correspondence
while the two injectivity `Nodup`s live in the wrapper. Prove the two halves separately and conjoin.

`Gotchas:` the second `Nodup` loop also starts at `i + 1`; reuse whatever T8 built for `no_repeat`
rather than proving it twice. The clause deliberately carries no "one declared group" condition —
if a proof seems to need one, the proof is wrong, not the specification.

### T11. Prove Indexed

The largest at 20 loops, and a monolith with no pointwise predicate to decompose along, so it is its
own task. Its ten sub-checks (`given_group_indexed`, `node_indexed`, `endpoint_indexed`,
`transform_indexed`, `group_slots_indexed`, `step_indexed`, `group_indexed`, plus `required_slots`
and `no_repeat` shared with Shape) map onto `WellIndexed`'s conjuncts, so the decomposition is by
helper even though the entry point is monolithic.

`Gotchas:` `required_slots` and `no_repeat` are already proved by T8 — reuse, do not restate. This
clause is what makes every other clause's range assumptions safe, so it is the one whose failure
would be silent: a weakened `Indexed` makes the whole specification vacuous rather than merely
incomplete.

### T13. Prove the ancestry bridge

The single hardest obligation, and the one you left to my judgement. **Prove it as written; change
no Rust.** Restating the Lean to the dense algorithm would make the specification follow the code,
which is the failure the previous session rejected. Rewriting the Rust to the specification's
recursion looks simpler and is not: `ancestorB` has no memoisation, so a lineage DAG with shared
parents would blow up exponentially where the dense pass is linear, and it would mean rebuilding and
re-staging four cross-compiled binaries on a host with no `cargo`, then re-running the differential
sweep, to buy a lemma that is standard.

The proof is one strong induction on `x` carrying the invariant `rows.len() = x` and `rows[y] = {f |
ancestorB q y f}` for every `y < x`. Under `WellIndexed` every parent of `x` is below `x`, hence
below `rows.len()`, so `anc_row`'s `p < rows.len()` guard never discards one and the step matches
`ancestorB`'s own recursion exactly.

`Gotchas:` both sides under-approximate identically when the ordering is violated — Rust skips a
parent that does not precede, Lean's `dite` returns false — so the lemma holds unconditionally in the
failing direction and needs `WellIndexed` only for the other. Keep that asymmetry explicit; it is
what makes the relation fail closed. The `ancestors` loop pushes into the structure it reads, so the
invariant has to be carried through the loop rather than established after it.

### T14. Prove Conformance and Emission

The two clauses that consume the table, sharing `satisfies`, `carries`, `descends` and
`access::bound_to`. With T13 in hand `descends (ancestors q) e f` rewrites to `ancestorB q e f` and
what remains is `anchorOk`'s `boundTo` resolution and the property containment.

`Gotchas:` `Satisfies` is deliberately non-recursive on the demand — the anchor's own properties are
checked when that binding is checked. A proof that starts recursing on the anchor has misread the
clause.

### T16. Prove `check_spec` and `check_correct`

`check` is an ancestry computation followed by a loop over `k < N_CLAUSES` accumulating an `ok` flag.
Prove `clause_holds p q anc k = ok (decide (conjunct k))` by the ten lemmas and a concrete case
split on `k`, then the outer loop by the T3 recipe on measure `N_CLAUSES − k`, then conjoin against
`ValidC` and transport across `valid_iff_validC`. `check_correct` is already written and follows in
one line.

`Gotchas:` `check` has no early return — every clause is evaluated even after one fails, which is
what keeps the ten lemmas independent. Preserve that independence in the proof; a chain where each
clause rests on the last is exactly what the code was written to avoid.

### T17. Gate the build and run the full suite

Run the extraction gate, `--lean-check` over the whole tree, and the solver suite with `PYTHONPATH`
pinned to this worktree, asserting the Rust backend. Then diff `fingerprints.json` against `HEAD`
and confirm it has not moved.

`Gotchas:` this plan touches no Rust and no Python, so a moved fingerprint is a bug in what was
done, not a result. `pytest` is not on PATH; go through `mamba run -n msm pytest`, and remember the
differential suites are opt-in behind `--python-solver`.

### T18. Write the run report and update the documents

`data/metasmith/plans/06-*.md` beside its five siblings, carrying this plan, the corrections the
evidence forced, and the run log. Then the documents the session made false: `solver-spec.md`'s
provability section, `docker/solver_witness/README.md` (still describing the pre-split crate as "35
of 37 functions translate", which has been wrong since the split), the `-xd` block in `dev.sh`, and
the scope brief.

`Gotchas:` the `-xd` finding is worth recording precisely rather than deleting — three concrete Lean
incompatibilities, each reproduced, are what stops the next session re-attempting it. Subtract as
well as add; a doc-touching task that only grew the files is not finished.

## Callouts

- **`cl_unique_producer` has two implementations of one judgement.** The shipped scan and the
  audit crate's `unique_producer_at`, tied only by a `debug_assert_eq!`. The gate runs the scan, so
  proving the scan is correct; but the audit could in principle mislocate a violation. Worth a
  follow-up, not worth widening this plan.
- **The Python backend has no runtime gate.** It emits no wire, so the wire-level checker cannot see
  it, and the differential sweep is what covers it. Any claim this session makes about "the solver
  being gated" means the Rust one.
- **The proof says nothing about completeness of the search.** A rejection means this plan is
  unsound, never that no sound plan exists. That distinction should survive into the report.
- **Sequencing for PUCT.** Nothing in this plan touches the search, and that is deliberate: the
  witness has to be proved against the current solver before it is worth anything as a guarantee for
  a replaced one.

## Autopilot

Execution of the plan above. Everything above this line is the record as approved; the corrections
the evidence forced are below.

### Live state

**The obligation is discharged.** `SolverProof.check_spec` and `SolverProof.check_correct` are
proved and depend on `[propext, Classical.choice, Quot.sound]` and nothing else, across a hundred
audited declarations. `docker/solver_witness/dev.sh --lean-check` reports PASS and exits 0.

No Rust and no Python changed. `git diff --name-only` over the session's commits matches no `.rs`,
`.py` or `.json` file at all, which is a stronger statement than the fingerprints agreeing: nothing
that could move a fingerprint was touched.

### What the plan got wrong

Three things, each overturned by measurement rather than by argument.

**1. The route the previous session recorded is a dead end, and the premise behind it was false.**
`data/metasmith/plans/05-*.md` records that `check_spec` needs a `-decreases-clauses` re-extraction,
because a `partial_fixpoint` gives only Scott induction. The first half is right and the second does
not follow.

The `-xd` extraction does not compile, for three independent reasons, each reproduced minimally:

- All 73 of its generated tactic tokens are dotted (`module.function_loop_decreases`), and Lean
  cannot resolve a dotted string token inside a tactic quotation. A file of 30 synthetic dotted
  entries gives 30 `unknown tactic` errors; the same file with undotted names gives none.
- It emits `partial_fixpoint` on the same definition as `termination_by`, which is a hard parse
  error, and `aeneas --help` offers no flag to suppress the fallback.
- It lowers every conditional to a dependent `if h:` inside a `do` block, which Lean 4.31.0 — the
  toolchain the pinned Aeneas itself selects — rejects with "unsupported `do` element". Five lines
  reproduce it, while the default extraction compiles in the same project.

None of that matters, because **a `partial_fixpoint` definition still carries `eq_def`**, the full
unfolding equation. Termination does not have to be a property of the definition; it is a theorem
about it, proved by well-founded induction on `bound - index` in the proof. Every one of the ~73
loops was proved that way over the default extraction, and so was the composition.

The deeper reason it works: `x ⦃ z => P z ⦄` is `Aeneas.Std.WP.spec`, which sends **both `fail` and
`div` to `False`. A triple already asserts termination.** `Basis.eq_of_spec` is the one place that
becomes the equation the obligation is stated in.

**2. The ten clause lemmas are not independent, and `lib.rs` says they are.** The plan repeated the
crate's own header — that no early return in `check` "keeps the ten clauses independent, which is
what lets the proof be one equation with ten lemmas rather than a chain where each rests on the
last". No early return keeps their EVALUATION independent. Five of the ten are equivalent to their
`ValidC` conjuncts only under `WellIndexed`, for three unrelated reasons:

| mechanism | clauses | polarity | witness |
|---|---|---|---|
| the conjunct is vacuous off the endpoint table where the checker fails closed | Conformance, Emission | spec true, checker false | `conformance_clause_needs_indexed` |
| a dense bit set of fixed width makes an id at or beyond that width invisible | Shape, Derived, Givens | **checker true, spec false** | `shapeR_not_shape`, `derivedR_not_derived`, `Givens.Counterexample.cx_*` |
| `NONE = usize::MAX` shares the id space it reports absence in | Givens | checker false, spec true | none built; needs a `usize::MAX`-valued id |

Each is machine-checked in the module that found it, and the second was confirmed by running the
crate as well as in Lean. `check` is not wrong — it is the conjunction and `cl_indexed` rejects
every witness — so `check_spec` still holds. It holds by a case split on `WellIndexed`, which
`cl_indexed_spec` being unconditional is what makes available.

That `cl_indexed_spec` held is structural rather than lucky: no judgement in that clause goes
through a dense bit set, and none reads a precomputed table, so neither mechanism can arise there.

**3. The obligation could not be discharged where it was stated.** `Spec.lean` declared
`check_spec`, and every proof module imports `Spec.lean` for its vocabulary, so the specification
cannot import them back. Both theorems moved to `Proof/Compose.lean`; `Spec.lean` keeps what has to
be read and believed, and names where the proof lives.

### What the proofs found about the checker

Nothing that makes `check` unsound, and four things that were not written down anywhere.

- **`cl_unique_producer` rests on a `Vec` invariant, not on a clause.** Its `seen` counter starts at
  `usize::MAX` and the scan reads `seen != NONE` as "an emitter was already found", so a step list of
  length `usize::MAX + 1` would let a second producer through. It is discharged from the `Vec` length
  bound. `cl_indexed` does not check it and could not.
- **`access::bound_to` conflates two answers**, returning `NONE` both for "unbound" and for "bound to
  endpoint `usize::MAX`". Retired the same way.
- **Three helpers inside `cl_indexed` are not total in their index**, unlike everything in
  `access.rs`: they re-index before testing their counter and so *fail* out of range rather than
  returning a zero reading. Sound as called, since the clause's own guards supply the range, and
  contained because all seven helpers are private and `solver_witness_audit` reaches only
  `pub fn cl_indexed`. It stops being contained the day one is made `pub` so the audit crate can
  locate a violation inside it.
- **`same_slots` and `derived_at` truncate silently.** The bit-width mechanism above is a latent
  hazard rather than only a proof obstacle: the comparison is correct only inside the conjunction
  that `cl_indexed` guards. The Rust repair — reject an id at or beyond the compared width instead
  of ignoring it — was deliberately not taken, since this plan changed no Rust and `check_spec` does
  not need it. It belongs in the followups.

### What the gate could not do

`--lean-check` was rewritten before any proof was written, and two of its defects would have made
the rest of the session worthless.

- It piped `lake build` into `tail`, so the pipeline's status was `tail`'s and a screenful of Lean
  errors ended in a pass.
- Its `sorry` scan was a text search over one file. Worse, **`lake` writes bytes that make `grep`
  call the log binary, and a binary `grep` matches nothing** — which in a gate reads as "no sorries
  found" and reports a pass over an unproved tree. Every grep over the log now passes `-a`. This was
  found only because it cost an agent a build cycle by printing nothing at all; no exit code showed
  it.

It now checks four things and all four were driven to failure before being trusted: a build error, a
declaration using `sorry` (taken from Lean's own per-declaration warning, since a text search cannot
tell a hole from prose about holes — it flagged `Audit.lean`'s own explanation), an `axiom` added in
a hand-written file, and `#print axioms` over every audited obligation. That last is not
theoretical: **the Aeneas standard library ships two `sorry`s**, in `core.slice.Slice.get_unchecked`
and its spec lemma. The witness does not reach them, and the audit is what says so.

### Working with subagents, since the plan called for them

Nine agents wrote the ten clause modules and the composition. Three things are worth carrying
forward.

- **The tool has to be concurrency-safe before they run.** `--lean-build` re-staged one shared tree
  and wrote one shared log, so two agents on disjoint modules read each other's errors against their
  own line numbers. Lake's own lock does not help: it locks the build directory, which is not the
  resource that collides. It takes an `flock` now.
- **Isolation buys correctness and costs duplication.** Telling each agent to own one file and not
  touch the shared basis kept them from colliding and guaranteed they would reinvent the same list
  and `Bool` helpers under different names. They avoided the collision themselves, by renaming and by
  inner namespaces; `Proof/Compose.lean` is the check that says so.
- **The brief must forbid weakening explicitly, and it worked.** Five agents hit statements that
  were false as briefed. Not one quietly added a hypothesis: each built a counterexample, stated both
  forms, and reported. The instruction that earned that was naming the failure — a lemma restated
  until it is provable is the thing this project exists to prevent — rather than only asking for a
  proof.

### The acceptance criteria, audited

| criterion | verdict |
|---|---|
| `--lean-check` exits non-zero on a build error, a `sorry`, and an added axiom | holds; all four paths driven to failure, including an axiom-audit path added after the Aeneas library's own sorries were found |
| `check_spec` and `check_correct` proved, no `sorry`, no added axiom | holds; `[propext, Classical.choice, Quot.sound]` across 100 audited declarations |
| every clause lemma an equation against `decide` of its `ValidC` conjunct | **holds with a correction.** Five are equivalent to their conjunct only under `WellIndexed`, which is a fact about the code, not a weakening. Each of those five also carries an unconditional exact form, and the composition uses both |
| proved against what `check` calls, `UniqueProducer` against the scan | holds |
| the ancestry bridge proved | holds, and unconditionally — no `WellIndexed` needed |
| no Rust or Python changed, fingerprints byte-identical, `SOLVER_RNG_VERSION` untouched | holds; no `.rs`, `.py` or `.json` file appears in the session's diff at all |
| suite green on the Rust engine, `Backend("solve") == "rust"` | holds, and matches the previous session exactly. `tests/metasmith/solver` on the Rust engine alone is 248 passed, 1 xfailed, 0 failed, with `Backend("solve")` and `Backend("check")` both `rust`; its 224 skips are all the documented `--python-solver` opt-in axis. With that axis included: **472 passed, 1 xfailed, 0 failed**, the same as the last session left |
| the documents no longer assert the superseded route | holds; `solver-spec.md`, `docker/solver_witness/README.md`, the `-xd` block in `dev.sh`, and the scope brief |

### Deviations

- **The compaction tasks were not run.** Context stayed well inside a 1M window throughout, so
  there was nothing to compact. T4, T7, T10, T12 and T15 are closed as deferred rather than done.
- **A task was added.** T15a, to de-duplicate helper lemmas the concurrent agents were expected to
  have collided over. It closed as a no-op: they had avoided the collision themselves, and
  `Proof/Compose.lean` is the check that says so.
- **The obligation moved file.** Forced by the import direction, not chosen. `Spec.lean` keeps the
  statement of what a correct plan is; `Proof/Compose.lean` carries the theorem.
- **`Proof/Bits.lean` skips `bits::subset`.** No clause reaches it, so it is unproved and stays
  unproved.

### Followups

- **Repair the bit-width truncation in the Rust.** `same_slots`, `same_props` and `derived_at`
  should reject an id at or beyond the width they compare at instead of ignoring it. That would make
  three clause lemmas unconditional and remove a hazard that is currently contained only by clause
  ordering. Not taken here because this plan changed no Rust.
- **`cl_unique_producer` has two implementations of one judgement.** The shipped scan and the audit
  crate's `unique_producer_at`, tied only by a `debug_assert_eq!`. The gate runs the scan and the
  scan is what is proved, so this is a reporting-fidelity gap rather than a soundness one.
- **The Python backend is still ungated.** It emits no wire, so the wire-level checker cannot see
  it, and the differential sweep is what covers it.
- **PUCT is the next task**, and it is now safe in the sense this plan set out to buy: a new
  selection policy moves every plan, and every plan the Rust engine emits is adjudicated by a
  checker proved to agree with the specification.

### One operational note

The `--python-solver` axis was killed once by the kernel for low memory, uncapped. It completes in
399 s under `systemd-run --user --scope -p MemoryMax=8G`, which is what the scope brief already says
to do for a speculative solve and which applies to this suite too. Nine Lean containers and a
differential sweep on one box is what made it bite.
