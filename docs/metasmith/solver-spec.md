# The solver's problem, and what makes a plan correct

## Purpose & Contents

The specification the plan witness adjudicates against. States what a problem is, what a plan is,
and the conditions a plan must meet to be sound. Carries the reasons; `src/solver_witness/lean/Spec.lean`
is normative and states the same thing in Lean, over the types the extraction emits.

This file describes the *problem*. Solver behaviour belongs in
`docs/workflow_solver/architecture.md`. The type system as a whole belongs in `architecture.md`.
Rules for writing a solver test belong in `tests/metasmith/solver/AGENTS.md`.

## Why this exists

Correctness is a relation between an artifact and a specification. Nothing verifies the search.
The search emits an answer and a small checker adjudicates it, which is the route SAT solvers took
with DRAT refutations and LP solvers with Farkas certificates. The proof burden falls from 1,400
lines of search plus a Rust port plus a ChaCha8 stream onto a loop-free predicate.

## The substrate is the wire, not the arena

The witness reads the encoded request and reply, not the Python objects. Properties are already
interned to integers there, nodes already carry integer parents, and every endpoint already has an
index. Checking the wire means the witness adjudicates the bytes that leave the process.

A node id is overloaded and this matters. The payload's node table serves as both a slot and a
declared input type. A node reached through `Problem.given` therefore carries a given endpoint's
real lineage, whose parents belong to no transform at all.

**CAUTION** Every index on the wire is unchecked. An out-of-range index reads as an empty property
list, and an empty demand is satisfied by anything, so a plan binding a slot id of one billion
would pass every other clause. Both the checker's accessors and the specification's are total and
yield the empty thing; the `Indexed` clause is what turns that into a rejection.

## Substitutability

A type is a set of properties. More properties means more specific.

An endpoint may fill a slot when the endpoint carries every property the slot demands. It may carry
more. The code form is `x.IsA(y)`, which is `y.properties ⊆ x.properties`, read as "x can be used in
place of y". A subtype satisfies a supertype's requirement and never the reverse.

That asymmetry is load-bearing in every consumer. Reversing it yields a planner that appears to work
while building wrong chains.

## An endpoint is an instance, and two of them are the same only when they are the same one

An endpoint is a type plus the endpoints it came from. Its identity is its position in the reply's
table, and **nothing in the specification compares two endpoints any other way**.

Two files made by different steps are two files even at the same type and the same lineage. A
relation that compares them by contents cannot say so, and the difference is not academic: it is
what lets a step anchor to the file one transform made while consuming something derived from the
file another transform made. `tests/metasmith/solver/test_spec_fixtures.py` builds exactly that plan.

**This replaces an earlier decision that lineage should compare by structure.** That decision was
made because comparing by position rejected seven of the eleven shipped templates. The seven
rejections were one defect in `rectify`, not a fact about the type system, and the section below is
the bug report.

## The defect that made structural comparison look necessary

`rectify.fix_endpoints` reconciles the search's endpoints into instances. The search builds them as
*values* — a fresh object per candidate application — so one logical product exists as several equal
objects and a consumer holds a different one from the one its producer emitted. Reconciling them by
signature is right, and is the only place identity is created.

Two things it must not do, and did:

1. **Reuse an instance across two producing steps.** That merges two transforms' outputs into one
   endpoint with two producers, which compiles to two processes writing one file.
2. **Rebuild the given step's products.** For the given step the rebuild is provably a copy with no
   change of content, because a given's parents are ancestors of a given by definition. Making the
   copy minted a second object for one input while the retained lineage still named the first, so
   one logical given became two rows — one per given that is also another given's ancestor.

A third source sat upstream of the solver: a caller may hand in the same logical input twice as two
objects, and five of the eleven templates do. The wire interner collapsed them for the Rust path, so
only the Python path ever saw the duplicates and the two backends disagreed about how many endpoints
a problem has.

All three are repaired. `source_node` is now set on every given a plan uses, where it was set on
none.

## Lineage is anchored to the step, and matched by identity

A slot may declare anchors: other slots of the same transform that its binding must descend from.
This is what lets a transform ask for "a compressed file that came from an image" when compression
itself is agnostic to content.

An endpoint fills a slot when two things hold together. It carries every property the slot demands.
And for each anchor the slot declares, **this step** bound that anchor to some endpoint, and the
filling endpoint is that endpoint or descends from it.

The anchoring is the half that is easy to get wrong. A relation asking only that *some* upstream
type be a supertype of each anchor accepts a sample crossover: given inputs `a1{A,m1}`, `a2{A,m2}`
and `b{B}` whose parent is `a2`, a transform requiring `x{A}` and `y{B} parents={x}` may bind
`x:=a1, y:=b`, because `b` does have an ancestor satisfying `x`. It is the wrong one. Resolving the
anchor to *the endpoint this step bound* is what carries the information that the step already
committed.

**The ancestor walk is transitive, and that is not decoration.** The lineage a step confers is its
inputs and its inputs' direct parents — one hop — so a three-level given chain loses its grandparent
at the first step, and only a transitive walk finds it again. No shipped template exercises this;
`test_spec_fixtures.py` builds a case that does, and asserts it resolves past one hop so the test
cannot pass for the wrong reason.

**The relation is reflexive.** An endpoint satisfies its own anchor.

## The givens are a parameter, not a step

The wire sends the givens as a synthetic step. The specification has no such step: a plan is the
transforms the search chose, and the givens are a parameter of the problem. The adapter strips the
step and pairs each presented given with the node `source_node` says it is.

Stripping deletes four exemptions and a boundary conjunct. They existed because after a timeline
merge the given step emits one sample's endpoint under another sample's slot, so the property subset
genuinely does not hold there. With no given step, the anomaly does not arise.

**CAUTION** Stripping is the one thing the adapter can get wrong, so it may fail rather than guess.
A zero-requirement transform and the given transform are shape-identical on the wire, and only
`given_index` tells them apart. A reply without exactly one such step is malformed, which is a
different answer from unsound.

**The pairing is deduplicated.** Two samples may share a structurally identical given — two
`read_metadata` endpoints with no lineage intern to one node — and the given step then presents that
one pair once per group.

**Nothing confines a plan to one declared group.** The groups are one per sample, and a multi-sample
workflow legitimately spans all of them. What keeps a single step from mixing two samples is the
lineage anchors, not group membership: a transform that declares no anchor between two inputs has
not asked for them to come from the same sample, and refusing anyway would be refusing on the
author's behalf.

## The transform collection is a sequence

Order reaches the plan. Permuting the transform list changes the plan on 5 of 12 generated problems,
up to 4 distinct plans over 5 orderings, and every resulting plan is sound. Soundness is
order-independent; the returned plan is not.

**WARNING** Two hosts whose `_metadata/index.yml` orderings differ plan differently for the same
template, and that reaches the plan fingerprint and the cache. The shipping path passes a dict keys
view, which sorts. A base library walks `manifest.items()` in build-product order, which does not.
Keeping the sequence puts a build product's file order into the contract. The alternative is to
canonicalise on a content key, which is a larger change and is not taken here.

## What a sound plan is

Ten clauses. `Spec.lean` is normative and this is the reading.

1. **Indexed.** Every id names something that exists, every parent precedes its child in both
   tables, and no transform names one requirement twice.
2. **Shape.** Every step binds exactly the slots its transform requires and emits exactly one
   endpoint per declared product slot.
3. **Conformance.** Every input binding fills its slot, properties and lineage together.
4. **Emission.** Every produced endpoint fills the slot it is emitted from.
5. **Derived.** A produced endpoint's declared lineage is exactly what its step confers — everything
   it consumed, and those endpoints' own parents.
6. **UniqueProducer.** One endpoint, one producing step.
7. **Provenance.** Every consumed endpoint is one some step emitted, or one the problem gave.
8. **Givens.** Every presented given is one the problem declared, paired one to one, with equal
   properties and a lineage edge on one side exactly when there is one on the other.
9. **Schedulable.** If one step emits what another consumes, the emitter comes first.
10. **Target.** Exactly one target application.

`Derived` is equality rather than containment on purpose. A product free to declare extra parents
can buy any anchor it likes, which is a crossover written by hand rather than found by the search.
It is also what lets the ancestor walk read the declared lineage alone instead of a union of two
relations, and it is what will catch the known generator defect: `expand_node` reuses produced
endpoints whose parents describe inputs the candidate no longer consumes.

There is no `Nonempty` clause. `Target` rules out the empty plan already.

Three things this does not claim. It says nothing about completeness, so a rejection does not mean
no sound plan exists. It says nothing about optimality. And it says nothing about whether the plan
is the one the search should have found.

### Soundness is a claim about a finished plan

The search returns a non-empty plan with no target step when its frontier runs out. `Target` rejects
it, correctly. The engine-level claim is therefore conditional: a reply marked complete must pass the
witness.

**CAUTION** Gate on that implication. A gate that demands every reply pass turns "no plan found"
into a hard error and breaks the regression that pins the behaviour.

## Writing the checker so it can be proved

`solver_witness` is judgement only. `solver_witness_audit` holds every name, every `String`, and
every loop that locates a violation, and it *depends on* the witness — so `charon cargo` run in the
witness cannot reach it and "never extracted" is a property of the dependency graph rather than a
flag. Each clause is a pointwise predicate plus a mechanical quantifier, and the audit crate calls
those same predicates, so there is one implementation of every judgement.

Five shapes do not survive extraction. Each was found by hitting it, each is narrower than the
folklore, and a function that hits one comes back as a hole while the Lean still compiles:

- A **borrowed slice returned from a branch** — "There should be no bottoms in the value". A slice
  *parameter* is fine. Every accessor returns a scalar: a count, or an element.
- A **`&mut` collection mutated inside a nested loop** — the loop fixed point cannot unify the borrow
  context across two depths. Sets are dense `Vec<bool>` moved through their loops instead.
- A **`return` inside a nested loop**, and a **`break` out of an outer loop**. Both are handled by
  carrying an `ok` flag in the loop conditions, which also short-circuits.
- A **`push`, or a borrow, held inside a loop that inlines another loop** — "Could not match the
  contexts". A nested *pure helper* is fine; that distinction is the whole fix.
- **`#[derive(Debug)]` over a struct holding `Vec<(NodeId, EpId)>`**, which makes Charon emit the
  tuple's `Debug` impl. The witness derives nothing.

**CAUTION** `docker/solver_witness/dev.sh -x` used to report success over an extraction full of
holes. `--gate` refuses any `sorry` and any `axiom` naming a crate function, and reports the
non-structural recursions a proof will have to pay for. Run it; do not trust the container's exit
code alone.

## The checker is proved

`SolverProof.check_spec` states that `check` returns exactly `decide (Valid p q)`, for every problem
and every plan. `check_correct` follows from it in one line. Both depend on `propext`,
`Classical.choice` and `Quot.sound` and on nothing else. `dev.sh --lean-check` builds the tree and
adjudicates it.

The proof is one module per clause under `src/solver_witness/lean/Proof/`, over the DEFAULT
extraction. A `partial_fixpoint` definition exposes its unfolding equation as `<name>.eq_def`, so
termination is a theorem about each loop rather than a measure attached to it. Loop lemmas are Aeneas
Hoare triples, because `spec` sends both `fail` and `div` to `False` and so already asserts
termination. `Proof/Basis.lean` carries the recipe and the traps.

**CAUTION** `-decreases-clauses` is a dead end on the pinned toolchain, and the earlier claim that
`check_spec` required it is false. Its output fails to compile three ways: dotted tactic tokens Lean
cannot resolve inside a quotation, `partial_fixpoint` emitted beside `termination_by`, and a
dependent `if h:` inside a `do` block that Lean 4.31.0 rejects. Do not re-attempt it.

## The clauses are not independent

`lib.rs` says evaluating all ten with no early return keeps them independent. It keeps their
*evaluation* independent. Five are equivalent to their specification conjuncts only under
`WellIndexed`, for three unrelated reasons, each machine-checked in the module that found it.

- **Conformance and Emission.** The conjunct is vacuously true off the endpoint table, because
  `epProps` is empty out of range and `ancestorB` is reflexive everywhere. The checker fails closed
  there instead, since `descends` reads a row that does not exist.
- **Shape, Derived and Givens.** `same_slots`, `same_props` and `derived_at` compare dense bit sets
  of a fixed width. An id at or beyond that width sets no bit on either side, so it is invisible. The
  checker ACCEPTS where the specification rejects.
- **Givens.** `given_node_of` and `given_ep_of` report absence with `NONE`, which is `usize::MAX` and
  lives in the same space as the ids they return.

`check` stays correct because it is the conjunction and `cl_indexed` rejects every witness. So
`check_spec` case-splits on `WellIndexed`, which `cl_indexed_spec` being unconditional is what makes
available.

**CAUTION** The bit-width truncation is a hazard outside the conjunction, not only a proof
obstacle. `same_slots` is correct only where `cl_indexed` already guards the width. Reusing it
elsewhere, or reordering the clauses, breaks it silently. The repair is to reject an id at or beyond
the compared width rather than ignore it.

## Three facts the proof found and the code does not state

- `cl_unique_producer` reads `seen != NONE` as "an emitter was already found", so a step list of
  length `usize::MAX + 1` would admit a second producer. The `Vec` length bound retires it.
  `cl_indexed` does not check this and could not.
- `access::bound_to` returns `NONE` both for "unbound" and for "bound to endpoint `usize::MAX`".
  The same bound retires it.
- Three private helpers inside `cl_indexed` re-index before testing their counter, so they *fail*
  out of range where every accessor in `access.rs` returns a zero reading. The clause's own guards
  supply the range, and `solver_witness_audit` reaches only `pub fn cl_indexed`. Making one `pub` so
  the audit crate can locate a violation inside it would break that.

## Where the contract was written down before

The thesis proposal's methods section is the fullest source and carries both formal sentences, `Eqv`
and `Lin`. The 2025 Amazon Research Awards proposal repeats them verbatim. Neither was written as a
specification for a checker, and `Lin` is the crossover-accepting formulation named above.

A deleted plan document carried the original numbered soundness definition, recoverable with
`git show 667fef4:plans/solver-performance-and-rust-port.md`. It named four conditions: inputs given
or produced earlier, an acyclic step graph, every requested target satisfied, and every declared
per-slot lineage constraint holding.
