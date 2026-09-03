# The solver's problem, and what makes a plan correct

## Purpose & Contents

The specification the plan witness adjudicates against. States what a problem is, what a plan
is, and the conditions a plan must meet to be sound. Settles the three questions the
implementations answer differently, and says which existing site must move to match.

This file describes the *problem*. Solver behaviour belongs in
`docs/workflow_solver/architecture.md`. The type system as a whole belongs in
`architecture.md`. Rules for writing a solver test belong in `tests/metasmith/solver/AGENTS.md`.

The specification is stated in Lean 4 in `src/solver_witness/Spec.lean`, and that file is
normative. This one carries the prose and the reasons.

## Why this exists

Correctness is a relation between an artifact and a specification. No specification existed.
Six sites decided ancestry and identity and they disagreed, not because anyone chose six
positions, but because nobody wrote down the one.

Nothing is verified about the search. The search emits an answer and a small checker adjudicates
it, which is the route SAT solvers took with DRAT refutations and LP solvers with Farkas
certificates. The proof burden falls from 1,400 lines of search plus a Rust port plus a ChaCha8
stream onto a loop-free predicate.

## The substrate is the wire, not the arena

The witness reads the encoded request and reply, not the Python objects. Properties are already
interned to integers there, nodes already carry integer parents, and every endpoint already has
an index. Checking the wire means the witness adjudicates the bytes that leave the process.

A node id is overloaded and this matters. The payload's node table serves as both a slot
(`DepId`) and a type (`EpId`). A node reached through `Problem.given` therefore carries a given
endpoint's real lineage, whose parents belong to no transform at all.

**CAUTION** Every index on the wire is unchecked. Lean's `getElem!` returns the `Inhabited`
default for an out-of-range index, which is an empty property list, and an empty demand is
satisfied by anything. A plan binding a slot id of one billion passes every other clause. The
Rust decoder range-checks and refuses. The specification states the same condition as
`WellIndexed`, and it is the first clause of soundness for that reason.

## Substitutability

A type is a set of properties. More properties means more specific.

An endpoint may fill a slot when the endpoint carries every property the slot demands. It may
carry more. The written form is `Eqv(Y, X) = ∀x∊X, x∊Y`, read as "Y can be used where X is
demanded". The code form is `x.IsA(y)`, which is `y.properties ⊆ x.properties`, read as "x can
be used in place of y". A subtype satisfies a supertype's requirement and never the reverse.

That asymmetry is load-bearing in every consumer. Reversing it yields a planner that appears to
work while building wrong chains.

**CAUTION** Two documents state the direction backwards relative to their own worked examples.
`docs/metasmith/source/usage/data.rst` says an output whose properties are a subset of the
required input's may be used as that input, then demonstrates `red_ball.IsA(ball)` six lines
later, which is the opposite. The thesis proposal's Figure 3.2 caption has the same inversion.
Correct `data.rst`. The lab deck, the grant's `Eqv`, and `Node.IsA` already agree.

## Lineage is anchored to the step, not to the plan

A slot may declare anchors: other slots of the same transform that its binding must descend
from. This is what lets a transform ask for "a compressed file that came from an image" when
compression itself is agnostic to content.

An endpoint fills a slot when two things hold together. It carries every property the slot
demands. And for each anchor the slot declares, **this step** bound that anchor to some
endpoint, and the filling endpoint is that endpoint or descends from it.

The anchoring is the part that is easy to get wrong, and the written specification gets it
wrong. `Lin(P, Yn) = ∀T∊P ∃i≤n, Eqv(Yi, T)` asks only that *some* upstream type be a supertype
of each prototype. That accepts a sample crossover. Given inputs `a1{A,m1}`, `a2{A,m2}` and
`b{B}` whose parent is `a2`, a transform requiring `x{A}` and `y{B} parents={x}` may bind
`x:=a1, y:=b`, because `b` does have an ancestor satisfying `x`. It is the wrong one.

The shipped libraries depend on the rejection. `aspire/fastp_qc.py` chains `sid parents={run}`,
`pair parents={sid}`, `r1 parents={pair}` precisely to stop one sample's reads meeting another's,
and the plan for `metagenomics_from_paired_reads` carries 44 such anchors.

Anchoring also removes the recursion. The anchor's own properties and lineage get checked when
that binding is checked, by the same clause over the same step. The relation is therefore
loop-free apart from the ancestry walk.

**The relation is reflexive.** An endpoint satisfies its own anchor. Three sources agree:
`Lin`'s `∃i≤n` admits `i = n`, `check_plan` short-circuits on `constraint is e or constraint ==
e`, and `validate_node._has_ancestor` seeds itself. Only `solver.py:_is_ancestor` is strict, and
it is the one that must move. No case in the corpus fires this either way, so settling it
changes no verdict.

## Decision 1: ancestry is the closure of declared parents

An endpoint's ancestors are the transitive closure of its declared `parents`. One source. Not
the step graph, and not the union of the two.

The step graph adds nothing. A produced endpoint is built carrying `inputs ∪ inputs' parents`,
and `rectify` rebuilds every produced endpoint from the finished plan so that the signatures
mean what they say. A given endpoint has no producing step to walk, because the given step
consumes nothing, and its declaration is the only thing recording that two givens of one sample
are siblings.

The union exists because that containment is not enforced anywhere. State the containment as a
clause instead of widening the relation to hide its absence. That clause is `rooted`: a produced
endpoint declares, among its parents, every endpoint its own step consumed. Given `rooted`, the
step graph reaches nothing the declared closure does not, so the union relation and this one
agree. Where they disagree, `rooted` names the reason.

`rooted` holds. It was checked against every plan the Rust engine returns for the eleven shipped
templates, both corpora and a 240-problem generated sweep, and it fired on none of them. The
narrow relation is therefore sufficient in practice and the union can go. `rooted` compares
endpoints by structure, for the reason the next section gives.

Classify the existing sites against the one relation.

| site | what it is |
|---|---|
| `solver.py:_is_ancestor` | the relation, correct, but switched off entirely under `mock_produced`, which is a bug |
| `solver.py:validate_node._has_ancestor` | step graph only. An optimization that is wrong as written. It rejects the plan the search just produced on 6 of 11 templates, which is why `_found_on` is 1 everywhere |
| `refine.rs` ancestry walk | the same relation, faithfully ported. Moves with it |
| `solver_verification.py:_Ancestry.parents_of` | the union. A workaround for the broken invariant, not a third opinion |
| `exhaustive_solvable._Ep` | value equality on an oracle's private type. Out of scope, and must not contradict |
| `check_plan` clause 5 reflexivity | reflexive where `_is_ancestor` is strict. Settled reflexive |

**CAUTION** `rooted` is the invariant the candidate generator is reported to break under
`mock_produced`. No plan measured so far breaks it, but the refiner is the path that would, and
the corpus barely exercises the refiner -- it runs a single iteration on ten of the eleven
generated cases. Read a `rooted` rejection as a real defect surfacing, not as a specification
error, and repair the generator.

## Decision 2: identity decides provenance, structure decides lineage

These are two questions and the writing only ever answered the second. "Instancing" is named as
one of three required features of the type system and then defined only for rules. The answer is
that neither notion wins outright, and which one applies depends on the clause.

**Provenance uses identity.** An endpoint must be emitted by some step. A structurally equal
endpoint emitted elsewhere does not discharge that, because a plan that consumes something
nothing made is not repaired by something else making a lookalike. An endpoint's identity is its
position in the arena, and the wire keeps it that way: the endpoint table is one row per object
and is deliberately not interned. Interning would collapse two endpoints with one producer each
into one endpoint with two producers, which is a different plan, introduced by the port rather
than found by it.

**Lineage uses structure.** Two endpoints with the same properties and the same lineage are the
same endpoint for the purpose of deciding whether a binding descends from an anchor.

That second rule is not a preference. Comparing lineage by arena position rejects **seven of the
eleven shipped templates**. The solver legitimately emits structural twins: one endpoint carried
over from the caller's own objects, and one minted during the search, with identical properties
and identical parents. A step then consumes one twin while the endpoints it produces record the
other in their lineage. On `pangenome_heatmap_from_assembly` the twins are two rows differing
only in whether `source_node` is set, and every downstream lineage check fails on the mismatch.

`check_plan` has always compared this way, testing `p is a or p == a` where `==` on a `Node` is
signature equality. Comparing by structure is what makes the witness agree with it, and it is
also what the informal formulation says: an endpoint fills a slot when, for each anchor the slot
declares, the endpoint has an ancestor that *is a* the anchor's binding, which is a relation
between endpoints and not a match on positions.

**CAUTION** Two rows with equal properties and equal parents are one endpoint to every lineage
clause and two endpoints to `provenance`. A clause that gets this backwards fails silently in one
direction: identity-for-lineage rejects sound plans, and structure-for-provenance accepts plans
that consume things nothing produced.

**CAUTION** One `EpId` can have two producing steps, legitimately. `rectify` rewrites some
structurally equal endpoints onto one object before the reply is encoded, and misses others,
which is why the twins survive at all. Any ancestry walk must union the producers rather than
assume one, and any clause quantifying over emitters must mean all of them.

## Decision 3: the transform collection is a sequence

Order reaches the plan. Permuting the transform list changes the plan on 5 of 12 generated
problems, up to 4 distinct plans over 5 orderings, and every resulting plan is sound.

Soundness is order-independent. The returned plan is not.

**WARNING** Two hosts whose `_metadata/index.yml` orderings differ plan differently for the same
template, and that reaches the plan fingerprint and the cache. The shipping path passes a dict
keys view, which sorts. A base library walks `manifest.items()` in build-product order, which
does not. Keeping the sequence puts a build product's file order into the contract. The
alternative is to canonicalise on a content key, which is a larger change and is not taken here.

## Well-formed problems

`Fills` requires the anchor to be bound in the same step, so a slot's anchors must themselves be
requirements of the same transform, declared earlier.

Declaration order is the reference space. A transform's requirement may only name parents
already added, and a target may only name targets declared before it.

`Transform._add_dependency` asserts membership at construction. The Rust generator refuses a
later anchor rather than guessing. Neither states the ordering, and the ordering is what makes
the condition usable, so state it here as `WellFormed`.

**CAUTION** Scope `WellFormed` to `requires` slots. Product slots may carry lineage and nothing
enforces where those parents live, and given-endpoint nodes carry parents belonging to no
transform at all. Quantifying over every node refuses every real problem.

## What a sound plan is

Ten clauses. `Spec.lean` is normative and this is the reading.

1. **Indexed.** Every id names something that exists.
2. **Nonempty.** A plan with no steps is not a plan.
3. **Shape.** Every step applies a declared transform, binding exactly the slots it requires and
   emitting exactly one endpoint per slot of each product group.
4. **Provenance.** Every consumed endpoint is one some step in the same plan emitted.
5. **Conformance.** Every input binding fills its slot, properties and lineage together.
6. **Emission.** Every produced endpoint fills the slot it is emitted from.
7. **Rooted.** Every produced endpoint declares, among its parents, every endpoint its step
   consumed.
8. **Givens.** The given step emits only endpoints whose properties match a declared input.
9. **Schedulable.** If one step emits what another consumes, the emitter comes first.
10. **Boundary.** Exactly one target application, exactly one given application, and the given
    one consumes nothing.

Three things this does not claim. It says nothing about completeness, so a rejection does not
mean no sound plan exists. It says nothing about optimality, which is `solver_bound.py`'s
question. It says nothing about whether the plan is the one the search should have found.

### Why the given step is its own case

The given transform's product groups are **alternatives**, one per sample. Every other
transform's are **conjunctive**, and a multi-output tool emits all of them. `NewProductGroup()`
is overloaded with these opposite meanings and the solver discriminates on whether the
application's transform is the given one.

Two clauses follow. `Shape` requires a group count match for every transform except the given
one, where branching hands the step one group per timeline and it emits fewer groups than it
declares. `Emission` exempts the given step entirely, because after `merge_states` fuses two
timelines the surviving given step emits one sample's endpoint under the other sample's slot,
and the property subset genuinely does not hold. `Givens` covers it instead.

Compare every group as a set of slots rather than by position. A transform may name the same
slot twice in `requires`, and `merge_states` appends the merged timeline's groups onto the
survivor's list, so group order after a merge is not declaration order.

### Foreignness has no counterpart here

`check_plan` allows at most one step applying a transform outside the problem, requiring nothing.
That clause has no image on the wire. A step names its transform by an index into the problem's
own transform table, so a foreign transform is not expressible. `Boundary` names the given step
outright instead of inferring it from absence, which is the better formulation.

### Soundness is a claim about a finished plan

The search returns a non-empty plan with no target step when its frontier runs out.
`Boundary` rejects it, correctly. The engine-level claim is therefore conditional: a reply
marked complete must pass the witness. An incomplete reply is a search that gave up, which is a
different answer and not an unsound one.

**CAUTION** Gate on that implication. A gate that demands every reply pass turns "no plan found"
into a hard error and breaks the regression that pins the behaviour.

## Where the contract was written down before

The thesis proposal's methods section is the fullest source and carries both formal sentences,
`Eqv` and `Lin`. The 2025 Amazon Research Awards proposal repeats them verbatim. Neither was
written as a specification for a checker, and the gaps are the three decisions above.

The only prose definition of a pipeline is the proposal's: an aggregate graph of paths extending
out from the target rule such that all type nodes function as both inputs and outputs, that is,
all data products are either produced or given. Clauses 4 and 8 are that sentence, split.

A deleted plan document carried the original numbered soundness definition, recoverable with
`git show 667fef4:plans/solver-performance-and-rust-port.md`. It named four conditions: inputs
given or produced earlier, an acyclic step graph, every requested target satisfied, and every
declared per-slot lineage constraint holding.
