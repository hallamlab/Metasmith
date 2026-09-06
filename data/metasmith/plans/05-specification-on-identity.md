<!-- Run report. The plan as approved, the corrections the evidence forced, and the
     run log. Copied here from ~/.claude/plans/, which is node-local and unversioned. -->

# Fix rectify, then specify the solver on identity

## Context

The previous session wrote a Lean specification for the plan witness. It did not align with the
eleven shipped workflows, and the repair it made — comparing endpoint lineage by *structure*
rather than by identity — was adopted because index comparison rejected seven of eleven templates.

That repair was scar tissue, not semantics. The seven rejections trace to one defect in `rectify`,
and every difficulty the specification has had since is downstream of it: a structural relation
cannot separate two distinct files that happen to share a type and a lineage, so the specification
certifies plans that mix them. Two rounds of adversarial review found three such plans, all
reachable from the shipped engine.

This plan repairs `rectify` first, then specifies the solver on identity, where the crossovers are
impossible by construction rather than excluded by a side condition. Measured: with the duplication
removed, an identity-only specification accepts all eleven templates with no structural relation
anywhere.

## What you said

> we need to define first that a type is collection of atomic properties

> then IsA(A, B) (A is a B) is true when B is a subset (or equal) of A

> I think the satisfies point is relavent when resolving. We want to bind the "anchor" instance
> produced by the transform with full lineage info rather than the direct type output from the
> transform. Yes, instancing is definately needed.

> is this not solved with the instancing/anchoring? all of these options are patches. why not
> specify matched=consumed?

> a transform must have unique requirements.

> the plan of transforms compose a DAG (note that there may be intrinsic cycles, but the
> explicitly planned workflow should not have cycles)

> A witness is certified if for all S, witness Certified(S) == Valid(S)

## The one bug behind all of it

`rectify.fix_endpoints` keys its `endpoint_map` by **signature** — `Map<EpSig, EpId>` in
`rectify.rs:169`, `dict[Endpoint, Endpoint]` in `solver.py:714`, and `Endpoint.__hash__` is
signature equality. Two consequences, and both have been observed:

1. **Two steps producing structurally equal outputs collapse into one endpoint with two
   producers.** The engine returns such a plan for a two-transform problem. Compiled, that is two
   processes writing one file. `model.rs`'s own header says this collapse must not happen.
2. **The given step's products are minted afresh while retained lineage still points at the
   originals**, so one logical given becomes two rows. This happens exactly when a given is also
   another given's ancestor: counts match on all eleven templates (3, 0, 0, 1, 2, 2, 0, 0, 2, 2, 1),
   and every twin pair is byte-identical apart from `source_node`.

The fix is two small changes in both implementations: key the map by object identity, and map a
given endpoint to itself instead of minting a copy. For the given step the copy is provably a
no-op — `lineage` is empty and `parents(e) & inherent_parents` is `parents(e)`, because a given's
parents are ancestors of a given by definition.

**Measured consequence.** Canonicalising the twins the way a fixed `rectify` would, then running an
identity-only specification — no `Eqv`, no structural comparison anywhere — accepts **all eleven
templates**. Row counts drop by exactly the twin counts.

## The specification

```lean
namespace SolverSpec

abbrev PropId := Nat
/-- Index into `Problem.nodes`. The table is overloaded: a node is a slot on a
transform, and it is also the declared type of a given input. -/
abbrev NodeId := Nat
/-- Index into `Plan.endpoints`. This is identity, and it is the ONLY way this
specification compares two endpoints. -/
abbrev EpId := Nat
abbrev TrId := Nat

/-- Mutual inclusion. Property lists and slot lists are sets in everything but
representation. -/
def SameSet {α : Type} (a b : List α) : Prop :=
  (∀ x ∈ a, x ∈ b) ∧ (∀ x ∈ b, x ∈ a)

-- 1 ------------------------------------------------------------------------
/-- A type is a collection of atomic properties. Nothing is inside a property. -/
abbrev Ty := List PropId

-- 2 ------------------------------------------------------------------------
/-- `IsA A B` -- "A is a B" -- when B's properties are contained in A's. More
properties means more specific, so a subtype satisfies a supertype's demand and
never the reverse. -/
def IsA (A B : Ty) : Prop := ∀ x ∈ B, x ∈ A

-- 3 ------------------------------------------------------------------------
/-- An endpoint is an INSTANCE: a type, plus the endpoints it came from. Two
endpoints are the same endpoint when they are the same row. Nothing here
identifies two rows by their contents, and that is the whole point: two files
made by different steps are two files even at the same type and lineage. -/
structure Endpoint where
  props   : Ty
  parents : List EpId

/-- A slot, or a declared given. `parents` carries both roles of the overloaded
node table: on a `requires` slot they are ANCHORS -- other slots of the same
transform whose bindings this one must descend from -- and on a given they are
that input's own lineage, belonging to no transform. -/
structure Node where
  props   : Ty
  parents : List NodeId

structure Transform where
  requires : List NodeId
  produces : List (List NodeId)

/-- `given` keeps its GROUPS. They are alternatives, one per sample, and
flattening them lets a plan draw one input from one sample and another from
another -- a crossover no other clause catches.

`nProps` bounds the property space so a property set can be a dense bit row,
which makes `SameSet` and `IsA` pointwise and decidable without sorting.

`givenTr` names the synthetic transform the wire uses for the givens. The
predicate needs it because a zero-requirement transform and the given transform
are otherwise indistinguishable on the wire, and "the adapter stripped the right
step" would otherwise be an unstated assumption inside the trusted base. -/
structure Problem where
  nProps     : Nat
  nodes      : Array Node
  transforms : Array Transform
  given      : List (List NodeId)
  givenTr    : TrId
  targetTr   : TrId

/-- An application. First class, because one transform may be applied many times
in one plan, and because `used` is what compiles to Nextflow. -/
structure Step where
  transform : TrId
  used      : List (NodeId × EpId)
  produced  : List (List (NodeId × EpId))

structure Plan where
  endpoints : Array Endpoint
  /-- The givens: the endpoint the plan uses, paired with the node the problem
  declared it as. The wire supplies the pairing in `source_node`; the `givens`
  clause checks it rather than trusting it. -/
  givens    : List (EpId × NodeId)
  steps     : List Step
  /-- Whether the search finished. Deliberately NOT read by any clause: an
  incomplete plan is a search that gave up, which is a different answer from a
  wrong one. The gate pairs it with the verdict; `Valid` ignores it, and the
  equivalence theorem obliges the checker to ignore it too. -/
  complete  : Bool

@[inline] def ep (q : Plan) (e : EpId) : Endpoint := q.endpoints[e]!
@[inline] def nd (p : Problem) (d : NodeId) : Node := p.nodes[d]!

def Emits (s : Step) (e : EpId) : Prop := ∃ g ∈ s.produced, ∃ b ∈ g, b.2 = e

-- 4 ------------------------------------------------------------------------
/-- Every id names something that exists, every parent precedes its child in
both tables, and no transform names one requirement twice.

The range conditions stop the specification being vacuous rather than merely
incomplete: `getElem!` returns the default for an out-of-range index, which is an
empty property list, and an empty demand is satisfied by anything.

The ordering conditions give `Ancestor` its termination measure and make the
endpoint table a topological order, so the checker can walk it in one increasing
pass. Measured: zero violations in either table across all eleven templates.

`requires.Nodup` is a well-formedness condition: a transform must have unique
requirements. Two structurally identical requirements intern to one `NodeId`, and
`Shape` would then accept one binding for two inputs. Zero shipped transforms
have one. -/
def WellIndexed (p : Problem) (q : Plan) : Prop :=
  p.targetTr < p.transforms.size ∧ p.givenTr < p.transforms.size ∧
  (∀ grp ∈ p.given, ∀ n ∈ grp, n < p.nodes.size) ∧
  (∀ gn ∈ q.givens, gn.1 < q.endpoints.size ∧ gn.2 < p.nodes.size) ∧
  (∀ i, ∀ _ : i < p.nodes.size,     ∀ a ∈ (nd p i).parents, a < i) ∧
  (∀ i, ∀ _ : i < q.endpoints.size, ∀ f ∈ (ep q i).parents, f < i) ∧
  (∀ n ∈ p.nodes.toList,     ∀ x ∈ n.props, x < p.nProps) ∧
  (∀ e ∈ q.endpoints.toList, ∀ x ∈ e.props, x < p.nProps) ∧
  (∀ t ∈ p.transforms.toList,
      t.requires.Nodup ∧
      (∀ d ∈ t.requires, d < p.nodes.size) ∧
      (∀ g ∈ t.produces, ∀ d ∈ g, d < p.nodes.size)) ∧
  (∀ s ∈ q.steps,
      s.transform < p.transforms.size ∧
      (∀ b ∈ s.used, b.1 < p.nodes.size ∧ b.2 < q.endpoints.size) ∧
      (∀ g ∈ s.produced, ∀ b ∈ g, b.1 < p.nodes.size ∧ b.2 < q.endpoints.size))

-- 5 ------------------------------------------------------------------------
/-- The transitive closure of an endpoint's declared parents, reflexive. Compared
by identity: `f` is an ancestor of `e` when `f` is literally reachable, not when
something equal to `f` is.

The closure is required rather than decorative. The lineage a step confers is its
inputs and its inputs' DIRECT parents, one hop, so a three-level given chain
loses its grandparent at the first step and only a transitive walk finds it. -/
inductive Ancestor (q : Plan) : EpId → EpId → Prop where
  | refl {e : EpId}     : Ancestor q e e
  | step {e m f : EpId} : m ∈ (ep q e).parents → m < e → Ancestor q m f → Ancestor q e f

-- 6 ------------------------------------------------------------------------
/-- What this step bound to slot `a`. Total: `none` when the step never bound it,
which keeps `Inst` well defined on any plan and removes the need for a
well-formedness hypothesis about anchors. -/
def boundTo : List (NodeId × EpId) → NodeId → Option EpId
  | [],             _ => none
  | (d, e) :: rest, a => if d == a then some e else boundTo rest a

/-- INSTANCING. A slot's anchors name other slots. `Inst` resolves each to the
endpoint THIS step bound to it -- the instance, carrying full lineage -- rather
than leaving it as the transform's declared output type. -/
def Inst (p : Problem) (used : List (NodeId × EpId)) (d : NodeId) : List (Option EpId) :=
  (nd p d).parents.map (boundTo used)

-- 7 ------------------------------------------------------------------------
/-- May endpoint `A` fill slot `d`, in a step whose bindings are `used`?
Properties and lineage in one relation.

The anchor's binding must be the endpoint `A` actually descends from -- matched
is consumed, the same instance. With identity this is what makes a crossover
impossible rather than merely unlikely: an endpoint derived from one file cannot
satisfy an anchor bound to a different file, however alike the two files are.

Non-recursive on the demand: the anchor's own properties and lineage are checked
when THAT binding is checked, by this same clause over this same step. -/
def Satisfies (p : Problem) (q : Plan) (used : List (NodeId × EpId))
    (A : EpId) (d : NodeId) : Prop :=
  IsA (ep q A).props (nd p d).props ∧
  ∀ o ∈ Inst p used d, ∃ f, o = some f ∧ Ancestor q A f

-- 8 ------------------------------------------------------------------------
def slots (bs : List (NodeId × EpId)) : List NodeId := bs.map Prod.fst

/-- The step honours its transform's contract: it binds exactly the slots the
transform requires, and emits exactly one endpoint per declared product slot.
Without this a step that binds nothing passes every clause below vacuously.

No exemption for a given step, because there is no given step. -/
def Shape (p : Problem) (s : Step) : Prop :=
  let t := p.transforms[s.transform]!
  (slots s.used).Nodup ∧
  SameSet (slots s.used) t.requires ∧
  s.produced.length = t.produces.length ∧
  ∀ gd ∈ s.produced.zip t.produces, SameSet (slots gd.1) gd.2

/-- The lineage a step confers on what it produces: everything it consumed, and
those endpoints' own parents. The engine's formula, at `solver.py:594` and again
in `rectify`. -/
def Confers (q : Plan) (used : List (NodeId × EpId)) : List EpId :=
  let ins := used.map Prod.snd
  ins ++ ins.flatMap (fun f => (ep q f).parents)

/-- A produced endpoint's declared lineage is EXACTLY what its step confers, as a
set of the same endpoints.

Equality, not containment. Containment lets a plan declare extra parents, and a
plan free to declare extra parents can buy any anchor it likes. This clause is
what ties declared lineage to the step graph, and it is what lets `Ancestor` read
the declared closure alone instead of a union of two relations. It is also what
will catch the known generator defect: `expand_node` reuses produced endpoints
whose parents describe inputs the candidate no longer consumes. -/
def Derived (q : Plan) (used : List (NodeId × EpId)) (e : EpId) : Prop :=
  SameSet (ep q e).parents (Confers q used)

-- 9 ------------------------------------------------------------------------
/-- A valid plan. -/
structure Valid (p : Problem) (q : Plan) : Prop where
  indexed        : WellIndexed p q
  shape          : ∀ s ∈ q.steps, Shape p s
  /-- Every input binding fits the slot it filled. -/
  conformance    : ∀ s ∈ q.steps, ∀ b ∈ s.used, Satisfies p q s.used b.2 b.1
  /-- Every produced endpoint fits the slot it left... -/
  emission       : ∀ s ∈ q.steps, ∀ g ∈ s.produced, ∀ b ∈ g, Satisfies p q s.used b.2 b.1
  /-- ...and carries the lineage its step confers, no more and no less. -/
  derived        : ∀ s ∈ q.steps, ∀ g ∈ s.produced, ∀ b ∈ g, Derived q s.used b.2
  /-- One endpoint, one producer. Two steps emitting one endpoint is two
  processes writing one file, and `rectify` produced exactly that. -/
  uniqueProducer : ∀ i j : Fin q.steps.length, ∀ e,
                     Emits (q.steps.get i) e → Emits (q.steps.get j) e → i = j
  /-- Nothing is consumed out of thin air. By identity. -/
  provenance     : ∀ s ∈ q.steps, ∀ b ∈ s.used,
                     (∃ s' ∈ q.steps, Emits s' b.2) ∨ (∃ gn ∈ q.givens, gn.1 = b.2)
  /-- The givens are what the problem declared, and the pairing the wire supplied
  is a real correspondence rather than a claim.

  **CORRECTED DURING T6, against the plan as approved.** The clause said "all
  from ONE group", adopting the adversarial review's F4. That is wrong and it
  rejects real work: the groups are one per sample, and a multi-sample workflow
  legitimately spans all of them. `test_solver`'s multi-given cases build exactly
  that, and the engine's own plan for them consumes from every group. What keeps
  one step from mixing two samples is the lineage anchors in `conformance`, not
  group membership -- so a problem whose transforms declare no anchors has not
  asked for the samples to be kept apart, and the plan F4 called a crossover
  violates nothing the author stated. Membership in SOME declared group is what
  remains, which is the anti-invention condition.

  The pairing is also DEDUPLICATED before it reaches this clause. Two samples may
  share a structurally identical given -- two `read_metadata` endpoints with no
  lineage intern to one node -- and the given step then presents that one pair
  once per group.

  Lineage is checked, not only properties, and it is the half that matters. A
  given's parents are the ONLY record that two givens belong to the same sample:
  the given step consumes nothing, so the step graph cannot see the relationship,
  which is why a step-graph-only ancestry rejected 6 of 11 templates. A `givens`
  clause comparing properties alone would let a plan present one sample's reads
  under another sample's declaration.

  The pairing is injective on both sides, so this is a graph isomorphism between
  the presented givens and one declared group -- not a many-to-one claim that
  every presented given "matches something". That is well posed because the
  declared given set is CLOSED UNDER PARENTS: every parent of a declared given is
  itself a declared given, measured with zero exceptions on all eleven templates.
  So the pairing covers the whole lineage graph and no edge dangles.

  Checked index-wise, so nothing recurses across the two tables. -/
  givens         : (q.givens.map Prod.fst).Nodup ∧ (q.givens.map Prod.snd).Nodup ∧
                     (∀ gn ∈ q.givens, ∃ grp ∈ p.given, gn.2 ∈ grp) ∧
                     (∀ gn ∈ q.givens, SameSet (ep q gn.1).props (nd p gn.2).props) ∧
                     (∀ gn ∈ q.givens, ∀ f ∈ (ep q gn.1).parents,
                        ∃ hn ∈ q.givens, hn.1 = f ∧ hn.2 ∈ (nd p gn.2).parents) ∧
                     (∀ gn ∈ q.givens, ∀ a ∈ (nd p gn.2).parents,
                        ∃ hn ∈ q.givens, hn.2 = a ∧ hn.1 ∈ (ep q gn.1).parents)
  /-- The planned workflow is a DAG. A list that is a topological order is itself
  the proof, so this replaces a separate cycle check. Declared LINEAGE may still
  contain cycles, and does; this clause is about the step graph only. -/
  schedulable    : ∀ cj ∈ q.steps.zipIdx, ∀ b ∈ cj.1.used,
                     ∀ pi ∈ q.steps.zipIdx, Emits pi.1 b.2 → pi.2 < cj.2
  /-- Exactly one application of the target. This also rules out the empty plan,
  which is why there is no separate `nonempty` clause. -/
  target         : (q.steps.filter (fun s => s.transform == p.targetTr)).length = 1

-- 10 -----------------------------------------------------------------------
/-
  The obligation. `check` is `solver_witness::check`, extracted through Charon
  and Aeneas, so it arrives in Aeneas's `Result` monad and the statement is over
  that value.

  Stated as ONE EQUATION rather than as the biconditional directly. The
  biconditional alone does not pin the negative case: a checker that FAILS on
  every invalid plan satisfies `check p q = .ok true ↔ Valid p q` vacuously. The
  equation gives totality, soundness and completeness together.
-/
-- theorem check_spec (p : Problem) (q : Plan) :
--     check p q = .ok (decide (Valid p q)) := by
--   sorry
--
-- theorem check_correct (p : Problem) (q : Plan) :
--     check p q = .ok true ↔ Valid p q := by
--   rw [check_spec]; simp

end SolverSpec
```

## What was measured

| claim | verdict |
|---|---|
| a type is a set of atomic properties | holds |
| `IsA(A,B)` iff `B ⊆ A` | holds, and two repo docs state it backwards |
| `Satisfies` between two types, uninstanced | accepts a crossover |
| `Satisfies` instanced but compared structurally | still accepts one, on an engine-reachable plan |
| **identity-only spec on a canonicalised plan** | **accepts 11 of 11 templates** |
| `rectify` twins | counts match "givens that are also a given's ancestor" exactly, 11/11 |
| endpoints with two producers | 0 shipped, but the engine returns one on a two-transform problem |
| transforms with a duplicate requirement | 0 shipped |
| both tables arrive parents-first | 0 violations in either |
| `Derived` as exact equality | 238 produced endpoints, 0 violations |
| given endpoints matching a declared given, lineage included | 1,055 endpoints, 0 failures |
| the declared given set is closed under parents | 0 non-given parents, on all eleven |
| the `givens` clause as an injective isomorphism onto one group | fails today on the 7 twinned templates; **holds on all eleven once `rectify` is fixed** |
| structural spec, prototyped in Python | 11/11 templates, 11/11 corpus, 54/54 random sweep |
| `Ancestor.step` (the transitive case) | fires on 0 of 11 templates; needs its own fixture |

## Issues

- **I1.** `rectify` keys its endpoint map by signature, so it collapses distinct products and
  duplicates givens. Everything below is downstream of this.
- **I2.** The specification was derived from the checker rather than from the type system, and its
  structural comparison certifies plans that mix two distinct files.
- **I3.** The obligation is one-directional, so the specification says nothing about what the
  checker rejects.
- **I4.** Three clauses are unfalsifiable on the shipped corpus: the lineage half of `conformance`,
  `Ancestor.step`, and `uniqueProducer`.
- **I5.** The candidate generator disables lineage entirely under `mock_produced`.
- **I6.** `Spec.lean` hand-writes structures mirroring the Rust ones, so the theorem would be about
  a twin rather than about `check`.
- **I7.** `dev.sh` reports success on an extraction that emitted `axiom` and `sorry`.

## High-level goals

- **G1.** Make the engine return one endpoint per logical object, so identity means something.
- **G2.** Derive correctness from the type system, so every clause follows from what a type is.
- **G3.** State it so no plan that mixes two samples, or two lookalike files, is valid.
- **G4.** Pin the checker to the specification in both directions.
- **G5.** Show every clause rejecting something, rather than trusting that it would.

## Acceptance criteria

- No plan the engine returns contains one endpoint with two producers, and consumes nothing no step
  emitted, across the eleven templates and both corpora. **Corrected during T2:** two rows with the
  same properties and the same lineage are NOT necessarily one logical endpoint, and the original
  wording said they were. A step with two product groups legitimately emits two alike files
  (`gen/product-groups` does), and under identity those are correctly two endpoints. One producer
  per endpoint and no orphan consumption are the invariants that actually distinguish the defect.
- `source_node` is set on every given endpoint the plan uses, which it is on none today, and the
  endpoint-to-node pairing it supplies is injective onto exactly one declared given group.
- The witness accepts all eleven templates, `CORPUS` and `STRESS_CORPUS`, comparing by identity.
- The three adversarial cases are rejected, each by its own clause: the lookalike-crossover by
  `conformance`, the two-producer plan by `uniqueProducer`, the two-sample crossover by `givens`.
- Every clause has a decoy derived from an accepted reply, and each trips the clause it targets.
- `dev.sh` fails the build on `axiom` or `sorry`, and reports any `divergent`.
- `check_spec` is stated as the equation, with each clause's lemma proved or listed as outstanding.
- Tests assert `Backend("solve") == "rust"` before claiming anything about the engine.

## Tasks

1. **T1.** Repair `rectify` in both implementations, and re-baseline the fingerprints (G1).
2. **T2.** Prove the repair on the corpus: no twins, no shared producers, identity spec green (G1).
3. **T3.** Compact.
4. **T4.** Spike the three extraction shapes the checker design rests on (G4).
5. **T5.** Make `dev.sh` fail on `axiom`, `sorry` and report `divergent` (G4).
6. **T6.** Split the crate in two: the extracted predicate, and a reporting wrapper (G4).
7. **T7.** Move the givens from a plan step to a problem parameter, keeping the groups (G2).
8. **T8.** Compact.
9. **T9.** Land the specification over the extracted types (G2, G3).
10. **T10.** Build the adversarial fixtures and the per-clause decoys (G5).
11. **T11.** Prove the clause lemmas and `check_spec` (G4).
12. **T12.** Gate the engine on the checker and run the full corpus (G5).
13. **T13.** Update `docs/metasmith/solver-spec.md` and the scope brief (G2).
14. **T14.** Debrief.

## Approach by task

### T1. Repair `rectify`

Two changes, in `solver.py:715` and the port at `rectify.rs:186`. Key `endpoint_map` by object
identity — `dict` keyed by `id(e)` in Python, `Map<EpId, EpId>` in Rust — rather than by signature.
And for the given step, map each product to itself rather than minting a copy.

`Gotchas:` every plan fingerprint moves. `tests/solver/fingerprints.json` and `SOLVER_RNG_VERSION`
both need re-baselining, and the Rust half must move with the Python half bit for bit. Re-baseline
only after T2 confirms the new plans are right — a fingerprint regenerated over a wrong plan is
worse than a failing test. The two implementations must be changed together or the differential
tests will diverge for the right reason and look like the wrong one.

### T2. Prove the repair

Solve the eleven templates and both corpora on both backends. Assert: zero structural collisions
among endpoint rows, zero endpoints with more than one producing step, `source_node` set on every
given the plan uses, and the identity-only predicate green on every case. The prototype used to
establish this is in the job scratch directory and should be ported into
`src/metasmith/testing/` as the reference checker.

`Gotchas:` `source_node` is set on **zero** given products today, so that assertion is the sharpest
evidence the fix landed. Cap every speculative solve with `systemd-run --user --scope -p
MemoryMax=8G`; an uncapped large-target solve reached 47 GB and took out sibling sessions.

### T4. Spike the extraction shapes

Three throwaway crates of about thirty lines, extracted and grepped. One: a `Vec` built by `push`
at loop depth one with a nested pure helper. Two: a dense bit set accumulated **by move** —
`fn union(a: Vec<bool>, b: &[bool]) -> Vec<bool>` — inside a nested loop. Three: whether
`Vec<Vec<bool>>` survives Charon at all.

The second is the go/no-go for the checker design. Move-accumulation is what replaces the `&mut`
collection Aeneas could not lower. If it fails, the fallback is for the adapter to supply the
ancestor table as an untrusted hint that the witness only verifies.

`Gotchas:` an hour, not a day. Do not start T6 before it passes.

### T5. Make the extraction gate real

`dev.sh` reports success on an extraction that emitted ten axioms. Grep for `axiom` and `sorry` and
exit non-zero. Report `divergent` separately: not a failure, but each costs a fixpoint-unfolding
lemma later and you need the count before budgeting T11.

### T6. Split the crate in two

`solver_witness` is extracted and holds judgement only. `solver_witness_audit` depends on it, is
never extracted, and holds the loops that collect which clause failed plus every `String`. Charon
cannot reach the audit crate because the dependency points the other way, so "never extracted" is
structural rather than a flag.

Every clause becomes a pointwise `bool` predicate plus a mechanical quantifier, and the audit
wrapper calls those same predicates — one implementation of each judgement, so the wrapper cannot
disagree with what was proved.

`Gotchas:` no `&mut` parameter may appear in the extracted crate; make that a grep in `dev.sh`.
`Clause::name` returning `&'static str` moves to audit, which alone removes one of the two known
extraction failures. **Do not write the ancestor walk as a worklist with a `seen` set** — it needs
two `&mut` collections live across a nested loop, the exact shape that failed twice. The endpoint
table is parents-first, so one increasing pass computes reachability instead.

### T7. Move the givens to a problem parameter

Keep `given : List (List NodeId)` and pass `givenTr` through. In `plan_of`, partition the reply's
steps on the given index, put that step's products and their `source_node` into `Plan.givens` as
pairs, and drop the step. If there is not exactly one such step, fail the adapter with a distinct
malformed verdict rather than proceeding — the adapter may fail, never decide.

`Gotchas:` do not match given groups positionally against the step's product groups.
`merge_states` appends a merged timeline's groups onto the survivor's, so group order after a merge
is not declaration order; unordered membership sidesteps it. Merged timelines are otherwise
unexercised — no shipped template merges — but review confirmed stripping the step dissolves the
anomaly that forced the old `emission` exemption.

This deletes four given-step exemptions and one boundary conjunct.

### T9. Land the specification

Write `Spec.lean` over the structures Aeneas emitted, reaching contents through `Vec.val`, not over
hand-written twins. This is why it follows extraction rather than preceding it.

`Gotchas:` `List.Nodup`, `List.zip`, `List.zipIdx` and `List.flatMap` are core, but `flatMap` was
`bind` on older toolchains and the nightly is chosen by charon. The `m < e` guard in `Ancestor` is
not decoration: `Valid` must be decidable for the theorem to typecheck, and `WellIndexed` is one of
its own fields, so it cannot be assumed while deciding a sibling.

### T10. Adversarial fixtures and decoys

Port the three counterexamples into `tests/metasmith/solver` as fixtures, each asserted rejected by
its own clause: two transforms producing the same type and lineage with the anchor bound to one and
the input derived from the other; the two-producer plan; the two-sample crossover. Add the
three-level `deepchain` case, the only known case where `Ancestor.step` fires at all.

Derive every decoy by mutating a reply the checker has already accepted, reusing the `DECOYS` map
and `apply_decoy` in `src/metasmith/testing/witness_check.py`. Add a `derived` decoy that gives a
produced endpoint one extra parent.

`Gotchas:` `_decoy_givens` sets a property id past the end of the interned table, and `WellIndexed`
now range-checks property ids, so it will trip `indexed` instead of `givens`. Rewrite it to use a
valid property id no declared given carries. Assert the targeted clause is among those violated,
never that something failed.

### T11. Prove the clause lemmas and `check_spec`

State every loop lemma as an equation, never an implication: one induction then serves both
directions. Prove one lemma per accessor first, so no clause proof discharges a bounds obligation
again.

`Gotchas:` completeness is where an over-approximating clause shows up. Watch `schedulable` for the
off-by-one — today's code writes `ix >= jx`, forbidding a step from consuming its own output.
Decide that deliberately and make the specification match. Dropping the structural relation removes
what the earlier design named as the hardest lemma, so this is materially smaller than it was.

### T12. Gate and run the corpus

Check the plan the engine returns, on both backends, after `merge_states` and `rectify`. Gate on the
implication that a reply marked complete must pass, never that every reply must, or "no plan found"
becomes a hard error.

### T13. Documentation

Rewrite `docs/metasmith/solver-spec.md`: its "structure decides lineage" decision is now wrong and
the reasons behind it are the bug report in T1. Correct the `IsA` direction in
`docs/metasmith/source/usage/data.rst`. Update `.awm/context.md`, whose refiner and ancestry
cautions the repair changes.

## Still unsure about

- **Whether T1 changes which plans the search finds, not just how they are encoded.** Keying the map
  by identity means a produced endpoint no longer merges with an equal one, so `prune_steps` and the
  refiner see a slightly different graph. The fingerprints will move; T2 must confirm the plans are
  still right rather than merely different. If the plans change materially, say so and we decide
  whether to keep the merge behaviour behind the checker instead.

## Callouts

- **The refiner's candidate generator disables lineage under `mock_produced`.** The specification
  states an invariant that path breaks. Read a rejection there as the defect surfacing.
- **An anchor outside its transform's `requires` has three behaviours today.** `Valid` makes it
  unsatisfiable through `Inst`'s `none`, `_satisfies_lineage` raises `KeyError`, and `check_plan`
  reports an unbound slot. Pick one in T13.
- **The search dispatches through `solver_backend`.** A test that does not pin the backend passes
  having exercised the Python solver.
- **Performance was not measured.** `check_plan` runs in 0.47 ms against a 12 s solve, so there is
  headroom, but confirm rather than assume.

---

## Autopilot

Unattended execution of the approved plan above. Everything above this line is the principal's
record: execute it, cite it, do not edit it — except the one acceptance criterion already corrected
in place during T2, which is marked as such.

**Guardrails.**

- Working directory `~/agentic_workspace/projects/metasmith/engine/solver`, branch `feat/solver`.
  Never commit in the workspace root checkout.
- **Cap every speculative solve** with `systemd-run --user --scope -p MemoryMax=8G`. An uncapped
  large-target solve reached 47 GB and took out sibling sessions twice.
- **Never bare `git stash`** — the stack is shared with other worktrees. Use a WIP commit.
- Python is `PYTHONPATH="$PWD/src" mamba run -n msm …`, always pinned to this worktree.
- `pytest` is not on PATH; go through `mamba run -n msm pytest`, not `dev/metasmith.sh -tt`.
- Rust: no cargo on this host. `src/workflow_solver/dev.sh -b1` cross-builds one target in docker
  (~90 s cold, ~30 s warm). `-b1` stages all four slots and three of them go stale — only
  `x86_64-linux` is rebuilt, which is the only one this host runs. Do a full `-b` before anything
  that ships.
- Extraction is `docker/solver_witness/dev.sh -x <crate-dir>`; the image is already built (26 GB).
  `--gate` adjudicates an extraction and now exits non-zero on `sorry` or an axiomatised crate
  function.

**Failure modes I will not be around to catch.**

- A green test run that exercised the *python* solver. Every claim about the engine asserts
  `Backend("solve") == "rust"` first.
- `dev.sh -x` reporting success over an extraction full of holes. The gate is the guard; do not
  trust the exit code of the container command alone.
- A regenerated fingerprint over a wrong plan. Fingerprints move only after the plan is checked.
- `check_plan` reporting ok while downgrading a real defect to a note. It did exactly that for
  "equal but not identical" during T1. It is a second opinion, never the gate.

**Resume handles.**

- Plan: this file. Todo list: `TaskList`. Wire captures: `/home/tony/.claude/jobs/786eb167/tmp/`
  (`wire/` pre-repair, `wire3/` post-repair).
- Reference checker: `src/metasmith/testing/solver_spec.py`; its tests
  `tests/metasmith/solver/test_plan_spec.py`.
- Session commits start at `882c19d`, parent `9346d22`.

### Live state

**Complete and committed: T1, T2, T4, T5, T6, T7, T9, T10, T12, T13.** Suite is 472 passed,
1 xfailed, 0 failures on both solvers. The extraction is clean — 230 definitions, no `sorry`, no
axiomatised crate function, no external axiom — and `Spec.lean` type-checks against it.

**Outstanding: T11, the proof of `check_spec`.** The statement is on disk and typechecks;
`dev.sh --lean-check` reports it as the one obligation still stated with `sorry`. The next step is
known and is one command: `dev.sh -xd src/solver_witness` extracts with termination measures rather
than `partial_fixpoint` and prints how many are outstanding (219 template lines over 73 loops, each
`len - i` and `simp; omega`). That re-extraction is the prerequisite, not an optimisation — see the
run log entry for why Scott induction cannot give an equation.

**Remaining: T14, the debrief.**

### Run log

**2026-09-03 23:55 — T4 extraction spikes: GO.** One throwaway crate carrying all three shapes,
extracted in one pass so the gate names which fails. Nothing failed. 8 of 8 transparent functions
translated, zero `sorry`, zero axiomatised crate functions, and no `FunsExternal_Template.lean`
was generated at all — nothing needed an external model.

- **S0b, the go/no-go**, holds. `union(a: Vec<bool>, b: &[bool]) -> Vec<bool>` accumulated **by
  move** inside a nested loop lowers cleanly: `anc_row_loop` comes out as a recursive definition
  carrying the moved `Vec` as loop-carried state, with `union` looping inside it. This is what
  replaces the `&mut` collection Aeneas could not lower, and the hint-verification fallback in the
  plan's T4 is **not needed**.
- **S0a** (push at loop depth one with a nested pure helper) and **S0c** (`Vec<Vec<bool>>` through
  Charon) both hold.
- **Budget for T11:** roughly one fixpoint-unfolding lemma per loop. Six loops in the spike
  produced six non-structural recursions.

**Deviation from the plan's description, in my favour:** Aeneas emits `partial_fixpoint`, not
`divergent`. The gate as written in T5 counted only `divergent` and would have reported zero on an
extraction full of them. Widened to count both.

**2026-09-04 00:40 — T6 and T7 landed together; the extraction is clean for the first time.**
`solver_witness` is now judgement only, `solver_witness_audit` holds every name and loop that
locates a violation, and the dependency points so that `charon cargo` in the witness cannot reach
the audit crate. Extraction: **230 definitions, zero `sorry`, zero axiomatised crate functions,
zero external axioms.** The previous version had five judgement functions axiomatised --
`check_rest` among them -- and three `sorry`. 73 non-structural recursions is the T11 budget.

Four extraction constraints were discovered by hitting them, and each cost a rewrite:

1. **A borrowed slice returned from a branch does not lower.** `if d < len { &self.nodes[d].props }
   else { &[] }` gives "There should be no bottoms in the value", and every function reaching one
   becomes a hole. Every accessor now returns a scalar -- a count or an element. Slice *parameters*
   are fine; it is the conditional *return* that fails.
2. **No `return` inside a nested loop** -- "Returns inside of nested loops are not supported yet".
3. **No `break` out of an outer loop** -- "Breaks to outer loops are not supported yet". Both are
   handled by carrying an `ok` flag in the loop conditions and returning once, which also
   short-circuits, so it costs nothing at runtime.
4. **A `Vec::push`, or a borrow, held inside a loop that *inlines* another loop** gives "Could not
   match the contexts". The T4 spike only proved a nested *pure helper* works, which is exactly the
   fix: `shape_at` and `cl_indexed` were split into helpers and both then lowered. This is the same
   error that defeated the previous version, and the shape is narrower than "nested loops".

A fifth, smaller: `#[derive(Debug)]` on a struct holding `Vec<(NodeId, EpId)>` makes Charon emit
the tuple's `Debug` impl, which it cannot lower. The last remaining hole in an otherwise complete
extraction was a formatter nothing calls. The witness derives nothing now.

**Deviation: the `givens` clause lost its "one declared group" condition.** See the corrected
comment in the specification above. Adopting the adversarial review's F4 was a mistake and the
shipped tests caught it — three multi-given cases failed, and the engine's own plan for them
consumes from every group. Lineage anchors are what prevent a crossover; group membership never
was.

**Deviation: the differential sweep's timeout whitelist is now per-profile.** Naming `sink` cases
one at a time did not converge -- a different stream crossed the 20 s cap on each run. The uncapped
sweep agrees **512 of 512 in 244 s, the same before and after this change**, so the profile sits at
the boundary and load decides which case crosses. Only the reference side is exempted; an engine
that blows the cap still fails.

Suite: 462 passed, 1 xfailed, 0 failures, both solvers.

**2026-09-04 02:10 — T9, T10, T12, T13 landed. T11 is scoped, not done.**

`Spec.lean` compiles against `SolverWitness.Types` and `SolverWitness.Funs`, and `Valid` carries a
`Decidable` instance so `check_spec` is well-typed as an equation and `check_correct` — the
principal's `∀S. Certified(S) == Valid(S)` — follows from it in one line. Five obstacles, each a
constraint rather than a preference, are recorded in the commit and in the specification.

**The finding that matters for T11: a `partial_fixpoint` cannot give an equation.** Its induction
principle is Scott induction, which proves *partial* correctness — if the function returns, the
answer is right — and says nothing about whether it returns. `check_spec` asserts totality, so it
is not provable over the default extraction at all. `aeneas -decreases-clauses` emits terminating
definitions plus a template of measures; `dev.sh -xd` runs it and counts them. 219 template lines
over 73 loops. Learning this cost about an hour and would have cost days to discover by writing
proofs against the wrong extraction.

**Deviation: `Ancestor` is a computation, not an inductive `Prop`.** Asserting a `Prop` and then
owing a `Decidable` instance is how the previous specification ended up unable to decide anything.
The relation is computable — the checker computes it — so the definition says so.

**Deviation: T12 gates the shipping path only.** The Rust engine adjudicates every complete reply
before emitting it, and that is pinned in both directions. The Python backend has no runtime gate:
it produces no wire, so the wire-level checker cannot see it. The differential suite covers it
instead — 512 of 512 agreed, uncapped.

**A defect fixed in passing:** `cl_target`'s counter was `i32` by inference, which made a plain
count carry an overflow obligation. Widened to `usize`; no signed scalar survives in the extraction.

