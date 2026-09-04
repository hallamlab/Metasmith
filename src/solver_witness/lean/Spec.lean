/-
  What a correct plan is.

  This file is normative. `docs/metasmith/solver-spec.md` carries the prose and
  the reasons; `src/solver_witness/src/` is the executable checker that must
  satisfy it, and `check_spec` at the bottom is the obligation relating the two.

  **It is written over the types Aeneas emitted, not over hand-written twins of
  them.** The version this replaces declared its own `Problem` and `Plan` and
  proved things about those, with nothing relating them to what was extracted --
  so the theorem, had it been proved, would have been about a different object.
  Importing `SolverWitness.Types` and `SolverWitness.Funs` is what makes
  `check_spec` a statement about the function the engine actually runs.

  The wire representation is coerced to plain lists of `Nat` on the way in. Every
  quantifier below is then bounded by a list, which is what makes `Valid`
  decidable without Mathlib's order or finiteness machinery -- and decidability
  is not optional here, because `check_spec` is stated as an equation against
  `decide`.
-/

import SolverWitness.Types
import SolverWitness.Funs

namespace SolverSpec

open Aeneas Aeneas.Std
open solver_witness

/-! ## The wire, as plain lists -/

abbrev Ids := List Nat

/- **Every `Prop`-valued definition below is `@[reducible]`, and that is load
bearing rather than cosmetic.** `check_spec` is an equation against `decide`, so
`Valid` must carry a `Decidable` instance -- and instance resolution does not
unfold a plain `def`. Written without the attribute, every clause that reaches
one reports "failed to synthesize Decidable", while the clauses written inline
resolve fine, which makes the failure look like it is about the clause rather
than about the definition it goes through. -/

def nats (v : alloc.vec.Vec Std.Usize) : Ids :=
  v.val.map (fun x => x.val)

def idPairs (v : alloc.vec.Vec (Std.Usize × Std.Usize)) : List (Nat × Nat) :=
  v.val.map (fun b => (b.1.val, b.2.val))

def idLists (v : alloc.vec.Vec (alloc.vec.Vec Std.Usize)) : List Ids :=
  v.val.map nats

def pairLists (v : alloc.vec.Vec (alloc.vec.Vec (Std.Usize × Std.Usize))) :
    List (List (Nat × Nat)) :=
  v.val.map idPairs

/-- Out of range yields the empty thing, matching the checker's accessors. A
panic is not a verdict, so both sides are total and `Indexed` is what turns an
id that names nothing into a rejection. -/
def emptyNode : types.Node :=
  { props := alloc.vec.Vec.new Std.Usize, parents := alloc.vec.Vec.new Std.Usize }

def emptyEndpoint : types.Endpoint :=
  { props := alloc.vec.Vec.new Std.Usize, parents := alloc.vec.Vec.new Std.Usize }

def nd (p : types.Problem) (d : Nat) : types.Node := p.nodes.val.getD d emptyNode
def ep (q : types.Plan) (e : Nat) : types.Endpoint := q.endpoints.val.getD e emptyEndpoint

def nodeProps (p : types.Problem) (d : Nat) : Ids := nats (nd p d).props
def nodeParents (p : types.Problem) (d : Nat) : Ids := nats (nd p d).parents
def epProps (q : types.Plan) (e : Nat) : Ids := nats (ep q e).props
def epParents (q : types.Plan) (e : Nat) : Ids := nats (ep q e).parents

def nNodes (p : types.Problem) : Nat := p.nodes.val.length
def nTransforms (p : types.Problem) : Nat := p.transforms.val.length
def nEndpoints (q : types.Plan) : Nat := q.endpoints.val.length
def nProps (p : types.Problem) : Nat := p.n_props.val
def givenTr (p : types.Problem) : Nat := p.given_tr.val
def targetTr (p : types.Problem) : Nat := p.target_tr.val
def givenGroups (p : types.Problem) : List Ids := idLists p.given
def givens (q : types.Plan) : List (Nat × Nat) := idPairs q.givens

def requiresOf (p : types.Problem) (t : Nat) : Ids :=
  match p.transforms.val[t]? with
  | some tr => nats tr.requires
  | none => []

def producesOf (p : types.Problem) (t : Nat) : List Ids :=
  match p.transforms.val[t]? with
  | some tr => idLists tr.produces
  | none => []

structure StepView where
  transform : Nat
  used : List (Nat × Nat)
  produced : List (List (Nat × Nat))

def stepView (s : types.Step) : StepView :=
  { transform := s.transform.val, used := idPairs s.used, produced := pairLists s.produced }

def steps (q : types.Plan) : List StepView := q.steps.val.map stepView

/-! ## 1. A type is a collection of atomic properties -/

/-- `IsA A B` -- "A is a B" -- when B's properties are contained in A's. More
properties means more specific, so a subtype satisfies a supertype's demand and
never the reverse. -/
@[reducible] def IsA (a b : Ids) : Prop := ∀ x ∈ b, x ∈ a

/-- Mutual inclusion. Property lists and slot lists are sets in everything but
representation. -/
@[reducible] def SameSet (a b : Ids) : Prop := (∀ x ∈ a, x ∈ b) ∧ (∀ x ∈ b, x ∈ a)

/-! ## 2. Well-indexed -/

/-- Every id names something that exists, every parent precedes its child in both
tables, and no transform names one requirement twice.

The range conditions stop the specification being vacuous rather than merely
incomplete: an out-of-range index reads as the empty node above, and an empty
demand is satisfied by anything.

The ordering conditions are what make the checker's ancestor closure a single
increasing pass, and what make `Ancestor` below well founded. The encoder emits
both tables parents-first, so a violation is a malformed reply.

`Nodup` on `requires` is a well-formedness condition: a transform must have
unique requirements. Two structurally identical ones intern to a single node id,
and `Shape` would then accept one binding for two inputs. -/
@[reducible] def WellIndexed (p : types.Problem) (q : types.Plan) : Prop :=
  targetTr p < nTransforms p ∧ givenTr p < nTransforms p ∧
  (∀ g ∈ givenGroups p, ∀ n ∈ g, n < nNodes p) ∧
  (∀ gn ∈ givens q, gn.1 < nEndpoints q ∧ gn.2 < nNodes p) ∧
  (∀ d < nNodes p, ∀ a ∈ nodeParents p d, a < d) ∧
  (∀ e < nEndpoints q, ∀ f ∈ epParents q e, f < e) ∧
  (∀ d < nNodes p, ∀ x ∈ nodeProps p d, x < nProps p) ∧
  (∀ e < nEndpoints q, ∀ x ∈ epProps q e, x < nProps p) ∧
  (∀ t < nTransforms p,
      (requiresOf p t).Nodup ∧
      (∀ d ∈ requiresOf p t, d < nNodes p) ∧
      (∀ g ∈ producesOf p t, ∀ d ∈ g, d < nNodes p)) ∧
  (∀ s ∈ steps q,
      s.transform < nTransforms p ∧
      (∀ b ∈ s.used, b.1 < nNodes p ∧ b.2 < nEndpoints q) ∧
      (∀ g ∈ s.produced, ∀ b ∈ g, b.1 < nNodes p ∧ b.2 < nEndpoints q))

/-! ## 3. Ancestry, by identity -/

/-- The reflexive-transitive closure of an endpoint's declared parents.

Compared by INDEX. An endpoint's identity is its position, and nothing here
identifies two rows by their contents: two files made by different steps are two
files even at the same type and the same lineage. A structural relation cannot
say so, which is what let a step anchor to one file and consume something derived
from another.

The `m < e` guard is inside the definition rather than carried as a hypothesis.
`Valid` must be decidable for the theorem below to typecheck, and `WellIndexed`
is one of its own fields, so it cannot be assumed while deciding a sibling. Under
`WellIndexed` the guard never fires, and a parent that does not decrease simply
makes the relation false -- which fails closed.

It is written as a `dite` so the proof `h : m < e` is in scope AT the recursive
call. Stated as a plain conjunct, Lean cannot see the measure decrease and
reports "Could not find a decreasing measure".

**Stated as a computation, not as an inductive `Prop`.** `check_spec` below is an
equation against `decide`, so every relation `Valid` mentions has to be decidable,
and `Decidable` for a well-founded inductive relation is an obligation somebody
has to discharge before the theorem can even be typed. This relation genuinely is
computable -- the checker computes it -- so saying so in the definition is more
honest than asserting a `Prop` and then proving what was true by construction.
The previous specification asserted inductive relations and could decide nothing,
which is part of why none of it was ever checked. -/
def ancestorB (q : types.Plan) (e f : Nat) : Bool :=
  e == f || (epParents q e).any (fun m => if h : m < e then ancestorB q m f else false)
termination_by e
decreasing_by exact h

@[reducible] def Ancestor (q : types.Plan) (e f : Nat) : Prop := ancestorB q e f = true

/-! ## 4. Instancing, and satisfaction -/

/-- What this step bound to slot `a`. The first match, so no clause needs `Shape`
to hold before a binding can be resolved. -/
def boundTo : List (Nat × Nat) → Nat → Option Nat
  | [], _ => none
  | b :: rest, a => if b.1 = a then some b.2 else boundTo rest a

/-- INSTANCING, one anchor at a time. The anchor names another slot; `boundTo`
resolves it to the endpoint THIS step bound to that slot -- the instance,
carrying full lineage -- rather than leaving it as the transform's declared
output type. The filling endpoint must then descend from that very endpoint.

Bool-valued, and the `Option` is matched here rather than in the `Prop` that uses
it. Written as `∃ f, boundTo used a = some f ∧ Ancestor q e f`, the existential is
unbounded and nothing can decide it -- and `check_spec` is an equation against
`decide`. An anchor this step never bound is `none`, and no endpoint fills a slot
whose anchor is unbound. -/
def anchorOk (q : types.Plan) (used : List (Nat × Nat)) (e a : Nat) : Bool :=
  match boundTo used a with
  | some f => ancestorB q e f
  | none => false

/-- May endpoint `e` fill slot `d`, in a step whose bindings are `used`?
Properties and lineage in one relation.

The anchor's binding must be the endpoint `e` actually descends from -- matched
is consumed, the same instance. With identity that makes a crossover impossible
rather than merely unlikely: an endpoint derived from one file cannot satisfy an
anchor bound to a different file, however alike the two files are.

Non-recursive on the demand: the anchor's own properties and lineage are checked
when THAT binding is checked, by this same clause over this same step. -/
@[reducible] def Satisfies (p : types.Problem) (q : types.Plan)
    (used : List (Nat × Nat)) (e d : Nat) : Prop :=
  IsA (epProps q e) (nodeProps p d) ∧
  ∀ a ∈ nodeParents p d, anchorOk q used e a = true

/-! ## 5. The remaining vocabulary -/

@[reducible] def Emits (s : StepView) (e : Nat) : Prop := ∃ g ∈ s.produced, ∃ b ∈ g, b.2 = e

def slotsOf (bs : List (Nat × Nat)) : Ids := bs.map Prod.fst

/-- The step binds exactly the slots its transform requires, and emits exactly
one endpoint per declared product slot. Without this a step that binds nothing
passes every clause below vacuously.

No exemption for a given step, because there is no given step: the givens are a
parameter and the adapter strips it. That removed four exemptions and a boundary
conjunct, all of them case splits a completeness proof would have had to carry. -/
@[reducible] def Shape (p : types.Problem) (s : StepView) : Prop :=
  (slotsOf s.used).Nodup ∧
  SameSet (slotsOf s.used) (requiresOf p s.transform) ∧
  s.produced.length = (producesOf p s.transform).length ∧
  ∀ gd ∈ s.produced.zip (producesOf p s.transform), SameSet (slotsOf gd.1) gd.2

/-- The lineage a step confers on what it produces: everything it consumed, and
those endpoints' own parents. One hop, which is why `Ancestor` is transitive -- a
three-level given chain loses its grandparent at the first step. -/
def Confers (q : types.Plan) (used : List (Nat × Nat)) : Ids :=
  (used.map Prod.snd) ++ (used.map Prod.snd).flatMap (fun f => epParents q f)

/-- A produced endpoint's declared lineage is EXACTLY what its step confers.

Equality, not containment. A product free to declare extra parents can buy any
anchor it likes, which is a crossover written by hand rather than found by the
search. This is what ties the declared lineage to the step graph, and it is what
lets `Ancestor` read the declared closure alone instead of a union of two
relations. -/
@[reducible] def Derived (q : types.Plan) (used : List (Nat × Nat)) (e : Nat) : Prop :=
  SameSet (epParents q e) (Confers q used)

/-! ## 6. A valid plan -/

structure Valid (p : types.Problem) (q : types.Plan) : Prop where
  indexed        : WellIndexed p q
  shape          : ∀ s ∈ steps q, Shape p s
  /-- Every input binding fits the slot it filled. -/
  conformance    : ∀ s ∈ steps q, ∀ b ∈ s.used, Satisfies p q s.used b.2 b.1
  /-- Every produced endpoint fits the slot it left... -/
  emission       : ∀ s ∈ steps q, ∀ g ∈ s.produced, ∀ b ∈ g, Satisfies p q s.used b.2 b.1
  /-- ...and carries the lineage its step confers, no more and no less. -/
  derived        : ∀ s ∈ steps q, ∀ g ∈ s.produced, ∀ b ∈ g, Derived q s.used b.2
  /-- One endpoint, one producer. Two steps emitting one endpoint is two
  processes writing one file, and `rectify` produced exactly that until it was
  repaired. -/
  uniqueProducer : ∀ e < nEndpoints q, ∀ i ∈ (steps q).zipIdx, ∀ j ∈ (steps q).zipIdx,
                     Emits i.1 e → Emits j.1 e → i.2 = j.2
  /-- Nothing is consumed out of thin air. -/
  provenance     : ∀ s ∈ steps q, ∀ b ∈ s.used,
                     (∃ s' ∈ steps q, Emits s' b.2) ∨ (∃ gn ∈ givens q, gn.1 = b.2)
  /-- Every presented given is one the problem declared, paired one to one, and
  the pairing the wire supplied is a real correspondence: equal properties, and a
  lineage edge on one side exactly when there is one on the other.

  There is deliberately NO condition confining a plan to one declared group. The
  groups are one per sample and a multi-sample workflow legitimately spans all of
  them. What keeps a single step from mixing two samples is the lineage anchors
  in `conformance`, not group membership -- so a problem whose transforms declare
  no anchors has not asked for the samples to be kept apart. -/
  givens         : ((givens q).map Prod.fst).Nodup ∧ ((givens q).map Prod.snd).Nodup ∧
                   (∀ gn ∈ givens q, ∃ g ∈ givenGroups p, gn.2 ∈ g) ∧
                   (∀ gn ∈ givens q, SameSet (epProps q gn.1) (nodeProps p gn.2)) ∧
                   (∀ gn ∈ givens q, ∀ f ∈ epParents q gn.1,
                      ∃ hn ∈ givens q, hn.1 = f ∧ hn.2 ∈ nodeParents p gn.2) ∧
                   (∀ gn ∈ givens q, ∀ a ∈ nodeParents p gn.2,
                      ∃ hn ∈ givens q, hn.2 = a ∧ hn.1 ∈ epParents q gn.1)
  /-- The planned workflow is a DAG. A list that is a topological order is itself
  the proof, so this replaces a separate cycle check. Declared LINEAGE may still
  contain cycles, and does; this clause is about the step graph only. -/
  schedulable    : ∀ cj ∈ (steps q).zipIdx, ∀ b ∈ cj.1.used,
                     ∀ pi ∈ (steps q).zipIdx, Emits pi.1 b.2 → pi.2 < cj.2
  /-- Exactly one application of the target. This also rules out the empty plan,
  which is why there is no separate `nonempty` clause. -/
  target         : ((steps q).filter (fun s => s.transform == targetTr p)).length = 1

/-- The same ten conditions as a conjunction, which is what carries the
`Decidable` instance: Lean derives nothing for a structure of `Prop`s, and
`check_spec` cannot even be stated without it. Every quantifier below is bounded
by a list or by a `Nat`, so the instance is found rather than written. -/
@[reducible] def ValidC (p : types.Problem) (q : types.Plan) : Prop :=
  WellIndexed p q ∧
  (∀ s ∈ steps q, Shape p s) ∧
  (∀ s ∈ steps q, ∀ b ∈ s.used, Satisfies p q s.used b.2 b.1) ∧
  (∀ s ∈ steps q, ∀ g ∈ s.produced, ∀ b ∈ g, Satisfies p q s.used b.2 b.1) ∧
  (∀ s ∈ steps q, ∀ g ∈ s.produced, ∀ b ∈ g, Derived q s.used b.2) ∧
  (∀ e < nEndpoints q, ∀ i ∈ (steps q).zipIdx, ∀ j ∈ (steps q).zipIdx,
      Emits i.1 e → Emits j.1 e → i.2 = j.2) ∧
  (∀ s ∈ steps q, ∀ b ∈ s.used,
      (∃ s' ∈ steps q, Emits s' b.2) ∨ (∃ gn ∈ givens q, gn.1 = b.2)) ∧
  (((givens q).map Prod.fst).Nodup ∧ ((givens q).map Prod.snd).Nodup ∧
      (∀ gn ∈ givens q, ∃ g ∈ givenGroups p, gn.2 ∈ g) ∧
      (∀ gn ∈ givens q, SameSet (epProps q gn.1) (nodeProps p gn.2)) ∧
      (∀ gn ∈ givens q, ∀ f ∈ epParents q gn.1,
          ∃ hn ∈ givens q, hn.1 = f ∧ hn.2 ∈ nodeParents p gn.2) ∧
      (∀ gn ∈ givens q, ∀ a ∈ nodeParents p gn.2,
          ∃ hn ∈ givens q, hn.2 = a ∧ hn.1 ∈ epParents q gn.1)) ∧
  (∀ cj ∈ (steps q).zipIdx, ∀ b ∈ cj.1.used,
      ∀ pi ∈ (steps q).zipIdx, Emits pi.1 b.2 → pi.2 < cj.2) ∧
  ((steps q).filter (fun s => s.transform == targetTr p)).length = 1

theorem valid_iff_validC (p : types.Problem) (q : types.Plan) :
    Valid p q ↔ ValidC p q :=
  ⟨fun v => ⟨v.indexed, v.shape, v.conformance, v.emission, v.derived,
             v.uniqueProducer, v.provenance, v.givens, v.schedulable, v.target⟩,
   fun ⟨a, b, c, d, e, f, g, h, i, j⟩ => ⟨a, b, c, d, e, f, g, h, i, j⟩⟩

-- The default instance-synthesis budget does not reach the bottom of a ten-way
-- conjunction whose every conjunct is itself a nested bounded quantifier over
-- reducible definitions. Each conjunct resolves on its own; only the whole does
-- not, which makes the failure read as a missing instance rather than as a
-- limit being hit.
set_option synthInstance.maxSize 1000 in
set_option synthInstance.maxHeartbeats 4000000 in
instance (p : types.Problem) (q : types.Plan) : Decidable (Valid p q) :=
  decidable_of_iff _ (valid_iff_validC p q).symm

/-! ## 7. The obligation

  `solver_witness.check` is the extracted checker, so it arrives in Aeneas's
  `Result` monad and the statement is over that value.

  Stated as ONE EQUATION rather than as the biconditional directly. The
  biconditional alone does not pin the negative case: a checker that FAILS on
  every invalid plan satisfies `check p q = .ok true ↔ Valid p q` vacuously. The
  equation gives totality, soundness and completeness together, and it is what
  the clause-by-clause decomposition naturally produces.

  **The obligation is DISCHARGED in `Proof/Compose.lean`, not here**, and the
  direction of the imports is why: every proof module imports this file for the
  vocabulary above, so this file cannot import them back. What stays here is the
  statement of what a correct plan is, which is the part that has to be read and
  believed. `SolverProof.check_spec` is the theorem, and `SolverProof.check_correct`
  -- `∀ S. Certified(S) = Valid(S)` -- follows from it in one line.
-/

end SolverSpec
