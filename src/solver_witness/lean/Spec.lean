/-
  What a correct plan is.

  This file is normative. `docs/metasmith/solver-spec.md` carries the prose and
  the reasons; `src/lib.rs` is the executable checker that must satisfy it, and
  `theorem check_sound` at the bottom is the obligation relating the two.

  Deliberately no Mathlib. Set equality on lists is written out as mutual
  inclusion rather than reached for through `Finset`, so this builds against a
  bare Lean toolchain in seconds. The specification is small enough that the
  dependency would cost more than it saves.

  The types are the wire format. Properties are interned to integers, nodes
  carry integer parents, and every endpoint has an index, so the specification
  talks about the bytes that leave the process rather than about the solver's
  in-memory arenas.
-/

namespace SolverWitness

abbrev PropId := Nat
/-- Index into `Problem.nodes`: a slot on a transform, or the declared type of a
given input. The node table serves both roles. -/
abbrev NodeId := Nat
/-- Index into `Plan.endpoints`. Position, not structure -- see `Same`. -/
abbrev EpId := Nat
/-- Index into `Problem.transforms`. -/
abbrev TrId := Nat

/-- Mutual inclusion. Property lists and slot lists are sets in everything but
representation, and comparing them by position is wrong in both places it is
tempting: `requires` may name one slot twice, and a merged step's product groups
are not in declaration order. -/
def SameSet {α : Type} (a b : List α) : Prop :=
  (∀ x ∈ a, x ∈ b) ∧ (∀ x ∈ b, x ∈ a)

structure Node where
  props : List PropId
  /-- Lineage anchors. For a `requires` slot these are earlier slots of the same
  transform; for a given node they are that endpoint's real lineage and belong
  to no transform at all. -/
  parents : List NodeId

structure Transform where
  requires : List NodeId
  /-- One inner list per product group. For the given transform the groups are
  *alternatives*, one per sample; for every other transform they are conjunctive
  and all of them are emitted. -/
  produces : List (List NodeId)

structure Problem where
  nodes : Array Node
  transforms : Array Transform
  givenTr : TrId
  callerTrs : List TrId
  targetTr : TrId
  given : List (List NodeId)

structure Endpoint where
  props : List PropId
  parents : List EpId

structure Step where
  transform : TrId
  used : List (NodeId × EpId)
  produced : List (List (NodeId × EpId))

structure Plan where
  endpoints : Array Endpoint
  steps : List Step
  /-- Whether the search finished. An incomplete plan is a search that gave up,
  which is a different answer from a wrong one. -/
  complete : Bool

/-- Every id names something that exists.

Without this the specification is vacuous rather than merely incomplete:
`getElem!` returns the `Inhabited` default for an out-of-range index, which is
an empty property list, and an empty demand is satisfied by anything. A plan
binding a slot id of one billion would pass every clause below. -/
def WellIndexed (p : Problem) (q : Plan) : Prop :=
  p.givenTr < p.transforms.size ∧
  p.targetTr < p.transforms.size ∧
  (∀ n ∈ p.nodes.toList, ∀ a ∈ n.parents, a < p.nodes.size) ∧
  (∀ t ∈ p.transforms.toList,
      (∀ d ∈ t.requires, d < p.nodes.size) ∧
      (∀ g ∈ t.produces, ∀ d ∈ g, d < p.nodes.size)) ∧
  (∀ e ∈ q.endpoints.toList, ∀ f ∈ e.parents, f < q.endpoints.size) ∧
  (∀ grp ∈ p.given, ∀ n ∈ grp, n < p.nodes.size) ∧
  (∀ s ∈ q.steps,
      s.transform < p.transforms.size ∧
      (∀ b ∈ s.used, b.1 < p.nodes.size ∧ b.2 < q.endpoints.size) ∧
      (∀ g ∈ s.produced, ∀ b ∈ g, b.1 < p.nodes.size ∧ b.2 < q.endpoints.size))

/-- Two endpoints are the same *for lineage* when their properties agree and
their lineage agrees, all the way down.

**Structure, not position, and this was measured rather than chosen.** Comparing
lineage by arena index rejects seven of the eleven shipped workflows. The solver
emits structural twins -- one endpoint carried over from the caller's own
objects, one minted during the search, identical properties and identical
parents -- and a step consumes one twin while the endpoints it produces record
the other. The Python checker has always compared this way.

Well founded because an endpoint's parents are emitted before it, so the
recursion descends a strictly decreasing index. -/
inductive Same (q : Plan) : EpId → EpId → Prop where
  | mk {x y : EpId} :
      SameSet (q.endpoints[x]!).props (q.endpoints[y]!).props →
      (∀ a ∈ (q.endpoints[x]!).parents, ∃ b ∈ (q.endpoints[y]!).parents, Same q a b) →
      (∀ b ∈ (q.endpoints[y]!).parents, ∃ a ∈ (q.endpoints[x]!).parents, Same q a b) →
      Same q x y

/-- Step `s` emits endpoint `e`: it came out of one of `s`'s output slots. -/
def Emits (s : Step) (e : EpId) : Prop :=
  ∃ g ∈ s.produced, ∃ b ∈ g, b.2 = e

/-- Ancestry: the transitive closure of an endpoint's *declared* parents, taken
up to `Same`.

One source, not the union of declared parents and the producing step's inputs.
The step graph adds nothing once `Sound.rooted` holds, and `rooted` is checked
rather than assumed. -/
inductive Ancestor (q : Plan) : EpId → EpId → Prop where
  | direct {e a f : EpId} : a ∈ (q.endpoints[e]!).parents → Same q a f → Ancestor q e f
  | trans {e m f : EpId} : m ∈ (q.endpoints[e]!).parents → Ancestor q m f → Ancestor q e f

/-- May endpoint `e` fill slot `d`, in a step whose bindings are `u`?

Properties and lineage are one test rather than two clauses. The anchoring to
`u` is what stops a sample crossover: without it, "descends from *an* endpoint
satisfying the anchor" lets one sample's reads fill a slot anchored to another
sample's, which is exactly what the shipped lineage chains exist to prevent.

Anchoring also removes the recursion. The anchor's own properties and lineage
are checked when *that* binding is checked, by the same clause over the same
step. -/
def Fills (p : Problem) (q : Plan) (u : List (NodeId × EpId)) (e : EpId) (d : NodeId) : Prop :=
  (∀ x ∈ (p.nodes[d]!).props, x ∈ (q.endpoints[e]!).props) ∧
  (∀ a ∈ (p.nodes[d]!).parents,
      ∃ f, (a, f) ∈ u ∧ (Same q e f ∨ Ancestor q e f))

/-- The slot half of a binding list. -/
def slots (bs : List (NodeId × EpId)) : List NodeId := bs.map Prod.fst

/-- Does this step honour the shape of its transform's contract?

The given transform is the exception on the output side: its product groups are
alternatives, and branching hands the step one group per timeline, so it emits
fewer groups than it declares. -/
def Applies (p : Problem) (s : Step) : Prop :=
  let t := p.transforms[s.transform]!
  (slots s.used).Nodup ∧
  SameSet (slots s.used) t.requires ∧
  (s.transform = p.givenTr →
      ∀ g ∈ s.produced, ∃ dg ∈ t.produces, SameSet (slots g) dg) ∧
  (s.transform ≠ p.givenTr →
      s.produced.length = t.produces.length ∧
      ∀ gd ∈ s.produced.zip t.produces, SameSet (slots gd.1) gd.2)

/-- A condition on the *problem*, not on the plan.

`Fills` requires the anchor to be bound in the same step, so an anchor has to be
one of that transform's own inputs, declared earlier -- otherwise the step could
not have chosen the anchor's binding by the time it needed it. Declaration order
is the reference space.

Scoped to `requires` slots on purpose. Product slots may carry lineage and
nothing constrains where those parents live, and given-endpoint nodes carry
parents belonging to no transform at all, so quantifying over every node would
refuse every real problem. -/
def WellFormed (p : Problem) : Prop :=
  ∀ t ∈ p.transforms.toList, ∀ i d, t.requires.get? i = some d →
    ∀ a ∈ (p.nodes[d]!).parents, ∃ j, j < i ∧ t.requires.get? j = some a

/-- The ten conditions. -/
structure Sound (p : Problem) (q : Plan) : Prop where
  /-- 0. Every id names something that exists. -/
  indexed : WellIndexed p q
  /-- 1. A plan with no steps is not a plan. -/
  nonempty : q.steps ≠ []
  /-- 2. Every step honours its transform's contract. -/
  shape : ∀ s ∈ q.steps, Applies p s
  /-- 3. Nothing is consumed out of thin air. Compared by *position*: a plan
  that consumes something nothing produced is not repaired by something else
  producing a lookalike. -/
  provenance : ∀ s ∈ q.steps, ∀ b ∈ s.used, ∃ s' ∈ q.steps, Emits s' b.2
  /-- 4. Every input binding fits its slot, properties and lineage together. -/
  conformance : ∀ s ∈ q.steps, ∀ b ∈ s.used, Fills p q s.used b.2 b.1
  /-- 5. Every produced endpoint fits the slot it left. The given step is exempt:
  after a timeline merge it emits one sample's endpoint under another sample's
  slot, so the property subset genuinely does not hold there. -/
  emission : ∀ s ∈ q.steps, s.transform ≠ p.givenTr →
      ∀ g ∈ s.produced, ∀ b ∈ g, Fills p q s.used b.2 b.1
  /-- 6. A produced endpoint declares, among its parents, every endpoint its own
  step consumed. This is what makes the narrow `Ancestor` sufficient: with it,
  the step graph reaches nothing the declared closure does not. -/
  rooted : ∀ s ∈ q.steps, s.transform ≠ p.givenTr →
      ∀ g ∈ s.produced, ∀ b ∈ g, ∀ c ∈ s.used,
        ∃ a ∈ (q.endpoints[b.2]!).parents, Same q a c.2
  /-- 7. The given step presents only endpoints matching a declared input. This
  is what stops a plan inventing its own starting data. -/
  givens : ∀ s ∈ q.steps, s.transform = p.givenTr →
      ∀ g ∈ s.produced, ∀ b ∈ g,
        ∃ grp ∈ p.given, ∃ n ∈ grp,
          SameSet (p.nodes[n]!).props (q.endpoints[b.2]!).props
  /-- 8. If one step emits what another consumes, the emitter comes first. A
  list that is a topological order is itself the proof that the graph is
  acyclic, so this replaces the separate ordering and cycle clauses. -/
  schedulable : ∀ i j : Fin q.steps.length, ∀ b ∈ (q.steps.get j).used,
      Emits (q.steps.get i) b.2 → i < j
  /-- 9. Exactly one target application and exactly one given application, and
  the given one consumes nothing. -/
  boundary :
    (q.steps.filter (fun s => s.transform == p.targetTr)).length = 1 ∧
    (q.steps.filter (fun s => s.transform == p.givenTr)).length = 1 ∧
    (∀ s ∈ q.steps, s.transform = p.givenTr → s.used = [])

/-
  The obligation.

  `check` is `solver_witness::check`, extracted through Charon and Aeneas. The
  statement is deliberately one-directional: when the witness accepts, the plan
  really is sound. The converse is not claimed and is not true -- a rejection
  says this plan is wrong, never that no plan exists.

  `WellFormed` is a hypothesis about the problem rather than a clause of
  soundness, because it is what makes `conformance` *satisfiable* at all: an
  anchor that is not one of the transform's own requirements can never be bound
  in the step that needs it. It is discharged by the library loader, which
  refuses to build such a transform, and by the Rust generator, which refuses a
  later anchor rather than guessing.

  Stated once the extraction exists, because Aeneas emits into a monad for
  anything partial and the extracted `check` will not be a bare `Bool`.
-/
-- theorem check_sound (p : Problem) (q : Plan) (hw : WellFormed p) :
--     check p q = true → Sound p q := by
--   sorry

end SolverWitness
