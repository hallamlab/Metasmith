/-
  `indexed` -- the clause that makes the other nine mean something.

  Read `Proof/Basis.lean` for the loop recipe, and `Proof/Steps.lean` and
  `Proof/Slots.lean` for the `drop` idiom, the `ok`-flag idiom and the `Nodup`
  shape this file reuses rather than re-derives. What follows is what the largest
  clause needs on top of them.

  ## Why this one has to be unconditional, and is

  Four of the ten clause lemmas hold only under `WellIndexed`, and the final
  composition discharges that hypothesis by case-splitting on THIS clause. So
  `cl_indexed_spec` may carry no hypothesis, and it may not be approximate in
  either direction: an over-permissive `cl_indexed` would make the branch where
  `WellIndexed` is assumed reachable on plans where it does not hold, and every
  conditional lemma downstream would then be discharged from nothing.

  It is unconditional and exact, and that is structural rather than lucky. The
  two mechanisms that broke the other four are both absent here. No judgement in
  this clause goes through a dense bit set, so there is no width at which an id
  becomes invisible -- the `same_slots` gap of `Proof/Slots`. And no judgement
  reads a precomputed table, so there is no row bound to fail closed at -- the
  `descends` gap of `Proof/Lineage`. Every comparison below is against the very
  table length the specification quantifies with, which is why the two cannot
  drift apart. `Proof/Access` supplies that: each accessor is proved total, so
  the correspondence holds for an out-of-range id as well as an in-range one,
  which is exactly the case the other clauses could not reach.

  ## The decomposition

  `cl_indexed` is a monolith -- twenty loops, no pointwise predicate to quantify
  over -- so the correspondence is made one HELPER at a time: `given_group_indexed`,
  `node_indexed`, `endpoint_indexed`, `transform_indexed` (with
  `group_slots_indexed` under it) and `step_indexed` (with `group_indexed` under
  it). Four spec-side predicates name what a whole row has to satisfy: `NodeOk`,
  `EpOk`, `TrOk`, `StepOk`. They are `@[reducible]` for the same reason
  `SolverSpec`'s definitions are -- the composition has to see through them both
  to find the `Decidable` instance and to close its `Iff` by `exact`.

  The composition is real work, and it is the one place the checker and the
  specification are organised differently. `node_indexed` decides parents AND
  properties in one call, and `endpoint_indexed` likewise, while `WellIndexed`
  states those four as SEPARATE conjuncts, interleaved node/endpoint/node/endpoint.
  So the final `Iff` turns `∀ d < n, (A d ∧ B d)` into `(∀ d < n, A d) ∧
  (∀ d < n, B d)`, twice, and re-orders. The two forms are equivalent, not equal,
  and nothing but the hand-written `Iff` says so.

  ## Four things the recipe does not say

  **A loop over a TABLE SIZE has no list to `drop`.** The node, endpoint and
  transform loops are bounded by a `Nat` rather than by a list, so the invariant
  is the range form `∀ d < N, i ≤ d → P d`. Write the bound FIRST: that is the
  shape `Nat.decidableBallLT` matches, and the same statement as
  `∀ d, i ≤ d → d < N → P d` synthesizes no instance, with an error that reads as
  if the predicate were at fault. `Nat.eq_or_lt_of_le` on the head is this form's
  answer to `List.mem_cons`, and `ball_range_cons` below packages it.

  **A rebuilt record is not the argument you passed -- but structure eta says it
  is.** Aeneas hoists a struct's fields to separate loop parameters and rebuilds
  the struct at each call it makes: `cl_indexed_loop2` takes six fields and calls
  `node_indexed { n_props := .., nodes := .., .. }`. State the loop lemma with
  the program applied to `p.n_props, p.nodes, …` and the rebuilt term is
  `Problem.mk p.n_props p.nodes …`, which unification accepts for `p` by
  structure eta. That is why no lemma below rewrites a record, and why none of
  them needs to: `step with node_indexed_spec p i` matches the rebuilt call as
  it stands. Do NOT reach for `rw` here -- eta is a defeq the elaborator does and
  `rw`'s syntactic match does not.

  **A table size is a parameter, not a projection.** Every helper takes `NN`,
  `NE`, `NT` as plain `Nat`s with a defining equation for the `Usize` the checker
  compares against. It keeps a `Vec`'s `.val` out of the `decide` in the
  postcondition, per `Proof/Steps`' first trap, and it is what lets
  `group_indexed_spec` be applied to a rebuilt copy of `q` without its
  postcondition mentioning that copy.

  **`cl_indexed_loop1` returns a pair.** It threads the `Plan` through, so the
  bind that consumes it is a pattern match that `simp only [bind_tc_ok]` leaves
  standing. `ok_of_spec`, `Prod.mk.injEq` and a `show` of the destructured
  program are the way through -- the idiom `Proof/Steps` established for
  `cl_unique_producer`. Everything after that bind is factored into
  `cl_indexed_tail_spec` so the `show` is written once rather than once per
  branch of the `target_tr` test.
-/

import SolverWitness.Types
import SolverWitness.Funs
import SolverWitness.Spec
import SolverWitness.Proof.Basis
import SolverWitness.Proof.Access
import SolverWitness.Proof.Steps
import SolverWitness.Proof.Slots

set_option maxHeartbeats 2000000
set_option maxRecDepth 8000
set_option synthInstance.maxSize 1000
set_option synthInstance.maxHeartbeats 4000000

namespace SolverProof

open Aeneas Aeneas.Std Result
open solver_witness

namespace Indexed

/-! ## What each helper decides -/

/-- One node's row: parents precede it, properties name real properties. -/
@[reducible] def NodeOk (p : types.Problem) (d : Nat) : Prop :=
  (∀ a ∈ SolverSpec.nodeParents p d, a < d) ∧
  (∀ x ∈ SolverSpec.nodeProps p d, x < SolverSpec.nProps p)

/-- One endpoint's row, and the same two conditions. -/
@[reducible] def EpOk (p : types.Problem) (q : types.Plan) (e : Nat) : Prop :=
  (∀ f ∈ SolverSpec.epParents q e, f < e) ∧
  (∀ x ∈ SolverSpec.epProps q e, x < SolverSpec.nProps p)

/-- One transform: unique requirements, and every slot it names is a node. -/
@[reducible] def TrOk (p : types.Problem) (NN : Nat) (t : Nat) : Prop :=
  (SolverSpec.requiresOf p t).Nodup ∧
  (∀ d ∈ SolverSpec.requiresOf p t, d < NN) ∧
  (∀ g ∈ SolverSpec.producesOf p t, ∀ d ∈ g, d < NN)

/-- One step: a real transform, and every binding names a real slot and a real
endpoint. -/
@[reducible] def StepOk (NN NE NT : Nat) (s : SolverSpec.StepView) : Prop :=
  s.transform < NT ∧
  (∀ b ∈ s.used, b.1 < NN ∧ b.2 < NE) ∧
  (∀ g ∈ s.produced, ∀ b ∈ g, b.1 < NN ∧ b.2 < NE)

/-- Everything `cl_indexed` decides after the two transform-id tests: the six
loops, as one proposition. Factored out so the pair-returning bind is stepped
once rather than once per branch of the `target_tr` test. -/
@[reducible] def Rest (p : types.Problem) (q : types.Plan) : Prop :=
  (∀ g ∈ SolverSpec.givenGroups p, ∀ n ∈ g, n < SolverSpec.nNodes p) ∧
  (∀ gn ∈ SolverSpec.givens q, gn.1 < SolverSpec.nEndpoints q ∧ gn.2 < SolverSpec.nNodes p) ∧
  (∀ d < SolverSpec.nNodes p, NodeOk p d) ∧
  (∀ e < SolverSpec.nEndpoints q, EpOk p q e) ∧
  (∀ t < SolverSpec.nTransforms p, TrOk p (SolverSpec.nNodes p) t) ∧
  (∀ s ∈ SolverSpec.steps q,
      StepOk (SolverSpec.nNodes p) (SolverSpec.nEndpoints q) (SolverSpec.nTransforms p) s)

/-! ## Two list lemmas the siblings do not already carry -/

/-- One head and one tail, where the head is read with the default the two-level
accessors fall back to. `Proof/Slots`' `drop_cons_getElem!` is the same lemma
with the default taken from an `Inhabited` instance. -/
theorem drop_cons_getD {α} (L : List α) (dflt : α) (j : Nat) (h : j < L.length) :
    L.drop j = (L[j]?).getD dflt :: L.drop (j + 1) := by
  rw [List.drop_eq_getElem_cons h, List.cons.injEq]
  exact ⟨by simp [List.getElem?_eq_getElem h], rfl⟩

/-- The range form's `List.mem_cons`. -/
theorem ball_range_cons {N i : Nat} {P : Nat → Prop} (hi : i < N) :
    (∀ d < N, i ≤ d → P d) ↔ (P i ∧ ∀ d < N, i + 1 ≤ d → P d) := by
  constructor
  · intro h
    exact ⟨h i hi (Nat.le_refl _), fun d hd hge => h d hd (by omega)⟩
  · rintro ⟨hP, h⟩ d hd hge
    rcases Nat.eq_or_lt_of_le hge with heq | hlt
    · rw [← heq]; exact hP
    · exact h d hd (by omega)

/-! ## `given_group_indexed` -- one declared given group

The loop re-indexes `p.given` on every iteration and does so BEFORE it tests the
counter, so the group index has to be in range or the computation fails. It is
called only under `cl_indexed`'s own guard, which is what makes the hypothesis
sound rather than a weakening. -/

theorem given_group_indexed_loop_spec (p : types.Problem) (g nn : Std.Usize)
    (hg : g.val < p.given.val.length) (NN : Nat) (hnn : nn.val = NN)
    (L : SolverSpec.Ids) (hL : (p.given.val[g.val]'hg).val.map (fun x => x.val) = L) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), L.length - j.val ≤ m →
      clauses.given_group_indexed_loop p g nn ok1 j ⦃ r =>
        r = (ok1 && decide (∀ n ∈ L.drop j.val, n < NN)) ⦄ := by
  have hlen : L.length = (p.given.val[g.val]'hg).val.length := Slots.ids_length _ _ hL
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.given_group_indexed_loop.eq_def]
    split
    · next hh =>
      rw [vec_index_ok p.given g hg]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        exfalso
        have hja : j.val < (p.given.val[g.val]'hg).val.length := by scalar_tac
        omega
      · next hge =>
        have hja : (p.given.val[g.val]'hg).val.length ≤ j.val := by scalar_tac
        have hnil : L.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.given_group_indexed_loop.eq_def]
    split
    · next hh =>
      rw [vec_index_ok p.given g hg]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hja : j.val < (p.given.val[g.val]'hg).val.length := by scalar_tac
        have hjL : j.val < L.length := by omega
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : L.drop j.val = L[j.val]! :: L.drop (j.val + 1) :=
          Slots.drop_cons_getElem! L j.val hjL
        have hxv : L[j.val]! = ((p.given.val[g.val]'hg).val[j.val]'hja).val :=
          Slots.ids_getElem! _ L hL j.val hja
        rw [vec_index_ok _ j hja]
        simp only [bind_tc_ok]
        split
        · next hbad =>
          have hno : ¬ (∀ n ∈ L.drop j.val, n < NN) := by
            intro hc
            have h1 := hc L[j.val]! (by rw [hcons]; exact List.mem_cons_self)
            rw [hxv, ← hnn] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false j1 (by omega)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok =>
          have hyes : L[j.val]! < NN := by rw [hxv, ← hnn]; scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          have hiff : (∀ n ∈ L.drop j1.val, n < NN) ↔ (∀ n ∈ L.drop j.val, n < NN) := by
            rw [hj1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hyes
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true j1 (by omega)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
      · next hge =>
        have hja : (p.given.val[g.val]'hg).val.length ≤ j.val := by scalar_tac
        have hnil : L.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem given_group_indexed_spec (p : types.Problem) (g nn : Std.Usize)
    (hg : g.val < p.given.val.length) (NN : Nat) (hnn : nn.val = NN)
    (L : SolverSpec.Ids) (hL : (p.given.val[g.val]'hg).val.map (fun x => x.val) = L) :
    clauses.given_group_indexed p g nn ⦃ r => r = decide (∀ n ∈ L, n < NN) ⦄ := by
  rw [clauses.given_group_indexed.eq_def]
  refine WP.spec_mono (given_group_indexed_loop_spec p g nn hg NN hnn L hL
    L.length true 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp

/-! ## `node_indexed` -- one node's parents and properties -/

theorem node_indexed_loop0_spec (p : types.Problem) (d np : Std.Usize)
    (hnp : np.val = (SolverSpec.nodeParents p d.val).length) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), np.val - j.val ≤ m →
      clauses.node_indexed_loop0 p d ok1 np j ⦃ r =>
        r = (ok1 && decide (∀ a ∈ (SolverSpec.nodeParents p d.val).drop j.val, a < d.val)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.node_indexed_loop0.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.nodeParents p d.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.node_indexed_loop0.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hj : j.val < (SolverSpec.nodeParents p d.val).length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : (SolverSpec.nodeParents p d.val).drop j.val
            = ((SolverSpec.nodeParents p d.val)[j.val]'hj)
                :: (SolverSpec.nodeParents p d.val).drop (j.val + 1) :=
          List.drop_eq_getElem_cons hj
        step as ⟨ x, hx ⟩
        have hxv : x.val = (SolverSpec.nodeParents p d.val)[j.val]'hj := by
          rw [hx, List.getElem?_eq_getElem hj]; rfl
        split
        · next hbad =>
          have hno : ¬ (∀ a ∈ (SolverSpec.nodeParents p d.val).drop j.val, a < d.val) := by
            intro hc
            have h1 := hc _ (by rw [hcons]; exact List.mem_cons_self)
            rw [← hxv] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok =>
          have hyes : ((SolverSpec.nodeParents p d.val)[j.val]'hj) < d.val := by
            rw [← hxv]; scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          have hiff : (∀ a ∈ (SolverSpec.nodeParents p d.val).drop j1.val, a < d.val)
              ↔ (∀ a ∈ (SolverSpec.nodeParents p d.val).drop j.val, a < d.val) := by
            rw [hj1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hyes
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.nodeParents p d.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem node_indexed_loop1_spec (p : types.Problem) (d nx : Std.Usize)
    (hnx : nx.val = (SolverSpec.nodeProps p d.val).length) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), nx.val - j.val ≤ m →
      clauses.node_indexed_loop1 p d ok1 j nx ⦃ r =>
        r = (ok1 && decide (∀ x ∈ (SolverSpec.nodeProps p d.val).drop j.val,
              x < SolverSpec.nProps p)) ⦄ := by
  have hnpv : SolverSpec.nProps p = p.n_props.val := rfl
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.node_indexed_loop1.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.nodeProps p d.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.node_indexed_loop1.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hj : j.val < (SolverSpec.nodeProps p d.val).length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : (SolverSpec.nodeProps p d.val).drop j.val
            = ((SolverSpec.nodeProps p d.val)[j.val]'hj)
                :: (SolverSpec.nodeProps p d.val).drop (j.val + 1) :=
          List.drop_eq_getElem_cons hj
        step as ⟨ x, hx ⟩
        have hxv : x.val = (SolverSpec.nodeProps p d.val)[j.val]'hj := by
          rw [hx, List.getElem?_eq_getElem hj]; rfl
        split
        · next hbad =>
          have hno : ¬ (∀ y ∈ (SolverSpec.nodeProps p d.val).drop j.val,
              y < SolverSpec.nProps p) := by
            intro hc
            have h1 := hc _ (by rw [hcons]; exact List.mem_cons_self)
            rw [← hxv, hnpv] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok =>
          have hyes : ((SolverSpec.nodeProps p d.val)[j.val]'hj) < SolverSpec.nProps p := by
            rw [← hxv, hnpv]; scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          have hiff : (∀ y ∈ (SolverSpec.nodeProps p d.val).drop j1.val,
                y < SolverSpec.nProps p)
              ↔ (∀ y ∈ (SolverSpec.nodeProps p d.val).drop j.val, y < SolverSpec.nProps p) := by
            rw [hj1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hyes
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.nodeProps p d.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem node_indexed_spec (p : types.Problem) (d : Std.Usize) :
    clauses.node_indexed p d ⦃ r => r = decide (NodeOk p d.val) ⦄ := by
  rw [clauses.node_indexed.eq_def]
  step as ⟨ np, hnp ⟩
  rw [eq_of_spec (node_indexed_loop0_spec p d np hnp np.val true 0#usize (by scalar_tac))]
  simp only [bind_tc_ok]
  step as ⟨ nx, hnx ⟩
  refine WP.spec_mono (node_indexed_loop1_spec p d nx hnx nx.val _ 0#usize
    (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  have hd1 : (SolverSpec.nodeParents p d.val).drop (0#usize).val
      = SolverSpec.nodeParents p d.val := List.drop_zero
  have hd2 : (SolverSpec.nodeProps p d.val).drop (0#usize).val
      = SolverSpec.nodeProps p d.val := List.drop_zero
  refine Bool.eq_iff_iff.mpr ?_
  simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.true_and, hd1, hd2]

/-! ## `endpoint_indexed` -- the same two conditions, one table down -/

theorem endpoint_indexed_loop0_spec (q : types.Plan) (e np : Std.Usize)
    (hnp : np.val = (SolverSpec.epParents q e.val).length) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), np.val - j.val ≤ m →
      clauses.endpoint_indexed_loop0 q e ok1 np j ⦃ r =>
        r = (ok1 && decide (∀ f ∈ (SolverSpec.epParents q e.val).drop j.val, f < e.val)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.endpoint_indexed_loop0.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.epParents q e.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.endpoint_indexed_loop0.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hj : j.val < (SolverSpec.epParents q e.val).length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : (SolverSpec.epParents q e.val).drop j.val
            = ((SolverSpec.epParents q e.val)[j.val]'hj)
                :: (SolverSpec.epParents q e.val).drop (j.val + 1) :=
          List.drop_eq_getElem_cons hj
        step as ⟨ x, hx ⟩
        have hxv : x.val = (SolverSpec.epParents q e.val)[j.val]'hj := by
          rw [hx, List.getElem?_eq_getElem hj]; rfl
        split
        · next hbad =>
          have hno : ¬ (∀ f ∈ (SolverSpec.epParents q e.val).drop j.val, f < e.val) := by
            intro hc
            have h1 := hc _ (by rw [hcons]; exact List.mem_cons_self)
            rw [← hxv] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok =>
          have hyes : ((SolverSpec.epParents q e.val)[j.val]'hj) < e.val := by
            rw [← hxv]; scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          have hiff : (∀ f ∈ (SolverSpec.epParents q e.val).drop j1.val, f < e.val)
              ↔ (∀ f ∈ (SolverSpec.epParents q e.val).drop j.val, f < e.val) := by
            rw [hj1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hyes
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.epParents q e.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem endpoint_indexed_loop1_spec (p : types.Problem) (q : types.Plan) (e nx : Std.Usize)
    (hnx : nx.val = (SolverSpec.epProps q e.val).length) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), nx.val - j.val ≤ m →
      clauses.endpoint_indexed_loop1 p q e ok1 j nx ⦃ r =>
        r = (ok1 && decide (∀ x ∈ (SolverSpec.epProps q e.val).drop j.val,
              x < SolverSpec.nProps p)) ⦄ := by
  have hnpv : SolverSpec.nProps p = p.n_props.val := rfl
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.endpoint_indexed_loop1.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.epProps q e.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.endpoint_indexed_loop1.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hj : j.val < (SolverSpec.epProps q e.val).length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : (SolverSpec.epProps q e.val).drop j.val
            = ((SolverSpec.epProps q e.val)[j.val]'hj)
                :: (SolverSpec.epProps q e.val).drop (j.val + 1) :=
          List.drop_eq_getElem_cons hj
        step as ⟨ x, hx ⟩
        have hxv : x.val = (SolverSpec.epProps q e.val)[j.val]'hj := by
          rw [hx, List.getElem?_eq_getElem hj]; rfl
        split
        · next hbad =>
          have hno : ¬ (∀ y ∈ (SolverSpec.epProps q e.val).drop j.val,
              y < SolverSpec.nProps p) := by
            intro hc
            have h1 := hc _ (by rw [hcons]; exact List.mem_cons_self)
            rw [← hxv, hnpv] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok =>
          have hyes : ((SolverSpec.epProps q e.val)[j.val]'hj) < SolverSpec.nProps p := by
            rw [← hxv, hnpv]; scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          have hiff : (∀ y ∈ (SolverSpec.epProps q e.val).drop j1.val, y < SolverSpec.nProps p)
              ↔ (∀ y ∈ (SolverSpec.epProps q e.val).drop j.val, y < SolverSpec.nProps p) := by
            rw [hj1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hyes
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.epProps q e.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem endpoint_indexed_spec (p : types.Problem) (q : types.Plan) (e : Std.Usize) :
    clauses.endpoint_indexed p q e ⦃ r => r = decide (EpOk p q e.val) ⦄ := by
  rw [clauses.endpoint_indexed.eq_def]
  step as ⟨ np, hnp ⟩
  rw [eq_of_spec (endpoint_indexed_loop0_spec q e np hnp np.val true 0#usize (by scalar_tac))]
  simp only [bind_tc_ok]
  step as ⟨ nx, hnx ⟩
  refine WP.spec_mono (endpoint_indexed_loop1_spec p q e nx hnx nx.val _ 0#usize
    (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  have hd1 : (SolverSpec.epParents q e.val).drop (0#usize).val
      = SolverSpec.epParents q e.val := List.drop_zero
  have hd2 : (SolverSpec.epProps q e.val).drop (0#usize).val
      = SolverSpec.epProps q e.val := List.drop_zero
  refine Bool.eq_iff_iff.mpr ?_
  simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.true_and, hd1, hd2]

/-! ## `transform_indexed` -- uniqueness, requirements, product groups

`no_repeat` on `required_slots` is `Proof/Slots`' work, reused rather than
restated: both lemmas there are total in the transform id, which is what lets
this one be total too. -/

theorem group_slots_indexed_loop_spec (p : types.Problem) (t gi nn ns : Std.Usize)
    (NN : Nat) (hnn : nn.val = NN)
    (hns : ns.val = (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).length) :
    ∀ (m : Nat) (ok1 : Bool) (k : Std.Usize), ns.val - k.val ≤ m →
      clauses.group_slots_indexed_loop p t gi nn ok1 ns k ⦃ r =>
        r = (ok1 && decide (∀ d ∈ (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop k.val,
              d < NN)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 k hk
    rw [clauses.group_slots_indexed_loop.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop k.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 k hk
    rw [clauses.group_slots_indexed_loop.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hj : k.val < (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).length := by
          scalar_tac
        have hbnd : k.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop k.val
            = ((((SolverSpec.producesOf p t.val)[gi.val]?).getD [])[k.val]'hj)
                :: (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop (k.val + 1) :=
          List.drop_eq_getElem_cons hj
        step as ⟨ x, hx ⟩
        have hxv : x.val = (((SolverSpec.producesOf p t.val)[gi.val]?).getD [])[k.val]'hj := by
          rw [hx, List.getElem?_eq_getElem hj]; rfl
        split
        · next hbad =>
          have hno : ¬ (∀ d ∈ (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop k.val,
              d < NN) := by
            intro hc
            have h1 := hc _ (by rw [hcons]; exact List.mem_cons_self)
            rw [← hxv, ← hnn] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ k1, hk1 ⟩
          refine WP.spec_mono (ih false k1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok =>
          have hyes : ((((SolverSpec.producesOf p t.val)[gi.val]?).getD [])[k.val]'hj) < NN := by
            rw [← hxv, ← hnn]; scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ k1, hk1 ⟩
          have hk1' : k1.val = k.val + 1 := by scalar_tac
          have hiff :
              (∀ d ∈ (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop k1.val, d < NN)
              ↔ (∀ d ∈ (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop k.val,
                   d < NN) := by
            rw [hk1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hyes
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true k1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
      · have hnil : (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop k.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem group_slots_indexed_spec (p : types.Problem) (t gi nn : Std.Usize)
    (NN : Nat) (hnn : nn.val = NN) :
    clauses.group_slots_indexed p t gi nn ⦃ r =>
      r = decide (∀ d ∈ ((SolverSpec.producesOf p t.val)[gi.val]?).getD [], d < NN) ⦄ := by
  rw [clauses.group_slots_indexed.eq_def]
  step as ⟨ ns, hns ⟩
  refine WP.spec_mono (group_slots_indexed_loop_spec p t gi nn ns NN hnn hns ns.val
    true 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp

theorem transform_indexed_loop0_spec (p : types.Problem) (t nn nr : Std.Usize)
    (NN : Nat) (hnn : nn.val = NN)
    (hnr : nr.val = (SolverSpec.requiresOf p t.val).length) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), nr.val - j.val ≤ m →
      clauses.transform_indexed_loop0 p t nn ok1 nr j ⦃ r =>
        r = (ok1 && decide (∀ d ∈ (SolverSpec.requiresOf p t.val).drop j.val, d < NN)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.transform_indexed_loop0.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.requiresOf p t.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.transform_indexed_loop0.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hj : j.val < (SolverSpec.requiresOf p t.val).length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : (SolverSpec.requiresOf p t.val).drop j.val
            = ((SolverSpec.requiresOf p t.val)[j.val]'hj)
                :: (SolverSpec.requiresOf p t.val).drop (j.val + 1) :=
          List.drop_eq_getElem_cons hj
        step as ⟨ x, hx ⟩
        have hxv : x.val = (SolverSpec.requiresOf p t.val)[j.val]'hj := by
          rw [hx, List.getElem?_eq_getElem hj]; rfl
        split
        · next hbad =>
          have hno : ¬ (∀ d ∈ (SolverSpec.requiresOf p t.val).drop j.val, d < NN) := by
            intro hc
            have h1 := hc _ (by rw [hcons]; exact List.mem_cons_self)
            rw [← hxv, ← hnn] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok =>
          have hyes : ((SolverSpec.requiresOf p t.val)[j.val]'hj) < NN := by
            rw [← hxv, ← hnn]; scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          have hiff : (∀ d ∈ (SolverSpec.requiresOf p t.val).drop j1.val, d < NN)
              ↔ (∀ d ∈ (SolverSpec.requiresOf p t.val).drop j.val, d < NN) := by
            rw [hj1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hyes
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.requiresOf p t.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem transform_indexed_loop1_spec (p : types.Problem) (t nn ng : Std.Usize)
    (NN : Nat) (hnn : nn.val = NN)
    (hng : ng.val = (SolverSpec.producesOf p t.val).length) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), ng.val - j.val ≤ m →
      clauses.transform_indexed_loop1 p t nn ok1 j ng ⦃ r =>
        r = (ok1 && decide (∀ g ∈ (SolverSpec.producesOf p t.val).drop j.val,
              ∀ d ∈ g, d < NN)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.transform_indexed_loop1.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.producesOf p t.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.transform_indexed_loop1.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hj : j.val < (SolverSpec.producesOf p t.val).length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : (SolverSpec.producesOf p t.val).drop j.val
            = (((SolverSpec.producesOf p t.val)[j.val]?).getD [])
                :: (SolverSpec.producesOf p t.val).drop (j.val + 1) :=
          drop_cons_getD _ [] j.val hj
        step with group_slots_indexed_spec p t j nn NN hnn as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : ∀ d ∈ ((SolverSpec.producesOf p t.val)[j.val]?).getD [], d < NN :=
            of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          have hiff : (∀ g ∈ (SolverSpec.producesOf p t.val).drop j1.val, ∀ d ∈ g, d < NN)
              ↔ (∀ g ∈ (SolverSpec.producesOf p t.val).drop j.val, ∀ d ∈ g, d < NN) := by
            rw [hj1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hP
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hbf =>
          have hnP : ¬ (∀ d ∈ ((SolverSpec.producesOf p t.val)[j.val]?).getD [], d < NN) :=
            of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hno : ¬ (∀ g ∈ (SolverSpec.producesOf p t.val).drop j.val, ∀ d ∈ g, d < NN) := by
            intro hc
            exact hnP (hc _ (by rw [hcons]; exact List.mem_cons_self))
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
      · have hnil : (SolverSpec.producesOf p t.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem transform_indexed_spec (p : types.Problem) (t nn : Std.Usize)
    (NN : Nat) (hnn : nn.val = NN) :
    clauses.transform_indexed p t nn ⦃ r => r = decide (TrOk p NN t.val) ⦄ := by
  rw [clauses.transform_indexed.eq_def]
  step with required_slots_spec p t as ⟨ v, hv ⟩
  step with no_repeat_spec (alloc.vec.Vec.deref v) (SolverSpec.requiresOf p t.val) hv
    as ⟨ b, hb ⟩
  step as ⟨ nr, hnr ⟩
  rw [eq_of_spec (transform_indexed_loop0_spec p t nn nr NN hnn hnr nr.val b 0#usize
    (by scalar_tac))]
  simp only [bind_tc_ok]
  step as ⟨ ng, hng ⟩
  refine WP.spec_mono (transform_indexed_loop1_spec p t nn ng NN hnn hng ng.val _ 0#usize
    (by scalar_tac)) ?_
  intro r hr
  rw [hr, hb]
  have hd1 : (SolverSpec.requiresOf p t.val).drop (0#usize).val
      = SolverSpec.requiresOf p t.val := List.drop_zero
  have hd2 : (SolverSpec.producesOf p t.val).drop (0#usize).val
      = SolverSpec.producesOf p t.val := List.drop_zero
  refine Bool.eq_iff_iff.mpr ?_
  simp only [Bool.and_eq_true, decide_eq_true_eq, hd1, hd2]
  constructor
  · rintro ⟨⟨a1, a2⟩, a3⟩
    exact ⟨a1, a2, a3⟩
  · rintro ⟨a1, a2, a3⟩
    exact ⟨⟨a1, a2⟩, a3⟩

/-! ## `step_indexed` -- one step's bindings and product groups -/

theorem group_indexed_loop_spec (q : types.Plan) (si gi nn ne : Std.Usize)
    (NN NE : Nat) (hnn : nn.val = NN) (hne : ne.val = NE)
    (hsi : si.val < q.steps.val.length)
    (hgi : gi.val < (q.steps.val[si.val]'hsi).produced.val.length)
    (G : List (Nat × Nat))
    (hG : SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi) = G) :
    ∀ (m : Nat) (ok1 : Bool) (k : Std.Usize), G.length - k.val ≤ m →
      clauses.group_indexed_loop q si gi nn ne ok1 k ⦃ r =>
        r = (ok1 && decide (∀ b ∈ G.drop k.val, b.1 < NN ∧ b.2 < NE)) ⦄ := by
  have hgl : G.length = ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length := by
    rw [← hG, idPairs_length]
  intro m
  induction m with
  | zero =>
    intro ok1 k hk
    rw [clauses.group_indexed_loop.eq_def]
    split
    · next hh =>
      rw [vec_index_ok q.steps si hsi]
      simp only [bind_tc_ok]
      rw [vec_index_ok _ gi hgi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        exfalso
        have hka : k.val < ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length := by
          scalar_tac
        omega
      · next hge =>
        have hka : ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length ≤ k.val := by
          scalar_tac
        have hnil : G.drop k.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 k hk
    rw [clauses.group_indexed_loop.eq_def]
    split
    · next hh =>
      rw [vec_index_ok q.steps si hsi]
      simp only [bind_tc_ok]
      rw [vec_index_ok _ gi hgi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hka : k.val < ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length := by
          scalar_tac
        have hbnd : k.val + 1 ≤ Usize.max := by scalar_tac
        obtain ⟨a, ee, hae⟩ :
            ∃ a ee, ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val[k.val]'hka
              = (a, ee) := ⟨_, _, rfl⟩
        have hcons : G.drop k.val = (a.val, ee.val) :: G.drop (k.val + 1) := by
          rw [← hG, idPairs_drop_cons _ k.val hka, hae]
        rw [vec_index_ok _ k hka, hae]
        show (do
            let ok2 ← if a ≥ nn then ok false
                      else (if ee ≥ ne then ok false else ok true)
            let k1 ← k + 1#usize
            clauses.group_indexed_loop q si gi nn ne ok2 k1) ⦃ r =>
              r = (ok1 && decide (∀ b ∈ G.drop k.val, b.1 < NN ∧ b.2 < NE)) ⦄
        split
        · next hbad =>
          have hno : ¬ (∀ b ∈ G.drop k.val, b.1 < NN ∧ b.2 < NE) := by
            intro hc
            have h1 := (hc (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self)).1
            rw [← hnn] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ k1, hk1 ⟩
          refine WP.spec_mono (ih false k1 (by omega)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok1 =>
          split
          · next hbad2 =>
            have hno : ¬ (∀ b ∈ G.drop k.val, b.1 < NN ∧ b.2 < NE) := by
              intro hc
              have h1 := (hc (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self)).2
              rw [← hne] at h1
              scalar_tac
            simp only [bind_tc_ok]
            step as ⟨ k1, hk1 ⟩
            refine WP.spec_mono (ih false k1 (by omega)) ?_
            intro r hr
            rw [hr, hh, decide_eq_false hno]
            simp
          · next hok2 =>
            have hyes : a.val < NN ∧ ee.val < NE := by
              rw [← hnn, ← hne]
              exact ⟨by scalar_tac, by scalar_tac⟩
            simp only [bind_tc_ok]
            step as ⟨ k1, hk1 ⟩
            have hk1' : k1.val = k.val + 1 := by scalar_tac
            have hiff : (∀ b ∈ G.drop k1.val, b.1 < NN ∧ b.2 < NE)
                ↔ (∀ b ∈ G.drop k.val, b.1 < NN ∧ b.2 < NE) := by
              rw [hk1', hcons]
              constructor
              · intro h y hy
                rcases List.mem_cons.mp hy with hy' | hy'
                · rw [hy']; exact hyes
                · exact h y hy'
              · intro h y hy
                exact h y (List.mem_cons_of_mem _ hy)
            refine WP.spec_mono (ih true k1 (by omega)) ?_
            intro r hr
            rw [hr, hh]
            simp only [Bool.true_and]
            exact decide_eq_decide.mpr hiff
      · next hge =>
        have hka : ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length ≤ k.val := by
          scalar_tac
        have hnil : G.drop k.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem group_indexed_spec (q : types.Plan) (si gi nn ne : Std.Usize)
    (NN NE : Nat) (hnn : nn.val = NN) (hne : ne.val = NE)
    (hsi : si.val < q.steps.val.length)
    (hgi : gi.val < (q.steps.val[si.val]'hsi).produced.val.length)
    (G : List (Nat × Nat))
    (hG : SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi) = G) :
    clauses.group_indexed q si gi nn ne ⦃ r =>
      r = decide (∀ b ∈ G, b.1 < NN ∧ b.2 < NE) ⦄ := by
  rw [clauses.group_indexed.eq_def]
  refine WP.spec_mono (group_indexed_loop_spec q si gi nn ne NN NE hnn hne hsi hgi G hG
    G.length true 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp

theorem step_indexed_loop0_spec (sts : alloc.vec.Vec types.Step) (si nn ne : Std.Usize)
    (NN NE : Nat) (hnn : nn.val = NN) (hne : ne.val = NE)
    (hsi : si.val < sts.val.length) (u : List (Nat × Nat))
    (hu : SolverSpec.idPairs (sts.val[si.val]'hsi).used = u) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), u.length - j.val ≤ m →
      clauses.step_indexed_loop0 sts si nn ne ok1 j ⦃ r =>
        r = (ok1 && decide (∀ b ∈ u.drop j.val, b.1 < NN ∧ b.2 < NE)) ⦄ := by
  have hul : u.length = (sts.val[si.val]'hsi).used.val.length := by
    rw [← hu, idPairs_length]
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.step_indexed_loop0.eq_def]
    split
    · next hh =>
      rw [vec_index_ok sts si hsi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        exfalso
        have hja : j.val < (sts.val[si.val]'hsi).used.val.length := by scalar_tac
        omega
      · next hge =>
        have hja : (sts.val[si.val]'hsi).used.val.length ≤ j.val := by scalar_tac
        have hnil : u.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.step_indexed_loop0.eq_def]
    split
    · next hh =>
      rw [vec_index_ok sts si hsi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hja : j.val < (sts.val[si.val]'hsi).used.val.length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        obtain ⟨a, ee, hae⟩ :
            ∃ a ee, (sts.val[si.val]'hsi).used.val[j.val]'hja = (a, ee) := ⟨_, _, rfl⟩
        have hcons : u.drop j.val = (a.val, ee.val) :: u.drop (j.val + 1) := by
          rw [← hu, idPairs_drop_cons _ j.val hja, hae]
        rw [vec_index_ok _ j hja, hae]
        show (do
            let ok2 ← if a ≥ nn then ok false
                      else (if ee ≥ ne then ok false else ok true)
            let j1 ← j + 1#usize
            clauses.step_indexed_loop0 sts si nn ne ok2 j1) ⦃ r =>
              r = (ok1 && decide (∀ b ∈ u.drop j.val, b.1 < NN ∧ b.2 < NE)) ⦄
        split
        · next hbad =>
          have hno : ¬ (∀ b ∈ u.drop j.val, b.1 < NN ∧ b.2 < NE) := by
            intro hc
            have h1 := (hc (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self)).1
            rw [← hnn] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by omega)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok1 =>
          split
          · next hbad2 =>
            have hno : ¬ (∀ b ∈ u.drop j.val, b.1 < NN ∧ b.2 < NE) := by
              intro hc
              have h1 := (hc (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self)).2
              rw [← hne] at h1
              scalar_tac
            simp only [bind_tc_ok]
            step as ⟨ j1, hj1 ⟩
            refine WP.spec_mono (ih false j1 (by omega)) ?_
            intro r hr
            rw [hr, hh, decide_eq_false hno]
            simp
          · next hok2 =>
            have hyes : a.val < NN ∧ ee.val < NE := by
              rw [← hnn, ← hne]
              exact ⟨by scalar_tac, by scalar_tac⟩
            simp only [bind_tc_ok]
            step as ⟨ j1, hj1 ⟩
            have hj1' : j1.val = j.val + 1 := by scalar_tac
            have hiff : (∀ b ∈ u.drop j1.val, b.1 < NN ∧ b.2 < NE)
                ↔ (∀ b ∈ u.drop j.val, b.1 < NN ∧ b.2 < NE) := by
              rw [hj1', hcons]
              constructor
              · intro h y hy
                rcases List.mem_cons.mp hy with hy' | hy'
                · rw [hy']; exact hyes
                · exact h y hy'
              · intro h y hy
                exact h y (List.mem_cons_of_mem _ hy)
            refine WP.spec_mono (ih true j1 (by omega)) ?_
            intro r hr
            rw [hr, hh]
            simp only [Bool.true_and]
            exact decide_eq_decide.mpr hiff
      · next hge =>
        have hja : (sts.val[si.val]'hsi).used.val.length ≤ j.val := by scalar_tac
        have hnil : u.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

/-- The `Plan` this loop rebuilds is `q` by structure eta, which is what lets
`group_indexed_spec q ..` match the call the body makes. -/
theorem step_indexed_loop1_spec (q : types.Plan) (si nn ne : Std.Usize)
    (NN NE : Nat) (hnn : nn.val = NN) (hne : ne.val = NE)
    (hsi : si.val < q.steps.val.length) (prod : List (List (Nat × Nat)))
    (hprod : SolverSpec.pairLists (q.steps.val[si.val]'hsi).produced = prod) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), prod.length - j.val ≤ m →
      clauses.step_indexed_loop1 q.endpoints q.givens q.steps si nn ne ok1 j ⦃ r =>
        r = (ok1 && decide (∀ g ∈ prod.drop j.val, ∀ b ∈ g, b.1 < NN ∧ b.2 < NE)) ⦄ := by
  have hpl : prod.length = (q.steps.val[si.val]'hsi).produced.val.length := by
    rw [← hprod, pairLists_length]
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.step_indexed_loop1.eq_def]
    split
    · next hh =>
      rw [vec_index_ok q.steps si hsi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        exfalso
        have hja : j.val < (q.steps.val[si.val]'hsi).produced.val.length := by scalar_tac
        omega
      · next hge =>
        have hja : (q.steps.val[si.val]'hsi).produced.val.length ≤ j.val := by scalar_tac
        have hnil : prod.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.step_indexed_loop1.eq_def]
    split
    · next hh =>
      rw [vec_index_ok q.steps si hsi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hja : j.val < (q.steps.val[si.val]'hsi).produced.val.length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : prod.drop j.val
            = SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[j.val]'hja)
                :: prod.drop (j.val + 1) := by
          rw [← hprod, pairLists_drop_cons _ j.val hja]
        step with group_indexed_spec q si j nn ne NN NE hnn hne hsi hja
          (SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[j.val]'hja)) rfl
          as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : ∀ y ∈ SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[j.val]'hja),
              y.1 < NN ∧ y.2 < NE := of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          have hiff : (∀ g ∈ prod.drop j1.val, ∀ y ∈ g, y.1 < NN ∧ y.2 < NE)
              ↔ (∀ g ∈ prod.drop j.val, ∀ y ∈ g, y.1 < NN ∧ y.2 < NE) := by
            rw [hj1', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hP
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true j1 (by omega)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hbf =>
          have hnP : ¬ (∀ y ∈ SolverSpec.idPairs
              ((q.steps.val[si.val]'hsi).produced.val[j.val]'hja), y.1 < NN ∧ y.2 < NE) :=
            of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hno : ¬ (∀ g ∈ prod.drop j.val, ∀ y ∈ g, y.1 < NN ∧ y.2 < NE) := by
            intro hc
            exact hnP (hc _ (by rw [hcons]; exact List.mem_cons_self))
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by omega)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
      · next hge =>
        have hja : (q.steps.val[si.val]'hsi).produced.val.length ≤ j.val := by scalar_tac
        have hnil : prod.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem step_indexed_spec (q : types.Plan) (si nn ne nt : Std.Usize)
    (NN NE NT : Nat) (hnn : nn.val = NN) (hne : ne.val = NE) (hnt : nt.val = NT)
    (hsi : si.val < q.steps.val.length)
    (sv : SolverSpec.StepView) (hsv : SolverSpec.stepView (q.steps.val[si.val]'hsi) = sv) :
    clauses.step_indexed q si nn ne nt ⦃ r => r = decide (StepOk NN NE NT sv) ⦄ := by
  have hu : SolverSpec.idPairs (q.steps.val[si.val]'hsi).used = sv.used := by
    rw [← hsv]; rfl
  have hprod : SolverSpec.pairLists (q.steps.val[si.val]'hsi).produced = sv.produced := by
    rw [← hsv]; rfl
  have htr : sv.transform = (q.steps.val[si.val]'hsi).transform.val := by
    rw [← hsv]; rfl
  rw [clauses.step_indexed.eq_def, vec_index_ok q.steps si hsi]
  simp only [bind_tc_ok]
  rw [eq_of_spec (step_indexed_loop0_spec q.steps si nn ne NN NE hnn hne hsi sv.used hu
    sv.used.length _ 0#usize (by scalar_tac))]
  simp only [bind_tc_ok]
  refine WP.spec_mono (step_indexed_loop1_spec q si nn ne NN NE hnn hne hsi sv.produced hprod
    sv.produced.length _ 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  have hd1 : sv.used.drop (0#usize).val = sv.used := List.drop_zero
  have hd2 : sv.produced.drop (0#usize).val = sv.produced := List.drop_zero
  refine Bool.eq_iff_iff.mpr ?_
  simp only [Bool.and_eq_true, decide_eq_true_eq, hd1, hd2]
  constructor
  · rintro ⟨⟨h1, h2⟩, h3⟩
    refine ⟨?_, h2, h3⟩
    rw [htr, ← hnt]
    scalar_tac
  · rintro ⟨k1, k2, k3⟩
    rw [htr, ← hnt] at k1
    exact ⟨⟨by scalar_tac, k2⟩, k3⟩

/-! ## The six loops of `cl_indexed` itself -/

theorem cl_indexed_loop0_spec (p : types.Problem) (nn : Std.Usize)
    (NN : Nat) (hnn : nn.val = NN) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), p.given.val.length - i.val ≤ m →
      clauses.cl_indexed_loop0 p.n_props p.nodes p.transforms p.given p.given_tr p.target_tr
        nn ok1 i ⦃ r =>
        r = (ok1 && decide (∀ g ∈ (SolverSpec.givenGroups p).drop i.val, ∀ n ∈ g, n < NN)) ⦄ := by
  have hgl : (SolverSpec.givenGroups p).length = p.given.val.length := idLists_length p.given
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.givenGroups p).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [hgl]; scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < p.given.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hiL : i.val < (SolverSpec.givenGroups p).length := by rw [hgl]; exact hi
        have hcons : (SolverSpec.givenGroups p).drop i.val
            = (((SolverSpec.givenGroups p)[i.val]?).getD [])
                :: (SolverSpec.givenGroups p).drop (i.val + 1) :=
          drop_cons_getD _ [] i.val hiL
        have hL : (p.given.val[i.val]'hi).val.map (fun x => x.val)
            = ((SolverSpec.givenGroups p)[i.val]?).getD [] := by
          simp only [SolverSpec.givenGroups, idLists_getElem?_lt p.given i hi,
            Option.getD_some, SolverSpec.nats]
        step with given_group_indexed_spec p i nn hi NN hnn
          (((SolverSpec.givenGroups p)[i.val]?).getD []) hL as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : ∀ n ∈ ((SolverSpec.givenGroups p)[i.val]?).getD [], n < NN :=
            of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          have hiff : (∀ g ∈ (SolverSpec.givenGroups p).drop i2.val, ∀ n ∈ g, n < NN)
              ↔ (∀ g ∈ (SolverSpec.givenGroups p).drop i.val, ∀ n ∈ g, n < NN) := by
            rw [hi2', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hP
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hbf =>
          have hnP : ¬ (∀ n ∈ ((SolverSpec.givenGroups p)[i.val]?).getD [], n < NN) :=
            of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hno : ¬ (∀ g ∈ (SolverSpec.givenGroups p).drop i.val, ∀ n ∈ g, n < NN) := by
            intro hc
            exact hnP (hc _ (by rw [hcons]; exact List.mem_cons_self))
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
      · have hnil : (SolverSpec.givenGroups p).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [hgl]; scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem cl_indexed_loop1_spec (q : types.Plan) (nn ne : Std.Usize) (NN NE : Nat)
    (hnn : nn.val = NN) (hne : ne.val = NE) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), q.givens.val.length - i.val ≤ m →
      clauses.cl_indexed_loop1 q nn ne ok1 i ⦃ r =>
        r = (q, ok1 && decide (∀ gn ∈ (SolverSpec.givens q).drop i.val,
              gn.1 < NE ∧ gn.2 < NN)) ⦄ := by
  have hgl : (SolverSpec.givens q).length = q.givens.val.length := givens_length q
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop1.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.givens q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [hgl]; scalar_tac)
        exact ok_spec (congrArg (Prod.mk q) (by rw [hh, hnil]; simp))
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (congrArg (Prod.mk q) (by rw [hf]; simp))
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop1.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < q.givens.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        obtain ⟨a, ee, hae⟩ : ∃ a ee, q.givens.val[i.val]'hi = (a, ee) := ⟨_, _, rfl⟩
        have hcons : (SolverSpec.givens q).drop i.val
            = (a.val, ee.val) :: (SolverSpec.givens q).drop (i.val + 1) := by
          rw [givens_drop_cons q i.val hi, hae]
        rw [vec_index_ok q.givens i hi, hae]
        show (do
            let ok2 ← if a ≥ ne then ok false
                      else (if ee ≥ nn then ok false else ok true)
            let i4 ← i + 1#usize
            clauses.cl_indexed_loop1 q nn ne ok2 i4) ⦃ r =>
              r = (q, ok1 && decide (∀ gn ∈ (SolverSpec.givens q).drop i.val,
                    gn.1 < NE ∧ gn.2 < NN)) ⦄
        split
        · next hbad =>
          have hno : ¬ (∀ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 < NE ∧ gn.2 < NN) := by
            intro hc
            have h1 := (hc (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self)).1
            rw [← hne] at h1
            scalar_tac
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
        · next hok1 =>
          split
          · next hbad2 =>
            have hno : ¬ (∀ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 < NE ∧ gn.2 < NN) := by
              intro hc
              have h1 := (hc (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self)).2
              rw [← hnn] at h1
              scalar_tac
            simp only [bind_tc_ok]
            step as ⟨ i2, hi2 ⟩
            refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
            intro r hr
            rw [hr, hh, decide_eq_false hno]
            simp
          · next hok2 =>
            have hyes : a.val < NE ∧ ee.val < NN := by
              rw [← hne, ← hnn]
              exact ⟨by scalar_tac, by scalar_tac⟩
            simp only [bind_tc_ok]
            step as ⟨ i2, hi2 ⟩
            have hi2' : i2.val = i.val + 1 := by scalar_tac
            have hiff : (∀ gn ∈ (SolverSpec.givens q).drop i2.val, gn.1 < NE ∧ gn.2 < NN)
                ↔ (∀ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 < NE ∧ gn.2 < NN) := by
              rw [hi2', hcons]
              constructor
              · intro h y hy
                rcases List.mem_cons.mp hy with hy' | hy'
                · rw [hy']; exact hyes
                · exact h y hy'
              · intro h y hy
                exact h y (List.mem_cons_of_mem _ hy)
            refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
            intro r hr
            rw [hr, hh]
            simp only [Bool.true_and]
            exact congrArg (Prod.mk q) (decide_eq_decide.mpr hiff)
      · have hnil : (SolverSpec.givens q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [hgl]; scalar_tac)
        exact ok_spec (congrArg (Prod.mk q) (by rw [hh, hnil]; simp))
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (congrArg (Prod.mk q) (by rw [hf]; simp))

theorem cl_indexed_loop2_spec (p : types.Problem) (nn : Std.Usize)
    (NN : Nat) (hnn : nn.val = NN) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), nn.val - i.val ≤ m →
      clauses.cl_indexed_loop2 p.n_props p.nodes p.transforms p.given p.given_tr p.target_tr
        nn ok1 i ⦃ r => r = (ok1 && decide (∀ d < NN, i.val ≤ d → NodeOk p d)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop2.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hyes : ∀ d < NN, i.val ≤ d → NodeOk p d := by
          intro d h1 h2; exfalso; scalar_tac
        refine ok_spec ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop2.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < NN := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with node_indexed_spec p i as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : NodeOk p i.val := of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          have hiff : (∀ d < NN, i2.val ≤ d → NodeOk p d)
              ↔ (∀ d < NN, i.val ≤ d → NodeOk p d) := by
            rw [hi2']
            exact ⟨fun h => (ball_range_cons hi).mpr ⟨hP, h⟩,
                   fun h => ((ball_range_cons hi).mp h).2⟩
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hbf =>
          have hnP : ¬ NodeOk p i.val := of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hno : ¬ (∀ d < NN, i.val ≤ d → NodeOk p d) := by
            intro hc
            exact hnP (hc i.val hi (Nat.le_refl _))
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
      · next hge =>
        have hyes : ∀ d < NN, i.val ≤ d → NodeOk p d := by
          intro d h1 h2; exfalso; scalar_tac
        refine ok_spec ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem cl_indexed_loop3_spec (p : types.Problem) (q : types.Plan) (ne : Std.Usize)
    (NE : Nat) (hne : ne.val = NE) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), ne.val - i.val ≤ m →
      clauses.cl_indexed_loop3 p.n_props p.nodes p.transforms p.given p.given_tr p.target_tr
        q ne ok1 i ⦃ r => r = (ok1 && decide (∀ e < NE, i.val ≤ e → EpOk p q e)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop3.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hyes : ∀ e < NE, i.val ≤ e → EpOk p q e := by
          intro e h1 h2; exfalso; scalar_tac
        refine ok_spec ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop3.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < NE := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with endpoint_indexed_spec p q i as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : EpOk p q i.val := of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          have hiff : (∀ e < NE, i2.val ≤ e → EpOk p q e)
              ↔ (∀ e < NE, i.val ≤ e → EpOk p q e) := by
            rw [hi2']
            exact ⟨fun h => (ball_range_cons hi).mpr ⟨hP, h⟩,
                   fun h => ((ball_range_cons hi).mp h).2⟩
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hbf =>
          have hnP : ¬ EpOk p q i.val := of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hno : ¬ (∀ e < NE, i.val ≤ e → EpOk p q e) := by
            intro hc
            exact hnP (hc i.val hi (Nat.le_refl _))
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
      · next hge =>
        have hyes : ∀ e < NE, i.val ≤ e → EpOk p q e := by
          intro e h1 h2; exfalso; scalar_tac
        refine ok_spec ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem cl_indexed_loop4_spec (p : types.Problem) (nn nt : Std.Usize)
    (NN NT : Nat) (hnn : nn.val = NN) (hnt : nt.val = NT) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), nt.val - i.val ≤ m →
      clauses.cl_indexed_loop4 p.n_props p.nodes p.transforms p.given p.given_tr p.target_tr
        nn nt ok1 i ⦃ r => r = (ok1 && decide (∀ t < NT, i.val ≤ t → TrOk p NN t)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop4.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hyes : ∀ t < NT, i.val ≤ t → TrOk p NN t := by
          intro t h1 h2; exfalso; scalar_tac
        refine ok_spec ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop4.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < NT := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with transform_indexed_spec p i nn NN hnn as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : TrOk p NN i.val := of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          have hiff : (∀ t < NT, i2.val ≤ t → TrOk p NN t)
              ↔ (∀ t < NT, i.val ≤ t → TrOk p NN t) := by
            rw [hi2']
            exact ⟨fun h => (ball_range_cons hi).mpr ⟨hP, h⟩,
                   fun h => ((ball_range_cons hi).mp h).2⟩
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hbf =>
          have hnP : ¬ TrOk p NN i.val := of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hno : ¬ (∀ t < NT, i.val ≤ t → TrOk p NN t) := by
            intro hc
            exact hnP (hc i.val hi (Nat.le_refl _))
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
      · next hge =>
        have hyes : ∀ t < NT, i.val ≤ t → TrOk p NN t := by
          intro t h1 h2; exfalso; scalar_tac
        refine ok_spec ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

theorem cl_indexed_loop5_spec (q : types.Plan) (nn ne nt : Std.Usize)
    (NN NE NT : Nat) (hnn : nn.val = NN) (hne : ne.val = NE) (hnt : nt.val = NT) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), q.steps.val.length - i.val ≤ m →
      clauses.cl_indexed_loop5 q nn ne nt ok1 i ⦃ r =>
        r = (ok1 && decide (∀ s ∈ (SolverSpec.steps q).drop i.val, StepOk NN NE NT s)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop5.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.steps q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [steps_length]; scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_indexed_loop5.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hsi : i.val < q.steps.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : (SolverSpec.steps q).drop i.val
            = SolverSpec.stepView (q.steps.val[i.val]'hsi)
                :: (SolverSpec.steps q).drop (i.val + 1) :=
          steps_drop_cons q i.val hsi
        step with step_indexed_spec q i nn ne nt NN NE NT hnn hne hnt hsi
          (SolverSpec.stepView (q.steps.val[i.val]'hsi)) rfl as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : StepOk NN NE NT (SolverSpec.stepView (q.steps.val[i.val]'hsi)) :=
            of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          have hiff : (∀ s ∈ (SolverSpec.steps q).drop i2.val, StepOk NN NE NT s)
              ↔ (∀ s ∈ (SolverSpec.steps q).drop i.val, StepOk NN NE NT s) := by
            rw [hi2', hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hP
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hbf =>
          have hnP : ¬ StepOk NN NE NT (SolverSpec.stepView (q.steps.val[i.val]'hsi)) :=
            of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hno : ¬ (∀ s ∈ (SolverSpec.steps q).drop i.val, StepOk NN NE NT s) := by
            intro hc
            exact hnP (hc _ (by rw [hcons]; exact List.mem_cons_self))
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hno]
          simp
      · have hnil : (SolverSpec.steps q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [steps_length]; scalar_tac)
        exact ok_spec (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_spec (by rw [hf]; simp)

/-- Everything after the two transform-id tests, in one lemma so that the pair
returned by `cl_indexed_loop1` is destructured once rather than once per branch
of that test. -/
theorem cl_indexed_tail_spec (p : types.Problem) (q : types.Plan) (nn ne nt : Std.Usize)
    (hnn : nn.val = SolverSpec.nNodes p) (hne : ne.val = SolverSpec.nEndpoints q)
    (hnt : nt.val = SolverSpec.nTransforms p) (ok1 : Bool) :
    (do
      let ok2 ← clauses.cl_indexed_loop0 p.n_props p.nodes p.transforms p.given p.given_tr
                  p.target_tr nn ok1 0#usize
      let (q1, ok3) ← clauses.cl_indexed_loop1 q nn ne ok2 0#usize
      let ok4 ← clauses.cl_indexed_loop2 p.n_props p.nodes p.transforms p.given p.given_tr
                  p.target_tr nn ok3 0#usize
      let ok5 ← clauses.cl_indexed_loop3 p.n_props p.nodes p.transforms p.given p.given_tr
                  p.target_tr q1 ne ok4 0#usize
      let ok6 ← clauses.cl_indexed_loop4 p.n_props p.nodes p.transforms p.given p.given_tr
                  p.target_tr nn nt ok5 0#usize
      clauses.cl_indexed_loop5 q1 nn ne nt ok6 0#usize) ⦃ r =>
        r = (ok1 && decide (Rest p q)) ⦄ := by
  step with cl_indexed_loop0_spec p nn (SolverSpec.nNodes p) hnn
    p.given.val.length ok1 0#usize (by scalar_tac) as ⟨ ok2, hok2 ⟩
  obtain ⟨v, hv, hvP⟩ := ok_of_spec (cl_indexed_loop1_spec q nn ne
    (SolverSpec.nNodes p) (SolverSpec.nEndpoints q) hnn hne
    q.givens.val.length ok2 0#usize (by scalar_tac))
  obtain ⟨vq, vb⟩ := v
  rw [Prod.mk.injEq] at hvP
  obtain ⟨hv1, hv2⟩ := hvP
  rw [hv]
  show (do
      let ok4 ← clauses.cl_indexed_loop2 p.n_props p.nodes p.transforms p.given p.given_tr
                  p.target_tr nn vb 0#usize
      let ok5 ← clauses.cl_indexed_loop3 p.n_props p.nodes p.transforms p.given p.given_tr
                  p.target_tr vq ne ok4 0#usize
      let ok6 ← clauses.cl_indexed_loop4 p.n_props p.nodes p.transforms p.given p.given_tr
                  p.target_tr nn nt ok5 0#usize
      clauses.cl_indexed_loop5 vq nn ne nt ok6 0#usize) ⦃ r =>
        r = (ok1 && decide (Rest p q)) ⦄
  rw [hv1]
  step with cl_indexed_loop2_spec p nn (SolverSpec.nNodes p) hnn nn.val vb 0#usize
    (by scalar_tac) as ⟨ ok4, hok4 ⟩
  step with cl_indexed_loop3_spec p q ne (SolverSpec.nEndpoints q) hne ne.val ok4 0#usize
    (by scalar_tac) as ⟨ ok5, hok5 ⟩
  step with cl_indexed_loop4_spec p nn nt (SolverSpec.nNodes p) (SolverSpec.nTransforms p)
    hnn hnt nt.val ok5 0#usize (by scalar_tac) as ⟨ ok6, hok6 ⟩
  refine WP.spec_mono (cl_indexed_loop5_spec q nn ne nt (SolverSpec.nNodes p)
    (SolverSpec.nEndpoints q) (SolverSpec.nTransforms p) hnn hne hnt
    q.steps.val.length ok6 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr, hok6, hok5, hok4, hv2, hok2]
  refine Bool.eq_iff_iff.mpr ?_
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  -- Each `drop` is at index zero and each range condition starts at zero, so both
  -- directions are `exact` and never `rw`: `List.drop 0 l` reduces to `l`, and a
  -- rewrite would have to find a `↑0#usize` that `simp` has already normalised in
  -- some of the hypotheses and not in others.
  constructor
  · rintro ⟨⟨⟨⟨⟨⟨h0, h3⟩, h4⟩, h5⟩, h6⟩, h7⟩, h8⟩
    exact ⟨h0, h3, h4, fun d hd => h5 d hd (Nat.zero_le _),
           fun e he => h6 e he (Nat.zero_le _), fun t ht => h7 t ht (Nat.zero_le _), h8⟩
  · rintro ⟨h0, h3, h4, h5, h6, h7, h8⟩
    exact ⟨⟨⟨⟨⟨⟨h0, h3⟩, h4⟩, fun d hd _ => h5 d hd⟩, fun e he _ => h6 e he⟩,
             fun t ht _ => h7 t ht⟩, h8⟩

end Indexed

open Indexed

/-! ## The clause

The only declaration in this file at `SolverProof`, where the composition looks
for it. Everything it rests on is in `SolverProof.Indexed`. -/

/-- The `indexed` conjunct of `SolverSpec.ValidC`, UNCONDITIONALLY and exactly.

No hypothesis, and none is available: the composition discharges the four
conditional clause lemmas by case-splitting on this one, so anything assumed
here would be assumed by all of them. See the module header for why the two
mechanisms that made `shape`, `derived`, `conformance` and `emission`
conditional cannot arise in this clause. -/
theorem cl_indexed_spec (p : types.Problem) (q : types.Plan) :
    clauses.cl_indexed p q ⦃ r => r = decide (SolverSpec.WellIndexed p q) ⦄ := by
  have hTT : SolverSpec.targetTr p = p.target_tr.val := rfl
  have hGT : SolverSpec.givenTr p = p.given_tr.val := rfl
  rw [clauses.cl_indexed.eq_def]
  step as ⟨ nn, hnn ⟩
  step as ⟨ ne, hne ⟩
  step as ⟨ nt, hnt ⟩
  split
  · next htt =>
    have hP1 : SolverSpec.targetTr p < SolverSpec.nTransforms p := by
      rw [hTT, ← hnt]; scalar_tac
    simp only [bind_tc_ok]
    refine WP.spec_mono (cl_indexed_tail_spec p q nn ne nt hnn hne hnt _) ?_
    intro r hr
    rw [hr]
    refine Bool.eq_iff_iff.mpr ?_
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    constructor
    · rintro ⟨hg, k3, k4, k5, k6, k7, k8⟩
      have hP2 : SolverSpec.givenTr p < SolverSpec.nTransforms p := by
        rw [hGT, ← hnt]; scalar_tac
      exact ⟨hP1, hP2, k3, k4, fun d hd => (k5 d hd).1, fun e he => (k6 e he).1,
             fun d hd => (k5 d hd).2, fun e he => (k6 e he).2, k7, k8⟩
    · rintro ⟨-, m2, m3, m4, m5a, m6a, m5b, m6b, m7, m8⟩
      rw [hGT, ← hnt] at m2
      exact ⟨by scalar_tac, m3, m4, fun d hd => ⟨m5a d hd, m5b d hd⟩,
             fun e he => ⟨m6a e he, m6b e he⟩, m7, m8⟩
  · next htt =>
    have hnP1 : ¬ (SolverSpec.targetTr p < SolverSpec.nTransforms p) := by
      rw [hTT, ← hnt]
      intro hc
      exact htt (by scalar_tac)
    simp only [bind_tc_ok]
    refine WP.spec_mono (cl_indexed_tail_spec p q nn ne nt hnn hne hnt false) ?_
    intro r hr
    rw [hr]
    simp only [Bool.false_and]
    exact (decide_eq_false (fun hc => hnP1 hc.1)).symm

#print axioms SolverProof.cl_indexed_spec
#print axioms SolverProof.Indexed.cl_indexed_tail_spec
#print axioms SolverProof.Indexed.step_indexed_spec
#print axioms SolverProof.Indexed.transform_indexed_spec
#print axioms SolverProof.Indexed.node_indexed_spec
#print axioms SolverProof.Indexed.endpoint_indexed_spec
#print axioms SolverProof.Indexed.given_group_indexed_spec

end SolverProof
