/-
  `conformance` and `emission` -- the two clauses that read lineage.

  Read `Proof/Basis.lean` for the loop recipe and `Proof/Ancestry.lean` for the
  ancestry table this file consumes rather than re-derives. Both clauses reduce
  to one pointwise judgement, `clauses::satisfies`, so that judgement is proved
  once here and the two clause loops never see the three loops underneath it.

  ## What `satisfies` actually decides, and why it is not the specification's
  `Satisfies`

  `descends_spec` pins the checker's ancestry query as
  `decide (e < n) && ancestorB q e f`: the table is `n` rows tall and an endpoint
  at or beyond it descends from nothing, while the specification's `ancestorB` is
  REFLEXIVE at every index, table or no table. That guard does not cancel further
  up. A step that binds an out-of-range endpoint `e` to a slot whose anchor is
  bound to that same `e` satisfies the specification's conjunct by pure
  reflexivity and is rejected by the checker -- so the two disagree, and no
  amount of proof effort will make an unconditional clause lemma true.
  `conformance_clause_needs_indexed` at the bottom of this file is that
  disagreement as a theorem rather than as a claim, on a plan three rows long:

      nodes    = [ { props := [], parents := [0] } ]
      endpoints= []
      steps    = [ { used := [(0, 0)], produced := [] } ]

  `Satisfies p q [(0,0)] 0 0` holds -- `epProps q 0` is empty out of range, so
  `IsA` is vacuous, and `ancestorB q 0 0` is reflexivity -- while
  `cl_conformance` returns `false`, because `descends` reads row 0 of a table
  with no rows.

  `check` is not wrong on that plan: `cl_indexed` rejects it and the conjunction
  agrees. What is wrong is the assumption that the ten clause lemmas are
  INDEPENDENT. The specification says so itself, in `WellIndexed`'s own
  docstring -- "the range conditions stop the specification being vacuous" -- and
  a conjunct that is vacuous off the table cannot be equivalent to a checker that
  fails closed there.

  So each clause is proved TWICE, and both are audited:

  - `cl_conformance_spec_exact` / `cl_emission_spec_exact` carry no hypothesis
    beyond the pair describing the table argument, and pin the return value
    against `SatisfiesN`, which is `Satisfies` with the table's own range guard
    written in. This is what the checker computes, exactly, on every input.
  - `cl_conformance_spec` / `cl_emission_spec` are the forms the obligation
    wants, stated against `SolverSpec.Satisfies`, and each carries the range
    conjunct of `WellIndexed` that the two forms need to coincide. The final
    composition discharges it from `cl_indexed` exactly as it discharges `RowsOk`
    from `ancestors_spec` -- on the branch where `cl_indexed` fails, both sides
    of `check_spec` are `false` and neither clause is consulted.

  ## Two things the recipe does not say

  **A sentinel that lives in the value space needs a lemma, not a case split.**
  `access::bound_to` returns `NONE = usize::MAX` for an unbound slot, and the
  specification's `boundTo` returns `none`. Those are the same answer only
  because a `Vec` is no longer than `usize::MAX`, so no endpoint id can BE the
  sentinel. `anchorOkN` therefore carries `f ≠ NONE` as a conjunct rather than
  assuming it away, and `satisfiesN_iff` is where the length bound retires it.

  **The `used` list is the whole binding list, not the suffix being scanned.**
  Every loop below scans a suffix, but `satisfies` is called with the step's
  ENTIRE `used` slice at every index -- an anchor resolves against bindings the
  scan has already passed. Hoisting it to a parameter (`ul`) with its defining
  equation as a hypothesis is what keeps the two apart, and is also what keeps a
  `Vec` projection out of the `decide` in the postcondition, per `Proof/Steps`'
  first trap.

  **A table-shaped hypothesis costs nothing going down and everything coming
  back up.** `(anc, n, hlen, hok)` is threaded through five nested loops here
  purely as a parameter block: no loop reads the table, only `descends` does, so
  nothing about the invariants changes with depth. What that buys is the reason
  to hoist EVERY list the postcondition mentions to a parameter as well. Each
  wrapper (`cl_conformance_step_spec`, `cl_emission_group_spec`,
  `cl_emission_step_spec`) exists so the level above it names the list with a
  `rfl` and never has to rewrite one under a `decide`.

  Two smaller costs, both paid twice:

  - **The outer loops have nothing to `split` on.** `cl_conformance_loop0` binds
    the inner loop's result straight into the recursive call -- the `if ok then
    true else false` that every OTHER loop in the witness carries is absent
    there, because the value is already a `Bool`. `split` then fails with "could
    not split an `if` or `match`", naming the goal rather than the missing
    branch. Pass the literal to the induction hypothesis with `ih _` and finish
    with `decide_and_eq`.
  - **`scalar_tac` reads the whole context.** In the branch where an anchor is
    unbound, the hypotheses carry `boundTo`, the sentinel and the guarded
    judgement, and `scalar_tac` exhausts `maxRecDepth` inside its own `simp`
    while proving `na - i ≤ m`. `clear` the irrelevant hypotheses first, or
    prove the fact in an empty context and apply it -- which is all
    `usize_eq_of_val` is.
-/

import SolverWitness.Types
import SolverWitness.Funs
import SolverWitness.Spec
import SolverWitness.Proof.Basis
import SolverWitness.Proof.Bits
import SolverWitness.Proof.Access
import SolverWitness.Proof.Ancestry

set_option maxHeartbeats 2000000
-- `scalar_tac` preprocesses every hypothesis in scope, and the branches below
-- carry the whole guarded judgement in theirs. The default depth is reached
-- inside its `simp`, and the error names recursion rather than the context.
set_option maxRecDepth 8000
set_option synthInstance.maxSize 1000
set_option synthInstance.maxHeartbeats 4000000

namespace SolverProof

open Aeneas Aeneas.Std Result
open solver_witness

namespace Lineage

/-! ## Bridges

Local copies. Nothing here imports a sibling clause module, so the list lemmas
`Proof/Steps.lean` also needs are restated rather than shared. -/

theorem ok_holds {α} {x : α} {P : α → Prop} (h : P x) : (ok x : Result α) ⦃ r => P r ⦄ := by
  simp only [WP.spec_ok]
  exact h

/-- Usize equality, decided on the value. Proved here, in an empty context: the
branches that need it carry the whole guarded judgement in theirs, and
`scalar_tac` preprocesses every hypothesis it can see. -/
theorem usize_eq_of_val {x y : Std.Usize} (h : x.val = y.val) : x = y := by
  scalar_tac

theorem decide_and_eq {P Q R : Prop} [Decidable P] [Decidable Q] [Decidable R]
    (h : R ↔ (P ∧ Q)) : decide R = (decide P && decide Q) := by
  by_cases hp : P
  · by_cases hq : Q
    · rw [decide_eq_true (h.mpr ⟨hp, hq⟩)]
      simp [hp, hq]
    · rw [decide_eq_false (fun hc => hq (h.mp hc).2)]
      simp [hq]
  · rw [decide_eq_false (fun hc => hp (h.mp hc).1)]
    simp [hp]

/-- The specification's view of a `Slice` of bindings. `Slice` and
`alloc.vec.Vec` are two subtypes over one list, and `deref` is the identity on
it, so this and `SolverSpec.idPairs` are the same function. -/
def pairsOf (s : Slice (Std.Usize × Std.Usize)) : List (Nat × Nat) :=
  s.val.map (fun b => (b.1.val, b.2.val))

theorem pairsOf_deref (v : alloc.vec.Vec (Std.Usize × Std.Usize)) :
    pairsOf (alloc.vec.Vec.deref v) = SolverSpec.idPairs v := rfl

theorem pairsOf_length (s : Slice (Std.Usize × Std.Usize)) :
    (pairsOf s).length = s.val.length := by
  simp [pairsOf]

theorem pairsOf_drop_cons (s : Slice (Std.Usize × Std.Usize)) (i : Nat)
    (h : i < s.val.length) :
    (pairsOf s).drop i
      = ((s.val[i]'h).1.val, (s.val[i]'h).2.val) :: (pairsOf s).drop (i + 1) := by
  have h' : i < (pairsOf s).length := by rw [pairsOf_length]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  exact ⟨by simp [pairsOf], rfl⟩

theorem idPairs_len (v : alloc.vec.Vec (Std.Usize × Std.Usize)) :
    (SolverSpec.idPairs v).length = v.val.length := by
  simp [SolverSpec.idPairs]

theorem idPairs_cons (v : alloc.vec.Vec (Std.Usize × Std.Usize)) (i : Nat)
    (h : i < v.val.length) :
    (SolverSpec.idPairs v).drop i
      = ((v.val[i]'h).1.val, (v.val[i]'h).2.val)
          :: (SolverSpec.idPairs v).drop (i + 1) := by
  have h' : i < (SolverSpec.idPairs v).length := by rw [idPairs_len]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  exact ⟨by simp [SolverSpec.idPairs], rfl⟩

theorem pairLists_len (v : alloc.vec.Vec (alloc.vec.Vec (Std.Usize × Std.Usize))) :
    (SolverSpec.pairLists v).length = v.val.length := by
  simp [SolverSpec.pairLists]

theorem pairLists_cons (v : alloc.vec.Vec (alloc.vec.Vec (Std.Usize × Std.Usize)))
    (i : Nat) (h : i < v.val.length) :
    (SolverSpec.pairLists v).drop i
      = SolverSpec.idPairs (v.val[i]'h) :: (SolverSpec.pairLists v).drop (i + 1) := by
  have h' : i < (SolverSpec.pairLists v).length := by rw [pairLists_len]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  exact ⟨by simp [SolverSpec.pairLists], rfl⟩

theorem steps_len (q : types.Plan) :
    (SolverSpec.steps q).length = q.steps.val.length := by
  simp [SolverSpec.steps]

theorem steps_cons (q : types.Plan) (i : Nat) (h : i < q.steps.val.length) :
    (SolverSpec.steps q).drop i
      = SolverSpec.stepView (q.steps.val[i]'h) :: (SolverSpec.steps q).drop (i + 1) := by
  have h' : i < (SolverSpec.steps q).length := by rw [steps_len]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  exact ⟨by simp [SolverSpec.steps], rfl⟩

/-! ## The guarded judgement

`SatisfiesN` is `SolverSpec.Satisfies` with the two facts the checker's
representation adds: an anchor resolved to the sentinel is unbound, and the
ancestry table is only `n` rows tall. -/

/-- One anchor, as the checker decides it. -/
def anchorOkN (q : types.Plan) (n : Nat) (used : List (Nat × Nat)) (e a : Nat) : Bool :=
  match SolverSpec.boundTo used a with
  | some f => decide (f ≠ access.NONE.val ∧ e < n) && SolverSpec.ancestorB q e f
  | none => false

@[reducible] def SatisfiesN (p : types.Problem) (q : types.Plan) (n : Nat)
    (used : List (Nat × Nat)) (e d : Nat) : Prop :=
  SolverSpec.IsA (SolverSpec.epProps q e) (SolverSpec.nodeProps p d) ∧
  ∀ a ∈ SolverSpec.nodeParents p d, anchorOkN q n used e a = true

theorem anchorOkN_none (q : types.Plan) (n : Nat) (ul : List (Nat × Nat)) (e a : Nat)
    (h : (SolverSpec.boundTo ul a).getD access.NONE.val = access.NONE.val) :
    anchorOkN q n ul e a = false := by
  unfold anchorOkN
  cases hb : SolverSpec.boundTo ul a with
  | none => rfl
  | some v =>
    rw [hb] at h
    simp only [Option.getD_some] at h
    simp [h]

theorem anchorOkN_some (q : types.Plan) (n : Nat) (ul : List (Nat × Nat)) (e a fv : Nat)
    (hfv : fv = (SolverSpec.boundTo ul a).getD access.NONE.val)
    (hne : ¬ (fv = access.NONE.val)) :
    anchorOkN q n ul e a = (decide (e < n) && SolverSpec.ancestorB q e fv) := by
  unfold anchorOkN
  cases hb : SolverSpec.boundTo ul a with
  | none =>
    rw [hb] at hfv
    simp only [Option.getD_none] at hfv
    exact absurd hfv hne
  | some v =>
    rw [hb] at hfv
    simp only [Option.getD_some] at hfv
    subst hfv
    simp [hne]

/-- The guarded judgement is the specification's, on an endpoint the table
covers. `hn` is what retires the sentinel: a table no taller than `usize::MAX`
cannot contain the id the checker reserves for "unbound". -/
theorem satisfiesN_iff (p : types.Problem) (q : types.Plan) (n : Nat)
    (ul : List (Nat × Nat)) (e d : Nat) (he : e < n) (hn : n ≤ access.NONE.val) :
    SatisfiesN p q n ul e d ↔ SolverSpec.Satisfies p q ul e d := by
  unfold SatisfiesN SolverSpec.Satisfies
  refine and_congr Iff.rfl ?_
  refine forall_congr' (fun a => ?_)
  refine imp_congr Iff.rfl ?_
  unfold anchorOkN SolverSpec.anchorOk
  cases hb : SolverSpec.boundTo ul a with
  | none => simp
  | some v =>
    by_cases hv : v = access.NONE.val
    · have hz : SolverSpec.ancestorB q e v = false := by
        rw [hv]
        exact ancestorB_of_le q n access.NONE.val hn e he
      simp [hz]
    · simp [hv, he]

end Lineage

open Lineage

/-! ## `access::bound_to` -- the one accessor with a loop -/

theorem bound_to_loop_spec (used : Slice (Std.Usize × Std.Usize))
    (ul : List (Nat × Nat)) (hul : pairsOf used = ul) (a : Std.Usize) :
    ∀ (m : Nat) (i : Std.Usize), ul.length - i.val ≤ m →
      access.bound_to_loop used a i ⦃ r =>
        r.val = (SolverSpec.boundTo (ul.drop i.val) a.val).getD access.NONE.val ⦄ := by
  have hlen : ul.length = used.val.length := by rw [← hul, pairsOf_length]
  intro m
  induction m with
  | zero =>
    intro i hk
    rw [access.bound_to_loop.eq_def]
    dsimp only
    split
    · exfalso; scalar_tac
    · have hnil : ul.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
      refine ok_holds ?_
      rw [hnil]
      rfl
  | succ m ih =>
    intro i hk
    rw [access.bound_to_loop.eq_def]
    dsimp only
    split
    · next hlt =>
      have hi : i.val < used.val.length := by scalar_tac
      obtain ⟨x, y, hxy⟩ : ∃ x y, used.val[i.val]'hi = (x, y) := ⟨_, _, rfl⟩
      have hcons : ul.drop i.val = (x.val, y.val) :: ul.drop (i.val + 1) := by
        rw [← hul, pairsOf_drop_cons used i.val hi, hxy]
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      rw [eq_of_spec (Slice.index_usize_spec used i (by scalar_tac)), hxy]
      show (if x = a then ok y
            else do
              let i4 ← i + 1#usize
              access.bound_to_loop used a i4) ⦃ r =>
              r.val = (SolverSpec.boundTo (ul.drop i.val) a.val).getD access.NONE.val ⦄
      split
      · next heq =>
        have hxa : x.val = a.val := by rw [heq]
        have hb : SolverSpec.boundTo (ul.drop i.val) a.val = some y.val := by
          rw [hcons]
          simp [SolverSpec.boundTo, hxa]
        refine ok_holds ?_
        rw [hb]
        rfl
      · next hne =>
        have hxa : ¬ (x.val = a.val) := by
          intro hc; exact hne (by scalar_tac)
        have hb : SolverSpec.boundTo (ul.drop i.val) a.val
            = SolverSpec.boundTo (ul.drop (i.val + 1)) a.val := by
          rw [hcons]
          simp [SolverSpec.boundTo, hxa]
        step as ⟨ i4, hi4 ⟩
        have hi4' : i4.val = i.val + 1 := by scalar_tac
        refine WP.spec_mono (ih i4 (by scalar_tac)) ?_
        intro r hr
        rw [hr, hi4', ← hb]
    · have hnil : ul.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
      refine ok_holds ?_
      rw [hnil]
      rfl

/-- What the step bound to slot `a`, with the sentinel for "nothing". -/
theorem bound_to_spec (used : Slice (Std.Usize × Std.Usize))
    (ul : List (Nat × Nat)) (hul : pairsOf used = ul) (a : Std.Usize) :
    access.bound_to used a ⦃ r =>
      r.val = (SolverSpec.boundTo ul a.val).getD access.NONE.val ⦄ := by
  refine WP.spec_mono (bound_to_loop_spec used ul hul a ul.length 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp

/-! ## `carries` -- properties only -/

theorem carries_inner_spec (q : types.Plan) (e want nhave : Std.Usize)
    (hnh : nhave.val = (SolverSpec.epProps q e.val).length) :
    ∀ (m : Nat) (hit : Bool) (j : Std.Usize), nhave.val - j.val ≤ m →
      clauses.carries_loop0_loop0 q e nhave want hit j ⦃ r =>
        r = (hit || decide (want.val ∈ (SolverSpec.epProps q e.val).drop j.val)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro hit j hk
    rw [clauses.carries_loop0_loop0.eq_def]
    split
    · next hh => exact ok_holds (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.epProps q e.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        refine ok_holds ?_
        rw [hhf, hnil]
        simp
  | succ m ih =>
    intro hit j hk
    rw [clauses.carries_loop0_loop0.eq_def]
    split
    · next hh => exact ok_holds (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · next hlt =>
        have hj : j.val < (SolverSpec.epProps q e.val).length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        step with ep_prop_spec q e j as ⟨ x, hx ⟩
        have hxv : x.val = (SolverSpec.epProps q e.val)[j.val]'hj := by
          rw [hx, List.getElem?_eq_getElem hj]
          rfl
        have hcons : (SolverSpec.epProps q e.val).drop j.val
            = x.val :: (SolverSpec.epProps q e.val).drop (j.val + 1) := by
          rw [hxv]
          exact List.drop_eq_getElem_cons hj
        split
        · next heq =>
          have hyes : want.val ∈ (SolverSpec.epProps q e.val).drop j.val := by
            rw [hcons, ← heq]
            exact List.mem_cons_self
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf, decide_eq_true hyes]
          simp
        · next hne =>
          have hxa : ¬ (x.val = want.val) := by
            intro hc; exact hne (by scalar_tac)
          have hiff : (want.val ∈ (SolverSpec.epProps q e.val).drop (j.val + 1))
              ↔ (want.val ∈ (SolverSpec.epProps q e.val).drop j.val) := by
            rw [hcons]
            constructor
            · intro h; exact List.mem_cons_of_mem _ h
            · intro h
              rcases List.mem_cons.mp h with h' | h'
              · exact absurd h'.symm hxa
              · exact h'
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf, hj1']
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.epProps q e.val).drop j.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        refine ok_holds ?_
        rw [hhf, hnil]
        simp

theorem carries_hit_spec (q : types.Plan) (e want nhave : Std.Usize)
    (hnh : nhave.val = (SolverSpec.epProps q e.val).length) :
    clauses.carries_loop0_loop0 q e nhave want false 0#usize ⦃ r =>
      r = decide (want.val ∈ SolverSpec.epProps q e.val) ⦄ := by
  refine WP.spec_mono
    (carries_inner_spec q e want nhave hnh nhave.val false 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp

theorem carries_outer_spec (p : types.Problem) (q : types.Plan) (e d nwant nhave : Std.Usize)
    (hnw : nwant.val = (SolverSpec.nodeProps p d.val).length)
    (hnh : nhave.val = (SolverSpec.epProps q e.val).length) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), nwant.val - i.val ≤ m →
      clauses.carries_loop0 p q e d nwant nhave ok1 i ⦃ r =>
        r = (ok1 && decide (∀ x ∈ (SolverSpec.nodeProps p d.val).drop i.val,
                              x ∈ SolverSpec.epProps q e.val)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.carries_loop0.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.nodeProps p d.val).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ x ∈ (SolverSpec.nodeProps p d.val).drop i.val,
            x ∈ SolverSpec.epProps q e.val := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.carries_loop0.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < (SolverSpec.nodeProps p d.val).length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with node_prop_spec p d i as ⟨ want, hwant ⟩
        have hwv : want.val = (SolverSpec.nodeProps p d.val)[i.val]'hi := by
          rw [hwant, List.getElem?_eq_getElem hi]
          rfl
        have hcons : (SolverSpec.nodeProps p d.val).drop i.val
            = want.val :: (SolverSpec.nodeProps p d.val).drop (i.val + 1) := by
          rw [hwv]
          exact List.drop_eq_getElem_cons hi
        rw [eq_of_spec (carries_hit_spec q e want nhave hnh)]
        simp only [bind_tc_ok]
        split
        · next hhit =>
          have hin : want.val ∈ SolverSpec.epProps q e.val := of_decide_eq_true hhit
          step as ⟨ i1, hi1 ⟩
          have hiff : (∀ x ∈ (SolverSpec.nodeProps p d.val).drop i1.val,
                          x ∈ SolverSpec.epProps q e.val)
              ↔ (∀ x ∈ (SolverSpec.nodeProps p d.val).drop i.val,
                          x ∈ SolverSpec.epProps q e.val) := by
            rw [hi1, hcons]
            constructor
            · intro h x hx
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx']; exact hin
              · exact h x hx'
            · intro h x hx
              exact h x (List.mem_cons_of_mem _ hx)
          refine WP.spec_mono (ih true i1 (by scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hhit =>
          have hin : ¬ (want.val ∈ SolverSpec.epProps q e.val) :=
            fun hc => hhit (decide_eq_true hc)
          have hno : ¬ (∀ x ∈ (SolverSpec.nodeProps p d.val).drop i.val,
                          x ∈ SolverSpec.epProps q e.val) := by
            intro h
            exact hin (h want.val (by rw [hcons]; exact List.mem_cons_self))
          step as ⟨ i1, hi1 ⟩
          refine WP.spec_mono (ih false i1 (by scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hno).symm
      · have hnil : (SolverSpec.nodeProps p d.val).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ x ∈ (SolverSpec.nodeProps p d.val).drop i.val,
            x ∈ SolverSpec.epProps q e.val := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)

/-- Properties, and nothing else: the endpoint's are a superset of the slot's. -/
theorem carries_spec (p : types.Problem) (q : types.Plan) (e d : Std.Usize) :
    clauses.carries p q e d ⦃ r =>
      r = decide (SolverSpec.IsA (SolverSpec.epProps q e.val)
                                 (SolverSpec.nodeProps p d.val)) ⦄ := by
  rw [clauses.carries.eq_def]
  step with node_nprops_spec p d as ⟨ nwant, hnw ⟩
  step with ep_nprops_spec q e as ⟨ nhave, hnh ⟩
  refine WP.spec_mono
    (carries_outer_spec p q e d nwant nhave hnw hnh nwant.val true 0#usize
      (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  simp [SolverSpec.IsA]

/-! ## `satisfies` -- properties and lineage together -/

theorem satisfies_loop_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val)
    (used : Slice (Std.Usize × Std.Usize)) (ul : List (Nat × Nat)) (hul : pairsOf used = ul)
    (e d na : Std.Usize) (hna : na.val = (SolverSpec.nodeParents p d.val).length) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), na.val - i.val ≤ m →
      clauses.satisfies_loop p anc used e d ok1 na i ⦃ r =>
        r = (ok1 && decide (∀ a ∈ (SolverSpec.nodeParents p d.val).drop i.val,
                              anchorOkN q n ul e.val a = true)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.satisfies_loop.eq_def]
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.nodeParents p d.val).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ a ∈ (SolverSpec.nodeParents p d.val).drop i.val,
            anchorOkN q n ul e.val a = true := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.satisfies_loop.eq_def]
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < (SolverSpec.nodeParents p d.val).length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with node_parent_spec p d i as ⟨ a, ha ⟩
        have hav : a.val = (SolverSpec.nodeParents p d.val)[i.val]'hi := by
          rw [ha, List.getElem?_eq_getElem hi]
          rfl
        have hcons : (SolverSpec.nodeParents p d.val).drop i.val
            = a.val :: (SolverSpec.nodeParents p d.val).drop (i.val + 1) := by
          rw [hav]
          exact List.drop_eq_getElem_cons hi
        step with bound_to_spec used ul hul a as ⟨ f, hf ⟩
        split
        · next hfn =>
          have hfv : (SolverSpec.boundTo ul a.val).getD access.NONE.val = access.NONE.val := by
            rw [← hf, hfn]
          have hbad : anchorOkN q n ul e.val a.val = false := anchorOkN_none q n ul e.val a.val hfv
          have hno : ¬ (∀ x ∈ (SolverSpec.nodeParents p d.val).drop i.val,
                          anchorOkN q n ul e.val x = true) := by
            intro hcon
            have := hcon a.val (by rw [hcons]; exact List.mem_cons_self)
            rw [hbad] at this
            exact Bool.noConfusion this
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by clear hno hbad hfv hfn hf; scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hno).symm
        · next hfn =>
          have hfne : ¬ (f.val = access.NONE.val) := fun hc => hfn (usize_eq_of_val hc)
          have hval : anchorOkN q n ul e.val a.val
              = (decide (e.val < n) && SolverSpec.ancestorB q e.val f.val) :=
            anchorOkN_some q n ul e.val a.val f.val hf hfne
          rw [eq_of_spec (descends_spec q anc n hlen hok e f)]
          simp only [bind_tc_ok]
          split
          · next hd =>
            have hgood : anchorOkN q n ul e.val a.val = true := by rw [hval, hd]
            step as ⟨ i2, hi2 ⟩
            have hiff : (∀ x ∈ (SolverSpec.nodeParents p d.val).drop i2.val,
                            anchorOkN q n ul e.val x = true)
                ↔ (∀ x ∈ (SolverSpec.nodeParents p d.val).drop i.val,
                            anchorOkN q n ul e.val x = true) := by
              rw [hi2, hcons]
              constructor
              · intro hc x hx
                rcases List.mem_cons.mp hx with hx' | hx'
                · rw [hx']; exact hgood
                · exact hc x hx'
              · intro hc x hx
                exact hc x (List.mem_cons_of_mem _ hx)
            refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh]
            simp only [Bool.true_and]
            exact decide_eq_decide.mpr hiff
          · next hd =>
            have hbad : anchorOkN q n ul e.val a.val = false := by
              rw [hval]
              simpa using hd
            have hno : ¬ (∀ x ∈ (SolverSpec.nodeParents p d.val).drop i.val,
                            anchorOkN q n ul e.val x = true) := by
              intro hcon
              have := hcon a.val (by rw [hcons]; exact List.mem_cons_self)
              rw [hbad] at this
              exact Bool.noConfusion this
            step as ⟨ i2, hi2 ⟩
            refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh]
            simp only [Bool.false_and, Bool.true_and]
            exact (decide_eq_false hno).symm
      · have hnil : (SolverSpec.nodeParents p d.val).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ x ∈ (SolverSpec.nodeParents p d.val).drop i.val,
            anchorOkN q n ul e.val x = true := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)

/-- May endpoint `e` fill slot `d`, in a step whose bindings are `ul`? Exactly
what the checker decides, on every input. -/
theorem satisfies_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val)
    (used : Slice (Std.Usize × Std.Usize)) (ul : List (Nat × Nat)) (hul : pairsOf used = ul)
    (e d : Std.Usize) :
    clauses.satisfies p q anc used e d ⦃ r =>
      r = decide (SatisfiesN p q n ul e.val d.val) ⦄ := by
  rw [clauses.satisfies.eq_def]
  rw [eq_of_spec (carries_spec p q e d)]
  simp only [bind_tc_ok]
  step with node_nparents_spec p d as ⟨ na, hna ⟩
  refine WP.spec_mono
    (satisfies_loop_spec p q anc n hlen hok used ul hul e d na hna na.val
      (decide (SolverSpec.IsA (SolverSpec.epProps q e.val) (SolverSpec.nodeProps p d.val)))
      0#usize (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  have hd0 : (SolverSpec.nodeParents p d.val).drop (0#usize).val
      = SolverSpec.nodeParents p d.val := List.drop_zero
  rw [hd0]
  refine (decide_and_eq ?_).symm
  unfold SatisfiesN
  constructor
  · rintro ⟨h1, h2⟩; exact ⟨h1, h2⟩
  · rintro ⟨h1, h2⟩; exact ⟨h1, h2⟩


/-! ## 2. conformance -/

/-- One binding of one step. The endpoint and the slot are PARAMETERS: they
appear under `decide`, where no rewrite reaches. -/
theorem conformance_at_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) (si bi : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (hbi : bi.val < (q.steps.val[si.val]'hsi).used.val.length)
    (ul : List (Nat × Nat))
    (hul : SolverSpec.idPairs (q.steps.val[si.val]'hsi).used = ul)
    (a ee : Std.Usize)
    (hab : (q.steps.val[si.val]'hsi).used.val[bi.val]'hbi = (a, ee)) :
    clauses.conformance_at p q anc si bi ⦃ r =>
      r = decide (SatisfiesN p q n ul ee.val a.val) ⦄ := by
  rw [clauses.conformance_at.eq_def]
  dsimp only
  split
  · next h1 =>
    rw [index_eq q.steps si hsi]
    simp only [bind_tc_ok]
    split
    · next h2 =>
      rw [index_eq _ bi hbi, hab]
      show clauses.satisfies p q anc
          (alloc.vec.Vec.deref (q.steps.val[si.val]'hsi).used) ee a ⦃ r =>
            r = decide (SatisfiesN p q n ul ee.val a.val) ⦄
      exact satisfies_spec p q anc n hlen hok
        (alloc.vec.Vec.deref (q.steps.val[si.val]'hsi).used) ul
        (by rw [pairsOf_deref]; exact hul) ee a
    · next h2 => exfalso; scalar_tac
  · next h1 => exfalso; scalar_tac

theorem cl_conformance_inner_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) (i : Std.Usize) (hi : i.val < q.steps.val.length)
    (ul : List (Nat × Nat))
    (hul : SolverSpec.idPairs (q.steps.val[i.val]'hi).used = ul) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), ul.length - j.val ≤ m →
      clauses.cl_conformance_loop0_loop0 p q.endpoints q.givens q.steps anc ok1 i j ⦃ r =>
        r = (ok1 && decide (∀ b ∈ ul.drop j.val, SatisfiesN p q n ul b.2 b.1)) ⦄ := by
  have hlu : ul.length = (q.steps.val[i.val]'hi).used.val.length := by
    rw [← hul, idPairs_len]
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.cl_conformance_loop0_loop0.eq_def]
    split
    · next hh =>
      rw [index_eq q.steps i hi]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : ul.drop j.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ b ∈ ul.drop j.val, SatisfiesN p q n ul b.2 b.1 := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.cl_conformance_loop0_loop0.eq_def]
    split
    · next hh =>
      rw [index_eq q.steps i hi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hj : j.val < (q.steps.val[i.val]'hi).used.val.length := by scalar_tac
        obtain ⟨a, ee, hab⟩ :
            ∃ a ee, (q.steps.val[i.val]'hi).used.val[j.val]'hj = (a, ee) := ⟨_, _, rfl⟩
        have hcons : ul.drop j.val = (a.val, ee.val) :: ul.drop (j.val + 1) := by
          rw [← hul, idPairs_cons _ j.val hj, hab]
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        rw [eq_of_spec (conformance_at_spec p q anc n hlen hok i j hi hj ul hul a ee hab)]
        simp only [bind_tc_ok]
        split
        · next hb =>
          have hS : SatisfiesN p q n ul ee.val a.val := of_decide_eq_true hb
          step as ⟨ j1, hj1 ⟩
          have hiff : (∀ b ∈ ul.drop j1.val, SatisfiesN p q n ul b.2 b.1)
              ↔ (∀ b ∈ ul.drop j.val, SatisfiesN p q n ul b.2 b.1) := by
            rw [hj1, hcons]
            constructor
            · intro hc x hx
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx']; exact hS
              · exact hc x hx'
            · intro hc x hx
              exact hc x (List.mem_cons_of_mem _ hx)
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hb =>
          have hS : ¬ SatisfiesN p q n ul ee.val a.val := fun hc => hb (decide_eq_true hc)
          have hno : ¬ (∀ x ∈ ul.drop j.val, SatisfiesN p q n ul x.2 x.1) := by
            intro hc
            exact hS (hc (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self))
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hno).symm
      · have hnil : ul.drop j.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ b ∈ ul.drop j.val, SatisfiesN p q n ul b.2 b.1 := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)

theorem cl_conformance_step_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) (i : Std.Usize) (hi : i.val < q.steps.val.length)
    (ul : List (Nat × Nat))
    (hul : SolverSpec.idPairs (q.steps.val[i.val]'hi).used = ul) :
    clauses.cl_conformance_loop0_loop0 p q.endpoints q.givens q.steps anc true i 0#usize
      ⦃ r => r = decide (∀ b ∈ ul, SatisfiesN p q n ul b.2 b.1) ⦄ := by
  have hd0 : ul.drop (0#usize).val = ul := List.drop_zero
  refine WP.spec_mono
    (cl_conformance_inner_spec p q anc n hlen hok i hi ul hul ul.length true 0#usize
      (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  rw [hd0]
  simp

theorem cl_conformance_outer_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), q.steps.val.length - i.val ≤ m →
      clauses.cl_conformance_loop0 p q anc ok1 i ⦃ r =>
        r = (ok1 && decide (∀ s ∈ (SolverSpec.steps q).drop i.val,
                              ∀ b ∈ s.used, SatisfiesN p q n s.used b.2 b.1)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_conformance_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_len]; scalar_tac
        have hyes : ∀ s ∈ (SolverSpec.steps q).drop i.val,
            ∀ b ∈ s.used, SatisfiesN p q n s.used b.2 b.1 := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_conformance_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < q.steps.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hcons := steps_cons q i.val hi
        rw [eq_of_spec (cl_conformance_step_spec p q anc n hlen hok i hi
              (SolverSpec.idPairs (q.steps.val[i.val]'hi).used) rfl)]
        simp only [bind_tc_ok]
        step as ⟨ i2, hi2 ⟩
        refine WP.spec_mono (ih _ i2 (by scalar_tac)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hh, hi2, hcons]
        simp only [Bool.true_and]
        refine (decide_and_eq ?_).symm
        constructor
        · intro hc
          exact ⟨hc _ List.mem_cons_self, fun s hs => hc s (List.mem_cons_of_mem _ hs)⟩
        · rintro ⟨h1, h2⟩ s hs
          rcases List.mem_cons.mp hs with hs' | hs'
          · rw [hs']; exact h1
          · exact h2 s hs'
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_len]; scalar_tac
        have hyes : ∀ s ∈ (SolverSpec.steps q).drop i.val,
            ∀ b ∈ s.used, SatisfiesN p q n s.used b.2 b.1 := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)

/-- What `cl_conformance` decides, on every input, with no hypothesis beyond the
pair describing the table. `SatisfiesN` is the specification's `Satisfies` with
the table's own range guard; `cl_conformance_spec` below is what removes it. -/
theorem cl_conformance_spec_exact (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) :
    clauses.cl_conformance p q anc ⦃ r =>
      r = decide (∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used,
                    SatisfiesN p q n s.used b.2 b.1) ⦄ := by
  have hd0 : (SolverSpec.steps q).drop (0#usize).val = SolverSpec.steps q := List.drop_zero
  refine WP.spec_mono
    (cl_conformance_outer_spec p q anc n hlen hok q.steps.val.length true 0#usize
      (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  rw [hd0]
  simp

/-! ## 3. emission -/

theorem emission_at_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) (si gi ki : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (hgi : gi.val < (q.steps.val[si.val]'hsi).produced.val.length)
    (hki : ki.val < ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length)
    (ul : List (Nat × Nat))
    (hul : SolverSpec.idPairs (q.steps.val[si.val]'hsi).used = ul)
    (a ee : Std.Usize)
    (hab : ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val[ki.val]'hki = (a, ee)) :
    clauses.emission_at p q anc si gi ki ⦃ r =>
      r = decide (SatisfiesN p q n ul ee.val a.val) ⦄ := by
  rw [clauses.emission_at.eq_def]
  dsimp only
  split
  · next h1 =>
    rw [index_eq q.steps si hsi]
    simp only [bind_tc_ok]
    split
    · next h2 =>
      rw [index_eq _ gi hgi]
      simp only [bind_tc_ok]
      split
      · next h3 =>
        rw [index_eq _ ki hki, hab]
        show clauses.satisfies p q anc
            (alloc.vec.Vec.deref (q.steps.val[si.val]'hsi).used) ee a ⦃ r =>
              r = decide (SatisfiesN p q n ul ee.val a.val) ⦄
        exact satisfies_spec p q anc n hlen hok
          (alloc.vec.Vec.deref (q.steps.val[si.val]'hsi).used) ul
          (by rw [pairsOf_deref]; exact hul) ee a
      · next h3 => exfalso; scalar_tac
    · next h2 => exfalso; scalar_tac
  · next h1 => exfalso; scalar_tac

theorem cl_emission_inner_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) (i g : Std.Usize)
    (hi : i.val < q.steps.val.length)
    (hg : g.val < (q.steps.val[i.val]'hi).produced.val.length)
    (ul : List (Nat × Nat))
    (hul : SolverSpec.idPairs (q.steps.val[i.val]'hi).used = ul)
    (grp : List (Nat × Nat))
    (hgrp : SolverSpec.idPairs ((q.steps.val[i.val]'hi).produced.val[g.val]'hg) = grp) :
    ∀ (m : Nat) (ok1 : Bool) (k : Std.Usize), grp.length - k.val ≤ m →
      clauses.cl_emission_loop0_loop0_loop0 p q.endpoints q.givens q.steps anc ok1 i g k
        ⦃ r => r = (ok1 && decide (∀ b ∈ grp.drop k.val, SatisfiesN p q n ul b.2 b.1)) ⦄ := by
  have hlg : grp.length = ((q.steps.val[i.val]'hi).produced.val[g.val]'hg).val.length := by
    rw [← hgrp, idPairs_len]
  intro m
  induction m with
  | zero =>
    intro ok1 k hk
    rw [clauses.cl_emission_loop0_loop0_loop0.eq_def]
    split
    · next hh =>
      rw [index_eq q.steps i hi]
      simp only [bind_tc_ok]
      rw [index_eq _ g hg]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : grp.drop k.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ b ∈ grp.drop k.val, SatisfiesN p q n ul b.2 b.1 := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 k hk
    rw [clauses.cl_emission_loop0_loop0_loop0.eq_def]
    split
    · next hh =>
      rw [index_eq q.steps i hi]
      simp only [bind_tc_ok]
      rw [index_eq _ g hg]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hkk : k.val < ((q.steps.val[i.val]'hi).produced.val[g.val]'hg).val.length := by
          scalar_tac
        obtain ⟨a, ee, hab⟩ :
            ∃ a ee, ((q.steps.val[i.val]'hi).produced.val[g.val]'hg).val[k.val]'hkk
              = (a, ee) := ⟨_, _, rfl⟩
        have hcons : grp.drop k.val = (a.val, ee.val) :: grp.drop (k.val + 1) := by
          rw [← hgrp, idPairs_cons _ k.val hkk, hab]
        have hbnd : k.val + 1 ≤ Usize.max := by scalar_tac
        rw [eq_of_spec (emission_at_spec p q anc n hlen hok i g k hi hg hkk ul hul a ee hab)]
        simp only [bind_tc_ok]
        split
        · next hb =>
          have hS : SatisfiesN p q n ul ee.val a.val := of_decide_eq_true hb
          step as ⟨ k1, hk1 ⟩
          have hiff : (∀ b ∈ grp.drop k1.val, SatisfiesN p q n ul b.2 b.1)
              ↔ (∀ b ∈ grp.drop k.val, SatisfiesN p q n ul b.2 b.1) := by
            rw [hk1, hcons]
            constructor
            · intro hc x hx
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx']; exact hS
              · exact hc x hx'
            · intro hc x hx
              exact hc x (List.mem_cons_of_mem _ hx)
          refine WP.spec_mono (ih true k1 (by scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hb =>
          have hS : ¬ SatisfiesN p q n ul ee.val a.val := fun hc => hb (decide_eq_true hc)
          have hno : ¬ (∀ x ∈ grp.drop k.val, SatisfiesN p q n ul x.2 x.1) := by
            intro hc
            exact hS (hc (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self))
          step as ⟨ k1, hk1 ⟩
          refine WP.spec_mono (ih false k1 (by scalar_tac)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hno).symm
      · have hnil : grp.drop k.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ b ∈ grp.drop k.val, SatisfiesN p q n ul b.2 b.1 := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)

theorem cl_emission_group_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) (i g : Std.Usize)
    (hi : i.val < q.steps.val.length)
    (hg : g.val < (q.steps.val[i.val]'hi).produced.val.length)
    (ul : List (Nat × Nat))
    (hul : SolverSpec.idPairs (q.steps.val[i.val]'hi).used = ul)
    (grp : List (Nat × Nat))
    (hgrp : SolverSpec.idPairs ((q.steps.val[i.val]'hi).produced.val[g.val]'hg) = grp) :
    clauses.cl_emission_loop0_loop0_loop0 p q.endpoints q.givens q.steps anc true i g 0#usize
      ⦃ r => r = decide (∀ b ∈ grp, SatisfiesN p q n ul b.2 b.1) ⦄ := by
  have hd0 : grp.drop (0#usize).val = grp := List.drop_zero
  refine WP.spec_mono
    (cl_emission_inner_spec p q anc n hlen hok i g hi hg ul hul grp hgrp grp.length true
      0#usize (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  rw [hd0]
  simp

theorem cl_emission_middle_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) (i : Std.Usize) (hi : i.val < q.steps.val.length)
    (ul : List (Nat × Nat))
    (hul : SolverSpec.idPairs (q.steps.val[i.val]'hi).used = ul)
    (gs : List (List (Nat × Nat)))
    (hgs : SolverSpec.pairLists (q.steps.val[i.val]'hi).produced = gs) :
    ∀ (m : Nat) (ok1 : Bool) (g : Std.Usize), gs.length - g.val ≤ m →
      clauses.cl_emission_loop0_loop0 p q.endpoints q.givens q.steps anc ok1 i g ⦃ r =>
        r = (ok1 && decide (∀ grp ∈ gs.drop g.val, ∀ b ∈ grp,
                              SatisfiesN p q n ul b.2 b.1)) ⦄ := by
  have hlg : gs.length = (q.steps.val[i.val]'hi).produced.val.length := by
    rw [← hgs, pairLists_len]
  intro m
  induction m with
  | zero =>
    intro ok1 g hk
    rw [clauses.cl_emission_loop0_loop0.eq_def]
    split
    · next hh =>
      rw [index_eq q.steps i hi]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : gs.drop g.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ grp ∈ gs.drop g.val, ∀ b ∈ grp, SatisfiesN p q n ul b.2 b.1 := by
          rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 g hk
    rw [clauses.cl_emission_loop0_loop0.eq_def]
    split
    · next hh =>
      rw [index_eq q.steps i hi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hg : g.val < (q.steps.val[i.val]'hi).produced.val.length := by scalar_tac
        have hcons : gs.drop g.val
            = SolverSpec.idPairs ((q.steps.val[i.val]'hi).produced.val[g.val]'hg)
                :: gs.drop (g.val + 1) := by
          rw [← hgs, pairLists_cons _ g.val hg]
        have hbnd : g.val + 1 ≤ Usize.max := by scalar_tac
        rw [eq_of_spec (cl_emission_group_spec p q anc n hlen hok i g hi hg ul hul
              (SolverSpec.idPairs ((q.steps.val[i.val]'hi).produced.val[g.val]'hg)) rfl)]
        simp only [bind_tc_ok]
        step as ⟨ g1, hg1 ⟩
        refine WP.spec_mono (ih _ g1 (by scalar_tac)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hh, hg1, hcons]
        simp only [Bool.true_and]
        refine (decide_and_eq ?_).symm
        constructor
        · intro hc
          exact ⟨hc _ List.mem_cons_self, fun x hx => hc x (List.mem_cons_of_mem _ hx)⟩
        · rintro ⟨h1, h2⟩ x hx
          rcases List.mem_cons.mp hx with hx' | hx'
          · rw [hx']; exact h1
          · exact h2 x hx'
      · have hnil : gs.drop g.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ grp ∈ gs.drop g.val, ∀ b ∈ grp, SatisfiesN p q n ul b.2 b.1 := by
          rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)

theorem cl_emission_step_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) (i : Std.Usize) (hi : i.val < q.steps.val.length)
    (ul : List (Nat × Nat))
    (hul : SolverSpec.idPairs (q.steps.val[i.val]'hi).used = ul)
    (gs : List (List (Nat × Nat)))
    (hgs : SolverSpec.pairLists (q.steps.val[i.val]'hi).produced = gs) :
    clauses.cl_emission_loop0_loop0 p q.endpoints q.givens q.steps anc true i 0#usize
      ⦃ r => r = decide (∀ grp ∈ gs, ∀ b ∈ grp, SatisfiesN p q n ul b.2 b.1) ⦄ := by
  have hd0 : gs.drop (0#usize).val = gs := List.drop_zero
  refine WP.spec_mono
    (cl_emission_middle_spec p q anc n hlen hok i hi ul hul gs hgs gs.length true 0#usize
      (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  rw [hd0]
  simp

theorem cl_emission_outer_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), q.steps.val.length - i.val ≤ m →
      clauses.cl_emission_loop0 p q anc ok1 i ⦃ r =>
        r = (ok1 && decide (∀ s ∈ (SolverSpec.steps q).drop i.val,
                              ∀ g ∈ s.produced, ∀ b ∈ g,
                                SatisfiesN p q n s.used b.2 b.1)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_emission_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_len]; scalar_tac
        have hyes : ∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ g ∈ s.produced, ∀ b ∈ g,
            SatisfiesN p q n s.used b.2 b.1 := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_emission_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hi : i.val < q.steps.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hcons := steps_cons q i.val hi
        rw [eq_of_spec (cl_emission_step_spec p q anc n hlen hok i hi
              (SolverSpec.idPairs (q.steps.val[i.val]'hi).used) rfl
              (SolverSpec.pairLists (q.steps.val[i.val]'hi).produced) rfl)]
        simp only [bind_tc_ok]
        step as ⟨ i2, hi2 ⟩
        refine WP.spec_mono (ih _ i2 (by scalar_tac)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hh, hi2, hcons]
        simp only [Bool.true_and]
        refine (decide_and_eq ?_).symm
        constructor
        · intro hc
          exact ⟨hc _ List.mem_cons_self, fun s hs => hc s (List.mem_cons_of_mem _ hs)⟩
        · rintro ⟨h1, h2⟩ s hs
          rcases List.mem_cons.mp hs with hs' | hs'
          · rw [hs']; exact h1
          · exact h2 s hs'
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_len]; scalar_tac
        have hyes : ∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ g ∈ s.produced, ∀ b ∈ g,
            SatisfiesN p q n s.used b.2 b.1 := by rw [hnil]; simp
        refine ok_holds ?_
        rw [hh]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_holds (by rw [hf]; simp)

/-- What `cl_emission` decides, on every input. -/
theorem cl_emission_spec_exact (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val) :
    clauses.cl_emission p q anc ⦃ r =>
      r = decide (∀ s ∈ SolverSpec.steps q, ∀ g ∈ s.produced, ∀ b ∈ g,
                    SatisfiesN p q n s.used b.2 b.1) ⦄ := by
  have hd0 : (SolverSpec.steps q).drop (0#usize).val = SolverSpec.steps q := List.drop_zero
  refine WP.spec_mono
    (cl_emission_outer_spec p q anc n hlen hok q.steps.val.length true 0#usize
      (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  rw [hd0]
  simp

/-! ## The clause conjuncts

The two forms coincide on a plan whose steps reference endpoints the table
covers, which is `WellIndexed`'s range conjunct at `n = nEndpoints q`. -/

theorem sentinel_bound (anc : Slice (alloc.vec.Vec Bool)) (n : Nat)
    (hlen : anc.val.length = n) : n ≤ access.NONE.val := by
  have h := Slice.length_ineq anc
  rw [← hlen]
  simp only [access.NONE]
  scalar_tac

/-- `conformance`, as `SolverSpec.ValidC` states it. -/
theorem cl_conformance_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val)
    (hrange : ∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used, b.2 < n) :
    clauses.cl_conformance p q anc ⦃ r =>
      r = decide (∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used,
                    SolverSpec.Satisfies p q s.used b.2 b.1) ⦄ := by
  have hn := sentinel_bound anc n hlen
  refine WP.spec_mono (cl_conformance_spec_exact p q anc n hlen hok) ?_
  intro r hr
  refine hr.trans (decide_eq_decide.mpr ?_)
  constructor
  · intro hc s hs b hb
    exact (satisfiesN_iff p q n s.used b.2 b.1 (hrange s hs b hb) hn).mp (hc s hs b hb)
  · intro hc s hs b hb
    exact (satisfiesN_iff p q n s.used b.2 b.1 (hrange s hs b hb) hn).mpr (hc s hs b hb)

/-- `emission`, as `SolverSpec.ValidC` states it. -/
theorem cl_emission_spec (p : types.Problem) (q : types.Plan)
    (anc : Slice (alloc.vec.Vec Bool)) (n : Nat) (hlen : anc.val.length = n)
    (hok : RowsOk q n anc.val)
    (hrange : ∀ s ∈ SolverSpec.steps q, ∀ g ∈ s.produced, ∀ b ∈ g, b.2 < n) :
    clauses.cl_emission p q anc ⦃ r =>
      r = decide (∀ s ∈ SolverSpec.steps q, ∀ g ∈ s.produced, ∀ b ∈ g,
                    SolverSpec.Satisfies p q s.used b.2 b.1) ⦄ := by
  have hn := sentinel_bound anc n hlen
  refine WP.spec_mono (cl_emission_spec_exact p q anc n hlen hok) ?_
  intro r hr
  refine hr.trans (decide_eq_decide.mpr ?_)
  constructor
  · intro hc s hs g hg b hb
    exact (satisfiesN_iff p q n s.used b.2 b.1 (hrange s hs g hg b hb) hn).mp
      (hc s hs g hg b hb)
  · intro hc s hs g hg b hb
    exact (satisfiesN_iff p q n s.used b.2 b.1 (hrange s hs g hg b hb) hn).mpr
      (hc s hs g hg b hb)

/-! ## The divergence, as a theorem rather than a claim

The plan below is the one described at the head of this file: one node with no
properties and one anchor pointing at itself, no endpoints at all, and one step
binding that node's slot to endpoint `0`. `cl_indexed` rejects it -- endpoint `0`
is outside an empty table -- so `check` is right about it, and `check_spec` is
unharmed. What it rules out is an UNCONDITIONAL `cl_conformance_spec`. -/

namespace Lineage.Counterexample

def cxNode : types.Node :=
  { props := ⟨[], by scalar_tac⟩, parents := ⟨[0#usize], by scalar_tac⟩ }

def cxP : types.Problem :=
  { n_props := 0#usize
    nodes := ⟨[cxNode], by scalar_tac⟩
    transforms := ⟨[], by scalar_tac⟩
    given := ⟨[], by scalar_tac⟩
    given_tr := 0#usize
    target_tr := 0#usize }

def cxStep : types.Step :=
  { transform := 0#usize
    used := ⟨[(0#usize, 0#usize)], by scalar_tac⟩
    produced := ⟨[], by scalar_tac⟩ }

def cxQ : types.Plan :=
  { endpoints := ⟨[], by scalar_tac⟩, givens := ⟨[], by scalar_tac⟩,
    steps := ⟨[cxStep], by scalar_tac⟩ }

def cxAnc : Slice (alloc.vec.Vec Bool) := ⟨[], by scalar_tac⟩

theorem cx_rows : RowsOk cxQ 0 cxAnc.val := by
  intro i hi
  simp [cxAnc] at hi

theorem cx_anc : SolverSpec.ancestorB cxQ 0 0 = true := by
  rw [SolverSpec.ancestorB.eq_def]
  simp

theorem cx_sat : SolverSpec.Satisfies cxP cxQ [(0, 0)] 0 0 := by
  constructor
  · intro x hx
    simp [SolverSpec.nodeProps, SolverSpec.nd, cxP, cxNode, SolverSpec.nats] at hx
  · intro a ha
    have ha' : a = 0 := by
      simpa [SolverSpec.nodeParents, SolverSpec.nd, cxP, cxNode, SolverSpec.nats] using ha
    subst ha'
    simp [SolverSpec.anchorOk, SolverSpec.boundTo, cx_anc]

theorem cx_spec : ∀ s ∈ SolverSpec.steps cxQ, ∀ b ∈ s.used,
    SolverSpec.Satisfies cxP cxQ s.used b.2 b.1 := by
  intro s hs b hb
  have hs' : s = SolverSpec.stepView cxStep := by
    simpa [SolverSpec.steps, cxQ] using hs
  subst hs'
  have hu : (SolverSpec.stepView cxStep).used = [((0 : Nat), (0 : Nat))] := by
    simp [SolverSpec.stepView, SolverSpec.idPairs, cxStep]
  rw [hu] at hb ⊢
  have hb' : b = (0, 0) := by simpa using hb
  subst hb'
  exact cx_sat

theorem cx_rust : clauses.cl_conformance cxP cxQ cxAnc = ok false := by
  refine (eq_of_spec (cl_conformance_spec_exact cxP cxQ cxAnc 0 rfl cx_rows)).trans ?_
  congr 1
  refine decide_eq_false ?_
  intro hc
  have h1 : SatisfiesN cxP cxQ 0 (SolverSpec.stepView cxStep).used 0 0 := by
    refine hc (SolverSpec.stepView cxStep) (by simp [SolverSpec.steps, cxQ]) (0, 0) ?_
    simp [SolverSpec.stepView, SolverSpec.idPairs, cxStep]
  have h2 : anchorOkN cxQ 0 (SolverSpec.stepView cxStep).used 0 0 = true := by
    refine h1.2 0 ?_
    simp [SolverSpec.nodeParents, SolverSpec.nd, cxP, cxNode, SolverSpec.nats]
  simp [anchorOkN, SolverSpec.stepView, SolverSpec.idPairs, cxStep,
        SolverSpec.boundTo] at h2

end Lineage.Counterexample

/-- The checker rejects a plan the `conformance` conjunct accepts, so no
unconditional `cl_conformance_spec` exists. `cl_indexed` is what closes the gap,
and `cl_conformance_spec`'s range hypothesis is where the composition must
supply it. -/
theorem conformance_clause_needs_indexed :
    ∃ (p : types.Problem) (q : types.Plan) (anc : Slice (alloc.vec.Vec Bool)),
      anc.val.length = SolverSpec.nEndpoints q ∧
      RowsOk q (SolverSpec.nEndpoints q) anc.val ∧
      (∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used,
          SolverSpec.Satisfies p q s.used b.2 b.1) ∧
      clauses.cl_conformance p q anc = ok false :=
  ⟨Counterexample.cxP, Counterexample.cxQ, Counterexample.cxAnc, rfl,
   Counterexample.cx_rows, Counterexample.cx_spec, Counterexample.cx_rust⟩

/- What the obligations actually rest on. -/
#print axioms SolverProof.conformance_clause_needs_indexed
#print axioms SolverProof.Lineage.satisfiesN_iff
#print axioms SolverProof.bound_to_spec
#print axioms SolverProof.carries_spec
#print axioms SolverProof.satisfies_spec
#print axioms SolverProof.conformance_at_spec
#print axioms SolverProof.emission_at_spec
#print axioms SolverProof.cl_conformance_spec_exact
#print axioms SolverProof.cl_emission_spec_exact
#print axioms SolverProof.cl_conformance_spec
#print axioms SolverProof.cl_emission_spec

end SolverProof
