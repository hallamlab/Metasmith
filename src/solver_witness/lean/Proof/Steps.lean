/-
  Four clauses over the STEP LIST: `target`, `uniqueProducer`, `provenance` and
  `schedulable`. Read `Proof/Basis.lean` first for the loop recipe; what follows
  is what a clause whose conjunct quantifies over `List.zipIdx` needs on top of
  it, plus four traps that cost this file most of its builds.

  ## `emits` is proved once

  Three of the four clauses reach the same helper, `clauses::emits`, and it is
  the only place any of them looks at what a step produced. `emits_spec` is
  stated directly as `SolverSpec.Emits` on the step's `StepView`, so the three
  callers never see the two nested loops underneath.

  ## Quantifying over `zipIdx`

  `uniqueProducer` and `schedulable` are stated over `(steps q).zipIdx`, and the
  loops that decide them walk `q.steps` by index. The bridge is to phrase the
  loop postcondition over `((steps q).zipIdx).drop i` rather than over
  `(steps q).drop i` paired with index algebra: the wrapper then falls out at
  `i = 0` by `List.drop_zero` exactly as in `Basis`, and the inductive step is
  `steps_zipIdx_drop_cons` -- one head, whose index is literally `i`, and one
  tail.

  What the index-carrying form gives for free is `le_of_mem_zipIdx_drop`:
  everything still in the suffix has an index of at least `i`. `uniqueProducer`
  cannot be proved without it. The scan records the index of the first emitter it
  saw and rejects a second, so "the head is the only emitter" has to become "no
  LATER entry emits", and the only thing separating the head from the tail is
  that the tail's indices are strictly larger.

  A loop nested inside another contributes its verdict as a second `decide`, so
  the step of the outer induction is not `decide_eq_decide` but `decide_and_of_iff`:
  one iff, `outer i ↔ inner ∧ outer (i+1)`, and the `Bool` algebra is done once,
  here, rather than by a case split at each of the four sites.

  ## The scan, not the pointwise judgement

  `cl_unique_producer` does NOT call `unique_producer_at`, the pointwise
  predicate sitting just above it. It reimplements the judgement as a single
  O(n) pass carrying a `seen` index, so that is what is proved here. The loop
  invariant needs a second conjunct the specification has no counterpart for --
  once `seen` is set, no remaining step may emit -- and the sentinel is load
  bearing: `seen` starts at `NONE = usize::MAX`, so the proof owes `i ≠ NONE`
  for every index it records. `index_ne_NONE` discharges that from the `Vec`
  length bound, `i < steps.len() ≤ usize::MAX`; a checker whose step list could
  be `usize::MAX` long would be wrong rather than unproved.

  ## Four traps, all of them consequences of Basis's second warning

  `alloc.vec.Vec α` is a `def` for a subtype, so a term containing `v.val` is
  type-correct only once `Vec` unfolds -- which does not happen at `instances`
  transparency. The first three follow from that one fact, and the fourth is the
  same discipline applied to `@[reducible]`.

  1. **A postcondition must not mention any `Vec`'s `.val`.** Not for elegance:
     `Decidable` synthesis runs at `instances` transparency, so
     `decide (∀ b ∈ idPairs (q.steps.val[i]).used, ...)` reports "failed to
     synthesize Decidable" for an instance that is plainly derivable, while the
     same statement over an opaque `List (Nat × Nat)` resolves at once. Say it
     over `SolverSpec.steps`, `givens`, `pairLists` -- whose `.val` is inside the
     definition, where nothing has to unfold it -- or hoist the list to a
     parameter with its defining equation as a hypothesis, which is what `row`,
     `used` and `sv` below are for.

  2. **Never `rw`, `subst` or `dsimp` at anything in the postcondition.** The
     motive is the whole triple, `Eq.ndrec` re-checks it at `instances`
     transparency, and it fails with "invalid motive" naming the program rather
     than the postcondition. `subst hok` on `hok : ok1 = true` is the shape that
     bites, and `ok1` is not even the ill-typed part. Rewriting inside the
     PROGRAM is fine, which is what makes `vec_index_ok` and `eq_of_spec` usable
     at all. Reach the postcondition through `ok_spec`, which turns the triple
     into the plain equation where every rewrite is legal again, and through
     `WP.spec_mono` -- never directly.

  3. **`simp only [bind_tc_ok]` does not reduce a pattern-matching bind.**
     `let (_, e) ← Vec.index ...` leaves `let (_, e) := (a, ee)` standing, and
     `split` then fails with "could not split an `if` or `match`" because there
     is nothing left to split. `dsimp only` is the obvious fix and is exactly
     what `Proof/Access.lean` warns not to use after an index rewrite. `show`
     with the reduced program is the way through: the two differ by iota on a
     literal pair, so the defeq check is free.

  4. **The goal's copy of the postcondition is not the one you wrote.** By the
     time the recipe reaches the closing step, `SolverSpec.Emits` has been
     delta-reduced in the goal and its implication pushed -- `(∃ g ∈ …, …) → S`
     arrives as `∀ g ∈ …, … → S` -- while the SAME statement reached through the
     induction hypothesis is still folded. So `rw [decide_eq_true h]` reports
     "did not find an occurrence" of a pattern that is on the screen, and
     `exact decide_eq_decide.mpr h` reports an application type mismatch, because
     the two `decide`s carry `Decidable` instances built on different-looking
     propositions.

     Close with `exact`, never `rw`: it unifies at default transparency, which
     covers the delta. `decide_eq_decide`, `decide_and_of_iff` and `decide_or_of_iff`
     exist so that every closing step can be one. Where the pushed implication is
     what differs -- only `schedulable`, whose conjunct is the one with `Emits` in
     an antecedent -- reconcile with
     `simpa only [SolverSpec.Emits, exists_imp, and_imp] using h`: `simpa` rewrites
     hypothesis and goal with the same set, and `simp only` with exactly those
     three pushes both without contracting either, which a plain `simpa` does
     asymmetrically and then fails on.

  And one arithmetic note that is not about transparency: the fuel decrease of a
  loop over a hoisted list is `used.length - i ≤ m`, whose `length` is an opaque
  atom. `scalar_tac` does not close it; `omega` does.
-/

import SolverWitness.Types
import SolverWitness.Funs
import SolverWitness.Spec
import SolverWitness.Proof.Basis

set_option synthInstance.maxSize 1000
set_option synthInstance.maxHeartbeats 1000000
set_option maxHeartbeats 1000000

namespace SolverProof

open Aeneas Aeneas.Std Result
open solver_witness

/-! ## Bridges -/

/-- The triple on an `ok`, as the plain equation. Every rewrite that would be a
motive failure inside the triple is legal on the other side of this. -/
theorem ok_spec {α} {x : α} {P : α → Prop} (h : P x) : (ok x : Result α) ⦃ r => P r ⦄ := by
  simp only [WP.spec_ok]
  exact h

/-- `eq_of_spec` for a postcondition that does not pin the value. -/
theorem ok_of_spec {α} {x : Result α} {P : α → Prop} (h : x ⦃ z => P z ⦄) :
    ∃ v, x = ok v ∧ P v := by
  cases x with
  | ok v => exact ⟨v, rfl, by simpa only [WP.spec_ok] using h⟩
  | fail e => simp only [WP.spec_fail] at h
  | div => simp only [WP.spec_div] at h

/-- A loop whose body runs another loop reports two verdicts; this is how they
become one. Named away from `decide_and_eq`: `Proof/Lineage.lean` has its own,
oriented the other way round, and two same-named declarations in `SolverProof`
would collide the moment one module imports both. -/
theorem decide_and_of_iff {p q r : Prop} [Decidable p] [Decidable q] [Decidable r]
    (h : r ↔ (p ∧ q)) : (decide p && decide q) = decide r := by
  by_cases hp : p
  · by_cases hq : q
    · simp [hp, hq, h.mpr ⟨hp, hq⟩]
    · have hr : ¬ r := fun hr => hq (h.mp hr).2
      simp [hp, hq, hr]
  · have hr : ¬ r := fun hr => hp (h.mp hr).1
    simp [hp, hr]

/-- The same for a loop that stops on the first success rather than the first
failure. -/
theorem decide_or_of_iff {p q r : Prop} [Decidable p] [Decidable q] [Decidable r]
    (h : r ↔ (p ∨ q)) : (decide p || decide q) = decide r := by
  by_cases hp : p
  · simp [hp, h.mpr (Or.inl hp)]
  · by_cases hq : q
    · simp [hp, hq, h.mpr (Or.inr hq)]
    · have hr : ¬ r := fun hr => (h.mp hr).elim hp hq
      simp [hp, hq, hr]

/-- The extraction indexes a `Vec` through `Vec.index`; in range it is `ok` of
the element. -/
theorem vec_index_ok {α : Type} (v : alloc.vec.Vec α) (i : Std.Usize)
    (h : i.val < v.val.length) :
    alloc.vec.Vec.index (core.slice.index.SliceIndexUsizeSlice α) v i
      = ok (v.val[i.val]'h) := by
  rw [alloc.vec.Vec.index_slice_index]
  exact eq_of_spec (alloc.vec.Vec.index_usize_spec v i (by simpa using h))

theorem steps_length (q : types.Plan) :
    (SolverSpec.steps q).length = q.steps.val.length := by
  simp [SolverSpec.steps]

/-- One head and one tail of the step list, as the specification sees them. -/
theorem steps_drop_cons (q : types.Plan) (i : Nat) (h : i < q.steps.val.length) :
    (SolverSpec.steps q).drop i
      = SolverSpec.stepView (q.steps.val[i]'h) :: (SolverSpec.steps q).drop (i + 1) := by
  have h' : i < (SolverSpec.steps q).length := by rw [steps_length]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  exact ⟨by simp [SolverSpec.steps], rfl⟩

/-- The same, carrying the index the specification pairs with each step. -/
theorem steps_zipIdx_drop_cons (q : types.Plan) (i : Nat) (h : i < q.steps.val.length) :
    ((SolverSpec.steps q).zipIdx).drop i
      = (SolverSpec.stepView (q.steps.val[i]'h), i)
          :: ((SolverSpec.steps q).zipIdx).drop (i + 1) := by
  have h' : i < ((SolverSpec.steps q).zipIdx).length := by
    rw [List.length_zipIdx, steps_length]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  refine ⟨?_, rfl⟩
  rw [List.getElem_zipIdx]
  simp [SolverSpec.steps]

/-- What is still in the suffix was indexed at least `i`. This is what separates
the head of a `zipIdx` scan from its tail, and `uniqueProducer` rests on it. -/
theorem le_of_mem_zipIdx_drop {α} {l : List α} {i : Nat} {x : α × Nat}
    (hx : x ∈ (l.zipIdx).drop i) : i ≤ x.2 := by
  rw [List.mem_iff_getElem] at hx
  obtain ⟨t, ht, hxt⟩ := hx
  rw [List.getElem_drop, List.getElem_zipIdx] at hxt
  subst hxt
  simp

theorem idPairs_length (v : alloc.vec.Vec (Std.Usize × Std.Usize)) :
    (SolverSpec.idPairs v).length = v.val.length := by
  simp [SolverSpec.idPairs]

theorem idPairs_drop_cons (v : alloc.vec.Vec (Std.Usize × Std.Usize)) (i : Nat)
    (h : i < v.val.length) :
    (SolverSpec.idPairs v).drop i
      = ((v.val[i]'h).1.val, (v.val[i]'h).2.val) :: (SolverSpec.idPairs v).drop (i + 1) := by
  have h' : i < (SolverSpec.idPairs v).length := by rw [idPairs_length]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  exact ⟨by simp [SolverSpec.idPairs], rfl⟩

theorem pairLists_length (v : alloc.vec.Vec (alloc.vec.Vec (Std.Usize × Std.Usize))) :
    (SolverSpec.pairLists v).length = v.val.length := by
  simp [SolverSpec.pairLists]

theorem pairLists_drop_cons (v : alloc.vec.Vec (alloc.vec.Vec (Std.Usize × Std.Usize)))
    (i : Nat) (h : i < v.val.length) :
    (SolverSpec.pairLists v).drop i
      = SolverSpec.idPairs (v.val[i]'h) :: (SolverSpec.pairLists v).drop (i + 1) := by
  have h' : i < (SolverSpec.pairLists v).length := by rw [pairLists_length]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  exact ⟨by simp [SolverSpec.pairLists], rfl⟩

theorem givens_length (q : types.Plan) :
    (SolverSpec.givens q).length = q.givens.val.length := by
  rw [SolverSpec.givens, idPairs_length]

theorem givens_drop_cons (q : types.Plan) (i : Nat) (h : i < q.givens.val.length) :
    (SolverSpec.givens q).drop i
      = ((q.givens.val[i]'h).1.val, (q.givens.val[i]'h).2.val)
          :: (SolverSpec.givens q).drop (i + 1) := by
  rw [SolverSpec.givens, idPairs_drop_cons _ i h]

/-- The sentinel is `usize::MAX`, and a `Vec` is no longer than that, so an index
into the step list is never the sentinel. -/
theorem index_ne_NONE (q : types.Plan) (i : Std.Usize) (h : i.val < q.steps.val.length) :
    i ≠ access.NONE := by
  simp [access.NONE]
  scalar_tac

/-! ## `emits` -- the shared helper -/

/-- The inner loop: the slots of one product group still to look at. `row` is the
group as the specification reads it, hoisted to a parameter so that the
postcondition carries no `Vec`. -/
theorem emits_inner_spec
    (v : alloc.vec.Vec (alloc.vec.Vec (Std.Usize × Std.Usize))) (e g : Std.Usize)
    (hg : g.val < v.val.length) (row : List (Nat × Nat))
    (hrow : SolverSpec.idPairs (v.val[g.val]'hg) = row) :
    ∀ (m : Nat) (hit : Bool) (k : Std.Usize), row.length - k.val ≤ m →
      clauses.emits_loop0_loop0 v e hit g k ⦃ r =>
        r = (hit || decide (∃ b ∈ row.drop k.val, b.2 = e.val)) ⦄ := by
  have hrl : row.length = (v.val[g.val]'hg).val.length := by
    rw [← hrow, idPairs_length]
  intro m
  induction m with
  | zero =>
    intro hit k hk
    rw [clauses.emits_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh => exact ok_spec (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      rw [vec_index_ok v g hg]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : row.drop k.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hno : ¬ (∃ b ∈ row.drop k.val, b.2 = e.val) := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact (decide_eq_false hno).symm
  | succ m ih =>
    intro hit k hk
    rw [clauses.emits_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh => exact ok_spec (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      rw [vec_index_ok v g hg]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hk1 : k.val < (v.val[g.val]'hg).val.length := by scalar_tac
        obtain ⟨a, ee, hae⟩ :
            ∃ a ee, (v.val[g.val]'hg).val[k.val]'hk1 = (a, ee) := ⟨_, _, rfl⟩
        have hcons : row.drop k.val = (a.val, ee.val) :: row.drop (k.val + 1) := by
          rw [← hrow, idPairs_drop_cons _ k.val hk1, hae]
        have hbnd : k.val + 1 ≤ Usize.max := by scalar_tac
        rw [vec_index_ok _ k hk1, hae]
        show (do
            let hit1 ← if ee = e then ok true else ok false
            let k1 ← k + 1#usize
            clauses.emits_loop0_loop0 v e hit1 g k1) ⦃ r =>
              r = (hit || decide (∃ b ∈ row.drop k.val, b.2 = e.val)) ⦄
        split
        · next heq =>
          have hyes : ∃ b ∈ row.drop k.val, b.2 = e.val := by
            rw [hcons]
            exact ⟨(a.val, ee.val), List.mem_cons_self, by rw [heq]⟩
          simp only [bind_tc_ok]
          step as ⟨ k1, hk1' ⟩
          refine WP.spec_mono (ih true k1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.true_or, Bool.false_or]
          exact (decide_eq_true hyes).symm
        · next hne =>
          have hane : ¬ (ee.val = e.val) := by
            intro hc; exact hne (by scalar_tac)
          simp only [bind_tc_ok]
          step as ⟨ k1, hk1' ⟩
          have hiff : (∃ b ∈ row.drop k1.val, b.2 = e.val)
              ↔ (∃ b ∈ row.drop k.val, b.2 = e.val) := by
            rw [hk1', hcons]
            constructor
            · rintro ⟨x, hx, hx2⟩
              exact ⟨x, List.mem_cons_of_mem _ hx, hx2⟩
            · rintro ⟨x, hx, hx2⟩
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx'] at hx2; exact absurd hx2 hane
              · exact ⟨x, hx', hx2⟩
          refine WP.spec_mono (ih false k1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · have hnil : row.drop k.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hno : ¬ (∃ b ∈ row.drop k.val, b.2 = e.val) := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact (decide_eq_false hno).symm

/-- The outer loop: the product groups still to look at. -/
theorem emits_outer_spec (s : types.Step) (e : Std.Usize) :
    ∀ (m : Nat) (hit : Bool) (g : Std.Usize),
      (SolverSpec.pairLists s.produced).length - g.val ≤ m →
      clauses.emits_loop0 s e hit g ⦃ r =>
        r = (hit || decide (∃ gp ∈ (SolverSpec.pairLists s.produced).drop g.val,
              ∃ b ∈ gp, b.2 = e.val)) ⦄ := by
  have hpl : (SolverSpec.pairLists s.produced).length = s.produced.val.length :=
    pairLists_length s.produced
  intro m
  induction m with
  | zero =>
    intro hit g hk
    rw [clauses.emits_loop0.eq_def]
    dsimp only
    split
    · next hh => exact ok_spec (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.pairLists s.produced).drop g.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hno : ¬ (∃ gp ∈ (SolverSpec.pairLists s.produced).drop g.val,
            ∃ b ∈ gp, b.2 = e.val) := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact (decide_eq_false hno).symm
  | succ m ih =>
    intro hit g hk
    rw [clauses.emits_loop0.eq_def]
    dsimp only
    split
    · next hh => exact ok_spec (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · next hlt =>
        have hg : g.val < s.produced.val.length := by scalar_tac
        have hcons : (SolverSpec.pairLists s.produced).drop g.val
            = SolverSpec.idPairs (s.produced.val[g.val]'hg)
                :: (SolverSpec.pairLists s.produced).drop (g.val + 1) :=
          pairLists_drop_cons s.produced g.val hg
        have hin := eq_of_spec (emits_inner_spec s.produced e g hg
          (SolverSpec.idPairs (s.produced.val[g.val]'hg)) rfl
          (SolverSpec.idPairs (s.produced.val[g.val]'hg)).length false 0#usize (by scalar_tac))
        have hbnd : g.val + 1 ≤ Usize.max := by scalar_tac
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ g1, hg1 ⟩
        have hd0 : (SolverSpec.idPairs (s.produced.val[g.val]'hg)).drop (0#usize).val
            = SolverSpec.idPairs (s.produced.val[g.val]'hg) := List.drop_zero
        have hiff : (∃ gp ∈ (SolverSpec.pairLists s.produced).drop g.val,
              ∃ b ∈ gp, b.2 = e.val)
            ↔ ((∃ b ∈ (SolverSpec.idPairs (s.produced.val[g.val]'hg)).drop (0#usize).val,
                  b.2 = e.val)
                ∨ (∃ gp ∈ (SolverSpec.pairLists s.produced).drop g1.val,
                  ∃ b ∈ gp, b.2 = e.val)) := by
          rw [hd0, hg1, hcons]
          constructor
          · rintro ⟨x, hx, hx2⟩
            rcases List.mem_cons.mp hx with hx' | hx'
            · exact Or.inl (by rw [← hx']; exact hx2)
            · exact Or.inr ⟨x, hx', hx2⟩
          · rintro (h | ⟨x, hx, hx2⟩)
            · exact ⟨_, List.mem_cons_self, h⟩
            · exact ⟨x, List.mem_cons_of_mem _ hx, hx2⟩
        refine WP.spec_mono (ih _ g1 (by omega)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact decide_or_of_iff hiff
      · have hnil : (SolverSpec.pairLists s.produced).drop g.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hno : ¬ (∃ gp ∈ (SolverSpec.pairLists s.produced).drop g.val,
            ∃ b ∈ gp, b.2 = e.val) := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact (decide_eq_false hno).symm

/-- The helper, in the specification's own vocabulary. -/
theorem emits_spec (s : types.Step) (e : Std.Usize) :
    clauses.emits s e ⦃ r => r = decide (SolverSpec.Emits (SolverSpec.stepView s) e.val) ⦄ := by
  have h := emits_outer_spec s e (SolverSpec.pairLists s.produced).length false 0#usize
    (by scalar_tac)
  have hd0 : (SolverSpec.pairLists s.produced).drop (0#usize).val
      = SolverSpec.pairLists s.produced := List.drop_zero
  have hiff : (∃ gp ∈ (SolverSpec.pairLists s.produced).drop (0#usize).val,
        ∃ b ∈ gp, b.2 = e.val)
      ↔ SolverSpec.Emits (SolverSpec.stepView s) e.val := by
    rw [hd0]
    exact Iff.rfl
  refine WP.spec_mono h ?_
  intro r hr
  refine hr.trans ?_
  simp only [Bool.false_or]
  exact decide_eq_decide.mpr hiff

/-! ## 9. target -/

theorem cl_target_loop_spec (p : types.Problem) (q : types.Plan) :
    ∀ (m : Nat) (n i : Std.Usize), q.steps.val.length - i.val ≤ m → n.val ≤ i.val →
      clauses.cl_target_loop p q n i ⦃ r =>
        r.val = n.val + (((SolverSpec.steps q).drop i.val).filter
                   (fun s => s.transform == SolverSpec.targetTr p)).length ⦄ := by
  intro m
  induction m with
  | zero =>
    intro n i hk hni
    rw [clauses.cl_target_loop.eq_def]
    dsimp only
    split
    · exfalso; scalar_tac
    · have hnil : (SolverSpec.steps q).drop i.val = [] := by
        refine List.drop_eq_nil_iff.mpr ?_
        rw [steps_length]; scalar_tac
      exact ok_spec (by rw [hnil]; simp)
  | succ m ih =>
    intro n i hk hni
    rw [clauses.cl_target_loop.eq_def]
    dsimp only
    split
    · next hlt =>
      have hi : i.val < q.steps.val.length := by scalar_tac
      have hcons := steps_drop_cons q i.val hi
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      rw [vec_index_ok q.steps i hi]
      simp only [bind_tc_ok]
      split
      · next htr =>
        have hb : ((SolverSpec.stepView (q.steps.val[i.val]'hi)).transform
            == SolverSpec.targetTr p) = true := by
          simp only [SolverSpec.stepView, SolverSpec.targetTr, beq_iff_eq]
          rw [htr]
        have hcnt : (((SolverSpec.steps q).drop i.val).filter
              (fun s => s.transform == SolverSpec.targetTr p)).length
            = (((SolverSpec.steps q).drop (i.val + 1)).filter
              (fun s => s.transform == SolverSpec.targetTr p)).length + 1 := by
          rw [hcons, List.filter_cons, if_pos hb]
          simp
        have hbnd2 : n.val + 1 ≤ Usize.max := by scalar_tac
        step as ⟨ n1, hn1 ⟩
        step as ⟨ i2, hi2 ⟩
        refine WP.spec_mono (ih n1 i2 (by omega) (by omega)) ?_
        intro r hr
        rw [hr, hi2, hcnt, hn1]
        omega
      · next htr =>
        have hb : ¬ (((SolverSpec.stepView (q.steps.val[i.val]'hi)).transform
            == SolverSpec.targetTr p) = true) := by
          simp only [SolverSpec.stepView, SolverSpec.targetTr, beq_iff_eq]
          intro hc
          exact htr (by scalar_tac)
        have hcnt : (((SolverSpec.steps q).drop i.val).filter
              (fun s => s.transform == SolverSpec.targetTr p)).length
            = (((SolverSpec.steps q).drop (i.val + 1)).filter
              (fun s => s.transform == SolverSpec.targetTr p)).length := by
          rw [hcons, List.filter_cons, if_neg hb]
        simp only [bind_tc_ok]
        step as ⟨ i2, hi2 ⟩
        refine WP.spec_mono (ih n i2 (by omega) (by omega)) ?_
        intro r hr
        rw [hr, hi2, hcnt]
    · have hnil : (SolverSpec.steps q).drop i.val = [] := by
        refine List.drop_eq_nil_iff.mpr ?_
        rw [steps_length]; scalar_tac
      exact ok_spec (by rw [hnil]; simp)

theorem cl_target_spec (p : types.Problem) (q : types.Plan) :
    clauses.cl_target p q ⦃ r => r = decide
      (((SolverSpec.steps q).filter (fun s => s.transform == SolverSpec.targetTr p)).length
        = 1) ⦄ := by
  rw [clauses.cl_target.eq_def]
  obtain ⟨n, hn, hnv⟩ := ok_of_spec (cl_target_loop_spec p q q.steps.val.length 0#usize 0#usize
    (by scalar_tac) (by scalar_tac))
  have hnL : n.val
      = ((SolverSpec.steps q).filter (fun s => s.transform == SolverSpec.targetTr p)).length := by
    simpa using hnv
  rw [hn]
  simp only [bind_tc_ok]
  refine ok_spec (decide_eq_decide.mpr ?_)
  constructor
  · intro hc
    have h1 : n.val = 1 := by scalar_tac
    omega
  · intro hc
    have h1 : n.val = 1 := by omega
    scalar_tac

/-! ## 5. uniqueProducer

The scan, not `unique_producer_at`. `NoneEmits` is the second half of the loop
invariant: it has no counterpart in the specification, and without it the scan's
`seen` counter says nothing. -/

@[reducible] def AtMostOne (e : Nat) (l : List (SolverSpec.StepView × Nat)) : Prop :=
  ∀ x ∈ l, ∀ y ∈ l, SolverSpec.Emits x.1 e → SolverSpec.Emits y.1 e → x.2 = y.2

@[reducible] def NoneEmits (e : Nat) (l : List (SolverSpec.StepView × Nat)) : Prop :=
  ∀ x ∈ l, ¬ SolverSpec.Emits x.1 e

theorem cl_unique_producer_inner_spec (q : types.Plan) (e : Std.Usize) :
    ∀ (m : Nat) (ok1 : Bool) (seen i : Std.Usize), q.steps.val.length - i.val ≤ m →
      clauses.cl_unique_producer_loop0_loop0 q ok1 e seen i ⦃ r =>
        r = (q, ok1 && decide
          (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
           (seen ≠ access.NONE →
             NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val)))) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 seen i hk
    rw [clauses.cl_unique_producer_loop0_loop0.eq_def]
    dsimp only
    split
    · next hok =>
      split
      · exfalso; scalar_tac
      · have hnil : ((SolverSpec.steps q).zipIdx).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [List.length_zipIdx, steps_length]; scalar_tac
        have hyes : AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
            (seen ≠ access.NONE →
              NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val)) := by
          rw [hnil]
          exact ⟨by simp [AtMostOne], by simp [NoneEmits]⟩
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact congrArg (Prod.mk q) (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 seen i hk
    rw [clauses.cl_unique_producer_loop0_loop0.eq_def]
    dsimp only
    split
    · next hok =>
      split
      · next hlt =>
        have hi : i.val < q.steps.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hcons := steps_zipIdx_drop_cons q i.val hi
        have hTidx : ∀ x ∈ ((SolverSpec.steps q).zipIdx).drop (i.val + 1), i.val + 1 ≤ x.2 :=
          fun x hx => le_of_mem_zipIdx_drop hx
        rw [vec_index_ok q.steps i hi]
        simp only [bind_tc_ok]
        rw [eq_of_spec (emits_spec (q.steps.val[i.val]'hi) e)]
        simp only [bind_tc_ok]
        split
        · next hem =>
          have hE : SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) e.val :=
            of_decide_eq_true hem
          split
          · next hseen =>
            have hsn : seen ≠ access.NONE := by
              intro hc
              simp [hc] at hseen
            have hno : ¬ (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                (seen ≠ access.NONE →
                  NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val))) := by
              rintro ⟨-, h2⟩
              refine h2 hsn (SolverSpec.stepView (q.steps.val[i.val]'hi), i.val) ?_ hE
              rw [hcons]; exact List.mem_cons_self
            show (do
                let i2 ← i + 1#usize
                clauses.cl_unique_producer_loop0_loop0 q false e i i2) ⦃ r =>
                  r = (q, ok1 && decide
                    (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                     (seen ≠ access.NONE →
                       NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val)))) ⦄
            step as ⟨ i2, hi2 ⟩
            refine WP.spec_mono (ih false i i2 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hok]
            simp only [Bool.false_and, Bool.true_and]
            exact congrArg (Prod.mk q) (decide_eq_false hno).symm
          · next hseen =>
            have hsn : seen = access.NONE := by
              by_contra hc
              simp [hc] at hseen
            have hiNONE : i ≠ access.NONE := index_ne_NONE q i hi
            show (do
                let i2 ← i + 1#usize
                clauses.cl_unique_producer_loop0_loop0 q true e i i2) ⦃ r =>
                  r = (q, ok1 && decide
                    (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                     (seen ≠ access.NONE →
                       NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val)))) ⦄
            step as ⟨ i2, hi2 ⟩
            have hiff :
                (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i2.val) ∧
                  (i ≠ access.NONE →
                    NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i2.val)))
                ↔ (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                    (seen ≠ access.NONE →
                      NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val))) := by
              rw [hi2, hcons]
              constructor
              · rintro ⟨-, h2⟩
                have hno := h2 hiNONE
                refine ⟨?_, ?_⟩
                · intro x hx y hy hEx hEy
                  rcases List.mem_cons.mp hx with hx' | hx'
                  · rcases List.mem_cons.mp hy with hy' | hy'
                    · rw [hx', hy']
                    · exact absurd hEy (hno y hy')
                  · exact absurd hEx (hno x hx')
                · intro hc
                  exact absurd hsn hc
              · rintro ⟨h1, -⟩
                refine ⟨?_, ?_⟩
                · intro x hx y hy hEx hEy
                  exact h1 x (List.mem_cons_of_mem _ hx) y (List.mem_cons_of_mem _ hy) hEx hEy
                · intro _ x hx hEx
                  have hxy := h1 (SolverSpec.stepView (q.steps.val[i.val]'hi), i.val)
                    List.mem_cons_self x (List.mem_cons_of_mem _ hx) hE hEx
                  have hxi := hTidx x hx
                  omega
            refine WP.spec_mono (ih true i i2 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hok]
            simp only [Bool.true_and]
            exact congrArg (Prod.mk q) (decide_eq_decide.mpr hiff)
        · next hem =>
          have hE : ¬ SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) e.val :=
            fun hc => hem (decide_eq_true hc)
          show (do
              let i2 ← i + 1#usize
              clauses.cl_unique_producer_loop0_loop0 q true e seen i2) ⦃ r =>
                r = (q, ok1 && decide
                  (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                   (seen ≠ access.NONE →
                     NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val)))) ⦄
          step as ⟨ i2, hi2 ⟩
          have hiff :
              (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i2.val) ∧
                (seen ≠ access.NONE →
                  NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i2.val)))
              ↔ (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                  (seen ≠ access.NONE →
                    NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val))) := by
            rw [hi2, hcons]
            constructor
            · rintro ⟨h1, h2⟩
              refine ⟨?_, ?_⟩
              · intro x hx y hy hEx hEy
                rcases List.mem_cons.mp hx with hx' | hx'
                · rw [hx'] at hEx; exact absurd hEx hE
                · rcases List.mem_cons.mp hy with hy' | hy'
                  · rw [hy'] at hEy; exact absurd hEy hE
                  · exact h1 x hx' y hy' hEx hEy
              · intro hsn x hx hEx
                rcases List.mem_cons.mp hx with hx' | hx'
                · rw [hx'] at hEx; exact absurd hEx hE
                · exact h2 hsn x hx' hEx
            · rintro ⟨h1, h2⟩
              refine ⟨?_, ?_⟩
              · intro x hx y hy hEx hEy
                exact h1 x (List.mem_cons_of_mem _ hx) y (List.mem_cons_of_mem _ hy) hEx hEy
              · intro hsn x hx hEx
                exact h2 hsn x (List.mem_cons_of_mem _ hx) hEx
          refine WP.spec_mono (ih true seen i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hok]
          simp only [Bool.true_and]
          exact congrArg (Prod.mk q) (decide_eq_decide.mpr hiff)
      · have hnil : ((SolverSpec.steps q).zipIdx).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [List.length_zipIdx, steps_length]; scalar_tac
        have hyes : AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
            (seen ≠ access.NONE →
              NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val)) := by
          rw [hnil]
          exact ⟨by simp [AtMostOne], by simp [NoneEmits]⟩
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact congrArg (Prod.mk q) (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)

theorem cl_unique_producer_outer_spec (q : types.Plan) (ne : Std.Usize) :
    ∀ (m : Nat) (ok1 : Bool) (e : Std.Usize), ne.val - e.val ≤ m →
      clauses.cl_unique_producer_loop0 q ne ok1 e ⦃ r =>
        r = (ok1 && decide (∀ e' < ne.val, e.val ≤ e' →
              AtMostOne e' ((SolverSpec.steps q).zipIdx))) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 e hk
    rw [clauses.cl_unique_producer_loop0.eq_def]
    split
    · next hok =>
      split
      · exfalso; scalar_tac
      · have hyes : ∀ e' < ne.val, e.val ≤ e' → AtMostOne e' ((SolverSpec.steps q).zipIdx) := by
          intro e' h1 h2
          exfalso; scalar_tac
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 e hk
    rw [clauses.cl_unique_producer_loop0.eq_def]
    split
    · next hok =>
      split
      · next hlt =>
        have hen : e.val < ne.val := by scalar_tac
        obtain ⟨v, hv, hvP⟩ := ok_of_spec (cl_unique_producer_inner_spec q e
          q.steps.val.length true access.NONE 0#usize (by scalar_tac))
        obtain ⟨vq, vb⟩ := v
        rw [Prod.mk.injEq] at hvP
        obtain ⟨hv1, hv2⟩ := hvP
        have hbnd : e.val + 1 ≤ Usize.max := by scalar_tac
        rw [hv]
        show (do
            let e1 ← e + 1#usize
            clauses.cl_unique_producer_loop0 vq ne vb e1) ⦃ r =>
              r = (ok1 && decide (∀ e' < ne.val, e.val ≤ e' →
                    AtMostOne e' ((SolverSpec.steps q).zipIdx))) ⦄
        rw [hv1]
        step as ⟨ e1, he1 ⟩
        have hd0 : ((SolverSpec.steps q).zipIdx).drop (0#usize).val
            = (SolverSpec.steps q).zipIdx := List.drop_zero
        have hiff : (∀ e' < ne.val, e.val ≤ e' → AtMostOne e' ((SolverSpec.steps q).zipIdx))
            ↔ ((AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop (0#usize).val) ∧
                  (access.NONE ≠ access.NONE →
                    NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop (0#usize).val)))
                ∧ (∀ e' < ne.val, e1.val ≤ e' →
                    AtMostOne e' ((SolverSpec.steps q).zipIdx))) := by
          rw [hd0, he1]
          constructor
          · intro h
            exact ⟨⟨h e.val hen (Nat.le_refl _), fun hc => absurd rfl hc⟩,
                   fun e' h1 h2 => h e' h1 (by omega)⟩
          · rintro ⟨⟨hA, -⟩, hB⟩ e' h1 h2
            rcases Nat.eq_or_lt_of_le h2 with heq | hlt2
            · rw [← heq]; exact hA
            · exact hB e' h1 (by omega)
        refine WP.spec_mono (ih vb e1 (by omega)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hv2, hok]
        simp only [Bool.true_and]
        exact decide_and_of_iff hiff
      · have hyes : ∀ e' < ne.val, e.val ≤ e' → AtMostOne e' ((SolverSpec.steps q).zipIdx) := by
          intro e' h1 h2
          exfalso; scalar_tac
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)

theorem cl_unique_producer_spec (q : types.Plan) :
    clauses.cl_unique_producer q ⦃ r => r = decide
      (∀ e < SolverSpec.nEndpoints q, ∀ i ∈ (SolverSpec.steps q).zipIdx,
        ∀ j ∈ (SolverSpec.steps q).zipIdx,
          SolverSpec.Emits i.1 e → SolverSpec.Emits j.1 e → i.2 = j.2) ⦄ := by
  rw [clauses.cl_unique_producer.eq_def]
  unfold access.n_endpoints
  simp only [bind_tc_ok]
  have h := cl_unique_producer_outer_spec q (alloc.vec.Vec.len q.endpoints)
    (alloc.vec.Vec.len q.endpoints).val true 0#usize (by scalar_tac)
  have hne : (alloc.vec.Vec.len q.endpoints).val = SolverSpec.nEndpoints q := by
    simp [SolverSpec.nEndpoints]
  have hiff : (∀ e' < (alloc.vec.Vec.len q.endpoints).val, (0#usize).val ≤ e' →
        AtMostOne e' ((SolverSpec.steps q).zipIdx))
      ↔ (∀ e < SolverSpec.nEndpoints q, ∀ i ∈ (SolverSpec.steps q).zipIdx,
          ∀ j ∈ (SolverSpec.steps q).zipIdx,
            SolverSpec.Emits i.1 e → SolverSpec.Emits j.1 e → i.2 = j.2) := by
    rw [hne]
    constructor
    · intro hh e he i hi j hj hEi hEj
      exact hh e he (Nat.zero_le _) i hi j hj hEi hEj
    · intro hh e he _
      exact hh e he
  refine WP.spec_mono h ?_
  intro r hr
  refine hr.trans ?_
  simp only [Bool.true_and]
  exact decide_eq_decide.mpr hiff

/-! ## 6. provenance

The checker asks `is_given` FIRST and only then looks for an emitting step; the
specification lists the disjuncts the other way round. `provenance_at_spec` is
where the commutation is done, once. -/

@[reducible] def ProvOK (q : types.Plan) (e : Nat) : Prop :=
  (∃ s' ∈ SolverSpec.steps q, SolverSpec.Emits s' e) ∨
  (∃ gn ∈ SolverSpec.givens q, gn.1 = e)

theorem is_given_loop_spec (q : types.Plan) (e : Std.Usize) :
    ∀ (m : Nat) (hit : Bool) (i : Std.Usize), (SolverSpec.givens q).length - i.val ≤ m →
      clauses.is_given_loop q e hit i ⦃ r =>
        r = (hit || decide (∃ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 = e.val)) ⦄ := by
  have hgl : (SolverSpec.givens q).length = q.givens.val.length := givens_length q
  intro m
  induction m with
  | zero =>
    intro hit i hk
    rw [clauses.is_given_loop.eq_def]
    dsimp only
    split
    · next hh => exact ok_spec (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.givens q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hno : ¬ (∃ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 = e.val) := by
          rw [hnil]; simp
        refine ok_spec ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact (decide_eq_false hno).symm
  | succ m ih =>
    intro hit i hk
    rw [clauses.is_given_loop.eq_def]
    dsimp only
    split
    · next hh => exact ok_spec (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · next hlt =>
        have hi : i.val < q.givens.val.length := by scalar_tac
        obtain ⟨a, bb, hab⟩ : ∃ a bb, q.givens.val[i.val]'hi = (a, bb) := ⟨_, _, rfl⟩
        have hcons : (SolverSpec.givens q).drop i.val
            = (a.val, bb.val) :: (SolverSpec.givens q).drop (i.val + 1) := by
          rw [givens_drop_cons q i.val hi, hab]
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        rw [vec_index_ok q.givens i hi, hab]
        show (do
            let hit1 ← if a = e then ok true else ok false
            let i3 ← i + 1#usize
            clauses.is_given_loop q e hit1 i3) ⦃ r =>
              r = (hit || decide (∃ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 = e.val)) ⦄
        split
        · next heq =>
          have hyes : ∃ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 = e.val := by
            rw [hcons]
            exact ⟨(a.val, bb.val), List.mem_cons_self, by rw [heq]⟩
          simp only [bind_tc_ok]
          step as ⟨ i3, hi3 ⟩
          refine WP.spec_mono (ih true i3 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.true_or, Bool.false_or]
          exact (decide_eq_true hyes).symm
        · next hne =>
          have hane : ¬ (a.val = e.val) := by
            intro hc; exact hne (by scalar_tac)
          simp only [bind_tc_ok]
          step as ⟨ i3, hi3 ⟩
          have hiff : (∃ gn ∈ (SolverSpec.givens q).drop i3.val, gn.1 = e.val)
              ↔ (∃ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 = e.val) := by
            rw [hi3, hcons]
            constructor
            · rintro ⟨x, hx, hx2⟩
              exact ⟨x, List.mem_cons_of_mem _ hx, hx2⟩
            · rintro ⟨x, hx, hx2⟩
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx'] at hx2; exact absurd hx2 hane
              · exact ⟨x, hx', hx2⟩
          refine WP.spec_mono (ih false i3 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.givens q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hno : ¬ (∃ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 = e.val) := by
          rw [hnil]; simp
        refine ok_spec ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact (decide_eq_false hno).symm

theorem is_given_spec (q : types.Plan) (e : Std.Usize) :
    clauses.is_given q e ⦃ r => r = decide (∃ gn ∈ SolverSpec.givens q, gn.1 = e.val) ⦄ := by
  have h := is_given_loop_spec q e (SolverSpec.givens q).length false 0#usize (by scalar_tac)
  have hd0 : (SolverSpec.givens q).drop (0#usize).val = SolverSpec.givens q := List.drop_zero
  have hiff : (∃ gn ∈ (SolverSpec.givens q).drop (0#usize).val, gn.1 = e.val)
      ↔ (∃ gn ∈ SolverSpec.givens q, gn.1 = e.val) := by rw [hd0]
  refine WP.spec_mono h ?_
  intro r hr
  refine hr.trans ?_
  simp only [Bool.false_or]
  exact decide_eq_decide.mpr hiff

theorem provenance_at_loop_spec (q : types.Plan) (e : Std.Usize) :
    ∀ (m : Nat) (found : Bool) (i : Std.Usize), q.steps.val.length - i.val ≤ m →
      clauses.provenance_at_loop q.steps e found i ⦃ r =>
        r = (found || decide (∃ s' ∈ (SolverSpec.steps q).drop i.val,
              SolverSpec.Emits s' e.val)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro found i hk
    rw [clauses.provenance_at_loop.eq_def]
    dsimp only
    split
    · next hh => exact ok_spec (by rw [hh]; simp)
    · next hh =>
      have hhf : found = false := by simpa using hh
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_length]; scalar_tac
        have hno : ¬ (∃ s' ∈ (SolverSpec.steps q).drop i.val, SolverSpec.Emits s' e.val) := by
          rw [hnil]; simp
        refine ok_spec ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact (decide_eq_false hno).symm
  | succ m ih =>
    intro found i hk
    rw [clauses.provenance_at_loop.eq_def]
    dsimp only
    split
    · next hh => exact ok_spec (by rw [hh]; simp)
    · next hh =>
      have hhf : found = false := by simpa using hh
      split
      · next hlt =>
        have hi : i.val < q.steps.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hcons := steps_drop_cons q i.val hi
        rw [vec_index_ok q.steps i hi]
        simp only [bind_tc_ok]
        rw [eq_of_spec (emits_spec (q.steps.val[i.val]'hi) e)]
        simp only [bind_tc_ok]
        split
        · next hem =>
          have hE : SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) e.val :=
            of_decide_eq_true hem
          have hyes : ∃ s' ∈ (SolverSpec.steps q).drop i.val, SolverSpec.Emits s' e.val := by
            rw [hcons]
            exact ⟨_, List.mem_cons_self, hE⟩
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih true i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.true_or, Bool.false_or]
          exact (decide_eq_true hyes).symm
        · next hem =>
          have hE : ¬ SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) e.val :=
            fun hc => hem (decide_eq_true hc)
          step as ⟨ i2, hi2 ⟩
          have hiff : (∃ s' ∈ (SolverSpec.steps q).drop i2.val, SolverSpec.Emits s' e.val)
              ↔ (∃ s' ∈ (SolverSpec.steps q).drop i.val, SolverSpec.Emits s' e.val) := by
            rw [hi2, hcons]
            constructor
            · rintro ⟨x, hx, hx2⟩
              exact ⟨x, List.mem_cons_of_mem _ hx, hx2⟩
            · rintro ⟨x, hx, hx2⟩
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx'] at hx2; exact absurd hx2 hE
              · exact ⟨x, hx', hx2⟩
          refine WP.spec_mono (ih false i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_length]; scalar_tac
        have hno : ¬ (∃ s' ∈ (SolverSpec.steps q).drop i.val, SolverSpec.Emits s' e.val) := by
          rw [hnil]; simp
        refine ok_spec ?_
        rw [hhf]
        simp only [Bool.false_or]
        exact (decide_eq_false hno).symm

/-- The pointwise judgement, and the one place the disjuncts are commuted. The
binding's endpoint is a PARAMETER: naming it in the proof would not do, because
it appears under `decide` and no rewrite reaches there. -/
theorem provenance_at_spec (q : types.Plan) (si bi : Std.Usize) (a ee : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (hbi : bi.val < (q.steps.val[si.val]'hsi).used.val.length)
    (hab : (q.steps.val[si.val]'hsi).used.val[bi.val]'hbi = (a, ee)) :
    clauses.provenance_at q si bi ⦃ r => r = decide (ProvOK q ee.val) ⦄ := by
  rw [clauses.provenance_at.eq_def]
  dsimp only
  split
  · next h1 =>
    rw [vec_index_ok q.steps si hsi]
    simp only [bind_tc_ok]
    split
    · next h2 =>
      rw [vec_index_ok _ bi hbi, hab]
      show (do
          let found ← clauses.is_given q ee
          clauses.provenance_at_loop q.steps ee found 0#usize) ⦃ r =>
            r = decide (ProvOK q ee.val) ⦄
      rw [eq_of_spec (is_given_spec q ee)]
      simp only [bind_tc_ok]
      have hloop := provenance_at_loop_spec q ee q.steps.val.length
        (decide (∃ gn ∈ SolverSpec.givens q, gn.1 = ee.val)) 0#usize (by scalar_tac)
      have hd0 : (SolverSpec.steps q).drop (0#usize).val = SolverSpec.steps q := List.drop_zero
      have hiff : ProvOK q ee.val
          ↔ ((∃ gn ∈ SolverSpec.givens q, gn.1 = ee.val)
              ∨ (∃ s' ∈ (SolverSpec.steps q).drop (0#usize).val,
                  SolverSpec.Emits s' ee.val)) := by
        rw [hd0]
        exact Or.comm
      refine WP.spec_mono hloop ?_
      intro r hr
      refine hr.trans ?_
      exact decide_or_of_iff hiff
    · next h2 => exfalso; scalar_tac
  · next h1 => exfalso; scalar_tac

theorem cl_provenance_inner_spec (q : types.Plan) (i : Std.Usize)
    (hi : i.val < q.steps.val.length) (used : List (Nat × Nat))
    (hused : SolverSpec.idPairs (q.steps.val[i.val]'hi).used = used) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), used.length - j.val ≤ m →
      clauses.cl_provenance_loop0_loop0 q.endpoints q.givens q.steps ok1 i j ⦃ r =>
        r = (ok1 && decide (∀ b ∈ used.drop j.val, ProvOK q b.2)) ⦄ := by
  have hul : used.length = (q.steps.val[i.val]'hi).used.val.length := by
    rw [← hused, idPairs_length]
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.cl_provenance_loop0_loop0.eq_def]
    split
    · next hok =>
      rw [vec_index_ok q.steps i hi]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : used.drop j.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ b ∈ used.drop j.val, ProvOK q b.2 := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.cl_provenance_loop0_loop0.eq_def]
    split
    · next hok =>
      rw [vec_index_ok q.steps i hi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hj : j.val < (q.steps.val[i.val]'hi).used.val.length := by scalar_tac
        obtain ⟨a, ee, hab⟩ :
            ∃ a ee, (q.steps.val[i.val]'hi).used.val[j.val]'hj = (a, ee) := ⟨_, _, rfl⟩
        have hcons : used.drop j.val = (a.val, ee.val) :: used.drop (j.val + 1) := by
          rw [← hused, idPairs_drop_cons _ j.val hj, hab]
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        rw [eq_of_spec (provenance_at_spec q i j a ee hi hj hab)]
        simp only [bind_tc_ok]
        split
        · next hpv =>
          have hP : ProvOK q ee.val := of_decide_eq_true hpv
          step as ⟨ j1, hj1 ⟩
          have hiff : (∀ b ∈ used.drop j1.val, ProvOK q b.2)
              ↔ (∀ b ∈ used.drop j.val, ProvOK q b.2) := by
            rw [hj1, hcons]
            constructor
            · intro h x hx
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx']; exact hP
              · exact h x hx'
            · intro h x hx
              exact h x (List.mem_cons_of_mem _ hx)
          refine WP.spec_mono (ih true j1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hok]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hpv =>
          have hP : ¬ ProvOK q ee.val := fun hc => hpv (decide_eq_true hc)
          have hno : ¬ (∀ b ∈ used.drop j.val, ProvOK q b.2) := by
            intro h
            exact hP (h (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self))
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hok]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hno).symm
      · have hnil : used.drop j.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ b ∈ used.drop j.val, ProvOK q b.2 := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)

theorem cl_provenance_outer_spec (q : types.Plan) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), q.steps.val.length - i.val ≤ m →
      clauses.cl_provenance_loop0 q ok1 i ⦃ r =>
        r = (ok1 && decide (∀ s ∈ (SolverSpec.steps q).drop i.val,
              ∀ b ∈ s.used, ProvOK q b.2)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_provenance_loop0.eq_def]
    dsimp only
    split
    · next hok =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_length]; scalar_tac
        have hyes : ∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ b ∈ s.used, ProvOK q b.2 := by
          rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_provenance_loop0.eq_def]
    dsimp only
    split
    · next hok =>
      split
      · next hlt =>
        have hi : i.val < q.steps.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hcons := steps_drop_cons q i.val hi
        have hin := eq_of_spec (cl_provenance_inner_spec q i hi
          (SolverSpec.idPairs (q.steps.val[i.val]'hi).used) rfl
          (SolverSpec.idPairs (q.steps.val[i.val]'hi).used).length true 0#usize (by scalar_tac))
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ i2, hi2 ⟩
        have hd0 : (SolverSpec.idPairs (q.steps.val[i.val]'hi).used).drop (0#usize).val
            = SolverSpec.idPairs (q.steps.val[i.val]'hi).used := List.drop_zero
        have hiff : (∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ b ∈ s.used, ProvOK q b.2)
            ↔ ((∀ b ∈ (SolverSpec.idPairs (q.steps.val[i.val]'hi).used).drop (0#usize).val,
                  ProvOK q b.2)
                ∧ (∀ s ∈ (SolverSpec.steps q).drop i2.val, ∀ b ∈ s.used, ProvOK q b.2)) := by
          rw [hd0, hi2, hcons]
          constructor
          · intro h
            exact ⟨h _ List.mem_cons_self, fun x hx => h x (List.mem_cons_of_mem _ hx)⟩
          · rintro ⟨hA, hB⟩ x hx
            rcases List.mem_cons.mp hx with hx' | hx'
            · rw [hx']; exact hA
            · exact hB x hx'
        refine WP.spec_mono (ih _ i2 (by omega)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hok]
        simp only [Bool.true_and]
        exact decide_and_of_iff hiff
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_length]; scalar_tac
        have hyes : ∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ b ∈ s.used, ProvOK q b.2 := by
          rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)

theorem cl_provenance_spec (q : types.Plan) :
    clauses.cl_provenance q ⦃ r => r = decide
      (∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used,
        (∃ s' ∈ SolverSpec.steps q, SolverSpec.Emits s' b.2) ∨
        (∃ gn ∈ SolverSpec.givens q, gn.1 = b.2)) ⦄ := by
  rw [clauses.cl_provenance.eq_def]
  have h := cl_provenance_outer_spec q q.steps.val.length true 0#usize (by scalar_tac)
  have hd0 : (SolverSpec.steps q).drop (0#usize).val = SolverSpec.steps q := List.drop_zero
  have hiff : (∀ s ∈ (SolverSpec.steps q).drop (0#usize).val, ∀ b ∈ s.used, ProvOK q b.2)
      ↔ (∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used,
          (∃ s' ∈ SolverSpec.steps q, SolverSpec.Emits s' b.2) ∨
          (∃ gn ∈ SolverSpec.givens q, gn.1 = b.2)) := by
    rw [hd0]
  refine WP.spec_mono h ?_
  intro r hr
  refine hr.trans ?_
  simp only [Bool.true_and]
  exact decide_eq_decide.mpr hiff

/-! ## 8. schedulable -/

/-- The producer step is a PARAMETER, for the reason `provenance_at_spec` gives
about the binding's endpoint. -/
theorem schedulable_at_spec (q : types.Plan) (cj bi pi : Std.Usize) (a ee : Std.Usize)
    (sv : SolverSpec.StepView)
    (hcj : cj.val < q.steps.val.length) (hpi : pi.val < q.steps.val.length)
    (hbi : bi.val < (q.steps.val[cj.val]'hcj).used.val.length)
    (hab : (q.steps.val[cj.val]'hcj).used.val[bi.val]'hbi = (a, ee))
    (hsv : SolverSpec.stepView (q.steps.val[pi.val]'hpi) = sv) :
    clauses.schedulable_at q cj bi pi ⦃ r => r = decide
      (SolverSpec.Emits sv ee.val → pi.val < cj.val) ⦄ := by
  rw [clauses.schedulable_at.eq_def]
  dsimp only
  split
  · next h1 =>
    split
    · next h2 =>
      rw [vec_index_ok q.steps cj hcj]
      simp only [bind_tc_ok]
      split
      · next h3 =>
        rw [vec_index_ok q.steps pi hpi]
        simp only [bind_tc_ok]
        rw [vec_index_ok _ bi hbi, hab]
        show (do
            let b ← clauses.emits (q.steps.val[pi.val]'hpi) ee
            if b = true then ok (decide (pi < cj)) else ok true) ⦃ r =>
              r = decide (SolverSpec.Emits sv ee.val → pi.val < cj.val) ⦄
        rw [eq_of_spec (emits_spec (q.steps.val[pi.val]'hpi) ee)]
        simp only [bind_tc_ok]
        split
        · next hem =>
          have hE : SolverSpec.Emits sv ee.val := by
            rw [← hsv]; exact of_decide_eq_true hem
          refine ok_spec (decide_eq_decide.mpr ?_)
          constructor
          · intro hlt _
            scalar_tac
          · intro h
            have hlt := h hE
            scalar_tac
        · next hem =>
          have hE : ¬ SolverSpec.Emits sv ee.val := by
            rw [← hsv]
            exact fun hc => hem (decide_eq_true hc)
          exact ok_spec (decide_eq_true (fun hc => absurd hc hE)).symm
      · next h3 => exfalso; scalar_tac
    · next h2 => exfalso; scalar_tac
  · next h1 => exfalso; scalar_tac

/-- The innermost loop: the producers still to check against one binding. -/
theorem cl_schedulable_inner_spec (q : types.Plan) (j b : Std.Usize) (a ee : Std.Usize)
    (hj : j.val < q.steps.val.length)
    (hb : b.val < (q.steps.val[j.val]'hj).used.val.length)
    (hab : (q.steps.val[j.val]'hj).used.val[b.val]'hb = (a, ee)) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), q.steps.val.length - i.val ≤ m →
      clauses.cl_schedulable_loop0_loop0_loop0 q.endpoints q.givens q.steps ok1 j b i ⦃ r =>
        r = (ok1 && decide (∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i.val,
              SolverSpec.Emits x.1 ee.val → x.2 < j.val)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_schedulable_loop0_loop0_loop0.eq_def]
    dsimp only
    split
    · next hok =>
      split
      · exfalso; scalar_tac
      · have hnil : ((SolverSpec.steps q).zipIdx).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [List.length_zipIdx, steps_length]; scalar_tac
        have hyes : ∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i.val,
            SolverSpec.Emits x.1 ee.val → x.2 < j.val := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.cl_schedulable_loop0_loop0_loop0.eq_def]
    dsimp only
    split
    · next hok =>
      split
      · next hlt =>
        have hi : i.val < q.steps.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hcons := steps_zipIdx_drop_cons q i.val hi
        rw [eq_of_spec (schedulable_at_spec q j b i a ee
          (SolverSpec.stepView (q.steps.val[i.val]'hi)) hj hi hb hab rfl)]
        simp only [bind_tc_ok]
        split
        · next hsa =>
          have hS : SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) ee.val
              → i.val < j.val := of_decide_eq_true hsa
          step as ⟨ i2, hi2 ⟩
          have hiff : (∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i2.val,
                SolverSpec.Emits x.1 ee.val → x.2 < j.val)
              ↔ (∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i.val,
                SolverSpec.Emits x.1 ee.val → x.2 < j.val) := by
            rw [hi2, hcons]
            constructor
            · intro h x hx
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx']; exact hS
              · exact h x hx'
            · intro h x hx
              exact h x (List.mem_cons_of_mem _ hx)
          refine WP.spec_mono (ih true i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hok]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr (by simpa only [SolverSpec.Emits, exists_imp, and_imp] using hiff)
        · next hsa =>
          have hS : ¬ (SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) ee.val
              → i.val < j.val) := fun hc => hsa (decide_eq_true hc)
          have hno : ¬ (∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i.val,
              SolverSpec.Emits x.1 ee.val → x.2 < j.val) := by
            intro h
            exact hS (h _ (by rw [hcons]; exact List.mem_cons_self))
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hok]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false (by simpa [SolverSpec.Emits] using hno)).symm
      · have hnil : ((SolverSpec.steps q).zipIdx).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [List.length_zipIdx, steps_length]; scalar_tac
        have hyes : ∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i.val,
            SolverSpec.Emits x.1 ee.val → x.2 < j.val := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)

theorem cl_schedulable_middle_spec (q : types.Plan) (j : Std.Usize)
    (hj : j.val < q.steps.val.length) (used : List (Nat × Nat))
    (hused : SolverSpec.idPairs (q.steps.val[j.val]'hj).used = used) :
    ∀ (m : Nat) (ok1 : Bool) (b : Std.Usize), used.length - b.val ≤ m →
      clauses.cl_schedulable_loop0_loop0 q.endpoints q.givens q.steps ok1 j b ⦃ r =>
        r = (ok1 && decide (∀ bnd ∈ used.drop b.val,
              ∀ x ∈ (SolverSpec.steps q).zipIdx,
                SolverSpec.Emits x.1 bnd.2 → x.2 < j.val)) ⦄ := by
  have hul : used.length = (q.steps.val[j.val]'hj).used.val.length := by
    rw [← hused, idPairs_length]
  intro m
  induction m with
  | zero =>
    intro ok1 b hk
    rw [clauses.cl_schedulable_loop0_loop0.eq_def]
    split
    · next hok =>
      rw [vec_index_ok q.steps j hj]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : used.drop b.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ bnd ∈ used.drop b.val, ∀ x ∈ (SolverSpec.steps q).zipIdx,
            SolverSpec.Emits x.1 bnd.2 → x.2 < j.val := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 b hk
    rw [clauses.cl_schedulable_loop0_loop0.eq_def]
    split
    · next hok =>
      rw [vec_index_ok q.steps j hj]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hb : b.val < (q.steps.val[j.val]'hj).used.val.length := by scalar_tac
        obtain ⟨a, ee, hab⟩ :
            ∃ a ee, (q.steps.val[j.val]'hj).used.val[b.val]'hb = (a, ee) := ⟨_, _, rfl⟩
        have hcons : used.drop b.val = (a.val, ee.val) :: used.drop (b.val + 1) := by
          rw [← hused, idPairs_drop_cons _ b.val hb, hab]
        have hbnd : b.val + 1 ≤ Usize.max := by scalar_tac
        have hin := eq_of_spec (cl_schedulable_inner_spec q j b a ee hj hb hab
          q.steps.val.length true 0#usize (by scalar_tac))
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ b1, hb1 ⟩
        have hd0 : ((SolverSpec.steps q).zipIdx).drop (0#usize).val
            = (SolverSpec.steps q).zipIdx := List.drop_zero
        have hiff : (∀ bnd ∈ used.drop b.val, ∀ x ∈ (SolverSpec.steps q).zipIdx,
              SolverSpec.Emits x.1 bnd.2 → x.2 < j.val)
            ↔ ((∀ x ∈ ((SolverSpec.steps q).zipIdx).drop (0#usize).val,
                  SolverSpec.Emits x.1 ee.val → x.2 < j.val)
                ∧ (∀ bnd ∈ used.drop b1.val, ∀ x ∈ (SolverSpec.steps q).zipIdx,
                  SolverSpec.Emits x.1 bnd.2 → x.2 < j.val)) := by
          rw [hd0, hb1, hcons]
          constructor
          · intro h
            exact ⟨h _ List.mem_cons_self, fun y hy => h y (List.mem_cons_of_mem _ hy)⟩
          · rintro ⟨hA, hB⟩ y hy
            rcases List.mem_cons.mp hy with hy' | hy'
            · rw [hy']; exact hA
            · exact hB y hy'
        refine WP.spec_mono (ih _ b1 (by omega)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hok]
        simp only [Bool.true_and]
        exact decide_and_of_iff (by simpa only [SolverSpec.Emits, exists_imp, and_imp] using hiff)
      · have hnil : used.drop b.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ bnd ∈ used.drop b.val, ∀ x ∈ (SolverSpec.steps q).zipIdx,
            SolverSpec.Emits x.1 bnd.2 → x.2 < j.val := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)

theorem cl_schedulable_outer_spec (q : types.Plan) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), q.steps.val.length - j.val ≤ m →
      clauses.cl_schedulable_loop0 q ok1 j ⦃ r =>
        r = (ok1 && decide (∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop j.val,
              ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
                SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.cl_schedulable_loop0.eq_def]
    dsimp only
    split
    · next hok =>
      split
      · exfalso; scalar_tac
      · have hnil : ((SolverSpec.steps q).zipIdx).drop j.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [List.length_zipIdx, steps_length]; scalar_tac
        have hyes : ∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop j.val,
            ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
              SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2 := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.cl_schedulable_loop0.eq_def]
    dsimp only
    split
    · next hok =>
      split
      · next hlt =>
        have hj : j.val < q.steps.val.length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hcons := steps_zipIdx_drop_cons q j.val hj
        have hin := eq_of_spec (cl_schedulable_middle_spec q j hj
          (SolverSpec.idPairs (q.steps.val[j.val]'hj).used) rfl
          (SolverSpec.idPairs (q.steps.val[j.val]'hj).used).length true 0#usize (by scalar_tac))
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ j1, hj1 ⟩
        have hd0 : (SolverSpec.idPairs (q.steps.val[j.val]'hj).used).drop (0#usize).val
            = SolverSpec.idPairs (q.steps.val[j.val]'hj).used := List.drop_zero
        have hiff : (∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop j.val,
              ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
                SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2)
            ↔ ((∀ bnd ∈ (SolverSpec.idPairs (q.steps.val[j.val]'hj).used).drop (0#usize).val,
                  ∀ x ∈ (SolverSpec.steps q).zipIdx,
                    SolverSpec.Emits x.1 bnd.2 → x.2 < j.val)
                ∧ (∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop j1.val,
                  ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
                    SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2)) := by
          rw [hd0, hj1, hcons]
          constructor
          · intro h
            exact ⟨h _ List.mem_cons_self, fun y hy => h y (List.mem_cons_of_mem _ hy)⟩
          · rintro ⟨hA, hB⟩ y hy
            rcases List.mem_cons.mp hy with hy' | hy'
            · rw [hy']; exact hA
            · exact hB y hy'
        refine WP.spec_mono (ih _ j1 (by omega)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hok]
        simp only [Bool.true_and]
        exact decide_and_of_iff (by simpa only [SolverSpec.Emits, exists_imp, and_imp] using hiff)
      · have hnil : ((SolverSpec.steps q).zipIdx).drop j.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [List.length_zipIdx, steps_length]; scalar_tac
        have hyes : ∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop j.val,
            ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
              SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2 := by rw [hnil]; simp
        refine ok_spec ?_
        rw [hok]
        simp only [Bool.true_and]
        exact (decide_eq_true hyes).symm
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)

theorem cl_schedulable_spec (q : types.Plan) :
    clauses.cl_schedulable q ⦃ r => r = decide
      (∀ cj ∈ (SolverSpec.steps q).zipIdx, ∀ b ∈ cj.1.used,
        ∀ pi ∈ (SolverSpec.steps q).zipIdx, SolverSpec.Emits pi.1 b.2 → pi.2 < cj.2) ⦄ := by
  rw [clauses.cl_schedulable.eq_def]
  have h := cl_schedulable_outer_spec q q.steps.val.length true 0#usize (by scalar_tac)
  have hd0 : ((SolverSpec.steps q).zipIdx).drop (0#usize).val
      = (SolverSpec.steps q).zipIdx := List.drop_zero
  have hiff : (∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop (0#usize).val,
        ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
          SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2)
      ↔ (∀ cj ∈ (SolverSpec.steps q).zipIdx, ∀ b ∈ cj.1.used,
          ∀ pi ∈ (SolverSpec.steps q).zipIdx, SolverSpec.Emits pi.1 b.2 → pi.2 < cj.2) := by
    rw [hd0]
  refine WP.spec_mono h ?_
  intro r hr
  refine hr.trans ?_
  simp only [Bool.true_and]
  exact decide_eq_decide.mpr hiff

#print axioms SolverProof.emits_spec
#print axioms SolverProof.cl_target_spec
#print axioms SolverProof.cl_unique_producer_spec
#print axioms SolverProof.cl_provenance_spec
#print axioms SolverProof.cl_schedulable_spec

end SolverProof
