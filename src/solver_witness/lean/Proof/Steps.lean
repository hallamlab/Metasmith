/-
  Four clauses over the STEP LIST: `target`, `uniqueProducer`, `provenance` and
  `schedulable`. Read `Proof/Basis.lean` first for the loop recipe; what follows
  is what a clause whose conjunct quantifies over `List.zipIdx` needs on top of
  it, plus three traps that cost this file most of its builds.

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

  ## Three traps, all of them consequences of Basis's second warning

  `alloc.vec.Vec α` is a `def` for a subtype, so a term containing `v.val` is
  type-correct only once `Vec` unfolds -- which does not happen at `instances`
  transparency. Everything below follows from that one fact.

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
-/

import SolverWitness.Types
import SolverWitness.Funs
import SolverWitness.Spec
import SolverWitness.Proof.Basis

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
        exact ok_spec (by rw [hhf, hnil]; simp)
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
          refine WP.spec_mono (ih true k1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hhf, decide_eq_true hyes]
          simp
        · next hne =>
          have hane : ¬ (ee.val = e.val) := by
            intro hc; exact hne (by scalar_tac)
          have hiff : (∃ b ∈ row.drop (k.val + 1), b.2 = e.val)
              ↔ (∃ b ∈ row.drop k.val, b.2 = e.val) := by
            rw [hcons]
            constructor
            · rintro ⟨x, hx, hx2⟩
              exact ⟨x, List.mem_cons_of_mem _ hx, hx2⟩
            · rintro ⟨x, hx, hx2⟩
              rcases List.mem_cons.mp hx with hx' | hx'
              · exact absurd (hx' ▸ hx2) hane
              · exact ⟨x, hx', hx2⟩
          simp only [bind_tc_ok]
          step as ⟨ k1, hk1' ⟩
          have hk1'' : k1.val = k.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false k1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hhf, hk1'']
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · have hnil : row.drop k.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hhf, hnil]; simp)

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
        exact ok_spec (by rw [hhf, hnil]; simp)
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
        have hg1' : g1.val = g.val + 1 := by scalar_tac
        refine WP.spec_mono (ih _ g1 (by scalar_tac)) ?_
        intro r hr
        rw [hr, hhf, hg1']
        by_cases hem : ∃ b ∈ SolverSpec.idPairs (s.produced.val[g.val]'hg), b.2 = e.val
        · have h0 : ∃ b ∈ (SolverSpec.idPairs (s.produced.val[g.val]'hg)).drop (0#usize).val,
              b.2 = e.val := by simpa using hem
          have hyes : ∃ gp ∈ (SolverSpec.pairLists s.produced).drop g.val,
              ∃ b ∈ gp, b.2 = e.val := by
            rw [hcons]
            exact ⟨_, List.mem_cons_self, hem⟩
          rw [decide_eq_true h0, decide_eq_true hyes]
          simp
        · have h0 : ¬ (∃ b ∈ (SolverSpec.idPairs (s.produced.val[g.val]'hg)).drop (0#usize).val,
              b.2 = e.val) := by simpa using hem
          have hiff : (∃ gp ∈ (SolverSpec.pairLists s.produced).drop (g.val + 1),
                ∃ b ∈ gp, b.2 = e.val)
              ↔ (∃ gp ∈ (SolverSpec.pairLists s.produced).drop g.val,
                ∃ b ∈ gp, b.2 = e.val) := by
            rw [hcons]
            constructor
            · rintro ⟨x, hx, hx2⟩
              exact ⟨x, List.mem_cons_of_mem _ hx, hx2⟩
            · rintro ⟨x, hx, hx2⟩
              rcases List.mem_cons.mp hx with hx' | hx'
              · exact absurd (hx' ▸ hx2) hem
              · exact ⟨x, hx', hx2⟩
          rw [decide_eq_false h0]
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.pairLists s.produced).drop g.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hhf, hnil]; simp)

/-- The helper, in the specification's own vocabulary. -/
theorem emits_spec (s : types.Step) (e : Std.Usize) :
    clauses.emits s e ⦃ r => r = decide (SolverSpec.Emits (SolverSpec.stepView s) e.val) ⦄ := by
  have h := emits_outer_spec s e (SolverSpec.pairLists s.produced).length false 0#usize
    (by scalar_tac)
  refine WP.spec_mono h ?_
  intro r hr
  rw [hr]
  simp

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
        have hn1' : n1.val = n.val + 1 := by scalar_tac
        step as ⟨ i2, hi2 ⟩
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        refine WP.spec_mono (ih n1 i2 (by scalar_tac) (by scalar_tac)) ?_
        intro r hr
        rw [hr, hi2', hcnt, hn1']
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
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        refine WP.spec_mono (ih n i2 (by scalar_tac) (by scalar_tac)) ?_
        intro r hr
        rw [hr, hi2', hcnt]
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
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
          have hE : SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) e.val := by
            simpa using hem
          split
          · next hseen =>
            have hsn : seen ≠ access.NONE := by simpa using hseen
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
            refine WP.spec_mono (ih false i i2 (by scalar_tac)) ?_
            intro r hr
            rw [hr, decide_eq_false hno]
            simp
          · next hseen =>
            have hsn : seen = access.NONE := by simpa using hseen
            have hiNONE : i ≠ access.NONE := index_ne_NONE q i hi
            have hiff :
                (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop (i.val + 1)) ∧
                  (i ≠ access.NONE →
                    NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop (i.val + 1))))
                ↔ (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                    (seen ≠ access.NONE →
                      NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val))) := by
              rw [hcons]
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
            show (do
                let i2 ← i + 1#usize
                clauses.cl_unique_producer_loop0_loop0 q true e i i2) ⦃ r =>
                  r = (q, ok1 && decide
                    (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                     (seen ≠ access.NONE →
                       NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val)))) ⦄
            step as ⟨ i2, hi2 ⟩
            have hi2' : i2.val = i.val + 1 := by scalar_tac
            refine WP.spec_mono (ih true i i2 (by scalar_tac)) ?_
            intro r hr
            rw [hr, hi2', hok]
            simp only [Bool.true_and]
            exact congrArg (Prod.mk q) (decide_eq_decide.mpr hiff)
        · next hem =>
          have hE : ¬ SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) e.val := by
            simpa using hem
          have hiff :
              (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop (i.val + 1)) ∧
                (seen ≠ access.NONE →
                  NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop (i.val + 1))))
              ↔ (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                  (seen ≠ access.NONE →
                    NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val))) := by
            rw [hcons]
            constructor
            · rintro ⟨h1, h2⟩
              refine ⟨?_, ?_⟩
              · intro x hx y hy hEx hEy
                rcases List.mem_cons.mp hx with hx' | hx'
                · exact absurd (hx' ▸ hEx) hE
                · rcases List.mem_cons.mp hy with hy' | hy'
                  · exact absurd (hy' ▸ hEy) hE
                  · exact h1 x hx' y hy' hEx hEy
              · intro hsn x hx hEx
                rcases List.mem_cons.mp hx with hx' | hx'
                · exact absurd (hx' ▸ hEx) hE
                · exact h2 hsn x hx' hEx
            · rintro ⟨h1, h2⟩
              refine ⟨?_, ?_⟩
              · intro x hx y hy hEx hEy
                exact h1 x (List.mem_cons_of_mem _ hx) y (List.mem_cons_of_mem _ hy) hEx hEy
              · intro hsn x hx hEx
                exact h2 hsn x (List.mem_cons_of_mem _ hx) hEx
          show (do
              let i2 ← i + 1#usize
              clauses.cl_unique_producer_loop0_loop0 q true e seen i2) ⦃ r =>
                r = (q, ok1 && decide
                  (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop i.val) ∧
                   (seen ≠ access.NONE →
                     NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop i.val)))) ⦄
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true seen i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hi2', hok]
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
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
        have he1' : e1.val = e.val + 1 := by scalar_tac
        refine WP.spec_mono (ih vb e1 (by scalar_tac)) ?_
        intro r hr
        rw [hr, he1', hok, hv2]
        have hsimp : (AtMostOne e.val (((SolverSpec.steps q).zipIdx).drop (0#usize).val) ∧
            (access.NONE ≠ access.NONE →
              NoneEmits e.val (((SolverSpec.steps q).zipIdx).drop (0#usize).val)))
            ↔ AtMostOne e.val ((SolverSpec.steps q).zipIdx) := by
          simp only [show (0#usize : Std.Usize).val = 0 from rfl, List.drop_zero]
          constructor
          · exact fun h => h.1
          · exact fun h => ⟨h, fun hc => absurd rfl hc⟩
        by_cases hae : AtMostOne e.val ((SolverSpec.steps q).zipIdx)
        · have h1 := decide_eq_true (hsimp.mpr hae)
          have h2 : (∀ e' < ne.val, e.val + 1 ≤ e' → AtMostOne e' ((SolverSpec.steps q).zipIdx))
              ↔ (∀ e' < ne.val, e.val ≤ e' → AtMostOne e' ((SolverSpec.steps q).zipIdx)) := by
            constructor
            · intro h e' hlt' hle'
              rcases Nat.eq_or_lt_of_le hle' with heq | hlt2
              · rw [← heq]; exact hae
              · exact h e' hlt' (by omega)
            · intro h e' hlt' hle'
              exact h e' hlt' (by omega)
          rw [h1]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr h2
        · have h1 := decide_eq_false (fun h => hae (hsimp.mp h))
          have h2 : ¬ (∀ e' < ne.val, e.val ≤ e' → AtMostOne e' ((SolverSpec.steps q).zipIdx)) := by
            intro h
            exact hae (h e.val (by scalar_tac) (Nat.le_refl _))
          rw [h1, decide_eq_false h2]
          simp
      · have hyes : ∀ e' < ne.val, e.val ≤ e' → AtMostOne e' ((SolverSpec.steps q).zipIdx) := by
          intro e' h1 h2
          exfalso; scalar_tac
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
  refine WP.spec_mono h ?_
  intro r hr
  rw [hr]
  simp only [Bool.true_and]
  have hne : (alloc.vec.Vec.len q.endpoints).val = SolverSpec.nEndpoints q := by
    simp [SolverSpec.nEndpoints]
  refine decide_eq_decide.mpr ?_
  rw [hne]
  constructor
  · intro h e he i hi j hj hEi hEj
    exact h e he (by scalar_tac) i hi j hj hEi hEj
  · intro h e he _
    exact h e he

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
        exact ok_spec (by rw [hhf, hnil]; simp)
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
          refine WP.spec_mono (ih true i3 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hhf, decide_eq_true hyes]
          simp
        · next hne =>
          have hane : ¬ (a.val = e.val) := by
            intro hc; exact hne (by scalar_tac)
          have hiff : (∃ gn ∈ (SolverSpec.givens q).drop (i.val + 1), gn.1 = e.val)
              ↔ (∃ gn ∈ (SolverSpec.givens q).drop i.val, gn.1 = e.val) := by
            rw [hcons]
            constructor
            · rintro ⟨x, hx, hx2⟩
              exact ⟨x, List.mem_cons_of_mem _ hx, hx2⟩
            · rintro ⟨x, hx, hx2⟩
              rcases List.mem_cons.mp hx with hx' | hx'
              · exact absurd (hx' ▸ hx2) hane
              · exact ⟨x, hx', hx2⟩
          simp only [bind_tc_ok]
          step as ⟨ i3, hi3 ⟩
          have hi3' : i3.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false i3 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hhf, hi3']
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.givens q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_spec (by rw [hhf, hnil]; simp)

theorem is_given_spec (q : types.Plan) (e : Std.Usize) :
    clauses.is_given q e ⦃ r => r = decide (∃ gn ∈ SolverSpec.givens q, gn.1 = e.val) ⦄ := by
  have h := is_given_loop_spec q e (SolverSpec.givens q).length false 0#usize (by scalar_tac)
  refine WP.spec_mono h ?_
  intro r hr
  rw [hr]
  simp

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
        exact ok_spec (by rw [hhf, hnil]; simp)
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
          have hE : SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) e.val := by
            simpa using hem
          have hyes : ∃ s' ∈ (SolverSpec.steps q).drop i.val, SolverSpec.Emits s' e.val := by
            rw [hcons]
            exact ⟨_, List.mem_cons_self, hE⟩
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hhf, decide_eq_true hyes]
          simp
        · next hem =>
          have hE : ¬ SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) e.val := by
            simpa using hem
          have hiff : (∃ s' ∈ (SolverSpec.steps q).drop (i.val + 1), SolverSpec.Emits s' e.val)
              ↔ (∃ s' ∈ (SolverSpec.steps q).drop i.val, SolverSpec.Emits s' e.val) := by
            rw [hcons]
            constructor
            · rintro ⟨x, hx, hx2⟩
              exact ⟨x, List.mem_cons_of_mem _ hx, hx2⟩
            · rintro ⟨x, hx, hx2⟩
              rcases List.mem_cons.mp hx with hx' | hx'
              · exact absurd (hx' ▸ hx2) hE
              · exact ⟨x, hx', hx2⟩
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hhf, hi2']
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_length]; scalar_tac
        exact ok_spec (by rw [hhf, hnil]; simp)

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
      refine WP.spec_mono hloop ?_
      intro r hr
      rw [hr]
      simp only [show (0#usize : Std.Usize).val = 0 from rfl, List.drop_zero]
      by_cases hg : ∃ gn ∈ SolverSpec.givens q, gn.1 = ee.val
      · have hp : ProvOK q ee.val := Or.inr hg
        rw [decide_eq_true hg, decide_eq_true hp]
        simp
      · have hiff : (∃ s' ∈ SolverSpec.steps q, SolverSpec.Emits s' ee.val)
            ↔ ProvOK q ee.val := by
          constructor
          · exact fun h => Or.inl h
          · rintro (h | h)
            · exact h
            · exact absurd h hg
        rw [decide_eq_false hg]
        simp only [Bool.false_or]
        exact decide_eq_decide.mpr hiff
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
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
          have hP : ProvOK q ee.val := by simpa using hpv
          have hiff : (∀ b ∈ used.drop (j.val + 1), ProvOK q b.2)
              ↔ (∀ b ∈ used.drop j.val, ProvOK q b.2) := by
            rw [hcons]
            constructor
            · intro h x hx
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx']; exact hP
              · exact h x hx'
            · intro h x hx
              exact h x (List.mem_cons_of_mem _ hx)
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hj1', hok]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hpv =>
          have hP : ¬ ProvOK q ee.val := by simpa using hpv
          have hno : ¬ (∀ b ∈ used.drop j.val, ProvOK q b.2) := by
            intro h
            exact hP (h (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self))
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, decide_eq_false hno]
          simp
      · have hnil : used.drop j.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ b ∈ used.drop j.val, ProvOK q b.2 := by rw [hnil]; simp
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        refine WP.spec_mono (ih _ i2 (by scalar_tac)) ?_
        intro r hr
        rw [hr, hi2', hok]
        by_cases hh : ∀ b ∈ SolverSpec.idPairs (q.steps.val[i.val]'hi).used, ProvOK q b.2
        · have h0 : ∀ b ∈ (SolverSpec.idPairs (q.steps.val[i.val]'hi).used).drop (0#usize).val,
              ProvOK q b.2 := by simpa using hh
          have hiff : (∀ s ∈ (SolverSpec.steps q).drop (i.val + 1), ∀ b ∈ s.used, ProvOK q b.2)
              ↔ (∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ b ∈ s.used, ProvOK q b.2) := by
            rw [hcons]
            constructor
            · intro h x hx
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx']; exact hh
              · exact h x hx'
            · intro h x hx
              exact h x (List.mem_cons_of_mem _ hx)
          rw [decide_eq_true h0]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · have h0 : ¬ (∀ b ∈ (SolverSpec.idPairs (q.steps.val[i.val]'hi).used).drop (0#usize).val,
              ProvOK q b.2) := by simpa using hh
          have hno : ¬ (∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ b ∈ s.used, ProvOK q b.2) := by
            intro h
            exact hh (h _ (by rw [hcons]; exact List.mem_cons_self))
          rw [decide_eq_false h0, decide_eq_false hno]
          simp
      · have hnil : (SolverSpec.steps q).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [steps_length]; scalar_tac
        have hyes : ∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ b ∈ s.used, ProvOK q b.2 := by
          rw [hnil]; simp
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
  refine WP.spec_mono h ?_
  intro r hr
  rw [hr]
  simp

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
            rw [← hsv]; simpa using hem
          refine ok_spec (decide_eq_decide.mpr ?_)
          constructor
          · intro hlt _
            scalar_tac
          · intro h
            have hlt := h hE
            scalar_tac
        · next hem =>
          have hE : ¬ SolverSpec.Emits sv ee.val := by
            rw [← hsv]; simpa using hem
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
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
              → i.val < j.val := by simpa using hsa
          have hiff : (∀ x ∈ ((SolverSpec.steps q).zipIdx).drop (i.val + 1),
                SolverSpec.Emits x.1 ee.val → x.2 < j.val)
              ↔ (∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i.val,
                SolverSpec.Emits x.1 ee.val → x.2 < j.val) := by
            rw [hcons]
            constructor
            · intro h x hx
              rcases List.mem_cons.mp hx with hx' | hx'
              · rw [hx']; exact hS
              · exact h x hx'
            · intro h x hx
              exact h x (List.mem_cons_of_mem _ hx)
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hi2', hok]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · next hsa =>
          have hS : ¬ (SolverSpec.Emits (SolverSpec.stepView (q.steps.val[i.val]'hi)) ee.val
              → i.val < j.val) := by simpa using hsa
          have hno : ¬ (∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i.val,
              SolverSpec.Emits x.1 ee.val → x.2 < j.val) := by
            intro h
            exact hS (h _ (by rw [hcons]; exact List.mem_cons_self))
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, decide_eq_false hno]
          simp
      · have hnil : ((SolverSpec.steps q).zipIdx).drop i.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [List.length_zipIdx, steps_length]; scalar_tac
        have hyes : ∀ x ∈ ((SolverSpec.steps q).zipIdx).drop i.val,
            SolverSpec.Emits x.1 ee.val → x.2 < j.val := by rw [hnil]; simp
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
        have hb1' : b1.val = b.val + 1 := by scalar_tac
        refine WP.spec_mono (ih _ b1 (by scalar_tac)) ?_
        intro r hr
        rw [hr, hb1', hok]
        by_cases hh : ∀ x ∈ (SolverSpec.steps q).zipIdx,
            SolverSpec.Emits x.1 ee.val → x.2 < j.val
        · have h0 : ∀ x ∈ ((SolverSpec.steps q).zipIdx).drop (0#usize).val,
              SolverSpec.Emits x.1 ee.val → x.2 < j.val := by simpa using hh
          have hiff : (∀ bnd ∈ used.drop (b.val + 1), ∀ x ∈ (SolverSpec.steps q).zipIdx,
                SolverSpec.Emits x.1 bnd.2 → x.2 < j.val)
              ↔ (∀ bnd ∈ used.drop b.val, ∀ x ∈ (SolverSpec.steps q).zipIdx,
                SolverSpec.Emits x.1 bnd.2 → x.2 < j.val) := by
            rw [hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hh
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          rw [decide_eq_true h0]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · have h0 : ¬ (∀ x ∈ ((SolverSpec.steps q).zipIdx).drop (0#usize).val,
              SolverSpec.Emits x.1 ee.val → x.2 < j.val) := by simpa using hh
          have hno : ¬ (∀ bnd ∈ used.drop b.val, ∀ x ∈ (SolverSpec.steps q).zipIdx,
              SolverSpec.Emits x.1 bnd.2 → x.2 < j.val) := by
            intro h
            exact hh (h (a.val, ee.val) (by rw [hcons]; exact List.mem_cons_self))
          rw [decide_eq_false h0, decide_eq_false hno]
          simp
      · have hnil : used.drop b.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        have hyes : ∀ bnd ∈ used.drop b.val, ∀ x ∈ (SolverSpec.steps q).zipIdx,
            SolverSpec.Emits x.1 bnd.2 → x.2 < j.val := by rw [hnil]; simp
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
        exact ok_spec (by rw [hok, decide_eq_true hyes])
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
        have hj1' : j1.val = j.val + 1 := by scalar_tac
        refine WP.spec_mono (ih _ j1 (by scalar_tac)) ?_
        intro r hr
        rw [hr, hj1', hok]
        by_cases hh : ∀ bnd ∈ SolverSpec.idPairs (q.steps.val[j.val]'hj).used,
            ∀ x ∈ (SolverSpec.steps q).zipIdx, SolverSpec.Emits x.1 bnd.2 → x.2 < j.val
        · have h0 : ∀ bnd ∈ (SolverSpec.idPairs (q.steps.val[j.val]'hj).used).drop (0#usize).val,
              ∀ x ∈ (SolverSpec.steps q).zipIdx,
                SolverSpec.Emits x.1 bnd.2 → x.2 < j.val := by simpa using hh
          have hiff : (∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop (j.val + 1),
                ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
                  SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2)
              ↔ (∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop j.val,
                ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
                  SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2) := by
            rw [hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hh
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          rw [decide_eq_true h0]
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
        · have h0 : ¬ (∀ bnd ∈ (SolverSpec.idPairs (q.steps.val[j.val]'hj).used).drop (0#usize).val,
              ∀ x ∈ (SolverSpec.steps q).zipIdx,
                SolverSpec.Emits x.1 bnd.2 → x.2 < j.val) := by simpa using hh
          have hno : ¬ (∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop j.val,
              ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
                SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2) := by
            intro h
            exact hh (h _ (by rw [hcons]; exact List.mem_cons_self))
          rw [decide_eq_false h0, decide_eq_false hno]
          simp
      · have hnil : ((SolverSpec.steps q).zipIdx).drop j.val = [] := by
          refine List.drop_eq_nil_iff.mpr ?_
          rw [List.length_zipIdx, steps_length]; scalar_tac
        have hyes : ∀ cj ∈ ((SolverSpec.steps q).zipIdx).drop j.val,
            ∀ bnd ∈ cj.1.used, ∀ x ∈ (SolverSpec.steps q).zipIdx,
              SolverSpec.Emits x.1 bnd.2 → x.2 < cj.2 := by rw [hnil]; simp
        exact ok_spec (by rw [hok, decide_eq_true hyes])
    · next hok =>
      have hf : ok1 = false := by simpa using hok
      exact ok_spec (by rw [hf]; simp)

theorem cl_schedulable_spec (q : types.Plan) :
    clauses.cl_schedulable q ⦃ r => r = decide
      (∀ cj ∈ (SolverSpec.steps q).zipIdx, ∀ b ∈ cj.1.used,
        ∀ pi ∈ (SolverSpec.steps q).zipIdx, SolverSpec.Emits pi.1 b.2 → pi.2 < cj.2) ⦄ := by
  rw [clauses.cl_schedulable.eq_def]
  have h := cl_schedulable_outer_spec q q.steps.val.length true 0#usize (by scalar_tac)
  refine WP.spec_mono h ?_
  intro r hr
  rw [hr]
  simp

#print axioms SolverProof.emits_spec
#print axioms SolverProof.cl_target_spec
#print axioms SolverProof.cl_unique_producer_spec
#print axioms SolverProof.cl_provenance_spec
#print axioms SolverProof.cl_schedulable_spec

end SolverProof
