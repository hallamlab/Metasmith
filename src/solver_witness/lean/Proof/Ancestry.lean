/-
  `ancestors` and `descends` -- the dense reachability table, against the
  specification's per-query recursion.

  Read `Proof/Basis.lean` first for the loop recipe. This file adds the one shape
  that recipe does not cover: an outer loop that PUSHES INTO THE STRUCTURE IT
  READS.

  ## The two algorithms agree unconditionally

  The checker builds one row per endpoint in a single increasing pass, and row
  `x` unions in the row of every declared parent `p` for which `p < rows.len()`.
  The specification recurses on the parents `m` for which `m < e`. Those are the
  same parents: when the checker computes row `x`, `rows.len()` is exactly `x`.
  So no `WellIndexed` hypothesis appears below, and none is needed -- the two
  under-approximate identically rather than merely both under-approximating.

  What carries that fact is the loop invariant `rows.val.length = x.val`. It has
  to be threaded THROUGH the outer loop rather than established after it, because
  it is what the guard `p < rows.len()` means, and the guard is read on every
  iteration. `anc_row_loop_spec` therefore does not mention `rows` in its
  postcondition at all: it takes the table's length as a plain `Nat` `nr` and its
  contents as a hypothesis, so the row it computes is stated in `ancestorB` and
  `nr` alone. That is also what keeps `Slice` out of the `decide` that the
  postcondition's `any` carries -- the trap `Basis` describes.

  ## Why the specification needs a range lemma

  A row is `n` bits wide, so `descends anc e f` reads `false` for every `f` at or
  beyond the endpoint table. `ancestorB_of_le` is what says the specification
  agrees: ancestry never leaves the table, because the parent guard `m < e`
  bounds the recursion below `e < n`. Without it `descends_spec` would have to be
  stated for `f < n` only, and the clauses call `descends` with an arbitrary `f`.

  ## Four things the recipe in `Basis` does not say

  **`Basis`' `Vec` motive trap has a second storey.** Never `rw` a term whose
  type is `alloc.vec.Vec a` in a goal that projects `.val` out of it: the motive
  `fun z => z.val ...` is not type correct at `instances` transparency, and the
  error names an `↑z` rather than the rewrite. Push the `Vec`-level equation
  through `congrArg (fun (z : alloc.vec.Vec Bool) => z.val)` first and rewrite
  the resulting `List` equation. That is the whole content of `hvv` in
  `ancestors_loop_spec`, where the new table is `rows ++ [v]` and every entry of
  the invariant is a `.val` of one of its elements.

  **Prefer `exact` to `rw` across `alloc.vec.Vec.deref`.** It is a plain `def`
  building the subtype from the same list, so `(deref v).val` is `v.val`
  definitionally and `exact` sees it. A `rw` there is the trap above.

  **`dsimp only` belongs before a `split`, and only there.** It is needed when a
  `let` precedes the branch, as in `bits.eq_loop`. A `let` that follows a
  monadic bind is already zeta-reduced by `step`, and the second `dsimp only`
  then fails with "made no progress" -- an error, not a no-op.

  **`step with <lemma> as ⟨…⟩` is how an untagged spec is used.** None of
  `Proof/Access.lean`'s sixteen accessor lemmas carries `@[step]`, so `step`
  cannot find them; naming one in the `with` clause composes it exactly as a
  tagged lemma would. The same clause is what feeds `anc_row_spec` its `rows`
  hypothesis, which `step` could not have guessed.

  One smaller cost: `(x == f) = decide (x = f)` is not something `simp` will do
  on `Nat` -- it reports no progress. Case split on `x = f` instead.
-/

import SolverWitness.Types
import SolverWitness.Funs
import SolverWitness.Spec
import SolverWitness.Proof.Basis
import SolverWitness.Proof.Bits
import SolverWitness.Proof.Access

set_option maxHeartbeats 1000000

namespace SolverProof

open Aeneas Aeneas.Std Result
open solver_witness

/-! ## The specification side -/

/-- Ancestry stays inside the endpoint table.

The parent guard `m < e` keeps the recursion below its starting index, so a query
at a column at or beyond the table is `false` at every level. -/
theorem ancestorB_of_le (q : types.Plan) (n f : Nat) (hf : n ≤ f) :
    ∀ e, e < n → SolverSpec.ancestorB q e f = false := by
  intro e
  induction e using Nat.strong_induction_on with
  | _ e ih =>
    intro he
    rw [SolverSpec.ancestorB.eq_def]
    have hef : ¬ (e = f) := by omega
    have hany : ((SolverSpec.epParents q e).any
        (fun m => if h : m < e then SolverSpec.ancestorB q m f else false)) = false := by
      refine List.any_eq_false.mpr ?_
      intro m _
      by_cases hm : m < e
      · rw [dif_pos hm, ih m hm (by omega)]
        simp
      · rw [dif_neg hm]
        simp
    rw [hany]
    simp [hef]

/-- The checker's guard and the specification's guard are the same guard.

`anc_row` skips a parent at or beyond the table it has built; `ancestorB` skips a
parent that does not precede its child. With the table `x` rows long when row `x`
is computed, those discard the same parents -- which is why no `WellIndexed`
hypothesis appears anywhere in this file. -/
theorem row_eq_ancestorB (q : types.Plan) (x f : Nat) :
    (decide (x = f) ||
      (SolverSpec.epParents q x).any
        (fun p => decide (p < x) && SolverSpec.ancestorB q p f))
      = SolverSpec.ancestorB q x f := by
  rw [SolverSpec.ancestorB.eq_def]
  have h1 : ((x == f) : Bool) = decide (x = f) := by
    by_cases h : x = f
    · simp [h]
    · simp [h]
  have h2 : (fun (m : Nat) => if h : m < x then SolverSpec.ancestorB q m f else false)
      = (fun (m : Nat) => decide (m < x) && SolverSpec.ancestorB q m f) := by
    funext m
    by_cases hm : m < x
    · rw [dif_pos hm]; simp [hm]
    · rw [dif_neg hm]; simp [hm]
  rw [h1, h2]

/-! ## The table -/

/-- What a finished table says: every row is `n` bits wide, and column `f` of row
`p` is the specification's ancestry. -/
def RowsOk (q : types.Plan) (n : Nat) (rows : List (alloc.vec.Vec Bool)) : Prop :=
  ∀ p < rows.length,
    (rows[p]!).val.length = n ∧
    ∀ f < n, ((rows[p]!).val)[f]! = SolverSpec.ancestorB q p f

/-- The row loop, on the fuel `k` that bounds the iterations it has left.

`nr` is the table's length as a plain `Nat`: the postcondition mentions it under
a `decide`, and a `Slice` projection there is the motive failure `Basis`
describes. `hrows` is what the rows already hold, which is all this loop reads of
them. -/
theorem anc_row_loop_spec (q : types.Plan) (rows : Slice (alloc.vec.Vec Bool))
    (x : Usize) (n nr : Nat) (hnr : rows.val.length = nr)
    (hrows : ∀ p < nr, ∀ f < n, ((rows.val[p]!).val)[f]! = SolverSpec.ancestorB q p f) :
    ∀ (k : Nat) (acc : alloc.vec.Vec Bool) (kk j : Usize),
      kk.val - j.val ≤ k →
      kk.val = (SolverSpec.epParents q x.val).length →
      acc.val.length = n →
      clauses.anc_row_loop q rows x acc kk j ⦃ r =>
        r.val.length = n ∧
        ∀ f < n, r.val[f]! =
          (acc.val[f]! ||
            ((SolverSpec.epParents q x.val).drop j.val).any
              (fun p => decide (p < nr) && SolverSpec.ancestorB q p f)) ⦄ := by
  intro k
  induction k with
  | zero =>
    intro acc kk j hk hkk hacc
    rw [clauses.anc_row_loop.eq_def]
    split
    · exfalso; scalar_tac
    · have hge : (SolverSpec.epParents q x.val).length ≤ j.val := by omega
      have hnil : (SolverSpec.epParents q x.val).drop j.val = [] :=
        List.drop_eq_nil_iff.mpr hge
      refine ⟨hacc, fun f hf => ?_⟩
      rw [hnil]
      simp
  | succ k ih =>
    intro acc kk j hk hkk hacc
    rw [clauses.anc_row_loop.eq_def]
    split
    · rename_i hlt
      have hjkk : j.val < kk.val := by scalar_tac
      have hjk : j.val < (SolverSpec.epParents q x.val).length := by omega
      step with ep_parent_spec q x j as ⟨ p, hp ⟩
      have hpv : p.val = (SolverSpec.epParents q x.val)[j.val]'hjk := by
        rw [hp, List.getElem?_eq_getElem hjk]
        rfl
      have hdrop : (SolverSpec.epParents q x.val).drop j.val
          = p.val :: (SolverSpec.epParents q x.val).drop (j.val + 1) := by
        rw [hpv]
        exact List.drop_eq_getElem_cons hjk
      split
      · rename_i hplt
        have hpr : p.val < rows.val.length := by scalar_tac
        have hpn : p.val < nr := by omega
        step as ⟨ v, hv ⟩
        step as ⟨ acc1, hacc1len, hacc1pt ⟩
        step as ⟨ j1, hj1 ⟩
        have hj1' : j1.val = j.val + 1 := by scalar_tac
        have hacc1n : acc1.val.length = n := by rw [hacc1len, hacc]
        refine WP.spec_mono (ih acc1 kk j1 (by scalar_tac) hkk hacc1n) ?_
        rintro r ⟨hrlen, hrpt⟩
        refine ⟨hrlen, fun f hf => ?_⟩
        have hderefv : (alloc.vec.Vec.deref v).val = v.val := rfl
        have hvrow : rows.val[p.val]! = v := by
          rw [hv]
          exact getElem!_pos rows.val p.val hpr
        have hrow : v.val[f]! = SolverSpec.ancestorB q p.val f := by
          have h3 := hrows p.val hpn f hf
          rw [hvrow] at h3
          exact h3
        have haccf : acc1.val[f]! = (acc.val[f]! || SolverSpec.ancestorB q p.val f) := by
          rw [hacc1pt f (by omega), hderefv, hrow]
        have hgp : (decide (p.val < nr) && SolverSpec.ancestorB q p.val f)
            = SolverSpec.ancestorB q p.val f := by simp [hpn]
        rw [hrpt f hf, hj1', hdrop, List.any_cons, hgp, haccf, Bool.or_assoc]
      · rename_i hpge
        have hpn : ¬ (p.val < nr) := by scalar_tac
        simp only [bind_tc_ok]
        step as ⟨ j1, hj1 ⟩
        have hj1' : j1.val = j.val + 1 := by scalar_tac
        refine WP.spec_mono (ih acc kk j1 (by scalar_tac) hkk hacc) ?_
        rintro r ⟨hrlen, hrpt⟩
        refine ⟨hrlen, fun f hf => ?_⟩
        have hgp : (decide (p.val < nr) && SolverSpec.ancestorB q p.val f) = false := by
          simp [hpn]
        rw [hrpt f hf, hj1', hdrop, List.any_cons, hgp, Bool.false_or]
    · rename_i hge
      have hjge : kk.val ≤ j.val := by scalar_tac
      have hge' : (SolverSpec.epParents q x.val).length ≤ j.val := by omega
      have hnil : (SolverSpec.epParents q x.val).drop j.val = [] :=
        List.drop_eq_nil_iff.mpr hge'
      refine ⟨hacc, fun f hf => ?_⟩
      rw [hnil]
      simp

/-- One row: the endpoint itself, plus the row of every parent already in the
table. -/
theorem anc_row_spec (q : types.Plan) (rows : Slice (alloc.vec.Vec Bool))
    (nu x : Usize) (nr : Nat) (hnr : rows.val.length = nr)
    (hrows : ∀ p < nr, ∀ f < nu.val,
        ((rows.val[p]!).val)[f]! = SolverSpec.ancestorB q p f) :
    clauses.anc_row q rows nu x ⦃ r =>
      r.val.length = nu.val ∧
      ∀ f < nu.val, r.val[f]! =
        (decide (x.val = f) ||
          (SolverSpec.epParents q x.val).any
            (fun p => decide (p < nr) && SolverSpec.ancestorB q p f)) ⦄ := by
  rw [clauses.anc_row.eq_def]
  step as ⟨ acc, hacc ⟩
  step as ⟨ acc1, hacc1 ⟩
  step with ep_nparents_spec q x as ⟨ kk, hkk ⟩
  have hacc1n : acc1.val.length = nu.val := by rw [hacc1, hacc]; simp
  refine WP.spec_mono
    (anc_row_loop_spec q rows x nu.val nr hnr hrows kk.val acc1 kk 0#usize
      (by scalar_tac) hkk hacc1n) ?_
  rintro r ⟨hrlen, hrpt⟩
  refine ⟨hrlen, fun f hf => ?_⟩
  have hd : (SolverSpec.epParents q x.val).drop (0#usize).val
      = SolverSpec.epParents q x.val := by simp
  have hset : acc1.val[f]! = decide (x.val = f) := by
    rw [hacc1, hacc]
    by_cases hfx : f = x.val
    · have hxn : x.val < nu.val := by omega
      have h1 : ((List.replicate nu.val false).set x.val true)[f]! = true := by
        rw [hfx]
        exact List.set_getElem!_eq (List.replicate nu.val false) x.val x.val true
          ⟨by simpa using hxn, rfl⟩
      rw [h1]
      simp [hfx]
    · have h1 : ((List.replicate nu.val false).set x.val true)[f]! = false := by
        rw [List.set_getElem!_ne (List.replicate nu.val false) x.val f true
          (Or.inr (Or.inl hfx))]
        simp
      rw [h1]
      exact (decide_eq_false (fun hc => hfx hc.symm)).symm
  rw [hrpt f hf, hd, hset]

/-- The outer loop, on the fuel `k`.

`hlen` is the invariant that makes the whole thing work: the table's length is
exactly the row being computed, so `anc_row`'s `p < rows.len()` guard is the
specification's `m < e`. It is carried through the loop, not established after
it. -/
theorem ancestors_loop_spec (q : types.Plan) (nu : Usize) :
    ∀ (k : Nat) (rows : alloc.vec.Vec (alloc.vec.Vec Bool)) (x : Usize),
      nu.val - x.val ≤ k → x.val ≤ nu.val →
      rows.val.length = x.val → RowsOk q nu.val rows.val →
      clauses.ancestors_loop q nu rows x ⦃ r =>
        r.val.length = nu.val ∧ RowsOk q nu.val r.val ⦄ := by
  intro k
  induction k with
  | zero =>
    intro rows x hk hx hlen hok
    rw [clauses.ancestors_loop.eq_def]
    split
    · exfalso; scalar_tac
    · have hxn : x.val = nu.val := by scalar_tac
      exact ⟨by rw [hlen, hxn], hok⟩
  | succ k ih =>
    intro rows x hk hx hlen hok
    rw [clauses.ancestors_loop.eq_def]
    split
    · rename_i hlt
      have hxn : x.val < nu.val := by scalar_tac
      have hrows' : ∀ p < x.val, ∀ f < nu.val,
          (((alloc.vec.Vec.deref rows).val[p]!).val)[f]! = SolverSpec.ancestorB q p f :=
        fun p hp f hf => (hok p (by omega)).2 f hf
      dsimp only
      step with anc_row_spec q (alloc.vec.Vec.deref rows) nu x x.val hlen hrows'
        as ⟨ v, hvlen, hvpt ⟩
      have hmax : rows.val.length < Usize.max := by scalar_tac
      step as ⟨ rows1, hrows1 ⟩
      step as ⟨ x1, hx1 ⟩
      have hx1' : x1.val = x.val + 1 := by scalar_tac
      have hlen1 : rows1.val.length = x1.val := by
        rw [hrows1, hx1']
        simp [hlen]
      have hok1 : RowsOk q nu.val rows1.val := by
        rw [hrows1]
        intro p hp
        rw [List.length_append] at hp
        simp only [List.length_cons, List.length_nil] at hp
        by_cases hplt : p < rows.val.length
        · have hveq : (rows.val ++ [v])[p]! = rows.val[p]! :=
            List.getElem!_append_left rows.val [v] p hplt
          have hvv : ((rows.val ++ [v])[p]!).val = (rows.val[p]!).val :=
            congrArg (fun (z : alloc.vec.Vec Bool) => z.val) hveq
          rw [hvv]
          exact hok p hplt
        · have hpe : p = rows.val.length := by omega
          have hveq : (rows.val ++ [v])[p]! = v := by
            rw [List.getElem!_append_right rows.val [v] p (by omega), hpe]
            simp
          have hvv : ((rows.val ++ [v])[p]!).val = v.val :=
            congrArg (fun (z : alloc.vec.Vec Bool) => z.val) hveq
          rw [hvv, hpe, hlen]
          exact ⟨hvlen, fun f hf => by rw [hvpt f hf]; exact row_eq_ancestorB q x.val f⟩
      refine WP.spec_mono (ih rows1 x1 (by scalar_tac) (by scalar_tac) hlen1 hok1) ?_
      exact fun r hr => hr
    · have hxn : x.val = nu.val := by scalar_tac
      exact ⟨by rw [hlen, hxn], hok⟩

/-- The whole table: one row per endpoint, and every row the specification's
ancestry. No `WellIndexed` hypothesis -- the two agree on every plan. -/
theorem ancestors_spec (q : types.Plan) :
    clauses.ancestors q ⦃ r =>
      r.val.length = SolverSpec.nEndpoints q ∧
      RowsOk q (SolverSpec.nEndpoints q) r.val ⦄ := by
  rw [clauses.ancestors.eq_def]
  step with n_endpoints_spec q as ⟨ nu, hnu ⟩
  refine WP.spec_mono
    (ancestors_loop_spec q nu nu.val (alloc.vec.Vec.new (alloc.vec.Vec Bool)) 0#usize
      (by scalar_tac) (by scalar_tac) (by simp) (by intro p hp; simp at hp)) ?_
  rintro r ⟨h1, h2⟩
  rw [hnu] at h1 h2
  exact ⟨h1, h2⟩

/-- The query. Total: an endpoint outside the table descends from nothing, and a
column outside it is reached by nothing -- which is `ancestorB_of_le`. -/
theorem descends_spec (q : types.Plan) (anc : Slice (alloc.vec.Vec Bool))
    (n : Nat) (hlen : anc.val.length = n) (hok : RowsOk q n anc.val) (e f : Usize) :
    clauses.descends anc e f ⦃ r =>
      r = (decide (e.val < n) && SolverSpec.ancestorB q e.val f.val) ⦄ := by
  rw [clauses.descends.eq_def]
  dsimp only
  split
  · rename_i hlt
    have hea : e.val < anc.val.length := by scalar_tac
    have hen : e.val < n := by omega
    obtain ⟨hrl, hrp⟩ := hok e.val hea
    have hidx : anc.val[e.val]! = anc.val[e.val]'hea := getElem!_pos anc.val e.val hea
    step as ⟨ v, hv ⟩
    have hveq : v = anc.val[e.val]! := by rw [hv, hidx]
    subst hveq
    refine WP.spec_mono (get_spec (alloc.vec.Vec.deref (anc.val[e.val]!)) f) ?_
    intro r hr
    have hdl : (alloc.vec.Vec.deref (anc.val[e.val]!)).val.length = n := hrl
    have hL : (alloc.vec.Vec.deref (anc.val[e.val]!)).val[f.val]!
        = SolverSpec.ancestorB q e.val f.val := by
      by_cases hf : f.val < n
      · exact hrp f.val hf
      · have h1 : (alloc.vec.Vec.deref (anc.val[e.val]!)).val[f.val]! = false :=
          List.getElem!_length_le _ f.val (by rw [hdl]; omega)
        rw [h1]
        exact (ancestorB_of_le q n f.val (by omega) e.val hen).symm
    have hR : SolverSpec.ancestorB q e.val f.val
        = (decide (e.val < n) && SolverSpec.ancestorB q e.val f.val) := by simp [hen]
    exact hr.trans (hL.trans hR)
  · rename_i hge
    have h0 : anc.val.length ≤ e.val := by scalar_tac
    have hen : ¬ (e.val < n) := by omega
    have h : (decide (e.val < n) && SolverSpec.ancestorB q e.val f.val) = false := by
      simp [hen]
    exact h.symm

/- What the obligations actually rest on. `Audit.lean` prints this for the
   obligations; a helper that nothing audits yet prints its own. -/
#print axioms SolverProof.ancestorB_of_le
#print axioms SolverProof.row_eq_ancestorB
#print axioms SolverProof.anc_row_loop_spec
#print axioms SolverProof.anc_row_spec
#print axioms SolverProof.ancestors_loop_spec
#print axioms SolverProof.ancestors_spec
#print axioms SolverProof.descends_spec

end SolverProof
