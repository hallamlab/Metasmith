/-
  `bits::eq` — the worked example for every extracted loop.

  Read `Proof/Basis.lean` first: it states why a loop lemma is a triple rather
  than an equation, and sketches the fuel induction this file instantiates.

  Two things here are worth carrying to the next loop.

  **The postcondition is phrased over `List.drop`, not over a bounded `∀`.**
  `eq_loop a b i` decides `a.val.drop i = b.val.drop i`, so at `i = 0` the
  wrapper's postcondition falls out of `List.drop_zero` with no index algebra at
  all, and the inductive step is exactly `List.drop_eq_getElem_cons` -- one head
  comparison and one tail. A quantified form would need the same lemma anyway,
  and would not simplify at `i = 0`.

  **`decide` must never appear in a rewrite.** `Slice α` is a plain `def` for a
  subtype, so `a.val` is only type-correct once `Slice` unfolds -- which
  `rw` and `simp`'s motive check will not do at `instances` transparency. A goal
  that is `decide P` over such a term therefore fails with "the motive is not
  type correct", naming an `↑a` that looks perfectly well typed. The way through
  is never to rewrite the `decide`: prove the bare `P` (or `¬P`) as its own
  `have`, then discharge the triple with `decide_eq_true` / `decide_eq_false`.
  `WP.spec_mono` plays the same role for the recursive call, where the two
  postconditions differ only by an `Iff` on the underlying propositions.

  No `simp only [WP.spec_ok]` is needed to expose `ok`: `spec`, `theta` and
  `wp_return` are all plain definitions, so `x ⦃ r => r = c ⦄` on an `ok` is
  definitionally the equation, and `exact` sees through it.
-/

import SolverWitness.Proof.Basis

namespace SolverProof

open Aeneas Aeneas.Std Result
open solver_witness

/-- One step of a pointwise list comparison, as the suffixes the loop compares. -/
theorem drop_eq_cons_iff {α} (l₁ l₂ : List α) (i : Nat)
    (h₁ : i < l₁.length) (h₂ : i < l₂.length) :
    l₁.drop i = l₂.drop i ↔ (l₁[i] = l₂[i] ∧ l₁.drop (i + 1) = l₂.drop (i + 1)) := by
  rw [List.drop_eq_getElem_cons h₁, List.drop_eq_getElem_cons h₂, List.cons.injEq]

/-- Both suffixes are empty once the index has run off the end. -/
theorem drop_both_nil {α} (l₁ l₂ : List α) (i : Nat)
    (h₁ : l₁.length ≤ i) (h₂ : l₂.length ≤ i) :
    l₁.drop i = l₂.drop i :=
  (List.drop_eq_nil_iff.mpr h₁).trans (List.drop_eq_nil_iff.mpr h₂).symm

/-- The loop, on the fuel `k` that bounds the iterations it has left.

`hab` is what the wrapper's length check establishes, and the postcondition is
false without it: on a longer `b` the loop still returns `true`. -/
theorem eq_loop_spec (a b : Slice Bool) (hab : a.length = b.length) :
    ∀ (k : Nat) (i : Usize), a.length - i.val ≤ k →
      bits.eq_loop a b i ⦃ r => r = decide (a.val.drop i.val = b.val.drop i.val) ⦄ := by
  intro k
  induction k with
  | zero =>
    intro i hk
    rw [bits.eq_loop.eq_def]
    dsimp only
    split
    · exfalso; scalar_tac
    · exact (decide_eq_true
        (drop_both_nil a.val b.val i.val (by scalar_tac) (by scalar_tac))).symm
  | succ k ih =>
    intro i hk
    rw [bits.eq_loop.eq_def]
    dsimp only
    split
    · rename_i hlt
      have hia : i.val < a.val.length := by scalar_tac
      have hib : i.val < b.val.length := by scalar_tac
      step as ⟨ x, hx ⟩
      step as ⟨ y, hy ⟩
      split
      · rename_i hne
        have hne' : a.val[i.val] ≠ b.val[i.val] := by
          intro h
          rw [hx, hy] at hne
          simp [h] at hne
        exact (decide_eq_false (fun hc =>
          hne' ((drop_eq_cons_iff a.val b.val i.val hia hib).mp hc).1)).symm
      · rename_i heq
        have heq' : a.val[i.val] = b.val[i.val] := by
          rw [hx, hy] at heq
          simpa using heq
        step as ⟨ i2, hi2 ⟩
        have hiff : (a.val.drop i.val = b.val.drop i.val)
            ↔ (a.val.drop i2.val = b.val.drop i2.val) := by
          rw [hi2, drop_eq_cons_iff a.val b.val i.val hia hib]
          simp [heq']
        exact WP.spec_mono (ih i2 (by scalar_tac))
          (fun r hr => hr.trans (decide_eq_decide.mpr hiff.symm))
    · exact (decide_eq_true
        (drop_both_nil a.val b.val i.val (by scalar_tac) (by scalar_tac))).symm

/-- The wrapper: the length check, then the loop from `0`. -/
theorem eq_spec (a b : Slice Bool) :
    bits.eq a b ⦃ r => r = decide (a.val = b.val) ⦄ := by
  rw [bits.eq.eq_def]
  dsimp only
  split
  · have hlen : a.val.length ≠ b.val.length := by scalar_tac
    exact (decide_eq_false (fun he => hlen (by rw [he]))).symm
  · have hab : a.length = b.length := by scalar_tac
    have h := eq_loop_spec a b hab a.length 0#usize (by scalar_tac)
    simpa using h

/- What the two theorems actually rest on. `Audit.lean` prints this for the
   obligations; a helper that nothing audits yet prints its own, so a `sorry`
   reached through the Aeneas standard library cannot arrive here unnoticed. -/
#print axioms SolverProof.eq_loop_spec
#print axioms SolverProof.eq_spec

end SolverProof
