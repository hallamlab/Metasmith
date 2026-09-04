/-
  `bits` — the dense bit set, and the worked example for every extracted loop.

  Read `Proof/Basis.lean` first: it states why a loop lemma is a triple rather
  than an equation, and sketches the fuel induction this file instantiates.

  ## The scanning loop, `bits::eq`

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

  ## The accumulating loops, `zeros`, `union` and `of_ids`

  A scanning loop returns a decision. These three return the collection they have
  been writing into, and five things differ.

  **Say the postcondition over what is still owed, starting from the accumulator
  you were handed.** `zeros_loop n b i` returns `b ++ replicate (n - i) false` and
  `of_ids_loop ids b i` returns `setIds b (ids.drop i)`, each quantified over `b`
  as well as `i`, so the induction hypothesis applies to the accumulator the body
  has just produced. `union` cannot be phrased that way — it overwrites in place
  rather than extending — so it states `length` plus a bounded pointwise `∀`, and
  all the index algebra is pushed into one pure lemma, `union_step`, written over
  `List Bool` alone. That is deliberate: nothing stated over bare lists can trip
  the `Slice`/`Vec` motive check described above.

  **`Vec.push` can fail, so a loop that GROWS carries a length invariant.**
  `zeros_loop` takes `b.val.length = i.val`; that is what makes
  `b.val.length < Usize.max` while `i < n`, and without it the lemma is false,
  because `push` is allowed to fail and a triple asserts it does not. A loop that
  only overwrites needs no such hypothesis, and `bits.set` needs no bounds
  hypothesis at all: `List.set` and the Rust are both no-ops out of range, so one
  equation covers both branches of the `if`.

  **State entries with `getElem!`, not `getElem`.** Aeneas's list layer
  (`set_getElem!_eq`, `set_getElem!_ne`, `getElem!_replicate`,
  `getElem!_length_le`) is built on `l[j]!`, and on `Bool` its out-of-range
  default is exactly the `false` that `bits::get` returns. So the specs carry no
  bounds proof inside a term, and `get` is total by construction.

  **`alloc.vec.Vec.index_mut` arrives as ONE pair variable.**
  `step as ⟨ p, hp1, hp2 ⟩` is the pattern; a fourth name is refused with
  "expected ≤ 3 ids". Destructure with `obtain ⟨ _, back ⟩ := p`, restate
  `hp2` as `back = alloc.vec.Vec.set a i` (it is defeq), `subst` it, and the
  write-back applies away. `bits.set`, where that block is the whole computation,
  is the one place `step` splits the pair for you — do not infer the pattern from
  the shape of the spec.

  **`rw` inside a triple does not close the triple.** After rewriting an `ok`'s
  postcondition down to something true by `rfl`, the goal is still
  `ok x ⦃ r => … ⦄`, and `rw`'s trailing `rfl` only fires on an `Eq`. Finish with
  an explicit `exact rfl`.

  One more, cheap to hit twice: `subst h` on `h : j = i.val` eliminates `j`, so
  every later mention of `j` in that branch has to be written `i.val`.
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

/-! ## `get` and `set` -- the two operations with no loop -/

/-- Reading one bit. Total: out of range reads `false`, which is `[i]!` on `Bool`. -/
@[step]
theorem get_spec (b : Slice Bool) (i : Usize) :
    bits.get b i ⦃ r => r = b.val[i.val]! ⦄ := by
  rw [bits.get.eq_def]
  dsimp only
  split
  · have hib : i.val < b.val.length := by scalar_tac
    have hg : b.val[i.val]! = b.val[i.val] := getElem!_pos b.val i.val hib
    refine WP.spec_mono (Slice.index_usize_spec b i (by scalar_tac)) ?_
    intro r hr
    rw [hr, hg]
  · have hib : b.val.length ≤ i.val := by scalar_tac
    have hg : b.val[i.val]! = false := by simp [hib]
    rw [hg]
    exact rfl

/-- Setting one bit. `List.set` is a no-op out of range, and so is the Rust, so
one equation covers both branches. -/
@[step]
theorem set_spec (b : alloc.vec.Vec Bool) (i : Usize) :
    bits.set b i ⦃ r => r.val = b.val.set i.val true ⦄ := by
  rw [bits.set.eq_def]
  dsimp only
  split
  · have hib : i.val < b.val.length := by scalar_tac
    step as ⟨ x, back, hx, hback ⟩
    simp [hback]
  · have hib : b.val.length ≤ i.val := by scalar_tac
    rw [List.set_eq_of_length_le hib]
    exact rfl

/-! ## `zeros` -- the first accumulating loop -/

/-- One `push` of the accumulator, as the run of falses still owed. -/
theorem replicate_sub_succ {α} (x : α) (i i' n : Nat) (h : i < n) (h' : i' = i + 1) :
    x :: List.replicate (n - i') x = List.replicate (n - i) x := by
  subst h'
  have hn : n - i = (n - (i + 1)) + 1 := by omega
  rw [hn, List.replicate_succ]

/-- The loop, on the fuel `k` that bounds the iterations it has left.

`hbl` is the loop invariant `Vec.push` needs: the accumulator is exactly `i` long,
so it is shorter than `Usize.max` while `i < n`, and the push cannot fail. -/
theorem zeros_loop_spec (n : Usize) :
    ∀ (k : Nat) (b : alloc.vec.Vec Bool) (i : Usize),
      n.val - i.val ≤ k → b.val.length = i.val →
      bits.zeros_loop n b i ⦃ r => r.val = b.val ++ List.replicate (n.val - i.val) false ⦄ := by
  intro k
  induction k with
  | zero =>
    intro b i hk hbl
    rw [bits.zeros_loop.eq_def]
    split
    · exfalso; scalar_tac
    · have hz : n.val - i.val = 0 := by scalar_tac
      simp [hz]
  | succ k ih =>
    intro b i hk hbl
    rw [bits.zeros_loop.eq_def]
    split
    · have hin : i.val < n.val := by scalar_tac
      have hbmax : b.val.length < Usize.max := by scalar_tac
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      step as ⟨ b1, hb1 ⟩
      step as ⟨ i2, hi2 ⟩
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      have hb1l : b1.val.length = i2.val := by simp [hb1, hbl, hi2']
      refine WP.spec_mono (ih b1 i2 (by scalar_tac) hb1l) ?_
      intro r hr
      rw [hr, hb1, List.append_assoc, List.singleton_append,
        replicate_sub_succ false i.val i2.val n.val hin hi2']
    · have hz : n.val - i.val = 0 := by scalar_tac
      simp [hz]

/-- The wrapper: `n` falses. -/
@[step]
theorem zeros_spec (n : Usize) :
    bits.zeros n ⦃ r => r.val = List.replicate n.val false ⦄ := by
  rw [bits.zeros.eq_def]
  have h := zeros_loop_spec n n.val (alloc.vec.Vec.new Bool) 0#usize
    (by scalar_tac) (by simp)
  simpa using h

/-! ## `union` -- pointwise `or` into `a` -/

/-- The step of the `union` induction, as pure list arithmetic.

`a1` is `a` with index `i` already or-ed, `r` is what the loop returns from `i+1`,
and the conclusion is what the loop must return from `i`. Kept off the `Vec`
wrapper deliberately: nothing here can trip the `Slice`/`Vec` motive check. -/
theorem union_step {a a1 r bl : List Bool} {i i' : Nat}
    (hi : i' = i + 1)
    (hlen : a1.length = a.length)
    (hpt : ∀ j, j < a.length → a1[j]! = if j = i then (a[j]! || bl[j]!) else a[j]!)
    (hrlen : r.length = a1.length)
    (hr : ∀ j, j < a1.length → r[j]! = if j < i' then a1[j]! else (a1[j]! || bl[j]!)) :
    r.length = a.length ∧
    ∀ j, j < a.length → r[j]! = if j < i then a[j]! else (a[j]! || bl[j]!) := by
  subst hi
  refine ⟨hrlen.trans hlen, fun j hj => ?_⟩
  have hj1 : j < a1.length := by omega
  rw [hr j hj1, hpt j hj]
  rcases Nat.lt_trichotomy j i with h | h | h
  · have h1 : j < i + 1 := by omega
    have h2 : ¬ (j = i) := by omega
    simp [h1, h2, h]
  · subst h; simp
  · have h1 : ¬ (j < i + 1) := by omega
    have h2 : ¬ (j = i) := by omega
    have h3 : ¬ (j < i) := by omega
    simp [h1, h2, h3]

/-- The loop, on the fuel `k`. Indices below `i` are untouched, indices from `i`
carry the `or`; the length never moves, which is what lets the loop keep writing
`a[i]` while it iterates `a.len()`. -/
theorem union_loop_spec (b : Slice Bool) :
    ∀ (k : Nat) (a : alloc.vec.Vec Bool) (i : Usize), a.val.length - i.val ≤ k →
      bits.union_loop a b i ⦃ r =>
        r.val.length = a.val.length ∧
        ∀ j, j < a.val.length →
          r.val[j]! = if j < i.val then a.val[j]! else (a.val[j]! || b.val[j]!) ⦄ := by
  intro k
  induction k with
  | zero =>
    intro a i hk
    rw [bits.union_loop.eq_def]
    dsimp only
    split
    · exfalso; scalar_tac
    · refine ⟨rfl, fun j hj => ?_⟩
      rw [if_pos (by scalar_tac : j < i.val)]
  | succ k ih =>
    intro a i hk
    rw [bits.union_loop.eq_def]
    dsimp only
    split
    · have hia : i.val < a.val.length := by scalar_tac
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      step as ⟨ bb, hbb ⟩
      split
      · rename_i hbt
        -- The two-result spec of `index_mut` arrives as ONE pair variable here,
        -- so the write-back function has to be dug out and substituted by hand.
        step as ⟨ p, hp1, hp2 ⟩
        obtain ⟨ pv, back ⟩ := p
        have hbackv : back = alloc.vec.Vec.set a i := hp2
        subst hbackv
        step as ⟨ i2, hi2 ⟩
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        have hbi : b.val[i.val]! = true := by rw [← hbb]; exact hbt
        have ha1 : (alloc.vec.Vec.set a i true).val = a.val.set i.val true := by simp
        have ha1len : (alloc.vec.Vec.set a i true).val.length = a.val.length := by
          rw [ha1]; simp
        have ha1pt : ∀ j, j < a.val.length →
            (alloc.vec.Vec.set a i true).val[j]! =
              if j = i.val then (a.val[j]! || b.val[j]!) else a.val[j]! := by
          intro j hj
          rw [ha1]
          by_cases hji : j = i.val
          · subst hji
            rw [if_pos rfl, hbi, List.set_getElem!_eq a.val i.val i.val true ⟨hia, rfl⟩]
            simp
          · rw [if_neg hji, List.set_getElem!_ne a.val i.val j true (Or.inr (Or.inl hji))]
        refine WP.spec_mono (ih (alloc.vec.Vec.set a i true) i2 (by scalar_tac)) ?_
        rintro r ⟨hrlen, hrpt⟩
        exact union_step hi2' ha1len ha1pt hrlen hrpt
      · rename_i hbf
        step as ⟨ i2, hi2 ⟩
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        have hbi : b.val[i.val]! = false := by rw [← hbb]; simpa using hbf
        have ha1pt : ∀ j, j < a.val.length →
            a.val[j]! = if j = i.val then (a.val[j]! || b.val[j]!) else a.val[j]! := by
          intro j hj
          by_cases hji : j = i.val
          · subst hji; rw [if_pos rfl, hbi]; simp
          · rw [if_neg hji]
        refine WP.spec_mono (ih a i2 (by scalar_tac)) ?_
        rintro r ⟨hrlen, hrpt⟩
        exact union_step hi2' rfl ha1pt hrlen hrpt
    · refine ⟨rfl, fun j hj => ?_⟩
      rw [if_pos (by scalar_tac : j < i.val)]

/-- The wrapper: `a` or `b`, pointwise, at `a`'s length. -/
@[step]
theorem union_spec (a : alloc.vec.Vec Bool) (b : Slice Bool) :
    bits.union a b ⦃ r =>
      r.val.length = a.val.length ∧
      ∀ j, j < a.val.length → r.val[j]! = (a.val[j]! || b.val[j]!) ⦄ := by
  rw [bits.union.eq_def]
  refine WP.spec_mono (union_loop_spec b a.val.length a 0#usize (by scalar_tac)) ?_
  rintro r ⟨h1, h2⟩
  refine ⟨h1, fun j hj => ?_⟩
  rw [h2 j hj, if_neg (by scalar_tac : ¬ (j < (0#usize).val))]

/-! ## `of_ids` -- the set of the given ids -/

/-- What `of_ids`' loop does to its accumulator: set every listed index. -/
def setIds (s : List Bool) (ids : List Usize) : List Bool :=
  ids.foldl (fun s x => s.set x.val true) s

theorem setIds_nil (s : List Bool) : setIds s [] = s := rfl

theorem setIds_cons (s : List Bool) (x : Usize) (l : List Usize) :
    setIds s (x :: l) = setIds (s.set x.val true) l := rfl

theorem setIds_length (l : List Usize) :
    ∀ (s : List Bool), (setIds s l).length = s.length := by
  induction l with
  | nil => intro s; rfl
  | cons x xs ih => intro s; rw [setIds_cons, ih]; simp

/-- The membership reading: inside the original length, a bit is set exactly when
its index is listed. -/
theorem setIds_getElem! (l : List Usize) :
    ∀ (s : List Bool) (j : Nat), j < s.length →
      ((setIds s l)[j]! = true ↔ (s[j]! = true ∨ ∃ x ∈ l, x.val = j)) := by
  induction l with
  | nil => intro s j h; simp [setIds_nil]
  | cons x xs ih =>
    intro s j h
    have h' : j < (s.set x.val true).length := by simp [h]
    rw [setIds_cons, ih (s.set x.val true) j h']
    by_cases hxj : x.val = j
    · subst hxj
      have hset : (s.set x.val true)[x.val]! = true :=
        List.set_getElem!_eq s x.val x.val true ⟨h, rfl⟩
      simp [hset]
    · simp [hxj]

/-- The loop, on the fuel `k`: the ids still to come are the ones still to set. -/
theorem of_ids_loop_spec (ids : Slice Usize) :
    ∀ (k : Nat) (b : alloc.vec.Vec Bool) (i : Usize), ids.val.length - i.val ≤ k →
      bits.of_ids_loop ids b i ⦃ r => r.val = setIds b.val (ids.val.drop i.val) ⦄ := by
  intro k
  induction k with
  | zero =>
    intro b i hk
    rw [bits.of_ids_loop.eq_def]
    dsimp only
    split
    · exfalso; scalar_tac
    · have hnil : ids.val.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
      rw [hnil, setIds_nil]
      exact rfl
  | succ k ih =>
    intro b i hk
    rw [bits.of_ids_loop.eq_def]
    dsimp only
    split
    · have hii : i.val < ids.val.length := by scalar_tac
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      step as ⟨ x, hx ⟩
      step as ⟨ b1, hb1 ⟩
      step as ⟨ i3, hi3 ⟩
      have hi3' : i3.val = i.val + 1 := by scalar_tac
      have hd : ids.val.drop i.val = x :: ids.val.drop (i.val + 1) := by
        rw [hx]; exact List.drop_eq_getElem_cons hii
      refine WP.spec_mono (ih b1 i3 (by scalar_tac)) ?_
      intro r hr
      rw [hr, hb1, hi3', hd, setIds_cons]
    · have hnil : ids.val.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
      rw [hnil, setIds_nil]
      exact rfl

/-- The wrapper, in the form the clauses want: `n` bits, and bit `j` is set
exactly when `j` is one of the ids. Ids at or beyond `n` set nothing. -/
@[step]
theorem of_ids_spec (n : Usize) (ids : Slice Usize) :
    bits.of_ids n ids ⦃ r =>
      r.val.length = n.val ∧
      ∀ j, j < n.val → (r.val[j]! = true ↔ ∃ x ∈ ids.val, x.val = j) ⦄ := by
  rw [bits.of_ids.eq_def]
  step as ⟨ b0, hb0 ⟩
  have hb0len : b0.val.length = n.val := by rw [hb0]; simp
  refine WP.spec_mono (of_ids_loop_spec ids ids.val.length b0 0#usize (by scalar_tac)) ?_
  intro r hr
  have hfold : r.val = setIds b0.val ids.val := by simpa using hr
  refine ⟨?_, fun j hj => ?_⟩
  · rw [hfold, setIds_length, hb0len]
  · have hjb : j < b0.val.length := by omega
    have hbj : b0.val[j]! = false := by rw [hb0]; exact List.getElem!_replicate false hj
    rw [hfold, setIds_getElem! ids.val b0.val j hjb, hbj]
    simp

/- What the theorems actually rest on. `Audit.lean` prints this for the
   obligations; a helper that nothing audits yet prints its own, so a `sorry`
   reached through the Aeneas standard library cannot arrive here unnoticed. -/
#print axioms SolverProof.eq_loop_spec
#print axioms SolverProof.eq_spec
#print axioms SolverProof.get_spec
#print axioms SolverProof.set_spec
#print axioms SolverProof.zeros_loop_spec
#print axioms SolverProof.zeros_spec
#print axioms SolverProof.union_loop_spec
#print axioms SolverProof.union_spec
#print axioms SolverProof.of_ids_loop_spec
#print axioms SolverProof.of_ids_spec

end SolverProof
