/-
  `shape` and `derived` -- the two clauses decided by comparing DENSE BIT SETS.

  Read `Proof/Basis.lean` first for the loop recipe, and `Proof/Bits.lean` for
  the four `bits` primitives these clauses rest on. What follows is what a clause
  whose conjunct is a SET EQUALITY needs on top of that, and one finding that
  matters more than the proofs.

  ## The finding: neither clause is independent of `indexed`

  `lib.rs` says the ten clauses are independent -- "every accessor is total, so a
  malformed plan produces a meaningless ancestry table and is rejected by
  `Indexed` anyway ... which is what lets the proof be one equation with ten
  lemmas rather than a chain where each rests on the last". For these two that is
  NOT TRUE, and the reason is the representation rather than the judgement.

  `same_slots a b nn` is `bits::eq(of_ids(nn, a), of_ids(nn, b))`, and `of_ids`
  writes exactly `nn` bits. An id at or beyond `nn` sets no bit, so it is
  INVISIBLE to the comparison. What the checker decides is therefore not
  `SolverSpec.SameSet a b` but `SameSetLt nn a b` -- agreement below `nn` only.
  `derived_at` has the same shape at `ne = nEndpoints q`.

  So the unconditional clause lemma the obligation would like,

      clauses.cl_shape p q ⦃ r => r = decide (∀ s ∈ steps q, Shape p s) ⦄

  is FALSE. Take a problem with NO nodes and one transform that requires nothing
  and produces nothing, and a plan whose one step binds slot `0`. Then `nn = 0`,
  both bit sets are empty, `shape_at` accepts -- while `Shape` demands
  `SameSet [0] []`, which fails. `shapeR_not_shape` and `derivedR_not_derived`
  below are that gap, machine checked. The `derived` instance is the same shape
  one level down: an endpoint whose declared parent is `5` in a plan with one
  endpoint. Both were also run against the crate: `cl_shape` and `cl_derived`
  return `true` on them, and `cl_indexed` is the only clause that returns
  `false`.

  Two theorems are proved for each clause, and the pair is the honest answer:

  - `cl_shape_raw_spec` / `cl_derived_raw_spec` -- UNCONDITIONAL and exact, over
    `ShapeR` / `DerivedR`, which are the specification's `Shape` / `Derived` with
    every set comparison confined to the table the bits index.
  - `cl_shape_spec` / `cl_derived_spec` -- the conjunct of `SolverSpec.ValidC`
    verbatim, under `WellIndexed p q`, which is clause 0.

  `check` is still correct, and `check_spec` still goes through: `check` is the
  CONJUNCTION of the ten, so either `cl_indexed` is false -- and then both
  `check` and `Valid` are false -- or `WellIndexed` holds and the two lemmas
  above apply. The assembler has to make that case split rather than composing
  ten unconditional equations. The alternative repair is in the Rust: have
  `same_slots` and `derived_at` reject an id at or beyond the width they compare
  at, instead of ignoring it.

  ## Namespacing

  Everything general lives in `SolverProof.Slots`. The sibling clause modules
  already carry their own copies of `ok_spec` and `vec_index_ok`, and two modules
  declaring one name in `SolverProof` cannot be imported together. Only the
  theorems named after a `clauses::` function sit at `SolverProof`, where they
  cannot collide.

  `required_slots_spec` and `no_repeat_spec` are SHARED with the `indexed`
  clause, which calls `no_repeat` on `required_slots`. Both are stated for every
  transform id, in range or not.

  ## What the recipe in `Basis` does not say

  **`as` is a token.** `| cons a as ih =>` does not parse in this project:
  Aeneas's `step ... as` claims the identifier, and the error is "unexpected
  token 'as'; expected command", pointing at a line whose `induction` was fine.

  **Basis's postcondition rule extends to `Slice`.** A `decide` postcondition may
  not mention a `Slice`'s `.val` either -- `Decidable` synthesis runs at
  `instances` transparency and will not unfold it. Every `decide`-carrying
  statement below hoists its list to a parameter with a defining equation: `L`,
  `prod`, `decl`, `grp`, `u`, `sv`.

  **`rw [← hL]` is a dependent motive failure whenever `L` indexes a `getElem`.**
  `hL : l.map (·.val) = L` cannot be used right-to-left in a goal mentioning
  `L[j]'h`, because `h : j < L.length` is part of the motive. `subst hL` is fine,
  and it is why `ids_getElem!` and `pairs_getElem!` are stated over `getElem!`
  and proved by `subst`. `drop_cons_getElem!` is the `drop` idiom's head lemma in
  the same form, and mentions no `Slice` at all.

  **A `Nodup` conjunct wants two loops at two different lists.** `no_repeat`'s
  inner loop starts at `i + 1`, so the correspondence is over PAIRS, not over a
  decreasing prefix of one list. The `drop` idiom still applies, twice: the outer
  scan owes `(L.drop i).Nodup` and the inner owes `L[i]! ∉ L.drop j`, and
  `List.nodup_cons` is the single lemma that joins them at `j = i + 1`. Nothing
  else about the pair structure is needed.

  **A `zip` conjunct is the `drop` idiom on two lists at once.** `groups_match`
  walks the product groups by index and the specification quantifies over
  `s.produced.zip (producesOf p s.transform)`. State the loop over
  `(prod.drop i).zip (decl.drop i)`; the inductive step is `List.zip_cons_cons`
  -- one head, one tail -- exactly as for a single list. That form needs
  `prod.length = decl.length` as a hypothesis, which is sound because `shape_at`
  guards `groups_match` behind that very test; `mem_zip` is what carries a
  member of the zip back to its two sides for the range argument.

  **`rw [decide_eq_decide.mpr h]` leaves a `Decidable` instance as a side goal.**
  Rewriting under `decide` asks for the instance at the NEW proposition, and it
  is reported as "unsolved goals" naming a `Decidable`. Close a `Bool` equation
  with `Bool.eq_iff_iff.mpr`, then `simp only [Bool.and_eq_true,
  decide_eq_true_eq]`, and finish on the `Iff` where no instance is in play.

  **`step` normalises the program, and that retires `Access.index_eq`.** `step`
  simplifies with `step_simps`, one of which rewrites `Vec.index` to
  `Vec.index_usize`. After the first `step` in a proof, `rw [index_eq ...]`
  reports "did not find an occurrence of the pattern" for an index that is
  plainly there. `vec_index_usize_ok` is the same equation at the normalised
  head.

  **Pass a `deref`-typed hypothesis, never rewrite with it.** `same_slots` takes
  `Slice`s obtained by `Vec.deref`, and a lemma proved about `v.val` is accepted
  for `(deref v).val` by `exact` -- the two are defeq. `rw [hv]` on the same pair
  is refused, because `rw` matches syntactically.

  **An `ok` flag is a `&&` in the postcondition.** Every loop here carries one.
  State the spec as `r = (ok1 && decide P)`, `split` on the flag first, and the
  `false` branch is one `ok_post`. The recursive call that receives a computed
  flag then needs no case analysis on it at all -- `ih _ i2` takes it as given.

  **`rw [← hsv]` usually wants a trailing `rfl`.** `sv.produced` rewrites back to
  `(stepView s).produced`, and the remaining step to `pairLists s.produced` is
  delta on `stepView`, which `rw`'s closing `rfl` will not do.
-/

import SolverWitness.Types
import SolverWitness.Funs
import SolverWitness.Spec
import SolverWitness.Proof.Basis
import SolverWitness.Proof.Bits
import SolverWitness.Proof.Access

set_option maxHeartbeats 2000000

namespace SolverProof

open Aeneas Aeneas.Std Result
open solver_witness

namespace Slots

/-! ## Generic bridges

Everything in this namespace is local to this module deliberately: the sibling
clause proofs carry their own copies, and two modules declaring one name in
`SolverProof` cannot be imported together. -/

/-- The triple on an `ok`, as the plain proposition. -/
theorem ok_post {α} {x : α} {P : α → Prop} (h : P x) : (ok x : Result α) ⦃ r => P r ⦄ := by
  simp only [WP.spec_ok]
  exact h

/-- A `Usize` is its value. Needed pointwise wherever the checker compares two
ids with `=` and the specification compares the `Nat`s underneath. -/
theorem usize_eq_of_val {x y : Std.Usize} (h : x.val = y.val) : x = y :=
  Usize.bv_eq_imp_eq x y (BitVec.eq_of_toNat_eq h)

/-- Set equality WITHIN a range -- what a comparison of two `n`-bit sets decides.
Not `SolverSpec.SameSet`: an id at or beyond `n` sets no bit and is invisible to
both sides. -/
@[reducible] def SameSetLt (n : Nat) (a b : SolverSpec.Ids) : Prop :=
  ∀ j < n, (j ∈ a ↔ j ∈ b)

/-- Inside the range the two agree. -/
theorem sameSetLt_iff (n : Nat) (a b : SolverSpec.Ids)
    (ha : ∀ x ∈ a, x < n) (hb : ∀ x ∈ b, x < n) :
    SameSetLt n a b ↔ SolverSpec.SameSet a b := by
  constructor
  · intro h
    exact ⟨fun x hx => (h x (ha x hx)).mp hx, fun x hx => (h x (hb x hx)).mpr hx⟩
  · rintro ⟨h1, h2⟩ j _
    exact ⟨fun hj => h1 j hj, fun hj => h2 j hj⟩

/-- Two bit sets of one width are equal exactly when they agree pointwise.
Stated with `getElem!` because that is the form `bits`' specs are in. -/
theorem bool_list_eq_iff {u v : List Bool} (hl : u.length = v.length) :
    u = v ↔ ∀ j, j < u.length → (u[j]! = true ↔ v[j]! = true) := by
  constructor
  · rintro rfl j _; exact Iff.rfl
  · intro h
    refine List.ext_getElem hl (fun j h1 h2 => ?_)
    have hu : u[j]! = u[j]'h1 := getElem!_pos u j h1
    have hv : v[j]! = v[j]'h2 := getElem!_pos v j h2
    have hj := h j h1
    rw [hu, hv] at hj
    exact Bool.eq_iff_iff.mpr hj

/-- One head and one tail. Stated with `getElem!` so that nothing in it can
mention a `Slice`, which is what makes it usable inside a triple. -/
theorem drop_cons_getElem! {α} [Inhabited α] (L : List α) (j : Nat) (h : j < L.length) :
    L.drop j = L[j]! :: L.drop (j + 1) := by
  rw [List.drop_eq_getElem_cons h, List.cons.injEq]
  exact ⟨(getElem!_pos L j h).symm, rfl⟩

/-- `subst`, not `rw [← hL]`: the index proof of a `getElem` mentions `L`, so
rewriting `L` under it is a dependent motive the check refuses. -/
theorem ids_getElem! (l : List Std.Usize) (L : SolverSpec.Ids)
    (hL : l.map (fun x => x.val) = L) (j : Nat) (h : j < l.length) :
    L[j]! = (l[j]'h).val := by
  subst hL
  rw [getElem!_pos _ j (by simpa using h)]
  simp

/-- `step` normalises the program with `step_simps`, and one of those rewrites
`Vec.index` to `Vec.index_usize` -- so after the first `step` in a proof,
`Access.index_eq` no longer matches and this is the equation to use instead. -/
theorem vec_index_usize_ok {α : Type} (v : alloc.vec.Vec α) (i : Std.Usize)
    (h : i.val < v.val.length) :
    alloc.vec.Vec.index_usize v i = ok (v.val[i.val]'h) :=
  eq_of_spec (alloc.vec.Vec.index_usize_spec v i (by simpa using h))

/-- The same for a binding list. `subst` for the same reason as `ids_getElem!`. -/
theorem pairs_getElem! (l : List (Std.Usize × Std.Usize)) (u : List (Nat × Nat))
    (hu : l.map (fun b => (b.1.val, b.2.val)) = u) (j : Nat) (h : j < l.length) :
    u[j]! = ((l[j]'h).1.val, (l[j]'h).2.val) := by
  subst hu
  rw [getElem!_pos _ j (by simpa using h)]
  simp

theorem ids_length (l : List Std.Usize) (L : SolverSpec.Ids)
    (hL : l.map (fun x => x.val) = L) : L.length = l.length := by
  rw [← hL]; simp

theorem ids_mem (l : List Std.Usize) (L : SolverSpec.Ids)
    (hL : l.map (fun x => x.val) = L) (j : Nat) :
    j ∈ L ↔ ∃ x ∈ l, x.val = j := by
  rw [← hL]; exact List.mem_map

/-- Set equality WITHIN a range, lifted to the whole of `Shape`. This is what
`shape_at` decides; `shapeR_iff_shape` is where it becomes the specification's
`Shape`, and what that costs. -/
@[reducible] def ShapeR (p : types.Problem) (n : Nat) (s : SolverSpec.StepView) : Prop :=
  (SolverSpec.slotsOf s.used).Nodup ∧
  SameSetLt n (SolverSpec.slotsOf s.used) (SolverSpec.requiresOf p s.transform) ∧
  s.produced.length = (SolverSpec.producesOf p s.transform).length ∧
  ∀ gd ∈ s.produced.zip (SolverSpec.producesOf p s.transform),
    SameSetLt n (SolverSpec.slotsOf gd.1) gd.2

theorem steps_length (q : types.Plan) :
    (SolverSpec.steps q).length = q.steps.val.length := by
  simp [SolverSpec.steps]

theorem steps_drop_cons (q : types.Plan) (i : Nat) (h : i < q.steps.val.length) :
    (SolverSpec.steps q).drop i
      = SolverSpec.stepView (q.steps.val[i]'h) :: (SolverSpec.steps q).drop (i + 1) := by
  have h' : i < (SolverSpec.steps q).length := by rw [steps_length]; exact h
  rw [List.drop_eq_getElem_cons h', List.cons.injEq]
  exact ⟨by simp [SolverSpec.steps], rfl⟩

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

/-- A produced endpoint's declared lineage against what its step confers, with
both sides confined to the endpoint table -- what `derived_at` decides. -/
@[reducible] def DerivedR (q : types.Plan) (n : Nat) (u : List (Nat × Nat)) (e : Nat) : Prop :=
  SameSetLt n (SolverSpec.epParents q e) (SolverSpec.Confers q u)

/-- Two `n`-bit sets are equal exactly when the id sets they encode agree below
`n`. The one place a `bits` comparison becomes a statement about lists. -/
theorem bits_eq_iff_sameSetLt {u v : List Bool} {n : Nat} {A B : SolverSpec.Ids}
    (hu : u.length = n) (hv : v.length = n)
    (hA : ∀ j < n, (u[j]! = true ↔ j ∈ A)) (hB : ∀ j < n, (v[j]! = true ↔ j ∈ B)) :
    (u = v) ↔ SameSetLt n A B := by
  rw [bool_list_eq_iff (by rw [hu, hv])]
  constructor
  · intro h j hj
    rw [← hA j hj, ← hB j hj]
    exact h j (by omega)
  · intro h j hj
    have hj' : j < n := by omega
    rw [hA j hj', hB j hj']
    exact h j hj'

theorem mem_zip {α β} {l₁ : List α} {l₂ : List β} :
    ∀ {x : α × β}, x ∈ l₁.zip l₂ → x.1 ∈ l₁ ∧ x.2 ∈ l₂ := by
  induction l₁ generalizing l₂ with
  | nil => intro x h; simp at h
  | cons a rest ih =>
    intro x h
    cases l₂ with
    | nil => simp at h
    | cons b bs =>
      rw [List.zip_cons_cons] at h
      rcases List.mem_cons.mp h with h' | h'
      · subst h'; exact ⟨List.mem_cons_self, List.mem_cons_self⟩
      · exact ⟨List.mem_cons_of_mem _ (ih h').1, List.mem_cons_of_mem _ (ih h').2⟩

/-! ### The accumulator of a `bits::set` loop, over `Nat` ids -/

def setNats (s : List Bool) (ids : List Nat) : List Bool :=
  ids.foldl (fun s x => s.set x true) s

theorem setNats_nil (s : List Bool) : setNats s [] = s := rfl

theorem setNats_cons (s : List Bool) (x : Nat) (l : List Nat) :
    setNats s (x :: l) = setNats (s.set x true) l := rfl

theorem setNats_append (l1 : List Nat) :
    ∀ (s : List Bool) (l2 : List Nat), setNats s (l1 ++ l2) = setNats (setNats s l1) l2 := by
  intro s l2
  simp [setNats, List.foldl_append]

theorem setNats_length (l : List Nat) :
    ∀ (s : List Bool), (setNats s l).length = s.length := by
  induction l with
  | nil => intro s; rfl
  | cons x xs ih => intro s; rw [setNats_cons, ih]; simp

theorem setNats_getElem! (l : List Nat) :
    ∀ (s : List Bool) (j : Nat), j < s.length →
      ((setNats s l)[j]! = true ↔ (s[j]! = true ∨ j ∈ l)) := by
  induction l with
  | nil => intro s j h; simp [setNats_nil]
  | cons x xs ih =>
    intro s j h
    have h' : j < (s.set x true).length := by simp [h]
    rw [setNats_cons, ih (s.set x true) j h']
    by_cases hxj : j = x
    · subst hxj
      have hset : (s.set j true)[j]! = true := List.set_getElem!_eq s j j true ⟨h, rfl⟩
      simp [hset]
    · rw [List.set_getElem!_ne s x j true (Or.inr (Or.inl hxj))]
      simp [hxj]

/-- What one iteration of `confers` writes, flattened: the endpoint it consumed
and that endpoint's own parents. Same membership as `SolverSpec.Confers`, in the
order the loop visits them. -/
def confersList (q : types.Plan) (u : List (Nat × Nat)) : List Nat :=
  u.flatMap (fun b => b.2 :: SolverSpec.epParents q b.2)

theorem confersList_cons (q : types.Plan) (b : Nat × Nat) (u : List (Nat × Nat)) :
    confersList q (b :: u) = (b.2 :: SolverSpec.epParents q b.2) ++ confersList q u := by
  simp [confersList]

theorem mem_confersList (q : types.Plan) (u : List (Nat × Nat)) (m : Nat) :
    m ∈ confersList q u ↔ m ∈ SolverSpec.Confers q u := by
  simp only [confersList, SolverSpec.Confers, List.mem_flatMap, List.mem_append,
    List.mem_map, List.mem_cons]
  constructor
  · rintro ⟨b, hb, h | h⟩
    · exact Or.inl ⟨b, hb, h.symm⟩
    · exact Or.inr ⟨b.2, ⟨b, hb, rfl⟩, h⟩
  · rintro (⟨b, hb, h⟩ | ⟨f, ⟨b, hb, rfl⟩, h⟩)
    · exact ⟨b, hb, Or.inl h.symm⟩
    · exact ⟨b, hb, Or.inr h⟩

end Slots

open Slots

/-! ## `used_slots` and `produced_slots` -- the two collectors over a step -/

/-- The loop, on the fuel `k`. `out.val.length = i.val` is what `Vec.push` needs:
the accumulator is exactly `i` long, so it is shorter than `Usize.max` while the
loop runs. -/
theorem used_slots_loop_spec (q : types.Plan) (si : Std.Usize)
    (hsi : si.val < q.steps.val.length) :
    ∀ (k : Nat) (out : alloc.vec.Vec Std.Usize) (i : Std.Usize),
      (q.steps.val[si.val]'hsi).used.val.length - i.val ≤ k →
      out.val.length = i.val →
      clauses.used_slots_loop q si out i ⦃ r =>
        r.val = out.val ++ ((q.steps.val[si.val]'hsi).used.val.map Prod.fst).drop i.val ⦄ := by
  intro k
  induction k with
  | zero =>
    intro out i hk hol
    rw [clauses.used_slots_loop.eq_def, index_eq q.steps si hsi]
    simp only [bind_tc_ok]
    split
    · exfalso; scalar_tac
    · have hnil : ((q.steps.val[si.val]'hsi).used.val.map Prod.fst).drop i.val = [] :=
        List.drop_eq_nil_iff.mpr (by simp only [List.length_map]; scalar_tac)
      exact ok_post (by rw [hnil, List.append_nil])
  | succ k ih =>
    intro out i hk hol
    rw [clauses.used_slots_loop.eq_def, index_eq q.steps si hsi]
    simp only [bind_tc_ok]
    split
    · have hiu : i.val < (q.steps.val[si.val]'hsi).used.val.length := by scalar_tac
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      have hmax : out.val.length < Usize.max := by scalar_tac
      obtain ⟨d, e, hde⟩ :
          ∃ d e, (q.steps.val[si.val]'hsi).used.val[i.val]'hiu = (d, e) := ⟨_, _, rfl⟩
      rw [index_eq _ i hiu, hde]
      show (do
          let out1 ← alloc.vec.Vec.push out d
          let i3 ← i + 1#usize
          clauses.used_slots_loop q si out1 i3) ⦃ r =>
            r.val = out.val
              ++ ((q.steps.val[si.val]'hsi).used.val.map Prod.fst).drop i.val ⦄
      step as ⟨ out1, hout1 ⟩
      step as ⟨ i3, hi3 ⟩
      have hi3' : i3.val = i.val + 1 := by scalar_tac
      have hout1l : out1.val.length = i3.val := by rw [hout1]; simp [hol, hi3']
      have hcons : ((q.steps.val[si.val]'hsi).used.val.map Prod.fst).drop i.val
          = d :: ((q.steps.val[si.val]'hsi).used.val.map Prod.fst).drop (i.val + 1) := by
        have h' : i.val < ((q.steps.val[si.val]'hsi).used.val.map Prod.fst).length := by
          simp only [List.length_map]; exact hiu
        rw [List.drop_eq_getElem_cons h', List.cons.injEq]
        exact ⟨by rw [List.getElem_map, hde], rfl⟩
      refine WP.spec_mono (ih out1 i3 (by scalar_tac) hout1l) ?_
      intro r hr
      rw [hr, hout1, hi3', List.append_assoc, List.singleton_append, ← hcons]
    · have hnil : ((q.steps.val[si.val]'hsi).used.val.map Prod.fst).drop i.val = [] :=
        List.drop_eq_nil_iff.mpr (by simp only [List.length_map]; scalar_tac)
      exact ok_post (by rw [hnil, List.append_nil])

theorem used_slots_spec (q : types.Plan) (si : Std.Usize)
    (hsi : si.val < q.steps.val.length) :
    clauses.used_slots q si ⦃ r =>
      r.val.map (fun x => x.val)
        = SolverSpec.slotsOf (SolverSpec.stepView (q.steps.val[si.val]'hsi)).used ⦄ := by
  rw [clauses.used_slots.eq_def]
  refine WP.spec_mono (used_slots_loop_spec q si hsi
    (q.steps.val[si.val]'hsi).used.val.length (alloc.vec.Vec.new Std.Usize) 0#usize
    (by scalar_tac) (by simp)) ?_
  intro r hr
  rw [hr]
  simp [SolverSpec.slotsOf, SolverSpec.stepView, SolverSpec.idPairs]

theorem produced_slots_loop_spec (q : types.Plan) (si gi : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (hgi : gi.val < (q.steps.val[si.val]'hsi).produced.val.length) :
    ∀ (k : Nat) (out : alloc.vec.Vec Std.Usize) (i : Std.Usize),
      ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length - i.val ≤ k →
      out.val.length = i.val →
      clauses.produced_slots_loop q si gi out i ⦃ r =>
        r.val = out.val
          ++ (((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.map Prod.fst).drop i.val ⦄ := by
  intro k
  induction k with
  | zero =>
    intro out i hk hol
    rw [clauses.produced_slots_loop.eq_def, index_eq q.steps si hsi]
    simp only [bind_tc_ok]
    rw [index_eq _ gi hgi]
    simp only [bind_tc_ok]
    split
    · exfalso; scalar_tac
    · have hnil :
          (((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.map Prod.fst).drop i.val = [] :=
        List.drop_eq_nil_iff.mpr (by simp only [List.length_map]; scalar_tac)
      exact ok_post (by rw [hnil, List.append_nil])
  | succ k ih =>
    intro out i hk hol
    rw [clauses.produced_slots_loop.eq_def, index_eq q.steps si hsi]
    simp only [bind_tc_ok]
    rw [index_eq _ gi hgi]
    simp only [bind_tc_ok]
    split
    · have hiu : i.val
          < ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length := by scalar_tac
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      have hmax : out.val.length < Usize.max := by scalar_tac
      obtain ⟨d, e, hde⟩ :
          ∃ d e, ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val[i.val]'hiu = (d, e) :=
        ⟨_, _, rfl⟩
      rw [index_eq _ i hiu, hde]
      show (do
          let out1 ← alloc.vec.Vec.push out d
          let i3 ← i + 1#usize
          clauses.produced_slots_loop q si gi out1 i3) ⦃ r =>
            r.val = out.val
              ++ (((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.map Prod.fst).drop i.val ⦄
      step as ⟨ out1, hout1 ⟩
      step as ⟨ i3, hi3 ⟩
      have hi3' : i3.val = i.val + 1 := by scalar_tac
      have hout1l : out1.val.length = i3.val := by rw [hout1]; simp [hol, hi3']
      have hcons :
          (((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.map Prod.fst).drop i.val
            = d :: (((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.map Prod.fst).drop
                (i.val + 1) := by
        have h' : i.val
            < (((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.map Prod.fst).length := by
          simp only [List.length_map]; exact hiu
        rw [List.drop_eq_getElem_cons h', List.cons.injEq]
        exact ⟨by rw [List.getElem_map, hde], rfl⟩
      refine WP.spec_mono (ih out1 i3 (by scalar_tac) hout1l) ?_
      intro r hr
      rw [hr, hout1, hi3', List.append_assoc, List.singleton_append, ← hcons]
    · have hnil :
          (((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.map Prod.fst).drop i.val = [] :=
        List.drop_eq_nil_iff.mpr (by simp only [List.length_map]; scalar_tac)
      exact ok_post (by rw [hnil, List.append_nil])

theorem produced_slots_spec (q : types.Plan) (si gi : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (hgi : gi.val < (q.steps.val[si.val]'hsi).produced.val.length) :
    clauses.produced_slots q si gi ⦃ r =>
      r.val.map (fun x => x.val)
        = SolverSpec.slotsOf
            (SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi)) ⦄ := by
  rw [clauses.produced_slots.eq_def]
  refine WP.spec_mono (produced_slots_loop_spec q si gi hsi hgi
    ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length
    (alloc.vec.Vec.new Std.Usize) 0#usize (by scalar_tac) (by simp)) ?_
  intro r hr
  rw [hr]
  simp [SolverSpec.slotsOf, SolverSpec.idPairs]

/-! ## `required_slots` and `declared_slots` -- the two collectors over a transform

`required_slots` is SHARED with the `indexed` clause, which calls `no_repeat` on
it. Both are therefore stated for every `t`, in range or not: `access` is total,
and out of range the specification's `requiresOf` is `[]` and the loop collects
nothing. -/

theorem required_slots_loop_spec (p : types.Problem) (t : Std.Usize) :
    ∀ (k : Nat) (out : alloc.vec.Vec Std.Usize) (n i : Std.Usize),
      n.val = (SolverSpec.requiresOf p t.val).length →
      n.val - i.val ≤ k → out.val.length = i.val →
      clauses.required_slots_loop p t out n i ⦃ r =>
        r.val.map (fun x => x.val)
          = out.val.map (fun x => x.val) ++ (SolverSpec.requiresOf p t.val).drop i.val ⦄ := by
  intro k
  induction k with
  | zero =>
    intro out n i hn hk hol
    rw [clauses.required_slots_loop.eq_def]
    split
    · exfalso; scalar_tac
    · have hnil : (SolverSpec.requiresOf p t.val).drop i.val = [] :=
        List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil, List.append_nil])
  | succ k ih =>
    intro out n i hn hk hol
    rw [clauses.required_slots_loop.eq_def]
    split
    · have hi : i.val < (SolverSpec.requiresOf p t.val).length := by scalar_tac
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      have hmax : out.val.length < Usize.max := by scalar_tac
      step as ⟨ x, hx ⟩
      have hxv : x.val = (SolverSpec.requiresOf p t.val)[i.val]'hi := by
        rw [hx, List.getElem?_eq_getElem hi]; rfl
      step as ⟨ out1, hout1 ⟩
      step as ⟨ i2, hi2 ⟩
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      have hout1l : out1.val.length = i2.val := by rw [hout1]; simp [hol, hi2']
      have hcons : (SolverSpec.requiresOf p t.val).drop i.val
          = x.val :: (SolverSpec.requiresOf p t.val).drop (i.val + 1) := by
        rw [hxv]; exact List.drop_eq_getElem_cons hi
      refine WP.spec_mono (ih out1 n i2 hn (by scalar_tac) hout1l) ?_
      intro r hr
      rw [hr, hout1, hi2', hcons]
      simp
    · have hnil : (SolverSpec.requiresOf p t.val).drop i.val = [] :=
        List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil, List.append_nil])

/-- SHARED with the `indexed` clause. Total in `t`. -/
theorem required_slots_spec (p : types.Problem) (t : Std.Usize) :
    clauses.required_slots p t ⦃ r =>
      r.val.map (fun x => x.val) = SolverSpec.requiresOf p t.val ⦄ := by
  rw [clauses.required_slots.eq_def]
  step as ⟨ n, hn ⟩
  refine WP.spec_mono (required_slots_loop_spec p t n.val
    (alloc.vec.Vec.new Std.Usize) n 0#usize hn (by scalar_tac) (by simp)) ?_
  intro r hr
  rw [hr]
  simp

theorem declared_slots_loop_spec (p : types.Problem) (t gi : Std.Usize) :
    ∀ (k : Nat) (out : alloc.vec.Vec Std.Usize) (n i : Std.Usize),
      n.val = (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).length →
      n.val - i.val ≤ k → out.val.length = i.val →
      clauses.declared_slots_loop p t gi out n i ⦃ r =>
        r.val.map (fun x => x.val)
          = out.val.map (fun x => x.val)
            ++ (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop i.val ⦄ := by
  intro k
  induction k with
  | zero =>
    intro out n i hn hk hol
    rw [clauses.declared_slots_loop.eq_def]
    split
    · exfalso; scalar_tac
    · have hnil : (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop i.val = [] :=
        List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil, List.append_nil])
  | succ k ih =>
    intro out n i hn hk hol
    rw [clauses.declared_slots_loop.eq_def]
    split
    · have hi : i.val < (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).length := by
        scalar_tac
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      have hmax : out.val.length < Usize.max := by scalar_tac
      step as ⟨ x, hx ⟩
      have hxv : x.val = (((SolverSpec.producesOf p t.val)[gi.val]?).getD [])[i.val]'hi := by
        rw [hx, List.getElem?_eq_getElem hi]; rfl
      step as ⟨ out1, hout1 ⟩
      step as ⟨ i2, hi2 ⟩
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      have hout1l : out1.val.length = i2.val := by rw [hout1]; simp [hol, hi2']
      have hcons : (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop i.val
          = x.val :: (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop (i.val + 1) := by
        rw [hxv]; exact List.drop_eq_getElem_cons hi
      refine WP.spec_mono (ih out1 n i2 hn (by scalar_tac) hout1l) ?_
      intro r hr
      rw [hr, hout1, hi2', hcons]
      simp
    · have hnil : (((SolverSpec.producesOf p t.val)[gi.val]?).getD []).drop i.val = [] :=
        List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil, List.append_nil])

theorem declared_slots_spec (p : types.Problem) (t gi : Std.Usize) :
    clauses.declared_slots p t gi ⦃ r =>
      r.val.map (fun x => x.val) = ((SolverSpec.producesOf p t.val)[gi.val]?).getD [] ⦄ := by
  rw [clauses.declared_slots.eq_def]
  step as ⟨ n, hn ⟩
  refine WP.spec_mono (declared_slots_loop_spec p t gi n.val
    (alloc.vec.Vec.new Std.Usize) n 0#usize hn (by scalar_tac) (by simp)) ?_
  intro r hr
  rw [hr]
  simp

/-! ## `no_repeat` -- the one loop whose correspondence is over PAIRS

SHARED with the `indexed` clause, which calls it on `required_slots`. The inner
loop starts at `i + 1`, so the postcondition cannot be a decreasing prefix of one
list: the outer scan owes `Nodup` of the suffix, and the inner scan owes that ONE
element is absent from a suffix. `List.nodup_cons` is the join between them, and
it is the only place a `Nodup` conjunct needs anything beyond the `drop`
idiom. -/

theorem no_repeat_inner_spec (a : Slice Std.Usize) (L : SolverSpec.Ids)
    (hL : a.val.map (fun x => x.val) = L) (i : Std.Usize) (hia : i.val < a.val.length) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), a.val.length - j.val ≤ m →
      clauses.no_repeat_loop0_loop0 a ok1 i j ⦃ r =>
        r = (ok1 && decide (∀ y ∈ L.drop j.val, L[i.val]! ≠ y)) ⦄ := by
  have hlen : L.length = a.val.length := ids_length a.val L hL
  intro m
  induction m with
  | zero =>
    intro ok1 j hk
    rw [clauses.no_repeat_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : L.drop j.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hk
    rw [clauses.no_repeat_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hja : j.val < a.val.length := by scalar_tac
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hjL : j.val < L.length := by omega
        have hcons : L.drop j.val = L[j.val]! :: L.drop (j.val + 1) :=
          drop_cons_getElem! L j.val hjL
        step as ⟨ x2, hx2 ⟩
        step as ⟨ x3, hx3 ⟩
        have hLi : L[i.val]! = x2.val := by rw [hx2]; exact ids_getElem! a.val L hL i.val hia
        have hLj : L[j.val]! = x3.val := by rw [hx3]; exact ids_getElem! a.val L hL j.val hja
        split
        · next heq =>
          have hbad : ¬ (∀ y ∈ L.drop j.val, L[i.val]! ≠ y) := by
            intro hc
            exact hc L[j.val]! (by rw [hcons]; exact List.mem_cons_self)
              (by rw [hLi, hLj, heq])
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          refine WP.spec_mono (ih false j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, decide_eq_false hbad]
          simp
        · next hne =>
          have hnev : L[i.val]! ≠ L[j.val]! := by
            intro hc
            rw [hLi, hLj] at hc
            exact hne (usize_eq_of_val hc)
          have hiff : (∀ y ∈ L.drop (j.val + 1), L[i.val]! ≠ y)
              ↔ (∀ y ∈ L.drop j.val, L[i.val]! ≠ y) := by
            rw [hcons]
            constructor
            · intro h y hy
              rcases List.mem_cons.mp hy with hy' | hy'
              · rw [hy']; exact hnev
              · exact h y hy'
            · intro h y hy
              exact h y (List.mem_cons_of_mem _ hy)
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true j1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, hj1']
          simp only [Bool.true_and]
          exact decide_eq_decide.mpr hiff
      · have hnil : L.drop j.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

theorem no_repeat_outer_spec (a : Slice Std.Usize) (L : SolverSpec.Ids)
    (hL : a.val.map (fun x => x.val) = L) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), a.val.length - i.val ≤ m →
      clauses.no_repeat_loop0 a ok1 i ⦃ r => r = (ok1 && decide (L.drop i.val).Nodup) ⦄ := by
  have hlen : L.length = a.val.length := ids_length a.val L hL
  intro m
  induction m with
  | zero =>
    intro ok1 i hk
    rw [clauses.no_repeat_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : L.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hk
    rw [clauses.no_repeat_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hia : i.val < a.val.length := by scalar_tac
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hiL : i.val < L.length := by omega
        have hcons : L.drop i.val = L[i.val]! :: L.drop (i.val + 1) :=
          drop_cons_getElem! L i.val hiL
        step as ⟨ j, hj ⟩
        have hj' : j.val = i.val + 1 := by scalar_tac
        have hin := eq_of_spec (no_repeat_inner_spec a L hL i hia a.val.length true j
          (by scalar_tac))
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ i2, hi2 ⟩
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        have hiff : (L.drop i.val).Nodup
            ↔ ((∀ y ∈ L.drop (i.val + 1), L[i.val]! ≠ y) ∧ (L.drop (i.val + 1)).Nodup) := by
          rw [hcons, List.nodup_cons]
          constructor
          · rintro ⟨h1, h2⟩
            exact ⟨fun y hy hc => h1 (by rw [hc]; exact hy), h2⟩
          · rintro ⟨h1, h2⟩
            exact ⟨fun hc => h1 _ hc rfl, h2⟩
        refine WP.spec_mono (ih _ i2 (by scalar_tac)) ?_
        intro r hr
        rw [hr, hh, hj', hi2']
        simp only [Bool.true_and]
        refine Bool.eq_iff_iff.mpr ?_
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        exact hiff.symm
      · have hnil : L.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

/-- SHARED with the `indexed` clause. No hypothesis but the naming of the id
list: the Rust needs none, and the `indexed` clause calls this on a transform's
requirement list rather than on a step's bindings. -/
theorem no_repeat_spec (a : Slice Std.Usize) (L : SolverSpec.Ids)
    (hL : a.val.map (fun x => x.val) = L) :
    clauses.no_repeat a ⦃ r => r = decide L.Nodup ⦄ := by
  rw [clauses.no_repeat.eq_def]
  refine WP.spec_mono (no_repeat_outer_spec a L hL a.val.length true 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp

/-! ## `same_slots` -- set equality, WITHIN THE NODE TABLE

`of_ids` writes `nn` bits, so an id at or beyond `nn` sets nothing and is
invisible to the comparison. The postcondition says exactly that. -/

theorem same_slots_spec (a b : Slice Std.Usize) (nn : Std.Usize)
    (la lb : SolverSpec.Ids)
    (hla : a.val.map (fun x => x.val) = la) (hlb : b.val.map (fun x => x.val) = lb) :
    clauses.same_slots a b nn ⦃ r => r = decide (SameSetLt nn.val la lb) ⦄ := by
  rw [clauses.same_slots.eq_def]
  step as ⟨ v, hv1, hv2 ⟩
  step as ⟨ w, hw1, hw2 ⟩
  have hlen : v.val.length = w.val.length := by rw [hv1, hw1]
  have hkey : (v.val = w.val) ↔ SameSetLt nn.val la lb := by
    rw [bool_list_eq_iff hlen]
    constructor
    · intro h j hj
      have hjv : j < v.val.length := by omega
      rw [ids_mem a.val la hla j, ids_mem b.val lb hlb j, ← hv2 j hj, ← hw2 j hj]
      exact h j hjv
    · intro h j hj
      have hj' : j < nn.val := by omega
      rw [hv2 j hj', hw2 j hj', ← ids_mem a.val la hla j, ← ids_mem b.val lb hlb j]
      exact h j hj'
  refine WP.spec_mono (eq_spec (alloc.vec.Vec.deref v) (alloc.vec.Vec.deref w)) ?_
  intro r hr
  rw [hr]
  exact decide_eq_decide.mpr hkey


/-! ## `groups_match` -- one product group against one declared group -/

theorem groups_match_loop_spec (p : types.Problem) (q : types.Plan) (si nn t : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (prod : List (List (Nat × Nat)))
    (hprod : SolverSpec.pairLists (q.steps.val[si.val]'hsi).produced = prod)
    (decl : List SolverSpec.Ids)
    (hdecl : SolverSpec.producesOf p t.val = decl)
    (hlen : prod.length = decl.length) :
    ∀ (k : Nat) (ok1 : Bool) (i : Std.Usize), prod.length - i.val ≤ k →
      clauses.groups_match_loop p q.endpoints q.givens q.steps si nn t ok1 i ⦃ r =>
        r = (ok1 && decide (∀ gd ∈ (prod.drop i.val).zip (decl.drop i.val),
                SameSetLt nn.val (SolverSpec.slotsOf gd.1) gd.2)) ⦄ := by
  have hpl : prod.length = (q.steps.val[si.val]'hsi).produced.val.length := by
    rw [← hprod]; exact pairLists_length _
  intro k
  induction k with
  | zero =>
    intro ok1 i hk
    rw [clauses.groups_match_loop.eq_def]
    dsimp only
    split
    · next hh =>
      rw [index_eq q.steps si hsi]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : prod.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ k ih =>
    intro ok1 i hk
    rw [clauses.groups_match_loop.eq_def]
    dsimp only
    split
    · next hh =>
      rw [index_eq q.steps si hsi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hgi : i.val < (q.steps.val[si.val]'hsi).produced.val.length := by scalar_tac
        have hip : i.val < prod.length := by omega
        have hid : i.val < decl.length := by omega
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hpcons : prod.drop i.val
            = SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[i.val]'hgi)
              :: prod.drop (i.val + 1) := by
          rw [← hprod]; exact pairLists_drop_cons _ i.val hgi
        have hdcons : decl.drop i.val = decl[i.val]! :: decl.drop (i.val + 1) :=
          drop_cons_getElem! decl i.val hid
        have hdv : ((SolverSpec.producesOf p t.val)[i.val]?).getD [] = decl[i.val]! := by
          rw [hdecl, List.getElem?_eq_getElem hid, getElem!_pos decl i.val hid]
          rfl
        step with produced_slots_spec q si i hsi hgi as ⟨ v3, hv3 ⟩
        step with declared_slots_spec p t i as ⟨ v4, hv4 ⟩
        step with same_slots_spec (alloc.vec.Vec.deref v3) (alloc.vec.Vec.deref v4) nn
            (SolverSpec.slotsOf
              (SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[i.val]'hgi)))
            decl[i.val]! hv3 (by rw [← hdv]; exact hv4) as ⟨ b, hb ⟩
        have hzip : (prod.drop i.val).zip (decl.drop i.val)
            = (SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[i.val]'hgi),
                decl[i.val]!) :: (prod.drop (i.val + 1)).zip (decl.drop (i.val + 1)) := by
          rw [hpcons, hdcons, List.zip_cons_cons]
        split
        · next hbt =>
          have hP : SameSetLt nn.val
              (SolverSpec.slotsOf
                (SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[i.val]'hgi)))
              decl[i.val]! := of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, hi2']
          simp only [Bool.true_and]
          refine Bool.eq_iff_iff.mpr ?_
          simp only [decide_eq_true_eq]
          rw [hzip]
          constructor
          · intro h gd hgd
            rcases List.mem_cons.mp hgd with h' | h'
            · rw [h']; exact hP
            · exact h gd h'
          · intro h gd hgd
            exact h gd (List.mem_cons_of_mem _ hgd)
        · next hbf =>
          have hnP : ¬ SameSetLt nn.val
              (SolverSpec.slotsOf
                (SolverSpec.idPairs ((q.steps.val[si.val]'hsi).produced.val[i.val]'hgi)))
              decl[i.val]! := of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hnot : ¬ (∀ gd ∈ (prod.drop i.val).zip (decl.drop i.val),
              SameSetLt nn.val (SolverSpec.slotsOf gd.1) gd.2) := by
            intro hc
            refine hnP (hc (SolverSpec.idPairs
              ((q.steps.val[si.val]'hsi).produced.val[i.val]'hgi), decl[i.val]!) ?_)
            rw [hzip]
            exact List.mem_cons_self
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hnot).symm
      · have hnil : prod.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

theorem groups_match_spec (p : types.Problem) (q : types.Plan) (si nn : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (prod : List (List (Nat × Nat)))
    (hprod : SolverSpec.pairLists (q.steps.val[si.val]'hsi).produced = prod)
    (decl : List SolverSpec.Ids)
    (hdecl : SolverSpec.producesOf p (q.steps.val[si.val]'hsi).transform.val = decl)
    (hlen : prod.length = decl.length) :
    clauses.groups_match p q si nn ⦃ r =>
      r = decide (∀ gd ∈ prod.zip decl, SameSetLt nn.val (SolverSpec.slotsOf gd.1) gd.2) ⦄ := by
  rw [clauses.groups_match.eq_def, index_eq q.steps si hsi]
  simp only [bind_tc_ok]
  refine WP.spec_mono (groups_match_loop_spec p q si nn (q.steps.val[si.val]'hsi).transform
    hsi prod hprod decl hdecl hlen prod.length true 0#usize (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp

/-! ## `shape_at` and `cl_shape` -- range-restricted -/

theorem shape_at_spec (p : types.Problem) (q : types.Plan) (si : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (sv : SolverSpec.StepView) (hsv : SolverSpec.stepView (q.steps.val[si.val]'hsi) = sv) :
    clauses.shape_at p q si ⦃ r => r = decide (ShapeR p (SolverSpec.nNodes p) sv) ⦄ := by
  have hpl : sv.produced.length = (q.steps.val[si.val]'hsi).produced.val.length := by
    rw [← hsv]; exact pairLists_length _
  have htr : SolverSpec.producesOf p sv.transform
      = SolverSpec.producesOf p (q.steps.val[si.val]'hsi).transform.val := by
    rw [← hsv]; rfl
  have hrq : SolverSpec.requiresOf p sv.transform
      = SolverSpec.requiresOf p (q.steps.val[si.val]'hsi).transform.val := by
    rw [← hsv]; rfl
  rw [clauses.shape_at.eq_def]
  dsimp only
  split
  · rw [index_eq q.steps si hsi]
    simp only [bind_tc_ok]
    step as ⟨ nn, hnn ⟩
    step with used_slots_spec q si hsi as ⟨ slots, hslots ⟩
    have hsl : slots.val.map (fun x => x.val) = SolverSpec.slotsOf sv.used := by
      rw [← hsv]; exact hslots
    step with no_repeat_spec (alloc.vec.Vec.deref slots) (SolverSpec.slotsOf sv.used) hsl
      as ⟨ b, hb ⟩
    split
    · next hbt =>
      have hnd : (SolverSpec.slotsOf sv.used).Nodup :=
        of_decide_eq_true (by rw [← hb]; exact hbt)
      step with required_slots_spec p (q.steps.val[si.val]'hsi).transform as ⟨ rv, hrv ⟩
      step with same_slots_spec (alloc.vec.Vec.deref slots) (alloc.vec.Vec.deref rv) nn
        (SolverSpec.slotsOf sv.used) (SolverSpec.requiresOf p sv.transform) hsl
        (by rw [hrq]; exact hrv) as ⟨ b1, hb1 ⟩
      split
      · next hb1t =>
        have hss : SameSetLt (SolverSpec.nNodes p) (SolverSpec.slotsOf sv.used)
            (SolverSpec.requiresOf p sv.transform) := by
          rw [← hnn]
          exact of_decide_eq_true (by rw [← hb1]; exact hb1t)
        step as ⟨ ng, hng ⟩
        split
        · next heq =>
          have hlen : sv.produced.length = (SolverSpec.producesOf p sv.transform).length := by
            rw [hpl, htr, ← hng]; scalar_tac
          refine WP.spec_mono (groups_match_spec p q si nn hsi sv.produced (by rw [← hsv]; rfl)
            (SolverSpec.producesOf p sv.transform) htr.symm hlen) ?_
          intro r hr
          rw [hr]
          refine Bool.eq_iff_iff.mpr ?_
          simp only [decide_eq_true_eq]
          rw [hnn]
          exact ⟨fun h => ⟨hnd, hss, hlen, h⟩, fun h => h.2.2.2⟩
        · next hne =>
          have hveq : (alloc.vec.Vec.len (q.steps.val[si.val]'hsi).produced).val = ng.val →
              False := fun hc => hne (usize_eq_of_val hc)
          have hnlen : sv.produced.length ≠ (SolverSpec.producesOf p sv.transform).length := by
            rw [hpl, htr, ← hng]
            intro hc
            exact hveq (by scalar_tac)
          exact ok_post (decide_eq_false (fun hc => hnlen hc.2.2.1)).symm
      · next hb1f =>
        have hns : ¬ SameSetLt (SolverSpec.nNodes p) (SolverSpec.slotsOf sv.used)
            (SolverSpec.requiresOf p sv.transform) := by
          rw [← hnn]
          exact of_decide_eq_false (by rw [← hb1]; simpa using hb1f)
        exact ok_post (decide_eq_false (fun hc => hns hc.2.1)).symm
    · next hbf =>
      have hnd : ¬ (SolverSpec.slotsOf sv.used).Nodup :=
        of_decide_eq_false (by rw [← hb]; simpa using hbf)
      exact ok_post (decide_eq_false (fun hc => hnd hc.1)).symm
  · exfalso; scalar_tac

theorem cl_shape_loop_spec (p : types.Problem) (q : types.Plan) :
    ∀ (k : Nat) (ok1 : Bool) (i : Std.Usize), q.steps.val.length - i.val ≤ k →
      clauses.cl_shape_loop p q ok1 i ⦃ r =>
        r = (ok1 && decide (∀ s ∈ (SolverSpec.steps q).drop i.val,
              ShapeR p (SolverSpec.nNodes p) s)) ⦄ := by
  intro k
  induction k with
  | zero =>
    intro ok1 i hk
    rw [clauses.cl_shape_loop.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.steps q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [steps_length]; scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ k ih =>
    intro ok1 i hk
    rw [clauses.cl_shape_loop.eq_def]
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
        step with shape_at_spec p q i hsi (SolverSpec.stepView (q.steps.val[i.val]'hsi)) rfl
          as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : ShapeR p (SolverSpec.nNodes p) (SolverSpec.stepView (q.steps.val[i.val]'hsi)) :=
            of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, hi2']
          simp only [Bool.true_and]
          refine Bool.eq_iff_iff.mpr ?_
          simp only [decide_eq_true_eq]
          rw [hcons]
          constructor
          · intro h x hx
            rcases List.mem_cons.mp hx with h' | h'
            · rw [h']; exact hP
            · exact h x h'
          · intro h x hx
            exact h x (List.mem_cons_of_mem _ hx)
        · next hbf =>
          have hnP : ¬ ShapeR p (SolverSpec.nNodes p)
              (SolverSpec.stepView (q.steps.val[i.val]'hsi)) :=
            of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hnot : ¬ (∀ s ∈ (SolverSpec.steps q).drop i.val,
              ShapeR p (SolverSpec.nNodes p) s) := by
            intro hc
            exact hnP (hc _ (by rw [hcons]; exact List.mem_cons_self))
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          refine WP.spec_mono (ih false i2 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hnot).symm
      · have hnil : (SolverSpec.steps q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [steps_length]; scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

/-- What the checker ACTUALLY decides: `Shape` with every set comparison confined
to the node table. Unconditional, and exact. -/
theorem cl_shape_raw_spec (p : types.Problem) (q : types.Plan) :
    clauses.cl_shape p q ⦃ r =>
      r = decide (∀ s ∈ SolverSpec.steps q, ShapeR p (SolverSpec.nNodes p) s) ⦄ := by
  rw [clauses.cl_shape.eq_def]
  refine WP.spec_mono (cl_shape_loop_spec p q q.steps.val.length true 0#usize
    (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp



/-! ## The gap, machine checked

Neither is a hypothetical about unreachable inputs: the hypotheses of each are
simultaneously satisfiable, and each describes a plan `cl_indexed` rejects and
this clause accepts. See the module header. -/

/-- A problem with no nodes, a transform requiring and producing nothing, and a
step that binds slot `0`. `shape_at` accepts it; `Shape` does not. -/
theorem shapeR_not_shape (p : types.Problem) (s : SolverSpec.StepView)
    (h0 : SolverSpec.nNodes p = 0)
    (hu : SolverSpec.slotsOf s.used = [0])
    (hr : SolverSpec.requiresOf p s.transform = [])
    (hn : s.produced = [])
    (hp : SolverSpec.producesOf p s.transform = []) :
    ShapeR p (SolverSpec.nNodes p) s ∧ ¬ SolverSpec.Shape p s := by
  constructor
  · refine ⟨?_, ?_, ?_, ?_⟩
    · rw [hu]; simp
    · rw [h0]
      intro j hj
      exact absurd hj (Nat.not_lt_zero j)
    · rw [hn, hp]; simp
    · rw [hn]
      intro gd hgd
      simp at hgd
  · intro hc
    have h5 := hc.2.1.1 0 (by rw [hu]; simp)
    rw [hr] at h5
    simp at h5

/-- A plan with one endpoint whose declared parent is `5`, in a step that
consumes nothing. `derived_at` accepts it; `Derived` does not. -/
theorem derivedR_not_derived (q : types.Plan) (e : Nat)
    (h1 : SolverSpec.nEndpoints q = 1)
    (hep : SolverSpec.epParents q e = [5]) :
    DerivedR q (SolverSpec.nEndpoints q) [] e ∧ ¬ SolverSpec.Derived q [] e := by
  constructor
  · intro j hj
    rw [h1] at hj
    have hj0 : j = 0 := by omega
    subst hj0
    rw [hep]
    simp [SolverSpec.Confers]
  · intro hc
    have h5 := hc.1 5 (by rw [hep]; simp)
    simp [SolverSpec.Confers] at h5

/-! ## The `shape` clause, as the specification states it

`shapeR_iff_shape` is where the range restriction is paid for, and `hwi` is what
pays: `same_slots` compares two sets of `nn` bits, so a slot id at or beyond the
node table sets nothing on either side and the comparison cannot see it. See the
module header. -/

theorem shapeR_iff_shape (p : types.Problem) (q : types.Plan)
    (hwi : SolverSpec.WellIndexed p q) (s : SolverSpec.StepView)
    (hs : s ∈ SolverSpec.steps q) :
    ShapeR p (SolverSpec.nNodes p) s ↔ SolverSpec.Shape p s := by
  obtain ⟨-, -, -, -, -, -, -, -, htr, hst⟩ := hwi
  obtain ⟨htrans, hused, hprodb⟩ := hst s hs
  obtain ⟨-, hreq, hpr⟩ := htr s.transform htrans
  have h1 : ∀ x ∈ SolverSpec.slotsOf s.used, x < SolverSpec.nNodes p := by
    intro x hx
    simp only [SolverSpec.slotsOf, List.mem_map] at hx
    obtain ⟨b, hb, hbx⟩ := hx
    rw [← hbx]
    exact (hused b hb).1
  have h2 : ∀ gd ∈ s.produced.zip (SolverSpec.producesOf p s.transform),
      (∀ x ∈ SolverSpec.slotsOf gd.1, x < SolverSpec.nNodes p) ∧
      (∀ x ∈ gd.2, x < SolverSpec.nNodes p) := by
    intro gd hgd
    obtain ⟨hg1, hg2⟩ := mem_zip hgd
    refine ⟨fun x hx => ?_, fun x hx => hpr gd.2 hg2 x hx⟩
    simp only [SolverSpec.slotsOf, List.mem_map] at hx
    obtain ⟨b, hb, hbx⟩ := hx
    rw [← hbx]
    exact (hprodb gd.1 hg1 b hb).1
  constructor
  · rintro ⟨n1, n2, n3, n4⟩
    refine ⟨n1, (sameSetLt_iff _ _ _ h1 hreq).mp n2, n3, fun gd hgd => ?_⟩
    exact (sameSetLt_iff _ _ _ (h2 gd hgd).1 (h2 gd hgd).2).mp (n4 gd hgd)
  · rintro ⟨n1, n2, n3, n4⟩
    refine ⟨n1, (sameSetLt_iff _ _ _ h1 hreq).mpr n2, n3, fun gd hgd => ?_⟩
    exact (sameSetLt_iff _ _ _ (h2 gd hgd).1 (h2 gd hgd).2).mpr (n4 gd hgd)

/-- The `shape` conjunct of `SolverSpec.ValidC`.

**`hwi` is not decoration.** See the module header: without it the equation is
FALSE, and the counterexample is a one-node problem. -/
theorem cl_shape_spec (p : types.Problem) (q : types.Plan)
    (hwi : SolverSpec.WellIndexed p q) :
    clauses.cl_shape p q ⦃ r =>
      r = decide (∀ s ∈ SolverSpec.steps q, SolverSpec.Shape p s) ⦄ := by
  refine WP.spec_mono (cl_shape_raw_spec p q) ?_
  intro r hr
  rw [hr]
  refine Bool.eq_iff_iff.mpr ?_
  simp only [decide_eq_true_eq]
  exact ⟨fun h s hs => (shapeR_iff_shape p q hwi s hs).mp (h s hs),
         fun h s hs => (shapeR_iff_shape p q hwi s hs).mpr (h s hs)⟩

/-! ## `confers` -- what a step's bindings confer, as a dense set -/

theorem confers_inner_spec (q : types.Plan) (e k : Std.Usize)
    (hk : k.val = (SolverSpec.epParents q e.val).length) :
    ∀ (m : Nat) (acc : alloc.vec.Vec Bool) (j : Std.Usize), k.val - j.val ≤ m →
      clauses.confers_loop0_loop0 q acc e k j ⦃ r =>
        r.val = setNats acc.val ((SolverSpec.epParents q e.val).drop j.val) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro acc j hm
    rw [clauses.confers_loop0_loop0.eq_def]
    split
    · exfalso; scalar_tac
    · have hnil : (SolverSpec.epParents q e.val).drop j.val = [] :=
        List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil, setNats_nil])
  | succ m ih =>
    intro acc j hm
    rw [clauses.confers_loop0_loop0.eq_def]
    split
    · have hj : j.val < (SolverSpec.epParents q e.val).length := by scalar_tac
      have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
      step as ⟨ x, hx ⟩
      have hxv : x.val = (SolverSpec.epParents q e.val)[j.val]'hj := by
        rw [hx, List.getElem?_eq_getElem hj]; rfl
      step as ⟨ acc1, hacc1 ⟩
      step as ⟨ j1, hj1 ⟩
      have hj1' : j1.val = j.val + 1 := by scalar_tac
      have hcons : (SolverSpec.epParents q e.val).drop j.val
          = x.val :: (SolverSpec.epParents q e.val).drop (j.val + 1) := by
        rw [hxv]; exact List.drop_eq_getElem_cons hj
      refine WP.spec_mono (ih acc1 j1 (by scalar_tac)) ?_
      intro r hr
      rw [hr, hacc1, hj1', hcons, setNats_cons]
    · have hnil : (SolverSpec.epParents q e.val).drop j.val = [] :=
        List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil, setNats_nil])

theorem confers_outer_spec (q : types.Plan) (used : Slice (Std.Usize × Std.Usize))
    (u : List (Nat × Nat)) (hu : used.val.map (fun b => (b.1.val, b.2.val)) = u) :
    ∀ (m : Nat) (acc : alloc.vec.Vec Bool) (i : Std.Usize), used.val.length - i.val ≤ m →
      clauses.confers_loop0 q used acc i ⦃ r =>
        r.val = setNats acc.val (confersList q (u.drop i.val)) ⦄ := by
  have hul : u.length = used.val.length := by rw [← hu]; simp
  intro m
  induction m with
  | zero =>
    intro acc i hm
    rw [clauses.confers_loop0.eq_def]
    dsimp only
    split
    · exfalso; scalar_tac
    · have hnil : u.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil]; simp [confersList, setNats_nil])
  | succ m ih =>
    intro acc i hm
    rw [clauses.confers_loop0.eq_def]
    dsimp only
    split
    · have hiu : i.val < used.val.length := by scalar_tac
      have hiu' : i.val < u.length := by omega
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      obtain ⟨d, e, hde⟩ : ∃ d e, used.val[i.val]'hiu = (d, e) := ⟨_, _, rfl⟩
      have hcons : u.drop i.val = (d.val, e.val) :: u.drop (i.val + 1) := by
        rw [drop_cons_getElem! u i.val hiu', pairs_getElem! used.val u hu i.val hiu, hde]
      rw [eq_of_spec (Slice.index_usize_spec used i (by scalar_tac)), hde]
      show (do
          let acc1 ← bits.set acc e
          let k ← access.ep_nparents q e
          let acc2 ← clauses.confers_loop0_loop0 q acc1 e k 0#usize
          let i2 ← i + 1#usize
          clauses.confers_loop0 q used acc2 i2) ⦃ r =>
            r.val = setNats acc.val (confersList q (u.drop i.val)) ⦄
      step as ⟨ acc1, hacc1 ⟩
      step as ⟨ k, hk ⟩
      step with confers_inner_spec q e k hk (SolverSpec.epParents q e.val).length acc1 0#usize
        (by scalar_tac) as ⟨ acc2, hacc2 ⟩
      step as ⟨ i2, hi2 ⟩
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      refine WP.spec_mono (ih acc2 i2 (by scalar_tac)) ?_
      intro r hr
      rw [hr, hi2', hacc2, hacc1, hcons, confersList_cons, setNats_append, setNats_cons]
      simp
    · have hnil : u.drop i.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil]; simp [confersList, setNats_nil])

theorem confers_spec (q : types.Plan) (used : Slice (Std.Usize × Std.Usize)) (ne : Std.Usize)
    (u : List (Nat × Nat)) (hu : used.val.map (fun b => (b.1.val, b.2.val)) = u) :
    clauses.confers q used ne ⦃ r =>
      r.val.length = ne.val ∧
      ∀ j < ne.val, (r.val[j]! = true ↔ j ∈ SolverSpec.Confers q u) ⦄ := by
  rw [clauses.confers.eq_def]
  step as ⟨ z, hz ⟩
  have hzl : z.val.length = ne.val := by rw [hz]; simp
  refine WP.spec_mono (confers_outer_spec q used u hu used.val.length z 0#usize
    (by scalar_tac)) ?_
  intro r hr
  have hr' : r.val = setNats z.val (confersList q u) := by simpa using hr
  refine ⟨by rw [hr', setNats_length, hzl], fun j hj => ?_⟩
  have hjz : j < z.val.length := by omega
  have hzj : z.val[j]! = false := by rw [hz]; exact List.getElem!_replicate false hj
  rw [hr', setNats_getElem! (confersList q u) z.val j hjz, hzj, mem_confersList]
  simp

/-! ## `derived_at` and `cl_derived` -- range-restricted -/

theorem derived_at_loop_spec (q : types.Plan) (e k : Std.Usize)
    (hk : k.val = (SolverSpec.epParents q e.val).length) :
    ∀ (m : Nat) (declared : alloc.vec.Vec Bool) (j : Std.Usize), k.val - j.val ≤ m →
      clauses.derived_at_loop q.endpoints q.givens q.steps e declared k j ⦃ r =>
        r.val = setNats declared.val ((SolverSpec.epParents q e.val).drop j.val) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro declared j hm
    rw [clauses.derived_at_loop.eq_def]
    split
    · exfalso; scalar_tac
    · have hnil : (SolverSpec.epParents q e.val).drop j.val = [] :=
        List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil, setNats_nil])
  | succ m ih =>
    intro declared j hm
    rw [clauses.derived_at_loop.eq_def]
    split
    · have hj : j.val < (SolverSpec.epParents q e.val).length := by scalar_tac
      have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
      step with ep_parent_spec q e j as ⟨ x, hx ⟩
      have hxv : x.val = (SolverSpec.epParents q e.val)[j.val]'hj := by
        rw [hx, List.getElem?_eq_getElem hj]; rfl
      step as ⟨ d1, hd1 ⟩
      step as ⟨ j1, hj1 ⟩
      have hj1' : j1.val = j.val + 1 := by scalar_tac
      have hcons : (SolverSpec.epParents q e.val).drop j.val
          = x.val :: (SolverSpec.epParents q e.val).drop (j.val + 1) := by
        rw [hxv]; exact List.drop_eq_getElem_cons hj
      refine WP.spec_mono (ih d1 j1 (by scalar_tac)) ?_
      intro r hr
      rw [hr, hd1, hj1', hcons, setNats_cons]
    · have hnil : (SolverSpec.epParents q e.val).drop j.val = [] :=
        List.drop_eq_nil_iff.mpr (by scalar_tac)
      exact ok_post (by rw [hnil, setNats_nil])

theorem derived_at_spec (q : types.Plan) (si gi ki : Std.Usize)
    (hsi : si.val < q.steps.val.length)
    (hgi : gi.val < (q.steps.val[si.val]'hsi).produced.val.length)
    (hki : ki.val < ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val.length)
    (u : List (Nat × Nat))
    (hu : SolverSpec.idPairs (q.steps.val[si.val]'hsi).used = u)
    (ep : Nat)
    (hep : (((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val[ki.val]'hki).2.val = ep) :
    clauses.derived_at q si gi ki ⦃ r =>
      r = decide (DerivedR q (SolverSpec.nEndpoints q) u ep) ⦄ := by
  rw [clauses.derived_at.eq_def]
  dsimp only
  split
  · rw [index_eq q.steps si hsi]
    simp only [bind_tc_ok]
    split
    · rw [index_eq _ gi hgi]
      simp only [bind_tc_ok]
      split
      · step as ⟨ ne, hne ⟩
        obtain ⟨d, e, hde⟩ :
            ∃ d e, ((q.steps.val[si.val]'hsi).produced.val[gi.val]'hgi).val[ki.val]'hki = (d, e) :=
          ⟨_, _, rfl⟩
        have hev : e.val = ep := by
          rw [← hep]
          exact congrArg (fun z : Std.Usize × Std.Usize => z.2.val) hde.symm
        rw [vec_index_usize_ok _ ki hki, hde]
        show (do
            let declared ← bits.zeros ne
            let k ← access.ep_nparents q e
            let declared1 ←
              clauses.derived_at_loop q.endpoints q.givens q.steps e declared k 0#usize
            let v2 ← clauses.confers q
              (alloc.vec.Vec.deref (q.steps.val[si.val]'hsi).used) ne
            bits.eq (alloc.vec.Vec.deref declared1) (alloc.vec.Vec.deref v2)) ⦃ r =>
              r = decide (DerivedR q (SolverSpec.nEndpoints q) u ep) ⦄
        step as ⟨ z, hz ⟩
        have hzl : z.val.length = ne.val := by rw [hz]; simp
        step as ⟨ k, hk ⟩
        step with derived_at_loop_spec q e k hk (SolverSpec.epParents q e.val).length z 0#usize
          (by scalar_tac) as ⟨ dec, hdec ⟩
        step with confers_spec q (alloc.vec.Vec.deref (q.steps.val[si.val]'hsi).used) ne u hu
          as ⟨ cv, hcv1, hcv2 ⟩
        have hdec' : dec.val = setNats z.val (SolverSpec.epParents q e.val) := by
          simpa using hdec
        have hdl : dec.val.length = ne.val := by rw [hdec', setNats_length, hzl]
        have hdm : ∀ j < ne.val, (dec.val[j]! = true ↔ j ∈ SolverSpec.epParents q ep) := by
          intro j hj
          have hjz : j < z.val.length := by omega
          have hzj : z.val[j]! = false := by rw [hz]; exact List.getElem!_replicate false hj
          rw [hdec', setNats_getElem! _ z.val j hjz, hzj, hev]
          simp
        have hkey : (dec.val = cv.val) ↔ DerivedR q (SolverSpec.nEndpoints q) u ep := by
          rw [← hne]
          exact bits_eq_iff_sameSetLt hdl hcv1 hdm hcv2
        refine WP.spec_mono (eq_spec (alloc.vec.Vec.deref dec) (alloc.vec.Vec.deref cv)) ?_
        intro r hr
        rw [hr]
        exact decide_eq_decide.mpr hkey
      · exfalso; scalar_tac
    · exfalso; scalar_tac
  · exfalso; scalar_tac

theorem cl_derived_inner_spec (q : types.Plan) (i g : Std.Usize)
    (hsi : i.val < q.steps.val.length)
    (hgi : g.val < (q.steps.val[i.val]'hsi).produced.val.length)
    (sv : SolverSpec.StepView) (hsv : SolverSpec.stepView (q.steps.val[i.val]'hsi) = sv)
    (grp : List (Nat × Nat))
    (hgrp : SolverSpec.idPairs ((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi) = grp) :
    ∀ (m : Nat) (ok1 : Bool) (k : Std.Usize), grp.length - k.val ≤ m →
      clauses.cl_derived_loop0_loop0_loop0 q.endpoints q.givens q.steps ok1 i g k ⦃ r =>
        r = (ok1 && decide (∀ b ∈ grp.drop k.val,
              DerivedR q (SolverSpec.nEndpoints q) sv.used b.2)) ⦄ := by
  have hgl : grp.length = ((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi).val.length := by
    rw [← hgrp]; simp [SolverSpec.idPairs]
  intro m
  induction m with
  | zero =>
    intro ok1 k hm
    rw [clauses.cl_derived_loop0_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      rw [index_eq q.steps i hsi]
      simp only [bind_tc_ok]
      rw [index_eq _ g hgi]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : grp.drop k.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 k hm
    rw [clauses.cl_derived_loop0_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      rw [index_eq q.steps i hsi]
      simp only [bind_tc_ok]
      rw [index_eq _ g hgi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hki : k.val < ((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi).val.length := by
          scalar_tac
        have hkg : k.val < grp.length := by omega
        have hbnd : k.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : grp.drop k.val = grp[k.val]! :: grp.drop (k.val + 1) :=
          drop_cons_getElem! grp k.val hkg
        have hgk : grp[k.val]!
            = ((((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi).val[k.val]'hki).1.val,
               (((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi).val[k.val]'hki).2.val) :=
          pairs_getElem! ((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi).val grp hgrp
            k.val hki
        step with derived_at_spec q i g k hsi hgi hki sv.used (by rw [← hsv]; rfl) grp[k.val]!.2
          (by rw [hgk]) as ⟨ b, hb ⟩
        split
        · next hbt =>
          have hP : DerivedR q (SolverSpec.nEndpoints q) sv.used grp[k.val]!.2 :=
            of_decide_eq_true (by rw [← hb]; exact hbt)
          simp only [bind_tc_ok]
          step as ⟨ k1, hk1 ⟩
          have hk1' : k1.val = k.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true k1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh, hk1']
          simp only [Bool.true_and]
          refine Bool.eq_iff_iff.mpr ?_
          simp only [decide_eq_true_eq]
          rw [hcons]
          constructor
          · intro h x hx
            rcases List.mem_cons.mp hx with h' | h'
            · rw [h']; exact hP
            · exact h x h'
          · intro h x hx
            exact h x (List.mem_cons_of_mem _ hx)
        · next hbf =>
          have hnP : ¬ DerivedR q (SolverSpec.nEndpoints q) sv.used grp[k.val]!.2 :=
            of_decide_eq_false (by rw [← hb]; simpa using hbf)
          have hnot : ¬ (∀ x ∈ grp.drop k.val,
              DerivedR q (SolverSpec.nEndpoints q) sv.used x.2) := by
            intro hc
            exact hnP (hc grp[k.val]! (by rw [hcons]; exact List.mem_cons_self))
          simp only [bind_tc_ok]
          step as ⟨ k1, hk1 ⟩
          refine WP.spec_mono (ih false k1 (by scalar_tac)) ?_
          intro r hr
          rw [hr, hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hnot).symm
      · have hnil : grp.drop k.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

theorem cl_derived_middle_spec (q : types.Plan) (i : Std.Usize)
    (hsi : i.val < q.steps.val.length)
    (sv : SolverSpec.StepView) (hsv : SolverSpec.stepView (q.steps.val[i.val]'hsi) = sv) :
    ∀ (m : Nat) (ok1 : Bool) (g : Std.Usize), sv.produced.length - g.val ≤ m →
      clauses.cl_derived_loop0_loop0 q.endpoints q.givens q.steps ok1 i g ⦃ r =>
        r = (ok1 && decide (∀ grp ∈ sv.produced.drop g.val, ∀ b ∈ grp,
              DerivedR q (SolverSpec.nEndpoints q) sv.used b.2)) ⦄ := by
  have hpl : sv.produced.length = (q.steps.val[i.val]'hsi).produced.val.length := by
    rw [← hsv]; exact pairLists_length _
  intro m
  induction m with
  | zero =>
    intro ok1 g hm
    rw [clauses.cl_derived_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      rw [index_eq q.steps i hsi]
      simp only [bind_tc_ok]
      split
      · exfalso; scalar_tac
      · have hnil : sv.produced.drop g.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 g hm
    rw [clauses.cl_derived_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      rw [index_eq q.steps i hsi]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hgi : g.val < (q.steps.val[i.val]'hsi).produced.val.length := by scalar_tac
        have hgp : g.val < sv.produced.length := by omega
        have hbnd : g.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : sv.produced.drop g.val
            = SolverSpec.idPairs ((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi)
              :: sv.produced.drop (g.val + 1) := by
          rw [← hsv]; exact pairLists_drop_cons _ g.val hgi
        have hin := eq_of_spec (cl_derived_inner_spec q i g hsi hgi sv hsv
          (SolverSpec.idPairs ((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi)) rfl
          (SolverSpec.idPairs ((q.steps.val[i.val]'hsi).produced.val[g.val]'hgi)).length
          true 0#usize (by scalar_tac))
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ g1, hg1 ⟩
        have hg1' : g1.val = g.val + 1 := by scalar_tac
        refine WP.spec_mono (ih _ g1 (by scalar_tac)) ?_
        intro r hr
        rw [hr, hh, hg1']
        simp only [Bool.true_and]
        refine Bool.eq_iff_iff.mpr ?_
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        rw [hcons]
        constructor
        · rintro ⟨h1, h2⟩ x hx
          rcases List.mem_cons.mp hx with h' | h'
          · rw [h']; exact h1
          · exact h2 x h'
        · intro h
          exact ⟨h _ List.mem_cons_self, fun x hx => h x (List.mem_cons_of_mem _ hx)⟩
      · have hnil : sv.produced.drop g.val = [] := List.drop_eq_nil_iff.mpr (by scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

theorem cl_derived_outer_spec (q : types.Plan) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), q.steps.val.length - i.val ≤ m →
      clauses.cl_derived_loop0 q ok1 i ⦃ r =>
        r = (ok1 && decide (∀ s ∈ (SolverSpec.steps q).drop i.val, ∀ grp ∈ s.produced,
              ∀ b ∈ grp, DerivedR q (SolverSpec.nEndpoints q) s.used b.2)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro ok1 i hm
    rw [clauses.cl_derived_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso; scalar_tac
      · have hnil : (SolverSpec.steps q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [steps_length]; scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hm
    rw [clauses.cl_derived_loop0.eq_def]
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
        have hin := eq_of_spec (cl_derived_middle_spec q i hsi
          (SolverSpec.stepView (q.steps.val[i.val]'hsi)) rfl
          (SolverSpec.stepView (q.steps.val[i.val]'hsi)).produced.length true 0#usize
          (by scalar_tac))
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ i2, hi2 ⟩
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        refine WP.spec_mono (ih _ i2 (by scalar_tac)) ?_
        intro r hr
        rw [hr, hh, hi2']
        simp only [Bool.true_and]
        refine Bool.eq_iff_iff.mpr ?_
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        rw [hcons]
        constructor
        · rintro ⟨h1, h2⟩ x hx
          rcases List.mem_cons.mp hx with h' | h'
          · rw [h']; exact h1
          · exact h2 x h'
        · intro h
          exact ⟨h _ List.mem_cons_self, fun x hx => h x (List.mem_cons_of_mem _ hx)⟩
      · have hnil : (SolverSpec.steps q).drop i.val = [] :=
          List.drop_eq_nil_iff.mpr (by rw [steps_length]; scalar_tac)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

/-- What the checker ACTUALLY decides: `Derived` with both lineage sets confined
to the endpoint table. Unconditional, and exact. -/
theorem cl_derived_raw_spec (q : types.Plan) :
    clauses.cl_derived q ⦃ r =>
      r = decide (∀ s ∈ SolverSpec.steps q, ∀ grp ∈ s.produced, ∀ b ∈ grp,
            DerivedR q (SolverSpec.nEndpoints q) s.used b.2) ⦄ := by
  rw [clauses.cl_derived.eq_def]
  refine WP.spec_mono (cl_derived_outer_spec q q.steps.val.length true 0#usize
    (by scalar_tac)) ?_
  intro r hr
  rw [hr]
  simp

theorem derivedR_iff_derived (p : types.Problem) (q : types.Plan)
    (hwi : SolverSpec.WellIndexed p q) (s : SolverSpec.StepView)
    (hs : s ∈ SolverSpec.steps q) (grp : List (Nat × Nat)) (hgrp : grp ∈ s.produced)
    (b : Nat × Nat) (hb : b ∈ grp) :
    DerivedR q (SolverSpec.nEndpoints q) s.used b.2 ↔ SolverSpec.Derived q s.used b.2 := by
  obtain ⟨-, -, -, -, -, hep, -, -, -, hst⟩ := hwi
  obtain ⟨-, hused, hprodb⟩ := hst s hs
  have hbe : b.2 < SolverSpec.nEndpoints q := (hprodb grp hgrp b hb).2
  have h1 : ∀ x ∈ SolverSpec.epParents q b.2, x < SolverSpec.nEndpoints q :=
    fun x hx => lt_trans (hep b.2 hbe x hx) hbe
  have h2 : ∀ x ∈ SolverSpec.Confers q s.used, x < SolverSpec.nEndpoints q := by
    intro x hx
    simp only [SolverSpec.Confers, List.mem_append, List.mem_map, List.mem_flatMap] at hx
    rcases hx with ⟨c, hc, hcx⟩ | ⟨f, ⟨c, hc, hcf⟩, hfx⟩
    · rw [← hcx]; exact (hused c hc).2
    · have hfe : f < SolverSpec.nEndpoints q := by rw [← hcf]; exact (hused c hc).2
      exact lt_trans (hep f hfe x hfx) hfe
  exact sameSetLt_iff _ _ _ h1 h2

/-- The `derived` conjunct of `SolverSpec.ValidC`.

**`hwi` is not decoration.** See the module header: the two lineage sets are
compared as `nEndpoints`-wide bit vectors, so a declared parent at or beyond the
endpoint table is invisible to both sides. -/
theorem cl_derived_spec (p : types.Problem) (q : types.Plan)
    (hwi : SolverSpec.WellIndexed p q) :
    clauses.cl_derived q ⦃ r =>
      r = decide (∀ s ∈ SolverSpec.steps q, ∀ grp ∈ s.produced, ∀ b ∈ grp,
            SolverSpec.Derived q s.used b.2) ⦄ := by
  refine WP.spec_mono (cl_derived_raw_spec q) ?_
  intro r hr
  rw [hr]
  refine Bool.eq_iff_iff.mpr ?_
  simp only [decide_eq_true_eq]
  constructor
  · intro h s hs grp hgrp b hb
    exact (derivedR_iff_derived p q hwi s hs grp hgrp b hb).mp (h s hs grp hgrp b hb)
  · intro h s hs grp hgrp b hb
    exact (derivedR_iff_derived p q hwi s hs grp hgrp b hb).mpr (h s hs grp hgrp b hb)

#print axioms SolverProof.used_slots_spec
#print axioms SolverProof.produced_slots_spec
#print axioms SolverProof.required_slots_spec
#print axioms SolverProof.declared_slots_spec
#print axioms SolverProof.no_repeat_spec
#print axioms SolverProof.same_slots_spec
#print axioms SolverProof.groups_match_spec
#print axioms SolverProof.shape_at_spec
#print axioms SolverProof.cl_shape_raw_spec
#print axioms SolverProof.shapeR_not_shape
#print axioms SolverProof.derivedR_not_derived
#print axioms SolverProof.shapeR_iff_shape
#print axioms SolverProof.cl_shape_spec
#print axioms SolverProof.confers_spec
#print axioms SolverProof.derived_at_spec
#print axioms SolverProof.cl_derived_raw_spec
#print axioms SolverProof.derivedR_iff_derived
#print axioms SolverProof.cl_derived_spec

end SolverProof
