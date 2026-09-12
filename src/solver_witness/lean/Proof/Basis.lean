/-
  The basis every clause proof stands on.

  The tree beside this file is one module per clause, and nothing imports a
  sibling: a clause proof reaches down to this file and to the extraction, never
  across. That is what lets the ten be written independently, and it mirrors the
  checker itself, where `check` evaluates all ten clauses with no early return so
  that no clause's meaning depends on another having already held.

  ## Why loop lemmas are stated as triples

  `x ⦃ z => z = c ⦄` is `Aeneas.Std.WP.spec x (fun z => z = c)`, and `spec` sends
  BOTH `fail` and `div` to `False`. A triple therefore already asserts that the
  computation terminates without failing -- which is the whole content the
  obligation needs, and the reason the extraction does not have to be re-made
  with termination measures to get it.

  Stating them this way is not a preference. Aeneas ships its primitive spec
  lemmas as triples and its `step` tactic composes triples; nothing composes raw
  equations, and a lemma phrased as `∃ r, f = ok r` is rejected by `step` with
  "Exists is not a supported spec statement". `eq_of_spec` below is the one place
  the triple world is converted to the equation `check_spec` is stated in.

  ## The recipe for an extracted loop

  Aeneas lowers every `while` in the witness to a self-recursive definition
  carrying the loop state, marked `partial_fixpoint`. Such a definition still
  exposes its unfolding equation as `<name>.eq_def`, and that is all a proof
  needs: termination is established by well-founded induction in the PROOF, on
  the measure `bound - index`, rather than by a measure attached to the
  definition. Do not reach for `fixpoint_induct` -- its motive must be
  admissible, and pinning a return value is not.

  Every loop in the witness has the shape `while [ok &&] i < bound { ... i += 1 }`,
  and this is the shape of the proof, measured on `bits.eq_loop`:

      theorem f_spec (..) (h : ..) :
          ∀ (k : Nat) (i : Usize), bound - i.val ≤ k → f .. i ⦃ r => r = c i ⦄
        intro k; induction k with
        | zero => ..                       -- in-bounds branch: `exfalso; scalar_tac`
        | succ k ih => ..
      rw [f.eq_def]; dsimp only; split      -- `dsimp only` is REQUIRED: the body
                                            -- opens `have i1 := ..`, and `split`
                                            -- will not see through the let_fun
      step as ⟨x, hx⟩                       -- steps one monadic bind

  Six things that cost builds to learn:

  1. **Say it over `List.drop i`, not as a bounded `∀ j, i ≤ j → ...`.** The
     inductive step is then exactly `List.drop_eq_getElem_cons` -- one head, one
     tail -- and the wrapper falls out at `i = 0` by `List.drop_zero` with no
     index algebra anywhere. An accumulating loop (`union`, `of_ids`) wants the
     same trick over `take i` and `set`.

  2. **Never let `decide` appear in a rewrite.** `Slice α` is a plain `def` for a
     subtype, so `a.val` only type-checks once `Slice` unfolds, which `rw` and
     `simp` motive checks will not do at `instances` transparency. Rewriting a
     goal `r = decide P` fails with "the motive is not type correct" and an
     application mismatch pointing at an `↑a` that looks perfectly well typed --
     an error that names nothing to do with the cause. Prove the bare `P` (or
     `¬P`) as its own `have`, then close with `decide_eq_true h` or
     `decide_eq_false h`, `.symm` since the goal is `c = decide P`.

  3. **`WP.spec_mono` is how the goal reaches the induction hypothesis.** The IH
     is stated at `i+1` and the goal at `i`, differing by an `Iff`, so
     `exact WP.spec_mono (ih i2 (by scalar_tac)) (fun r hr => hr.trans
     (decide_eq_decide.mpr hiff.symm))` -- and no rewriting under `decide`.

  4. **`step` finds the primitive specs unaided**, including
     `Slice.index_usize_spec` and `Usize.add_spec`, and discharges their
     preconditions itself (its default is `grind`, not `scalar_tac`) provided the
     bounds are already in context. Put `have hia : i.val < a.val.length := by
     scalar_tac` in first; the `drop` lemma needs it again anyway.

  5. **A tactic block inlined into a rewrite makes its implicits opaque.**
     `rw [foo.mpr (by scalar_tac)]` reports "did not find an instance of the
     pattern" for a pattern plainly present, because the block's goal is what
     would have fixed the implicit arguments. State it as a `have` with an
     explicit type.

  6. **`simp` can undo the rewrite you just made.** `List.getElem_cons_drop` is a
     simp lemma and folds `l[i] :: l.drop (i+1)` straight back to `l.drop i`.
     Close such a chain with `List.cons.injEq` inside the same `rw`, by `rfl`.

  `scalar_tac` discharges every arithmetic side condition here -- the fuel
  decrease, the bounds, and the exhausted-loop case -- but wants `exfalso` first
  when the goal is not itself arithmetic. And no `simp only [WP.spec_ok]` is
  needed anywhere: `spec`, `theta` and `wp_return` are plain definitions, so on
  an `ok` the triple IS the equation definitionally and a bare `exact` sees it.
-/

import SolverWitness.Types
import SolverWitness.Funs

namespace SolverProof

open Aeneas Aeneas.Std Result

/-- Discharge a triple to the equation the obligation is stated in.

The only bridge between the two worlds, and it is one `cases`: `spec` is `False`
on `fail` and on `div`, so the `ok` case is the only survivor and it carries the
value. -/
theorem eq_of_spec {α} {x : Result α} {c : α} (h : x ⦃ z => z = c ⦄) : x = ok c := by
  cases x with
  | ok v => simp only [WP.spec_ok] at h; simp [h]
  | fail e => simp only [WP.spec_fail] at h
  | div => simp only [WP.spec_div] at h

end SolverProof
