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
  definition.

  Every loop in the witness has the shape `while [ok &&] i < bound { … i += 1 }`,
  so every proof has the shape:

      induct on fuel `k` with `bound - i ≤ k`
      rw [<name>.eq_def]; dsimp only     -- `dsimp` matters: the body opens with
      split                              -- `have i1 := …`, which `split` will
      · … step through the monadic binds -- not see through on its own
        exact ih … (by scalar_tac)
      · exact trivial-case
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
