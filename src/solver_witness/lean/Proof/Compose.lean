/-
  Where the ten clause lemmas meet.

  This is the only module that imports every other, which makes it the first
  place a name declared twice across two sibling modules would be rejected. The
  siblings were written concurrently and each invented its own helpers, so two
  of them kept theirs in an inner namespace (`Slots`, `Lineage`) rather than at
  `SolverProof`. That is what keeps this file buildable, and it is why a helper
  belongs either in `Basis` or behind a module namespace, never bare.
-/

import SolverWitness.Proof.Basis
import SolverWitness.Proof.Bits
import SolverWitness.Proof.Access
import SolverWitness.Proof.Ancestry
import SolverWitness.Proof.Steps
import SolverWitness.Proof.Slots
import SolverWitness.Proof.Lineage

namespace SolverProof

open Aeneas Aeneas.Std Result
-- `types.*` lives under `solver_witness`; `Spec.lean` opens it the same way.
open solver_witness

/- The clause lemmas proved so far, reachable from one place. Four are stated
   under `WellIndexed`, which is the case split `check_spec` owes; the `_raw`
   and `_exact` variants beside them are unconditional. -/
#check @cl_target_spec
#check @cl_unique_producer_spec
#check @cl_provenance_spec
#check @cl_schedulable_spec
#check @cl_shape_spec
#check @cl_shape_raw_spec
#check @cl_derived_spec
#check @cl_derived_raw_spec
#check @cl_conformance_spec
#check @cl_conformance_spec_exact
#check @cl_emission_spec
#check @cl_emission_spec_exact
#check @ancestors_spec
#check @descends_spec

/-! ## The obligation

  Moved here from `Spec.lean` because the import direction forbids it there:
  every proof module imports the specification for its vocabulary, so the
  specification cannot import them back to use their lemmas.

  Stated as ONE EQUATION rather than as the biconditional. The biconditional
  alone does not pin the negative case -- a checker that FAILS on every invalid
  plan satisfies `check p q = ok true ↔ Valid p q` vacuously. The equation gives
  totality, soundness and completeness together.

  **The proof case-splits on `WellIndexed`.** Five of the ten clause lemmas --
  Shape, Derived, Conformance, Emission, Givens -- are equivalent to their
  conjuncts only under it, for three unrelated reasons, each machine-checked in
  the module that found it. On the branch where `cl_indexed` is false, `check` is
  false and so is `Valid`, and the unconditional `_raw`/`_exact` forms carry that
  branch. On the branch where it holds, its conjuncts ARE the hypotheses those
  five need. `cl_indexed_spec` is unconditional, which is what makes the split
  available at all.
-/

theorem check_spec (p : types.Problem) (q : types.Plan) :
    solver_witness.check p q = Result.ok (decide (SolverSpec.Valid p q)) := by
  sorry

theorem check_correct (p : types.Problem) (q : types.Plan) :
    solver_witness.check p q = Result.ok true ↔ SolverSpec.Valid p q := by
  rw [check_spec]; simp

end SolverProof
