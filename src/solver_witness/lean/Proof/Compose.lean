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

end SolverProof
