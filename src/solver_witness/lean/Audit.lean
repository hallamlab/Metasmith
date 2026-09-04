/-
  What the obligations actually rest on.

  A `sorry` this project wrote is caught by reading the source. A `sorry` it
  DEPENDS on is not, and the dependency is real: Aeneas's own standard library
  ships two, in `core.slice.Slice.get_unchecked` and in that function's spec
  lemma. Neither is on the witness's path today -- it indexes through the
  bounds-checked `Slice.index_usize` -- but "not today" is not a property anyone
  can maintain by remembering.

  `#print axioms` reports transitive dependence, so it answers the question the
  grep cannot. The only names permitted in the output are Lean's own three:
  `propext`, `Classical.choice`, `Quot.sound`. Anything else -- `sorryAx` above
  all -- is a hole, and `dev.sh --lean-check` fails on it.

  This file exists to be read by that gate. It declares nothing.
-/

import SolverWitness.Spec

#print axioms SolverSpec.check_spec
#print axioms SolverSpec.check_correct
