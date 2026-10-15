import Lake
open Lake DSL

require aeneas from "../aeneas"

package «solverWitnessSpec» {}

-- `globs` is load bearing. Without it `lean_lib SolverWitness` looks for a root
-- module `SolverWitness.lean` and reports "some modules have bad imports" for
-- the submodules it then never builds. There is no root file: the extraction
-- (`Types`, `Funs`) and the specification (`Spec`) are siblings under
-- `SolverWitness/`, and the specification imports the other two.
@[default_target] lean_lib SolverWitness where
  globs := #[.submodules `SolverWitness]
