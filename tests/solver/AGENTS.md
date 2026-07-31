# tests/solver

Solver correctness — given a set of transforms, a set of givens and a target,
does the planner choose a set of transform applications that is *sound*?

**This is not `tests/flow`.** Flow asks what the runtime does with a plan it is
handed. This axis asks whether the plan should ever have been handed over.

## What goes in this file

Why this axis is shaped the way it is, and the traps that make a green test
here meaningless. Not a test inventory — read the directory for that.

## The harness is the point

`src/metasmith/testing/solver_verification.py` carries the four pieces every
test here leans on, and new tests should reach for them rather than assert on
solver internals:

- **`plan_fingerprint`** — canonical form of a plan, by colour refinement over
  the step/endpoint DAG. Two plans that differ only in step order or in which
  `Endpoint` object carries a value fingerprint the same, which is the parity
  standard this project holds itself to: *topological equivalence*, not
  object-for-object identity.
- **`check_plan`** — the semantic adjudicator. It shares no code with the
  search, on purpose: verification is far simpler than search, and that
  asymmetry is the only reason it can judge output from an implementation we do
  not yet trust. A checker that imported solver internals would agree with the
  solver's bugs.
- **`generate_problem`** — random problems with dials for cycles, lineage
  density, duplicate transforms, product groups and multiple given groups.
  These dials exist because nobody has a list of the solver's edge cases; the
  generator covers the space they live in instead.
- **`forward_closure_solvable` / `exhaustive_solvable`** — oracles for "should
  this have been solvable at all". The second answers `None` at its cap rather
  than guessing, and a `None` must never be read as a `False`.

## Traps

**Never fingerprint anything that reads `instance_id`.** Leaf ids fall back to
a random per-call value for absent or deferred inputs, so such a fingerprint
differs between two runs of unchanged code — it has already produced one false
"the plans changed" verdict during this work.

**`solve_by_mcts` seeds numpy globally.** Test order can therefore change what
a later test sees. Any test that cares about the stream must set its own seed.

**Asserting "the solver handled cycles" proves almost nothing.** The
path-dependent loop rejection in `refine_mcts._is_valid` fires on *no* known
input — not on the shipped templates, not on the pre-existing loop test. A
cycle test that does not assert *which* rejection fired is the test that was
already here and already protected nothing.

**Signature collisions are semantics, not accidents.** Two distinct
`Application` objects can legitimately share a `Signature()` — six such states
occur solving the metagenomics template. Anything that treats a signature as an
identity (dedup, removal, memoization) is a correctness risk, not an
optimization.
