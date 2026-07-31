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

**The random stream decides more than it looks like it should.** About 0.6% of
generated problems change `check_plan` verdict on a change of PRNG alone, so an
`xfail(strict)` anchor here can XPASS because the stream moved rather than
because anything was fixed — re-anchor from a fresh sweep instead of promoting
it. `solver_rng.py` owns both halves of every decision (the ChaCha8 stream and
the rule that turns bits into an index) because either half left implicit is
somewhere the Rust port silently disagrees.

**Container layout is not allowed to reach the plan.** The solver iterates sets
in places where order decides which application lands on the frontier first,
and CPython's hash-table order is portable to nothing. Every such iteration now
goes through an explicit rank; `test_iteration_order.py` salts `Node.__hash__`
— leaving signatures, keys and equality untouched — and demands the plan not
move. Any new set iteration in the search needs a stated order or that test
will find it.

**Asserting "the solver handled cycles" proves almost nothing.** The
path-dependent loop rejection in `refine_mcts._is_valid` fires on none of the
shipped templates and none of the pre-existing tests; it takes a generated
cyclic instance to reach it, and it is incomplete when it does — the refiner
still accepts cyclic states, and `rectify` launders them into inputs no step
produces. A cycle test that does not assert *which* rejection fired is the test
that was already here and already protected nothing. `test_refiner_validity.py`
pins each link.

**A plan that reaches the target is not the same as a search that finished.**
`Solution.complete` answers "did the search merge in a solved timeline", which
is why it can be True for a plan the checker refuses and was, for a while,
False for every sound multi-sample plan. Assert soundness with `check_plan` and
completion with `complete`; they are different questions.

**Signature collisions are semantics, not accidents.** Two distinct
`Application` objects can legitimately share a `Signature()` — six such states
occur solving the metagenomics template. Anything that treats a signature as an
identity (dedup, removal, memoization) is a correctness risk, not an
optimization.

**A green run does not tell you which solver ran it.** `msm_solver` is used when
it is present and can do the job, and absent it the Python solver runs — so this
axis passes either way and would go on passing if the Rust side quietly stopped
being reached. `test_solver_engine.py` is where that is made visible:
`Backend(capability)` says which one, the resolution branches are driven with
fake binaries so the refusals fire on every machine, and
`METASMITH_SOLVER_ENGINE=python` forces the fallback so it is a path something
runs rather than a path that merely exists. Run the axis both ways when touching
either implementation.

**Two version constants, and they are not interchangeable.**
`SOLVER_RNG_VERSION` covers the decision contract, `SOLVER_WIRE_VERSION` the
envelope. They move for different reasons, and folding them into one is how the
last cross-language desync in this repo went unnoticed while the fast suite
stayed green. A binary whose either version disagrees is refused, loudly, and
metasmith falls back rather than solving with rules it does not share.
