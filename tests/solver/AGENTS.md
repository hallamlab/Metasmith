# tests/solver

Solver correctness — given a set of transforms, a set of givens and a target,
does the planner choose a set of transform applications that is *sound*?

**This is not `tests/flow`.** Flow asks what the runtime does with a plan it is
handed. This axis asks whether the plan should ever have been handed over.

**The engine is the subject; the python solver is not.** Roughly a third of the
files here were written to hold a new port against the implementation it
replaced, and that job is done — they now carry `@pytest.mark.python_solver` and
skip unless `--python-solver` is passed. A new test belongs on the default path,
which means adjudicating the engine's answer with `check_plan` rather than
comparing it to a second solver's. If you cannot state what a test asserts
without naming the python implementation, it is a port test, not a solver test.

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
somewhere the Rust port silently disagrees. Changing the guiding heuristic moves
anchors the same way, and can take one out of reach of the search entirely, so
the same rule applies to it.

**Container layout is not allowed to reach the plan.** The solver iterates sets
in places where order decides which application lands on the frontier first,
and CPython's hash-table order is portable to nothing. Every such iteration now
goes through an explicit rank; `test_iteration_order.py` salts `Node.__hash__`
— leaving signatures, keys and equality untouched — and demands the plan not
move. Any new set iteration in the search needs a stated order or that test
will find it.

**Asserting "the solver handled cycles" proves almost nothing.** The rejection
in `refine_mcts._is_valid` fires on none of the shipped templates and none of
the pre-existing tests; it takes a generated cyclic instance to reach it at all.
A cycle test that does not assert *which* rejection fired is the test that was
already here and already protected nothing. `test_refiner_validity.py` pins each
link, including that the anchor still *produces* cyclic candidates — otherwise
"none reached `rectify`" would pass for the wrong reason.

**Validity means schedulable, and the near-miss is instructive.** `_is_valid`
asks whether every step becomes runnable with *all* of its inputs available,
starting from the givens; that fails on a cycle and on an input nothing
produces, and on nothing else. It used to walk forward over *consumers* and
reject a repeated application signature along a path — which reached a step as
soon as **one** input was available and never asked about the others, so a cycle
off the side of the walk was invisible. `rectify` then flattened those cycles
onto one depth in `get_order` and rewrote a consumer before its producer,
converting the cycle into an unproduced input with nothing left to show where it
came from. Anything that weakens this check back toward "reachable" rather than
"schedulable" reintroduces that, and the symptom will not look like a cycle.

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

**A green run does not tell you which solver ran it.** `msm_solver` is the
default and the Python solver is the reversion, but both answer the same
`solve_by_mcts` — so this axis passes either way and would go on passing if the
Rust side quietly stopped being reached. `test_solver_engine.py` is where that is
made visible: `Backend(capability)` says which one, and the resolution branches
are driven with fake binaries staged into a patched `ENGINE_DIR`, so the refusals
fire on every machine rather than only on one that shipped a bad build.
`pytest --solver=python|rust` runs the whole axis on a stated implementation, and
`--solver=rust` *fails* rather than falling back when no binary is staged —
asking for one thing and silently getting the other is the bug, not the
workaround. Run it both ways when touching either implementation.

**Two version constants, and they are not interchangeable.**
`SOLVER_RNG_VERSION` covers the decision contract, `SOLVER_WIRE_VERSION` the
envelope. They move for different reasons, and folding them into one is how the
last cross-language desync in this repo went unnoticed while the fast suite
stayed green. A binary whose either version disagrees is refused, loudly, and
metasmith falls back rather than solving with rules it does not share.

**A reference that isn't a reference.** Both implementations are reached through
the same `problem.solve()`, so once the engine advertises `solve` a differential
test compares the engine against itself unless something stops it. Wrap the
reference side in `UsePythonSolver()` and *assert* `Backend("solve") == "python"`
inside it — the failure mode is a green run, and a green run is not something you
go looking at. `solver_differential._reference` is the only place the sweep
solves, for exactly this reason. The same applies to any test whose subject is
the Python implementation rather than the answer: `test_iteration_order.py` salts
CPython's hash layout and `test_refiner_validity.py` counts how often a branch of
the Python refiner fires, and neither means anything with the search running
elsewhere. Both pin the whole file. A test that instead asserts the *default* —
engine staged, nothing said, engine runs — has to unpin for its own duration, or
it reports on `--solver=` rather than on the code.

**One seed per problem hides a whole regime.** The stream decides how big a plan
the search settles on, and the refiner's cost climbs steeply with plan size:
`sink` at problem seed 24 solves to 7 steps in 0.15s under seed 42 and to 57
steps in 53s under 2³¹−1. A sweep that fixes the seed reports a corpus that is
uniformly cheap and never visits the regime where either implementation is under
load. `solver_differential` runs every problem under `SOLVE_SEEDS` for that
reason.

**A cap is not a verdict.** Because the expensive cases are expensive for *both*
sides, a differential sweep needs a time limit, and the tempting shape — skip the
case, count the rest — reports a percentage over a corpus it did not finish
reading. Capped cases are classified `unadjudicated`, kept apart from both
"agreed" and "disagreed", and printed with the flags that rerun them uncapped.
Sixteen of 16,000 hit the cap; fifteen were settled that way and agreed. The
sixteenth cannot be settled at the default `max_refine=256` by *either* side —
one refiner iteration on it costs the engine 22 seconds — so it is adjudicated
at the budgets that terminate and reported as exactly that.

**The engine's scratch arrays are safe only because nothing iterates them.**
`Refiner::score` keys every table it builds by `EpSig`, and `scratch.rs`
replaces those hash maps with flat arrays plus a generation stamp — worth ×3.4
on the refiner, because roughly 60% of the engine's instructions were in
`malloc` and `hashbrown` rather than in the graph work. The justification is not
that arrays are faster; it is that `produced_from`, `have`, `used_as_lineage`,
`product2producer` and the depth maps are *insert-and-look-up only*. A table that
is never iterated has no order to leak, so neither the map nor the array can
reach the plan. Anything that starts iterating one of them — a debug dump, a
"while we're here" summary — puts a container layout back on the path to the
plan, and `test_iteration_order.py` will not see it, because that test salts
*CPython's* hashing and this is the other implementation.

**`generate_child_nodes` is a generator, and that is load-bearing.** Its caller
adds each child's signature to `frontier_signatures` as it consumes them, so a
transform reached later in the same expansion sees the earlier ones' children
already blacklisted. Collecting every transform's children against one frozen
blacklist looks equivalent and is not: it leaves one extra application on the
frontier, which changes what the explore arm draws, which changes the plan. Cost
to find: one decision trace plus one frontier trace (`MSM_SOLVER_TRACE=1`).
