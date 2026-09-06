# tests/solver

Solver correctness — given a set of transforms, a set of givens and a target,
does the planner choose a set of transform applications that is *sound*?

**This is not `tests/flow`.** Flow asks what the runtime does with a plan it is
handed. This axis asks whether the plan should ever have been handed over.

**There is one solver.** Roughly a third of the files here once held the Rust
port against the Python implementation it replaced. Those are gone, along with
the implementation. A test here adjudicates the engine's answer with `check_plan`
or the witness; there is no second solver to compare against and no `--solver=`
to pick one. Where a property was real and only the counterparty was Python, it
is asserted about the engine directly — `test_distance_walk.py` reads the
distance table out of `msm_solver describe` rather than out of a Python
solution's telemetry.

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
  solver's bugs. **Pass `strict=True` when the plan came from a
  changed refiner.** It promotes the notes to violations and adds a
  signature-uniqueness check over produced endpoints. `rectify` keys its
  endpoint map by signature, so two steps emitting signature-equal endpoints are
  merged onto one producer and every consumer is rewired to whichever came last
  in `get_order`. The wire the witness reads is already collapsed, so
  `uniqueProducer` cannot see it, and `strict` is the only place it is visible.
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

**Container layout is not allowed to reach the plan.** The search iterates sets
in places where order decides which application lands on the frontier first, and
a hash map's order is portable to nothing. Every such iteration goes through an
explicit rank, and `det::Map` is what the engine uses so that a `HashMap`'s
iteration order cannot be picked up by accident. The test that guarded this by
salting CPython's `Node.__hash__` went with the Python solver, so there is now no
automated tripwire for it — a new set iteration in the search has to be given a
stated order by whoever writes it.

**Asserting "the solver handled cycles" proves almost nothing.** It takes a
generated cyclic instance to reach the refiner's validity rejection at all, and a
cycle test that does not assert *which* rejection fired protects nothing. The
test that pinned each link counted branches of the Python refiner and went with
it; `test_known_unsound.py` is what remains, and it asserts the outcome — a
cyclic transform graph yields a plan `check_plan` accepts — rather than the
mechanism.

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

**A green run can still be a run that solved nothing.** Every test here that
needs the engine skips when no binary is staged, so a checkout that forgot
`dev/metasmith.sh -bel` reports a tidy pass over an axis it never exercised.
`test_solver_engine.py` is where the resolution itself is the subject:
`Backend(capability)` answers "rust" or "none", and the branches are driven with
fake binaries staged into a patched `ENGINE_DIR`, so the refusals fire on every
machine rather than only on one that shipped a bad build. A solve with no usable
engine raises; it does not fall back, because there is nothing to fall back to.

**Two version constants, and they are not interchangeable.**
`SOLVER_RNG_VERSION` covers the decision contract, `SOLVER_WIRE_VERSION` the
envelope. They move for different reasons, and folding them into one is how the
last cross-language desync in this repo went unnoticed while the fast suite
stayed green. A binary whose either version disagrees is refused, loudly, and the
solve raises rather than proceeding under rules the two sides do not share. Bump
the Python constant and forget to rebuild and the whole axis skips —
`test_the_staged_engine_agrees_about_the_contract` is what says why.

**An arm that is not an arm.** The lesson the differential harness left behind
outlives it: when a test means to compare two configurations, assert that the two
were actually different before believing the comparison. An unset environment
variable comparing the engine against itself produced a published wall-clock
claim here that had to be retracted, and it looked exactly like a real result.
`test_selection_policy.py` now pins the inverse property — no environment
variable selects a different rule — so the confusion cannot come back the same
way.

**The solve seed no longer reaches the plan.** It used to decide how big a plan
the search settled on, and the refiner's cost climbs steeply with plan size, so a
sweep that fixed the seed reported a corpus that was uniformly cheap. PUCT ranks
rather than samples and `top_k` is 1, so selection draws nothing on 72 of 72
corpus and profile cases. `SOLVE_SEEDS` still exists and sweeping it is still
free, but it no longer buys coverage; what varies a problem's size now is the
*problem* seed and the generator dials.

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
plan, and nothing will see it: the test that salted a hash layout was CPython's
and went with the Python solver.

**`generate_child_nodes` is a generator, and that is load-bearing.** Its caller
adds each child's signature to `frontier_signatures` as it consumes them, so a
transform reached later in the same expansion sees the earlier ones' children
already blacklisted. Collecting every transform's children against one frozen
blacklist looks equivalent and is not: it leaves one extra application on the
frontier, which changes what the explore arm draws, which changes the plan. Cost
to find: one decision trace plus one frontier trace (`MSM_SOLVER_TRACE=1`).
