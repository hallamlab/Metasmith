# Making the solver provably correct

## Purpose & Contents

The design case for certifying solver output instead of verifying the search. Holds the argument
and the measurements that justified the approach.

**Status: the case was accepted and the work is done.** The specification is
`docs/metasmith/solver-spec.md`, stated in Lean at `src/solver_witness/lean/Spec.lean` over the
types the extraction emits, and the checker is `src/solver_witness/`. What remains is the proof of
`check_spec`, which `dev.sh --lean-check` counts as an outstanding obligation.

Three of the questions below were settled differently from the way this file proposes, and where
they disagree the specification wins:

- **Ancestry and identity.** This file proposed taking the union of the declared parents and the
  step graph, and left identity open. Both are settled: ancestry is the declared closure alone,
  and endpoints are compared by **identity** everywhere. The reason structural comparison looked
  necessary was one defect in `rectify`, not a fact about the type system.
- **The order to do the work in.** Repairing `rectify` had to come first, not second. Every other
  decision was downstream of it.
- **The proof burden.** "~150 lines of loop-free predicate evaluation" was optimistic. The checker
  is about a thousand lines, and five distinct shapes had to be avoided before any of it reached
  Lean at all; they are listed in the specification.

Solver behaviour belongs in `docs/workflow_solver/architecture.md`. Test-axis rules belong in
`tests/metasmith/solver/AGENTS.md`.

## Certify the answer, do not verify the search

Nobody verifies an MCTS. Emit a witness instead, and adjudicate the witness with a small trusted
checker. SAT solvers emit DRAT refutations. LP solvers emit Farkas certificates. CompCert
translation-validates its register allocator rather than proving the allocator.

The proof burden then falls from 1,400 lines of search plus the Rust port plus a ChaCha8 stream
onto ~150 lines of loop-free predicate evaluation.

`check_plan` is that checker. It shares no code with the search, so it does not care which
backend, seed or policy produced a plan. It runs only under `src/metasmith/testing/`, which makes
it a test fixture rather than a gate.

Two measurements say the gate is free:

| measurement | result |
|---|---|
| `check_plan` over the 11 shipped templates | 11/11 ok, 0 violations, 0 notes |
| `check_plan` against solve, `metagenomics_from_paired_reads` | 0.47 ms against 11.96 s |

## Write the specification first

Correctness is a relation between an artifact and a specification. No specification document
exists. Four implementations decide ancestry and identity, and they disagree.

| site | relation |
|---|---|
| `solver.py:534` `_is_ancestor` | declared `Endpoint.parents` closure, disabled entirely under `mock_produced` |
| `solver.py:766` `validate_node._has_ancestor` | step graph alone |
| `solver_verification.py:146` `_Ancestry.parents_of` | the union of both |
| `solver_verification.py:468` `exhaustive_solvable._Ep` | value equality, no object identity |

The step-graph relation cannot see that two given endpoints of one sample are siblings. It
therefore rejects the plan the search just produced on 6 of 11 templates, which is why
`_found_on` is 1 everywhere.

`check_plan` already carries the six clauses:

1. **Provenance** — some step produces every consumed endpoint.
2. **Conformance** — `d.properties ⊆ e.properties` for every binding of endpoint `e` to slot `d`.
3. **Acyclicity** — the producer-consumer step graph is a DAG, and the emitted list is a
   topological order of it.
4. **Target** — exactly one application of the target transform, with every requirement bound.
5. **Lineage** — for every slot `d` with anchor `a ∈ d.parents`, `a` is bound in the same step,
   and `used[d]` descends from `used[a]`.
6. **Foreignness** — at most one step applies a transform outside the problem, and it requires
   nothing.

Three decisions remain open. Settle each in the specification.

- **Ancestry.** Take the union. An endpoint descends from what its producer consumed and from
  what it was constructed carrying. Widening `validate_node` to the union failed before because
  the *generator* violates the invariant `e.parents ⊇ inputs of e's producer`. Repair the
  generator.
- **Endpoint identity.** Choose values or object identity. `Node.__eq__` is signature equality,
  `check_plan` uses `id()`, and `_Ep` uses values. `check_plan` downgrades "equal but not
  identical" to a note, which is that ambiguity left undecided.
- **Transform collection.** Choose set or sequence. See the next section.

## Black-box checking has three levels

**L1 witness checking.** `(problem, plan) → sound?`. Available now. Complete for soundness,
silent on everything else.

**L2 metamorphic checking.** Properties relating outputs on related inputs. Needs no oracle and
no reference implementation. `test_iteration_order.py` is one already.

**L3 generative specification.** Write the specification as a relation that runs backwards and
enumerates every sound plan for a small problem. This yields bounded oracles for the two
questions a checker cannot answer. Completeness fails when the enumerator finds a plan the
solver called unsolvable. Optimality fails when the enumerator beats the refiner.
`exhaustive_solvable` already enumerates reachable endpoint sets, then discards the plans and
returns a bool. Yield the plans instead.

### Transform order reaches the plan

Permuting the `transforms` list changes the plan on 5 of 12 generated problems, up to 4 distinct
plans over 5 orderings. Every resulting plan is sound.

`_transform_rank` (`solver.py:428`) ranks transforms by arrival position, and `_by_transform`
orders frontier expansion by that rank. The shipping path passes a dict keys view
(`workflow/plan.py:244`). A base library walks `manifest.items()` in insertion order out of the
`_metadata/index.yml` build product. Views sort, base libraries do not.

**CAUTION** Two hosts whose `index.yml` orderings differ plan differently for the same template.
This reaches the plan fingerprint and the cache.

Read the result as a specification decision. A set demands the solver canonicalise on a content
key. A sequence demands stable ordering from every caller, which puts a build product's file
order into the contract.

### No black-box technique checks intent

Give every clause a witness of its own necessity. Name a concrete plan the clause alone rejects,
and what that plan does at runtime. The failure mode is on record already. The cycle rejection in
`refine_mcts._is_valid` fires on none of the shipped templates and none of the pre-existing
tests, so it protects nothing.

Mutation-test the specification. Drop or weaken each clause. Demand that some corpus case flips
verdict.

## The remaining work

**Gate on the checker.** Run `check_plan` on the plan `solve_by_mcts` returns, on both backends,
after `merge_states` and `rectify`. The search then leaves the trusted computing base, and a
Python/Rust divergence becomes a reproducibility problem rather than a correctness one.

**Report three answers.** `Solution.complete == False` conflates "no plan exists" with "the
search gave up". `forward_closure_solvable` over-approximates reachability, so its fixed point
refutes solvability in linear time. Return `solved`, `unsolvable` with that certificate, or
`unknown`.

**Prove the checker.** Write a second checker with no cleverness. Enumerate every plan up to ~6
steps for problems below ~5 transforms. Assert the two agree. Escalate to SMT-bounded equivalence
or a mechanised proof only if a wrong plan becomes expensive.

**Certify optimality.** `solver_bound.py` already computes an admissible ceiling. It fires on 56
of 69 cases and none of the expensive ones, because the incumbent stays at negative infinity
whenever `validate_node` rejects everything. Repairing the generator unblocks it. State the claim
narrowly, as optimality over the input-swap neighbourhood.

**CAUTION** `solver_math.entropy` returns `Σ p·log₂p` with no minus sign. The objective rewards a
concentrated lineage assignment and peaks at 0. Any ceiling built on it must stay above the true
optimum.

**Make the search amenable to reasoning.** `Node.hash` is cached at construction, and `RefreshHash()`
recomputes it after `rectify._fix_endpoints` mutates `parents` (`solver.py:730`). A node's hash
changes while the node is a dict key. `rev_emap` (`solver.py:714`) is written on line 735 and
never read, which is that unclear identity story left in the tree. Make endpoints and
applications immutable. Separate a monotone integer identity from the structural signature.

## Sequence

1. Write `docs/metasmith/solver-spec.md`. Settle ancestry, identity and transform collection.
2. Repair the generator's lineage handling. Use `fosmid_inserts_from_pooled_reads`, at 7 steps
   and 23 candidates.
3. Gate `solve_by_mcts` on `check_plan`.
4. Return three answers, with the forward-closure certificate.
5. Add the naive reference checker and the bounded exhaustive cross-check.

Steps 1 to 4 deliver a machine-checked soundness proof with every plan, a checkable refutation
with every impossible answer, and a local optimality certificate with every refined plan.
