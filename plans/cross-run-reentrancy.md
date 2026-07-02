# Plan: Cross-run reentrancy ("resuming across runs")

**Status:** ACTIVE (autonomous). Started 2026-07-02.
**Branch:** feat/reentrancy-try1-release-alpha. Commits are checkpoints — safe to reset to any.
**Foundation:** single-point-of-provenance refactor COMPLETE (T0–T5; see
plans/single-point-of-provenance.md + memory `provenance-onchannel-id-design`).

## Big picture / goal

A second, independent invocation of the same pipeline on the same inputs should
**automatically** reuse cached intermediate + step results — i.e. an interrupted
or re-run pipeline resumes from the cache instead of recomputing. Step-level
cache HITS already work within/across runs *when the cache_key matches*; the
reentrancy gap is that keys don't match across independent runs.

## Gap CONFIRMED (2026-07-02, from keys.py + tests/cache/test_cross_workflow.py:20-25)

`lineage_key` bakes per-input `instance_id_bytes` into the cache_key. The
test_cross_workflow module header states the current design verbatim: "The
leaf-identity model (G8) makes 'exact same plan ran in two workspaces' hit only
when inputs come from the same library object — this is by design ... the
imported-row plumbing [does not require] P2 to also re-derive the same key,
because the leaf identities by definition differ." So today cross-run reuse
needs EITHER a shared library object (byte-stable leaf ids) OR manual
`metasmith data import-library`. **Reentrancy = make P2 auto-hit P1's cache
given the same INPUT FILES but a fresh library**, which requires an
input-stable (content/logical) leaf identity feeding `lineage_key`. Still need
the exact leaf-minting site (Explore agent mapping it).

## Working hypothesis of the gap (VERIFY before building)

- `caching/keys.py::lineage_key` bakes `(slot_key, instance_id_bytes)` per input
  into the cache_key; for leaves it uses "synthesized leaf-identity bytes for
  origin='leaf'". So cache_key transitively depends on leaf `instance_id`s.
- Prior journal + AGENTS.md:545 note "G8: leaf ids are unique per AddItem, so
  cross-build hits require `metasmith data import-library`." => two independent
  runs on identical inputs mint DIFFERENT leaf ids => different cache_keys =>
  cache miss => recompute. THIS is the reentrancy blocker (hypothesis).
- Likely fix direction: content-address leaf identity (stable from input
  bytes/path/identity) so identical inputs => identical cache_keys across runs,
  making resume automatic without a manual import step. Provenance-refactor
  context explicitly deferred "content-addressing leaves" to this session.

## Constraints (carry over from provenance session)

- Do NOT break pipeline results. Cache is an optimization: a hit must produce
  byte-identical outputs to a miss.
- Fast suite baseline: 378 passed / 7 skipped / 3 xfailed. Docker gate:
  test_e2e_trace + test_telemetry_e2e + test_group_cases + test_e2e_full_pipeline
  = 71 passed / 0 failed / 0 xfailed. Keep green.
- Run tests as: `PYTHONPATH=$(pwd)/src mamba run -n msm python -m pytest ...`
  (fast suite ~230s — background it). Docker gate is authoritative — run ONCE
  per checkpoint, iterate via fast + virtual runtime.
- Commit each verified checkpoint (Co-Authored-By: Claude Fable 5). Never
  close/delete any reentrancy-try1-* branch.
- Self-compact (reflection compact, pane %5) at clean seams.

## THE BLOCKER (confirmed by Explore map, 2026-07-02)

`models/libraries.py:566-584 _mint_leaf_id` = `multihash_key(uuid.uuid4().bytes +
time.time_ns())` — random per `AddItem` call (the "G8" design decision;
`AddItem` at libraries.py:548→561). Every step cache_key transitively bottoms
out in leaf ids (`workflow.py:1306-1314`); derived output ids descend from the
cache_key (`workflow.py:1332-1345`) and propagate downstream. So a fresh run on
identical inputs computes entirely different keys → misses every shard.
Everything else for auto-resume already works:
- per-step probe: `workflow.py:1236 _compute_cache_decisions` (store.probe +
  files_exist), guarded by cache_root existing + METASMITH_CACHE.
- synthetic Channel.of hit rewrite: trace `workflow.py:1469-1563`, codegen
  `workflow.py:2120-2169`.
- promote: `promote.py:502 promote_run`, upsert origin="lineage" (716-722).
- A deterministic leaf-id shape is already expressible + tolerated: the legacy
  fallback `_resolve_instance_meta` (libraries.py:601-603) =
  `KeyGenerator.FromStr(path+dtype+lib_key)`.
G8 asserted at `tests/cache/test_identity.py:64`; documented `AGENTS.md:545`,
`docs/source/usage/nextflow.rst:14-29`.

## Design decision: content+path-addressed leaf identity (default-on, opt-out)

Leaf `instance_id` becomes `multihash(blake3(file_bytes) ‖ relpath)` when the
resolved path is a readable file at mint time; fall back to the current random
id when the file is absent/unreadable (remote/lazy inputs → no cross-run reuse
for those, acceptable). Default-on matches the user's opt-out caching stance
(memory feedback-cacheable-default-true); kill-switch `METASMITH_LEAF_RANDOM=1`.
cache_keys legitimately CHANGE this session (that's the feature); only
results-correctness (a hit's outputs == a miss's outputs) is invariant.

**REVISED from pure-content (2026-07-02):** the first cut was
`multihash(blake3(file_bytes))` — pure content. That broke
`test_solver_scaling_cyanoverse` (Generate 30s→371s): the fixture writes 21,081
EMPTY files, so pure-content collapsed ALL of them to one leaf id (blake3 of
b""), re-triggering the solver's O(n²) id-collision path. Worse, it's a latent
CORRECTNESS bug — N distinct inputs that share bytes (empty/degenerate files,
byte-identical samples) would collapse to one identity and corrupt fan-out
(N inputs → 1 output). Folding the library-relative path into the digest fixes
both: a rerun over the same layout+bytes still reproduces the id (reentrancy
preserved — fresh libs use the same relpaths), while distinct files stay
distinct. Still false-hit-safe: content is part of the key, so same-path +
different-content ⇒ different id. Strictly safer than pure-content OR pure-path.

## Task breakdown

- [x] R0 — Orient: confirmed blocker + mechanism (above). DONE.
- [ ] R1 — Content-addressed leaf identity. Change `_mint_leaf_id` to
      content-hash when file present (deterministic), else random fallback.
      Decide origin tag + a stat-cache only if hashing cost bites. Keep the
      legacy deterministic fallback intact. Fast suite + reason about which
      tests pin G8.
- [ ] R2 — Auto cross-run resume proof. New test: run pipeline P in workspace A
      (promote), then run P in a FRESH workspace B on the SAME input files
      (fresh library, NO import-library) → B hits A's cache at compile time;
      assert hit==miss output parity. Update G8-pinning tests to the new model
      (test_identity.py:64, test_cross_workflow.py:20-25 docstring, AGENTS.md,
      docs). Docker gate.
- [ ] R3 — Robustness/edge: remote/absent inputs fall back cleanly; large-file
      hashing cost (stat-cache if needed); interaction with per-batch keying +
      the diamond fan-out; kill-switch still bypasses. Decide opt-out surface.

## Checkpoint log

- R0 DONE (no commit — orientation only). Blocker = random _mint_leaf_id;
  mechanism = content-addressed leaf id. Explore map captured above.
- R1 CODE DONE + logic-verified (2026-07-02, pre-commit). Changes:
  * `caching/keys.py`: added `content_multihash_key(path)` — streamed
    `KEY_PREFIX + blake3(file_bytes)` (1 MiB chunks, no full materialize).
  * `models/libraries.py::_mint_leaf_id`: content-address when the resolved
    path is a readable regular file at mint time; random uuid4+time_ns
    fallback when absent/unreadable; `METASMITH_LEAF_RANDOM=1` kill-switch
    forces legacy random. origin stays "leaf" (no ripple). AddItem comment
    refreshed.
  * `tests/cache/test_identity.py`: `test_addItem_unique_per_call` (pinned
    old always-random G8) SUPERSEDED by `test_addItem_content_addressed_
    when_file_present` (+ content-sensitivity) and
    `test_addItem_unique_per_call_when_file_absent` (fallback preserved);
    module coverage line G8→R1.
  Targeted run: tests/cache/test_identity.py + test_cross_run.py = 12 passed.
  **R2 proof already GREEN** (see below) — R1 is the whole feature.
  Full fast-suite gate + commit: PENDING.
- R2 TEST DRAFTED (tests/cache/test_cross_run.py, 3 tests, all pass in the
  targeted run): `test_fresh_library_same_inputs_hits_cache` (fresh library
  at a different location on identical bytes → same task key + full cache
  hit + identical result_fingerprints — the reentrancy proof),
  `test_perturbed_inputs_get_distinct_identity` (negative: different bytes →
  different id), `test_leaf_random_optout_disables_cross_run` (kill-switch).
  Remaining R2 work: doc/AGENTS updates (G8 surface) + full gate + commit.

## Open questions / risks

- Is stable leaf identity content-hash (bytes) or path/logical-id based? Content
  hashing large bioinformatics inputs may be expensive — may need a stat-based
  or user-declared stable id.
- Does making leaf ids stable break the G8 invariant that other code relies on?
  (uniqueness assumptions in solver / dedup.)
- Interaction with per-batch keying + the diamond fan-out just fixed.
