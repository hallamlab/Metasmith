# Reentrancy / caching — adversarial "try to break it" findings

Branch: `feat/reentrancy-try1-release-alpha`. Unattended adversarial pass to assess
release-readiness of cross-run/cross-host lineage caching. Priority order for the hunt:
**false HIT** (serves wrong output — catastrophic) > **false MISS** (wasted compute) >
corruption/race.

## TL;DR verdict

The **core mechanism is sound** — content+path-addressed leaf identity, canonical-CBOR keys,
promote atomicity, and cross-host key stability all hold up. But there is **one real
correctness hazard (F1)** that I confirmed end-to-end, plus a shipped-red test suite (F-TESTS,
now fixed) and a low-severity reuse footgun (F2). I would **not call it release-ready until F1
is decided on.**

| ID | Class | Severity | Status |
|----|-------|----------|--------|
| F1 | FALSE HIT — protocol/tool change not in cache key | **High (correctness)** | CONFIRMED e2e |
| F-TESTS | Fast suite ships RED (3 obsolete tests) | Medium (release gate) | **FIXED** this pass |
| F2 | FALSE MISS — raw path folded, not normalized | Low (reuse loss) | Confirmed |
| F3 | FALSE HIT — mutable container tag (`:latest`) | Low (inherent, mitigated) | By-design |
| F4 | Promote atomicity | — (sound) | Verified sound |
| F5/F6 | Cross-host key stability + `LEAF_RANDOM` opt-out | — (pass) | Verified pass |

---

## F1 — a transform's code is NOT in its cache identity (FALSE HIT) — CONFIRMED e2e

**What breaks.** The lineage `cache_key` is built from `transform_key` + `signature` + input
ids. Both `transform_key` (`TransformInstance.GetKey()` → `model.key`) and `signature`
(`str(model._hash)` → `model.hash`) are computed from **only the transform's I/O type
topology** (`solver.py:189`, `KeyGenerator.FromStr(str(self))` where `str(self)` is just
`{req-props}->{prod-props}`). The transform's **name and its `protocol` function body never
enter the key.** So:

- Edit a transform's protocol (change the command, fix a flag, fix path handling) while
  keeping the same declared input/output types → **identical cache_key** → a cross-run rerun
  short-circuits and serves the OLD output.
- More generally: two *different tools* that declare the same `in→out` types are
  **cache-indistinguishable**. Swap which one the planner uses and run 2 false-hits run 1.

This directly contradicts the cache layer's own contract. `caching/keys.py` docstring claims
the signature "Captures the input/output type topology **+ protocol identity** so that two
transforms with the same name but different bodies key differently." The producer of that
signature (`libraries.py:1773`) does **not** provide protocol identity — the source-hashing
code is commented out with the note *"use the transform model hash, since updates to script
should be able to use the existing nxf cache."* That was a deliberate choice for the OLD
Nextflow work-dir cache (reuse across script edits). Under the NEW cache — which
**short-circuits execution entirely** — the same choice becomes a silent-wrong-results hazard.
The two layers are inconsistent; the docstring believes protocol identity is folded in.

**Proof (unit).** Loaded the real `examples/echo_greeting.py`, captured its cache_key, edited
only the protocol (`hello`→`goodbye`, types unchanged), reloaded → `cache_key` byte-identical
(`1e20ff6b…908e8dcf`). `_hash`/`_key` unchanged.

**Proof (end-to-end, real cache machinery).** Via `virtual_runtime` (drives the actual
probe→synthetic-channel short-circuit→promote path, same code as docker):
- Run A: protocol writes `VARIANT=1`, cold → promote.
- Run B: same types, protocol writes `VARIANT=2`.
- Result: log emits *"cache probe matched 1/1 step(s); will short-circuit"*, run B executes
  **0 steps** and produces run A's `VARIANT=1` fingerprint. Stale output served for changed code.
- (Throwaway test kept at `scratchpad/test_f1_protocol_falsehit_probe.py`; deliberately NOT
  committed since it asserts the buggy behavior.)

**Real-world impact.** In the reference libraries (`metasmith-libraries`), fixing a bug in how
a transform invokes its tool — without bumping the container or changing types — would silently
reuse the pre-fix results on any host that already cached them.

**Recommended fix (maintainer's call — a semantic tradeoff, not auto-applied).** Fold a hash of
the protocol source (the transform `.py` bytes, or `inspect.getsource(protocol)`) into the
**lineage `signature` only**, keeping `model.key` for Nextflow process naming. That decouples
"cache correctness" from "nxf process identity" and restores the contract the `keys.py`
docstring already assumes. Cost: edits to a transform's body will (correctly) no longer reuse
prior cached results — which is the whole point.

## F-TESTS — the fast suite shipped RED (FIXED this pass)

Fast suite at HEAD was **3 failed / 398 passed / 6 skipped / 3 xfailed**. All 3 failures were
`test_cache_baseline.py::test_baseline_rerun_re_executes_everything[*]`. This is an
obsolete-by-design "before" test — its docstring says it *"will start failing once caching
lands"* and points to a `test_cache_execution.py` replacement that was **never created**. Its
sibling in the same file *was* migrated ("Replaces the obsoleted main-era assertion"); this one
was left behind — a half-finished migration. The forward-looking behavior it was meant to hand
off to (rerun on identical inputs → full cache hit) is covered **green** by
`tests/cache/test_cross_run.py`, `test_hit_miss.py`, and `test_cache_e2e.py`.

**Fixed:** removed the obsolete function; the file is now 6/6 green and the fast suite is
**398 passed / 0 failed**. Note this contradicts the R3 commit's "docker e2e gate green" claim —
the *fast* gate was not green.

## F2 — leaf id folds the raw path, not a normalized relative path (FALSE MISS, low)

`_mint_leaf_id` (`libraries.py:615`) folds `str(path)` verbatim, though its docstring says
"relative path." Verified:
- (a) same bytes + same **relative** path across two different library roots → **same** leaf id
  → cross-host reuse works. *(The headline feature is sound.)*
- (b) an **absolute** path (or differently-normalized path) between two runs — even same host —
  → divergent leaf id → cache miss.
- (c) same bytes + different relpath → distinct ids (no false collapse of fan-out).

Not a correctness bug; a reuse-loss footgun + doc/code mismatch. Fix: fold
`path.relative_to(self.location)` consistently before hashing.

## F4 — promote atomicity: SOUND

`promote.py` writes the manifest into `<key>.tmp/`, then `tmp.rename(final_dir)` (POSIX-atomic;
the race-loser gets OSError → rmtree → skip), and calls `store.upsert` **only after** the rename
succeeds. The hit path (`workflow.py:1375`) checks `store.files_exist(entry)` before trusting a
hit. There is **no window where the DB reports a hit but files are absent.** O_EXCL lock with
stale-PID reclaim guards concurrent promotes. Two low-severity robustness gaps worth a ticket,
neither a correctness/false-hit issue:
1. A crash *between* `rename` and `upsert` orphans `final_dir` with no DB row → that key
   recomputes forever (the next run's rename fails on the existing dir → skip → never upserts).
   `recover_orphan_tmp_dirs` only sweeps `.tmp`, not orphaned final dirs.
2. A stale lock from a crashed job on a *different* host (shared HPC cache) is never reclaimed
   (correctly — can't tell dead-remote from live-remote) → could defer that key's promote.

## F5 / F6 — cross-host key stability + opt-out: PASS

- Keys are byte-stable across hosts: `KeyGenerator` is sha256-based, `canonical_cbor` is
  RFC-8949 deterministic; no salted `hash()`/randomness in the key path. Empirically confirmed
  by F2(a).
- `METASMITH_LEAF_RANDOM=1` correctly disables cross-run reuse (two mints of identical
  bytes+path → different ids); default content-addressed mints match.

## F3 — mutable container tag (inherent, mitigated)

A container is `AddItem`'d like any file, so its leaf id = `content_multihash(.oci bytes) ⊕
relpath` — i.e. it addresses the docker **URL string**, not the resolved image digest. A pinned
tag (`tool:1.0.4--hdfd78af_1`) busts the cache on a version bump (good); a mutable tag
(`:latest`) does not → false hit if the image changes behind the same URL. Inherent to
tag addressing; mitigated by the reference libraries' use of pinned biocontainer tags. Worth a
one-line docs caveat.

## Not bugs (documented stances)

- A `cacheable=True` transform that reads undeclared external state (env var, wall clock,
  network, RNG) will false-hit. The framework provides no guard — this is the user's asserted
  determinism responsibility (default `cacheable=True`; opt-out `cacheable=False` / `METASMITH_CACHE=0`).

## F8 — shared version constant desync (REGRESSION from the F1 fix; FIXED)

Found by the real-docker e2e, not by any unit test. The F1 fix bumped `LIN_PAYLOAD_VERSION`
2→3 to invalidate stale cache shards, but that one constant was doing double duty: the
cache-key epoch AND the on-wire Nextflow lineage-envelope version. The envelope emitter is a
hardcoded Groovy literal `[v:2, entries:index[0]]` generated by `workflow.py` into every
`workflow.nf`; it did not move. The parser (`LinPayload.Unpack`, now expecting 3) then rejected
every real task with `unsupported lin payload version 2; expected 3`. Every containerized step
failed with exit 1, masked by `errorStrategy=ignore` → `0 written` promote, `[0] outputs`.

Why no unit test caught it: the fast suite's cache harnesses synthesize lin payloads via
`json.dumps` and the virtual runtime stubs execution, so the Groovy wire is never exercised —
the exact blind spot documented at R4.

Fix (commit eea1ef7): split into `CACHE_KEY_VERSION=3` (cache_key + sqlite epoch) and
`LIN_PAYLOAD_VERSION=2` (on-wire envelope, in lockstep with the Groovy emitter). Added
`tests/cache/test_wire_version_sync.py`, which greps the `workflow.py` emitter literal and pins
it to `LIN_PAYLOAD_VERSION` — this would have caught the desync at unit-test time.

## Round-2 adversarial hunt — no new false-hit

batch_size, group_by, and the cacheable flag are all definition-file literals, so they are
folded into `_protocol_source_hash` → the cache_key (empirically confirmed; guarded by
`tests/cache/test_structural_identity.py`). Empty-slot skips are safe (slot keys distinguish;
topology is in the signature). The `"+".join(ids)` separator is unambiguous over fixed-length
hex ids. Same-bytes+relpath leaves from different parents share a leaf id by design
(content-addressing), which is correct, not a collision.

## Real-docker content-level validation of the F1 fix — PASS

Driver `/tmp/claude-1000/caching_e2e_f1.py`, three runs against one shared agent home /
task_cache, greeting protocol loaded from a per-word editable copy of `examples/`:

- COLD `hello` → executed, promoted; published `hello world`.
- WARM `hello` → `cache probe matched 1/1 step(s); will short-circuit` — cross-run cache HIT.
- EDIT `goodbye` → no probe match → MISS → re-executed → published `goodbye world` (fresh).

`task_cache/` ended with two distinct shards — `…207d12a3…/out = "goodbye world"` and
`…205f85db…/out = "hello world"` — i.e. one cache_key per protocol variant, each holding the
correct bytes. The pre-fix catastrophic false-hit (an edited protocol serving stale output) is
resolved and proven at the byte level under real containers.

## Release verdict

READY. The catastrophic false-hit (F1) is fixed and proven end-to-end; the cross-host reuse
footgun (F2) is fixed; the shared-version regression introduced by the F1 fix (F8) is fixed,
proven, and guarded. Round-2 surfaced no further false-hit. Residual items are documented and
accepted: F3 (mutable `:latest` tags), F4 (crash-window orphan — robustness, not correctness),
and undeclared-input / non-determinism (the user's asserted `cacheable=True` responsibility).
