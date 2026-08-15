# Consolidation followups (2026-07)

This file transcribes findings from AWM scopes deleted in the 2026-07 metasmith
worktree consolidation. Sixteen worktrees were removed; several of their
`.awm/context.md` files (and one plan file) were the sole written record of
real findings — unfixed bugs, accepted residual risks, rejected designs, and
validation gaps. Nothing below is a completed item. Everything here is either
still open, was knowingly shipped as a residual risk, or was deliberately
rejected — read it as a punch list and a set of guardrails, not a changelog.

## Open bugs and weaknesses

**Promotion is a single post-execution pass — long/interrupted runs bank almost nothing.**
Nextflow `publishDir` stages outputs into `<cache_root>/<key>.tmp/` as tasks
finish, but `manifest.cbor`, the shard rename, and `CacheStore.upsert` all
happen in `promote_run`, called once from `agents.py` after the Nextflow
process returns. A run still in flight, or killed before Nextflow returns,
banks nothing. Observed: a GMCF_3495 pilot run reached 137 completed SLURM
tasks and staged 425 GB of output with **one** committed cache entry, and
that one was inherited from a previous run's kill. The inversion is the
point — the runs most likely to be interrupted are the ones with the most
to bank. Promoting per step, as each step's tasks drain, would cost one
manifest write per step and make an interrupted run resumable at step
granularity.
**Where:** `promote_run` in `caching/promote.py`, invoked once from `agents.py` post-Nextflow-return.

**`recover_orphan_tmp_dirs` can delete another run's finished, un-promoted work.**
It `rmtree`s every `<key>.tmp` directory lacking a `manifest.cbor`, and it
globs the **whole cache root**, not the workspace being promoted. Dirs the
promote loop already handled are renamed away and safe; everything else is
deleted, whichever run staged it. So promoting one run can destroy another
run's un-promoted staging, and deliberately holding a step back means moving
its `.tmp` aside — not just omitting it from this promote pass. Hundreds of
GB of finished work can disappear in a sweep that looks like the cache
working normally.
**Where:** `recover_orphan_tmp_dirs` in `caching/promote.py`.

**`trace.jsonl` records banked work, not run work.** Its rows are appended
from inside the promote loop, so a lone `session_start` row after a hundred
tasks ran is expected, not evidence of a lost buffer — but a reader who
assumes trace.jsonl mirrors execution will misdiagnose the single-pass-promote
weakness above as data loss when it is actually just deferred banking.
**Where:** `_metasmith/trace.jsonl`, written by the promote loop.

**ISSUE-6 [OPEN, verification scope] local executor silently ignores `-params-file` for memory cap.**
`local.nf` defaults `params.executor{cpus=8, memory='8 GB'}`. Passing
`RunWorkflow(params={"executor": {...}})` reaches the agent
(`runs/<key>/workflow.params.yml` shows the override correctly) but Nextflow
still reports the 8 GB cap: the `params{ executor{ memory='8 GB' } }` block in
`local.nf` wins over `-params-file` for the `executor{ memory = params.executor.memory }`
directive, because the executor scope resolves params at config-build time,
before the params-file merges. Confirmed via a `ProcessUnrecoverableException:
Process requirement exceeds available memory -- req: 64 GB; avail: 8 GB` on
`downloadUniRef50DB` (micb0/chamois has 16c/176 GB; anything requesting >8 GB —
diamond_uniref50 64 GB, comebin 32 GB, checkm2 16 GB — is rejected pre-run).
Recommended fix: override the executor cap via a literal `-config` block
(never via `params.*`), or auto-size the local executor to detected host
cpus/mem. Footgun noted alongside it: `errorStrategy='ignore'` makes a run
that hit this report "completed" with `[0] outputs` — check output count, not
run status.
**Where:** `local.nf` executor scope; surfaced via `RunWorkflow`.

**`Agent.Deploy` raises an opaque `AssertionError` instead of naming the failure.**
`agents.py:228`'s `assert len(res.out)==len(cmds)` fires with a bare
`AssertionError(['/arc/home/txyliu','login02'])` when remote home-resolution
silently drops a line (e.g. sockeye's `/scratch` is allocation-coded and
refuses `mkdir`, so `pwd -P` produces no output and the line count
undercounts). Flagged as a "core-robustness note, not yet fixed, low
priority" — the smoke-test symptom (wrong agent-home path) was worked around,
but the assertion itself still fails opaquely for any future cause of the
same shape. Should report "failed to resolve/create agent home at `<path>`"
instead.
**Where:** `agents.py:228`, `Agent.Deploy`.

**ISSUE-4 [WORKAROUND only, verification scope] source-checkout client computes a 404 container tag.**
Driving metasmith from a source tree (no `build_hash.txt`) degrades
`CONTAINER_TAG` to bare `0.18.5`, and `Agent.container`'s default
`docker://quay.io/hallamlab/metasmith:0.18.5` 404s against the actually
published `0.18.5-f83e7e7`. No core fix landed — the only fix is passing
`container=...` explicitly to `Agent(...)` when driving from source. Conda
installs are unaffected (hash is baked in).
**Where:** container-tag resolution in the deploy path when run from a source checkout.

**Verification-scope status summary** (so the list above isn't mistaken for exhaustive): ISSUE-1 (PYTHONPATH shadowing), ISSUE-3 (mksquashfs deploy crash), ISSUE-5 (`apptainer exec docker://` mksquashfs crash on large transform containers), ISSUE-7 (masked `TransformInstanceLibraryView` not stageable), ISSUE-8 (`HOME_ROOT` leaking into apptainer `--bind` sources), and ISSUE-9 (stale GTDB download URL) are all **FIXED** on that branch. ISSUE-2 (#240, external-bind-not-resident) is fixed by a deliberate fail-fast `FileNotFoundError` rather than an auto-transfer — see the `feat-hpc-deploy-smoke` note below for where this still leaves a smoke gap.

**`Orchestrator.group()` barriers non-parent streams until upstream channel close — unfixed (inbox #16).**
See the `orchestrator` retired-scope note below; kept here as a pointer since it is a live, actionable bug, not merely a historical note.

## Accepted residual risks

**F3 — mutable container tags (`:latest`) can produce a false cache hit.**
A container is added to the library like any other file, so its leaf id is
`content_multihash(.oci bytes) ⊕ relpath` — it addresses the docker **URL
string**, not the resolved image digest. A pinned tag
(`tool:1.0.4--hdfd78af_1`) correctly busts the cache on a version bump; a
mutable tag (`:latest`) does not, so if the image behind that URL changes,
subsequent runs false-hit on the stale cached output. Accepted as inherent to
tag-addressing and mitigated by the reference libraries' convention of using
pinned biocontainer tags — worth a one-line docs caveat, not a code fix.

**F4 — crash-window orphan gap in promote.** `caching/promote.py` writes the
manifest into `<key>.tmp/`, then does `tmp.rename(final_dir)` (POSIX-atomic;
the race-loser gets `OSError` → rmtree → skip), and calls `store.upsert`
**only after** the rename succeeds — so there is no window where the DB
reports a hit but files are absent (the mechanism itself is sound). The
accepted gap is narrower: a crash *between* `rename` and `upsert` orphans
`final_dir` with no DB row, so that cache key recomputes forever (the next
run's rename fails against the already-existing dir → skip → the DB row is
never written). `recover_orphan_tmp_dirs` only sweeps `.tmp` directories, not
orphaned final dirs, so it does not repair this case. Accepted as a
robustness gap, not a correctness/false-hit issue — worth a ticket, not a
blocker. (A second, related, but distinct gap: a stale lock from a crashed
job on a *different* host in a shared HPC cache is never reclaimed, correctly,
since a dead-remote can't be distinguished from a live-remote — this could
defer that key's promote indefinitely.)

Fixed in the same pass, for context on what's *not* residual: F1 (a
transform's protocol body was not part of its cache identity — false-hit,
confirmed e2e, fixed by folding a protocol-source hash into the lineage
signature), F2 (leaf id folded the raw path instead of a normalized relative
path — false-miss footgun, fixed), F-TESTS (fast suite shipped 3 red, an
abandoned test migration — fixed), and F8 (a shared version constant used for
both the cache-key epoch and the on-wire Nextflow lineage envelope desynced
after the F1 fix, failing every containerized step with a masked exit 1 —
fixed by splitting `CACHE_KEY_VERSION` from `LIN_PAYLOAD_VERSION` and adding
`tests/cache/test_wire_version_sync.py`). F5/F6 (cross-host key stability,
`METASMITH_LEAF_RANDOM` opt-out) were verified pass, not fixes.

## Deliberately rejected

**FANOUT-1 (commit `c114308`, "Per-Nextflow-task cache keying (FANOUT-1) +
assert_same_mount dual-bind") — excluded from the 0.19.1 consolidation.**

What it did: shifted the cache unit from `WorkflowStep` to Nextflow task, so
each fan-out step computed one `cache_key` per batch (= per Nextflow task)
instead of one per step. A rerun with partial input overlap (e.g. run 1 over
`[A, B]`, run 2 over `[A, C]`) would reuse `A`'s cached shard and only
recompute `C`, rather than recomputing the whole step. Mechanism:
`_compute_cache_decisions` built a `batches` list per step with per-batch
cache keys; `_batch_key` was threaded through the given-input lineage file at
planning time to route each task to its own shard directory; codegen emitted
three shapes (all-hit / all-miss / mixed) so a partially-cached step could mix
a live Nextflow channel with synthetic cached tuples via
`o.mix(...)`/`o.post(o.asStreams(...), k)`, preserving the Critic E#1
invariant (every tuple re-enters `o.post` before any downstream `o.group`
consumer). The mixed-hit path built its synthetic channel in
`_build_cached_synthetics` (`models/workflow.py`), which packs one Groovy
tuple literal — `[['_batch_key': '<hex>'], file('<path>')]` — per cached batch
into a single inline `Channel.of(t1, t2, ..., tn)` expression written directly
into the generated `workflow.nf`.

Why it was declined (per commit `4eac7fb`, "0.19.1: the aggregate for the
GMCF_3495 r1 run"): FANOUT-1 branched from the shared base on 2026-05-30 and
was written 2026-06-02, while `feat/reentrancy-try1-release-alpha` went on to
evolve the *same* caching subsystem for a further month (S3/S4/S5, R1/R3/R5)
without taking it. The cherry-pick conflicted on 19 hunks across 4 files.
Hand-merging a month-stale change into a month-evolved subsystem would have
produced a caching system matching neither branch's tested state — judged the
wrong artifact to carry into a long unattended pilot run whose purpose
included evaluating that very subsystem.

Underlying this is a scaling cliff the commit messages do not mention. It was
filed as an AWM `concern` by the session that landed `c114308`, on the same
day, and is reproduced here because that concern record does not survive the
scope deletion: `_build_cached_synthetics` inlines one tuple literal per
cached batch into a single Groovy `Channel.of(...)` call and does not bound
the size of the generated source. The concern estimated roughly 570 KB of
generated expression at cyanoverse scale, past practical JVM/Groovy compile
limits, producing an opaque Groovy compile failure on resume rather than a
graceful diagnostic. The unbounded-inlining mechanism is verifiable in the
`c114308` diff itself (`models/workflow.py`); the 570 KB figure and the
compile-failure symptom are the concern's estimate and were never reproduced.
It was never tested at that scale and never validated on real hardware.

Accepted cost of not taking it: sharding stays per-step (as it was before
FANOUT-1). Per commit `4eac7fb`: "a re-run where one sample of a 34-sample
fan-out changed [recomputes] the whole step. That is slower but correct;
FANOUT-1 is the optimisation, not the correctness fix." Diffstat of the
excluded commit: `AGENTS.md` +21/-, `caching/fs.py` +44, `caching/promote.py`
+286, `models/workflow.py` +434, `ops/cache.py` +51,
`testing/virtual_runtime.py` +97, plus two new test files
(`test_cache_promote.py`, `test_fan_out_partial_hit.py`) — 8 files, 1077
insertions / 196 deletions total. Branch tip for reference:
`feat/reentrancy-try1-caching` at `c114308` (preceded by `dcd5b9a`, `f1272e8`,
`27a507a`, `9d86540`).

## Validation gaps

**The release-ready caching/reentrancy system was never run live through the sockeye relay.**
It was declared release-ready on 2026-07-03, but the end-to-end proof
(COLD promote → WARM cross-host hit, cache_key byte-identical cross-host) was
executed on local docker + micb0 + sockeye as *separate* hosts proving
cache-key identity — not as a single live run relayed through sockeye's SLURM
relay path. (Cross-referenced against project memory: the closest matching
validation event is commit `cfd0236` R4, 2026-07-02.) A live-relayed run on
sockeye remains unexercised for this subsystem.

**FANOUT-1 was never tested at cyanoverse scale or on real hardware** — see
the "Deliberately rejected" section above. This is a distinct gap from the
sockeye-relay one: FANOUT-1's failure mode (Groovy source size blowing past
JVM limits) can only be observed at real multi-sample fan-out scale, which no
test suite (fast or e2e) currently exercises.

**`feat-hpc-deploy-smoke`'s G1/G2/G3 (deploy + trivial workflow end-to-end on sockeye/fir/mira) were RED at scope close**, blocked by inbox #240 (local transform-library path bound naively into the remote apptainer container — later addressed in the `verification` scope by a fail-fast `FileNotFoundError` rather than a fix that makes the bind work) and #241 (mira sandbox build fails under Ubuntu 24.10's `kernel.apparmor_restrict_unprivileged_userns=1` default). Only G4 (SIF-vs-sandbox probe verdict matches `bug_e2_fuse_wedge.md` expectations on all three hosts) and G6 (permanent `examples/` library, landed in `379c6ef`) closed green. Whether a full deploy+run now succeeds end-to-end on fir/mira post the #240 fail-fast fix was not re-verified before this scope was deleted.

## Re-audit checklist (from feat/audit)

The audit ran against 0.18.4, before `env/`, `caching/`, `gui/` and `std/`
existed in their current form. Every row was re-checked against the
consolidated tree on 2026-07-27; the **Now** column is that re-check, and it
supersedes the original verdict wherever the two disagree. One of them does:
S1 called `main/script_runner/` dead, and it is not. (The `main/` tree has since
moved under `research/metasmith/`; paths in the table below are pre-migration.)

| ID | Tier → Goal | Finding | Now (2026-07-27) |
|----|------|---------|---------------|
| S1 | A→G1 | Delete 7 stale `main/` subdirs (`relay_agent.old`, `paramiko`, `pyoxidizer`, `script_runner`, `container_bounce`, `tests`, `create_examples`; optionally `sql/`) | **PARTLY WRONG, rest DONE.** `main/script_runner/` is *not* dead: `src/metasmith/std/` — which did not exist when the audit ran — declares `std::oci_image_script_runner` → `docker://quay.io/hallamlab/metasmith_script_runner:0.1.1`, required by busco, cazy, bakta_db_full and both kofamscan transforms, and that Dockerfile is the repo's only recipe for the published image. **Keep it.** The other six were re-grepped at zero references and deleted. `main/sql/` also has zero references but the audit itself said "confirm before deletion" — still there, still a user call. |
| S2 | A→G1 | Dead `if False` arm, `models/paths.py:75` | **DONE, and it went further.** `_normalise` had zero callers anywhere — a private no-op whose docstring claimed `..`-collapsing it never did (`Path(str(Path(p)))` leaves `/a/b/../c` untouched). Deleted the whole function rather than tidying its dead branch. |
| S3 | A→G1 | 11 commented-out import lines, `coms/ipc.py:5-8,15-22` | **DONE.** |
| S4 | A→G1 | Leftover debug-print comment in `Agent.Pack()` | **DONE** (had drifted to `agents.py:359`). |
| S5 | A→G1 | Commented-out target-resolution block in `models/workflow.py` | **DONE** (had drifted to `:771-780`). |
| S6 | B→G2 | Rename `coms/cli/legacy.py` → `tools.py`/`misc.py` (not actually deprecated) | **STILL APPLIES, and grew.** It now wires four commands, not three — `get`, `lab`, `api` and `gui`. Nothing about `gui` is legacy. |
| S7 | B→G2 | Resolve `pyproject.toml` vs `setup.py` split (setup.py authoritative) | **STILL APPLIES, unchanged.** `pyproject.toml` still holds only `[tool.pytest.ini_options]` and no `[build-system]`. |
| S8 | B→G2 | `envs/dev.yml` drifted from `envs/base.yml` | **DONE** as option (b): `dev.yml` now states it is a packaging-tool overlay applied by `dev.sh --idev`, layered on base.yml, not a standalone env. |
| S9 | B→G2 | Container env name mismatch (`Dockerfile` default `for_container` vs `metasmith.def` and the `bin/` shebangs' `metasmith_env`) | **DONE, and half of it is moot.** `metasmith.def` no longer exists — the apptainer image is built from the docker image. `Dockerfile`'s default is now `metasmith_env`, matching the shebangs, so a hand-run `docker build .` no longer produces `/app` shims pointing at a non-existent env. |
| S10 | C→G3 | Split `agents.py` (1503 LOC) | **DONE 2026-07-27.** Now the `agents/` package: 10 modules, largest 494 LOC. Divided by which side of the wire runs the code — client (`targets`, `agent`, `workflow_ops`, `run_control`, `shell`, `gpu`, `portability`) versus agent host (`runner`, `collect`). |
| S11 | C→G3 | Split `models/workflow.py` (1703 LOC): extract Nextflow codegen, extract `_diagnose_plan_failure` family | **DONE 2026-07-27.** Now the `models/workflow/` package: 7 modules. Both extractions landed, plus `cache_decisions`. **Misses the ≤700 LOC target at one file:** `nextflow_codegen.py` is 964, of which `prepare_nextflow` is 801 — one function. Splitting it is a refactor of Groovy emitted against a strict-syntax parser, not a move, so it was left whole deliberately. That function is the remaining Tier C item in this file. |
| S12 | C→G3 | Split `models/libraries.py` (1435 LOC): extract `Size`/`Duration`/`Resources`, extract the transform/execution contract layer | **DONE 2026-07-27.** Now the `models/libraries/` package: 9 modules, largest `instances.py` at 739 (`DataInstance` 126 + `DataInstanceLibrary` 538 + the view). Both named extractions landed as `resources.py` and `execution.py`; the leaf-identity, transfer and telemetry blocks came out as mixins. |
| S13 | C→G3 | `models/solver.py` (1381 LOC) — optional | **UNCHANGED at 1367 LOC** — the only god-file that did not grow. Still defer. |
| S14 | D→G4 | Unify Pack/Unpack vs Save/Load vs to_dict/from_dict; collapse `DataInstance` id shadow fields | **HALF OF THIS IS NOW DANGEROUS.** The serialization unification still applies, but "remove `_key`/`legacy_key`" does not: since the reentrancy work, `instance_id` *is* the cache identity, `_key` tracks it for modern callers and `legacy_key` preserves the pre-content-addressing derivation. Deleting either changes cache keys, which silently invalidates or false-hits every cached run. Do not treat these as redundant copies. |
| S15 | D→G4 | Shared `Shell` protocol for `LiveShell` and `RemoteShell` | **STILL APPLIES** — both classes still exist, still structurally identical, still no shared ABC. |
| S16 | D→G4 | Replace `bootstrap.ExecuteStep`'s 9-parameter signature with an `ExecutionRequest` dataclass | **STILL APPLIES; now 10 parameters** (`host_local` was added). |
| S17 | D→G4 | Split `coms/ipc.py`: move string/time utils to `coms/utils.py` | **STILL APPLIES but shrunk in value** — the file is 268 LOC total, so the split buys a clean seam rather than relief from size. |
| S18 | E→G5 | Eliminate the method/free-function shadow pattern in `agents.py` | **ADDRESSED, NOT ELIMINATED (2026-07-27).** The S10 split made the shadow legible instead of removing it: the methods are `Agent.RunWorkflow`/`StageWorkflow`/`CheckWorkflow` in `agents/workflow_ops.py`, the free functions are `agents.runner.RunWorkflow` and siblings. Same name, but the module now says which side of the wire you are on. **Eliminating it is off the table:** both halves are public entry points and `coms/api.py` imports the free functions by bare name from the package. That import is why `runner` is imported last in the package `__init__` — it has to win those three names. `CollectResults` was free-only and shadows nothing, so it moved to `agents/collect.py`. |
| S19 | E→G5 | Map every CLI command to one `ops/<group>.<verb>`, or document the exceptions | **DONE** as "document the exceptions", in `ops/_common.py`. Three exceptions, not two: `e2e` (harness signalling), `run` (deliberate direct_run bypass) and `legacy.py`'s `get`/`lab`/`gui`/`api`. `transform`/`type` → `ops.transforms`/`ops.types` and `task` → `ops.workflow` are naming, not drift. |
| S20 | E→G5 | Audit `coms/api.py`'s `Api` RPC class; delete if dead | **NOT DEAD — do not delete.** Its caller is still `cli/legacy.py`, which registers it as the internal agent-to-agent RPC endpoint (`"not for manual use"`). Note `gui/app.py`'s `from .api import bp` is a *different* module (`gui/api.py`) and is not evidence either way. |

Everything marked STILL APPLIES is a citation-based proposal that has been
re-grepped but never executed.

The Tier C splits (S10/S11/S12) were executed on 2026-07-27, one commit each:
7258 lines across three files became 26 modules across three packages, with the
dotted import paths unchanged. S13 (`models/solver.py`, 1367 LOC) was left
alone deliberately — it is the one god-file that did not grow, and the audit
said defer. The one target missed is `prepare_nextflow` at 801 lines; see S11.

## Notes from retired scopes

**apptainer-cache** — context.md is the unedited startup boilerplate; no findings recorded.

**concurrent-relay** — context.md is the unedited startup boilerplate; no findings recorded. (The substantive work from this scope — errno-108 array fan-out fix, node-local control-plane staging landed at `a7497c7`, relay re-point pending sockeye topology observation — is preserved in project memory as `concurrent_relay_fanout_staging.md`, not lost, just not in this file.)

**live-shell-init-timeout** — context.md is the unedited startup boilerplate; no findings recorded.

**tmux_shell** — context.md is the unedited startup boilerplate; no findings recorded.

**orchestrator** — real, unfixed bug. `Orchestrator.group()` barriers non-parent
streams until the upstream channel closes, causing terminal aggregations
(e.g. a metagenomics W1 run's `assembly_stats`) to deadlock when upstream
uses `errorStrategy='ignore'` or retries that never reach a terminal state.
Surfaced 2026-05-28 from a W1 metagenomics resume: `p06__assembly_stats` was
declared but never submitted, in either the original 4-day run or its resume
(zero entries in either `trace.tsv`). Root cause: the non-parent-stream
buffer-and-flush-on-channel-close branch at `src/metasmith/nextflow_config/Orchestrator.groovy:218-241`
(within the broader `group()` branches at lines 182-241). The same barrier
blocks every downstream taxonomy/annotation transform that groups by `pair`
or `meta` — expected to surface identically on W2/W3 annotation steps. Four
candidate fixes were proposed, in preference order: (1) default `group_by` to
the most-recent declared input with per-sample identity rather than the
upstream-most type (solver-side inference, `models/solver.py` around lines
169-222); (2) make `Orchestrator.group()` emit incrementally for the 1-to-1
case (same per-item index on every input stream, `batch_size=1`); (3)
document the barrier explicitly in `group()`/`group_by` docstrings; (4) fail
loudly at plan time when a workflow implies a terminal aggregation over an
`errorStrategy='ignore'` upstream. Shared invariant with the caching/`nf-metasmith`
plugin work (must not be violated by any fix here): any tuple entering
`o.post(stream, k)` must populate `index_history[stream]` with that tuple
before downstream `o.group()` consumers observe it. This scope's branch was
`feat/orchestrator`; a separately-existing stale branch
`fix/orchestrator-channel-deadlock` in the bare repo is unrelated prior work
and should not be conflated with it. Inbox #16 (`scope:metasmith/dev`) is the
originating bug report and is still open as far as this record shows.

**feat-hpc-deploy-smoke** — goal was to smoke-test deploy + a trivial workflow
over SSH on sockeye, fir, and mira, confirming the release SIF-vs-sandbox
decision logic. G4 (probe verdict capture) and G6 (permanent `examples/`
library, `379c6ef`) closed green: sockeye (apptainer 1.3.1, setuid,
`use-sif`), fir (1.3.5, no setuid, `use-sif`, correctly avoiding the Bug E.4
1.3.x sandbox SIGBUS), and mira (1.4.5 conda-forge, no setuid, `use-sandbox`,
correctly avoiding the Bug E.2 squashfuse wedge) all matched
`MakeSandboxDecisionProbe`'s two-axis static check. G1/G2/G3 (actual
end-to-end deploy+run) stayed RED, blocked by inbox #240 (shared blocker:
`StageWorkflow` bound the local transform-library path naively into the
remote apptainer container, causing a mount failure on every remote host —
the same code path ran successfully in a separate `spanish-lakes/hpc-runs`
script, so the divergence was never traced from this scope) and #241 (mira:
sandbox build fails against Ubuntu 24.10's
`kernel.apparmor_restrict_unprivileged_userns=1` default; unblock is
`sudo sysctl kernel.apparmor_restrict_unprivileged_userns=0` or
`sudo apt install apptainer-suid`). #238 (sockeye: `module load apptainer`
needs `module load gcc/9.4.0` first; `/scratch/$USER` mkdir is refused,
allocation-coded) and #239 (superseded by #241) are minor carry-forwards.
Note #240 was later given a design resolution in the `verification` scope
(fail-fast `FileNotFoundError` naming the missing remote path, not an
auto-transfer) — but that is a deliberate-behavior decision, not proof that
G1/G2/G3's end-to-end smoke now passes; it was not re-run here before the
scope closed.

## Carried forward from the monorepo migration (2026-08-15)

The migration that folded the engine, the standard library, fabfos and ASPIRE
into one tree retired the `metasmith/monorepo` scope. These are the items that
outlived it. Each names the scope that should pick it up and the first move, so
none of them depends on a session's task list to survive. Five sibling defects
found alongside these were fixed in the wrap-up itself and are deliberately
absent — this file holds only what is still open.

**Nine tier-4 readers still resolve a retired data path.** They point at a base
that the migration replaced, and the sharp edge is that the replacement is not
the same data: anything quoted from an old run has to be **re-run, not
re-labelled**, because the numbers can legitimately differ. Belongs to a fabfos
scope. First move: enumerate the readers and confirm which published figures, if
any, were produced from the retired base.

**The two-point probe is not declared as a transform, so the community lane
cannot be re-measured.** Deferred by an explicit decision rather than forgotten.
It is blocked on a type question, and nobody should start coding before that is
settled: both probes currently write one result type, and two producers of a
single type give the planner nothing to tell them apart — so the declaration has
to introduce the distinction before it can be useful. Belongs to a fabfos scope.

**The four-plan binning comparison harness may be routing around a constraint
that no longer exists.** `TestCheckMParallelFork` in
`tests/metasmith_libraries/test_binning_routing.py` asserted that a comparison
must be driven as four separate planning calls because `TargetBuilder` refuses
the same target type twice. That is not the contract: it refuses the same type
*with the same parents*, and rows 32–37 in the same file exercise exactly the
same-type-different-parents shape the harness says is impossible. The test now
states the real contract and the harness is untouched, because whether it can
collapse to one plan is a library-design call, not a test fix. Belongs to
`libraries/mono`. First move: try the four lineages as one target set and see
whether the resulting plan is what the harness produces today.

**The logistics e2e tests cannot pass under the `local` preset.**
`getNcbiSra.py` (and five sibling `download*` transforms) declare
`memory=Size.GB(64)`, while `src/metasmith/nextflow_config/local.nf` pins the
local executor to `8 GB` and the tests override only cpus and queue size.
Nextflow refuses the process before it runs, and the visible symptom is the
unhelpful `No reads downloaded from SRA`. Host RAM is irrelevant — this fails on
a 58 GB machine. It is long-standing rather than new; it only became *visible*
when the library suite began collecting again. Note also that the class carries
`@pytest.mark.slow` and `@pytest.mark.network` but nothing deselects either for
that suite, and `network` is registered project-wide with a different meaning
("requires LIVESHELL_REMOTE_HOST"), so an opt-in-looking test is in fact
always-on. First move: decide between the test naming a memory override, the
preset defaulting to the host's real memory, and the suite honouring its own
markers — they are three different policies, not three spellings of one fix.

**A DVC-pinned build artifact whose objects leave every cache is
unrecoverable.** The rule the migration settled is written up under
"When a build artifact may be DVC-pinned" in `docs/metasmith/architecture.md`.
Recorded here as the residual risk it manages rather than removes:
`src/metasmith/engine.dvc` holds a four-target cross-compile that exists in no
other form, so one careless collection destroys it. The `scratch/gui-main` pin
was the same class of object and its bytes were **already gone** from the shared
cache when it was dropped — which is what the failure mode looks like in
practice, and the reason it was worth writing down.

### Two things the solver fix made visible

Both were hidden by the same mechanism and are recorded here because fixing the
engine is what exposed them. The differential gate's fixtures probed
`packaged_engine_path()` directly, which in a source checkout is a read-only
hardlink that cannot be executed — so the gate erred out in half a second,
reported "a binary is staged but failed its handshake", and never ran. A gate
that fails for an environmental reason, in the vocabulary of a build problem, is
worse than no gate; the fixtures now resolve through `GetEngine`, the same call
the planner makes.

**The two implementations disagree by one ULP on `log2`.** With the gate
running, `test_a_generated_script_agrees_draw_for_draw` fails on four of five
seeds. It is not a decision-rule divergence: `test_the_two_streams_are_the_same_stream`
passes, `draws` is identical on every seed, and all eight differing entries out
of ~10,000 are `log2` differing in the final digit — Rust's libm against
CPython's. Plans do not diverge from it, because the stream never drifts. The
open question is narrower and worth stating: a one-ULP difference in a score can
still flip an `argmax` at a near-tie, and while the hand-written tie cases pass,
that is not proof over generated corpora. Whatever policy is chosen — a ULP
tolerance, one side adopting the other's implementation, or asserting on `draws`
alone — say so next to `SOLVER_RNG_VERSION`, which currently asserts an
agreement that does not hold.

**Two of those tests are expensive, and the axis they sit in says they are not.**
`tests/metasmith/solver/` maps to `fast`, but
`test_the_engine_reads_the_shipped_templates` solves every shipped template and
`test_a_demand_with_several_producers_lists_them_in_rank_order` walks the
corpus; together they add roughly 35 minutes to a gate that was 16. They were
free only because they were broken. Note the axis is assigned by directory and
markers are additive, so adding `slow` does not remove `fast` — moving them is a
change to `_DIR_MARKERS` or to the file's location, not a one-line annotation.
