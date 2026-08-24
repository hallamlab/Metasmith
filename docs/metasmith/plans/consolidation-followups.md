# Open followups

## What goes in this file

The punch list: bugs that are known and unfixed, risks that were accepted knowingly, and
designs that were rejected on purpose. One entry per item, naming what is wrong and the first
move. Nothing that is finished belongs here — git holds that — and nothing that a test or a
module already states belongs here either.

## Open bugs

**A directory-typed given whose content changes below its top level keeps its leaf id, so the
cache serves stale results as if they were this run's.** A leaf id is absolute path plus
`mtime_ns` — no content, no size, no inode — and it stats the top-level directory only, so an
in-place edit or any create/delete/replace inside a subdirectory leaves the id where it was and
every cacheable step hits. Reproduced end-to-end against a real `dvc checkout`, with a
restat-only control that does not hit, so the hit is caused by the identity and nothing else.
Reachable here: `ref::ezpred_model` is directory-typed over a dvc `.dir` with 45 files below
depth 1, and 6 of 11 dvc-tracked reference dirs share that shape. `ref::kofamscan_profiles`
escapes only because its files sit flat at depth 1, which one reorganisation would undo. The
first move is content addressing with stat demoted to a memo key — that changes
`CACHE_KEY_VERSION` and invalidates every existing shard, so it is a migration, not a patch.
Ships unfixed in 0.21.0.

**Promotion is a single post-execution pass, so a long or interrupted run banks almost
nothing.** Nextflow stages outputs into `<cache_root>/<key>.tmp/` as tasks finish, but the
manifest, the shard rename and the store upsert all happen in one call after the Nextflow
process returns. A run killed before that returns banks nothing — and the runs most likely to
be interrupted are the ones with the most to bank. Promoting per step, as each step's tasks
drain, would cost one manifest write per step and make an interrupted run resumable. Cancel now
stops Nextflow's process group and leaves the driver alive to run this pass, so the natural next
piece is making the pass worth reaching.

**The orphan sweep can delete another run's finished, un-promoted work.** It removes every
`<key>.tmp` lacking a manifest, and it globs the whole cache root rather than the workspace
being promoted — so promoting one run can destroy another run's staging, and holding a step
back means moving its `.tmp` aside rather than merely omitting it from this pass.

**The local executor silently ignores `-params-file` for its memory cap.** The `params{
executor{ memory } }` block in `local.nf` wins over the params file, because the executor
scope resolves params at config-build time, before the file merges. Anything declaring more
than the preset's cap is refused before it runs. Override the cap through a literal `-config`
block rather than through `params.*`, or auto-size the executor to the host. Related footgun:
`errorStrategy='ignore'` makes a run that hit this report success with zero outputs — check
the output count, not the run status.

**The logistics e2e tests cannot pass under the `local` preset** for that reason: six
`download*` transforms declare far more memory than the preset allows, and the visible symptom
is `No reads downloaded from SRA`. Host RAM is irrelevant. The class also carries `slow` and
`network` marks that nothing deselects, and `network` is registered project-wide with a
different meaning, so an opt-in-looking test is always on. Decide between the test naming an
override, the preset defaulting to the host's real memory, and the suite honouring its marks —
three policies, not three spellings of one fix.

**`Agent.Deploy` raises a bare `AssertionError` instead of naming the failure.** It asserts
one output line per command, which undercounts whenever remote home-resolution drops a line —
an allocation-coded `/scratch` that refuses `mkdir`, for instance. It should report which path
it failed to resolve.

**A source checkout computes a container tag that 404s.** With no build hash, the tag degrades
to the bare version, while the published image carries the hash suffix. Pass `container=`
explicitly when driving from a source tree; conda installs bake the hash in.

**`Orchestrator.group()` barriers non-parent streams until the upstream channel closes**,
so a terminal aggregation deadlocks when upstream uses `errorStrategy='ignore'` or retries
that never reach a terminal state.

**The generated `stub:` block sorts a shared index value list in place.** Groovy's
`List.sort()` mutates, and index value lists are shared by reference across the DAG — this is
the one known writer, and it sits in generated code where the audit of `Orchestrator.groovy`
does not reach. `v.sort(false)` returns a copy and is the whole fix.

**The results index of a chunked step records only leaf ancestors and drops the chunk.**
Nothing reads that index today, so the loss is invisible; anything that later asks which chunk
produced a file gets a plausible answer built from the wrong level. Decide first whether the
index is a provenance record or a convenience lookup — the two want different walks.

**The two solver implementations disagree by one ULP on `log2`.** Not a decision-rule
divergence: the streams match and `draws` is identical on every seed, and every differing
entry is `log2`'s last digit — Rust's libm against CPython's. A one-ULP difference in a score
can still flip an `argmax` at a near-tie, so choose a policy — a tolerance, one side adopting
the other's implementation, or asserting on `draws` alone — and state it next to
`SOLVER_RNG_VERSION`, which currently asserts an agreement that does not hold. The differential
gate also runs against a staged binary older than the Rust source, so rebuild before treating
any of this as settled.

**Two solver tests are expensive and sit in an axis that says they are not.** One solves every
shipped template and one walks the corpus; together they dominate a gate meant to be quick.
The axis is assigned by directory and markers are additive, so adding `slow` does not remove
`fast` — moving them means changing `_DIR_MARKERS` or the file's location.

**Nine tier-4 readers still resolve a data path the monorepo migration retired.** The
replacement is not the same data, so anything quoted from an old run has to be re-run rather
than re-labelled.

**The two-point probe is not declared as a transform, so the community lane cannot be
re-measured.** Blocked on a type question rather than forgotten: both probes write one result
type, and two producers of a single type give the planner nothing to tell them apart, so the
declaration has to introduce the distinction first.

**The four-plan binning comparison harness may be routing around a constraint that no longer
exists.** `TargetBuilder` refuses the same target type with the same *parents*, not the same
type — and the same file exercises the same-type-different-parents shape the harness says is
impossible. Whether it collapses to one plan is a library-design call.

## Accepted risks

**An external mtime-touching event makes the next run cold, and one file is enough.** Stat
addressing bought the thing it was for — a 24 GB DIAMOND database or a 27k-file profiles tree
costs one stat instead of a full-tree hash — and the price is that only leaves inside a given
data library are stat-keyed, so moving one of them empties the whole hit set, including
downstream steps that never read it. `rsync -a` preserves mtimes, so staging does not itself
re-key; exposure is external events, and `dvc checkout` under a reference tree is the live one.
Mitigation is to pin the library: `restat_leaf_ids()` skips pinned libraries by design. The
same identity scheme fails the other way under *Open bugs*, and one fix answers both.

**A mutable container tag can produce a false cache hit.** A container's leaf id addresses the
docker URL string, not the resolved image digest, so a pinned tag busts the cache on a version
bump and `:latest` does not. Inherent to tag-addressing; mitigated by the convention of
pinning biocontainer tags.

**A crash between the promote rename and the store upsert orphans a shard.** The rename is
atomic and there is no window where the store reports a hit with files absent, but a crash in
that gap leaves a final directory with no row, and the key then recomputes forever because the
next rename fails against it. The orphan sweep only reaches `.tmp` directories. Related: a
stale lock from a crashed job on another host in a shared cache is never reclaimed, correctly,
since a dead remote cannot be distinguished from a live one.

**A DVC-pinned artifact whose objects leave every cache is unrecoverable.** The live instance is
`data/fabfos/runs/aska/gpr`: no remote is configured, the pinned generation exists nowhere but a
shared local cache that no longer holds it, and the files on disk are hardlinks into that cache —
so `dvc checkout --force` would delete them with nothing to restore. The rule that manages this
is under *When a build artifact may be DVC-pinned* in `docs/metasmith/architecture.md`.

## Deliberately rejected

**Per-Nextflow-task cache keying (FANOUT-1).** It moved the cache unit from the step to the
task, so a rerun with partial input overlap would reuse the shards it already had. Declined on
two grounds. It was written against a month-stale base and hand-merging it into a subsystem
that had since evolved would have produced a caching system matching neither branch's tested
state. And it inlines one Groovy tuple literal per cached batch into a single `Channel.of(...)`
with no bound on the generated source — at real fan-out scale that is a Groovy compile failure
on resume rather than a diagnostic. The accepted cost is that a re-run where one sample of a
fan-out changed recomputes the whole step: slower but correct. FANOUT-1 is the optimisation,
not the correctness fix.

**Deep-copying index value lists.** The lists are shared by reference across the DAG and the
no-writer rule is enforced only at the top-level map, but every production write was audited
and none mutates a value list. Copying changes the rendered index, which feeds
`file_instance_id`, which would orphan every existing cache shard. Reach for it the first time
a value-list mutation actually appears.

**A close-time "this stream contributed nothing" assertion.** A stream that delivers no items
is the legitimate optional-branch case, so asserting there fails runs that are behaving
correctly. The per-item guard catches the reported case earlier anyway.

## Validation gaps

**The caching and reentrancy path has never been run live through the sockeye relay.** The
cross-host proof was assembled from separate hosts showing cache-key identity, not from one
relayed run.

**FANOUT-1's scaling cliff was never reproduced.** Its failure mode is only observable at real
multi-sample fan-out scale, which no suite exercises.

**Deploy-and-run end to end on the HPC hosts was red at the last check** and has not been
re-verified since the bind failure was made fail-fast.
