# Open followups

## What goes in this file

The punch list: bugs that are known and unfixed, risks that were accepted knowingly, and
designs that were rejected on purpose. One entry per item, naming what is wrong and the first
move. Nothing that is finished belongs here — git holds that — and nothing that a test or a
module already states belongs here either.

## Open bugs

**The e2e docker cleanup chmods every pytest tmpdir on the host, and now times out doing it.**
`tests/metasmith/e2e/docker/test_cache_real_nextflow.py::_bind_root` walks UP from the work
directory until its parent is `/tmp`, which lands on `/tmp/pytest-of-tony` — the shared root of
every pytest run this host has ever done — and then `docker run … chmod -R a+rw` that whole tree
against a fixed 120s timeout. Measured: 57,030 files and 468 MB, none of it the test's own, and
a bare `docker run … chmod` on an EMPTY directory takes 16.7s on this box. Two tests fail on the
timeout. It is a feedback loop rather than bad luck: nextflow work directories are created
root-owned inside containers, so pytest's own `rm_rf` cannot remove them
(`PermissionError: Operation not permitted`) and leaves `garbage-*` trees behind — which is what
the chmod exists to fix, and what makes it slower every run. The first move is to bind and chmod
the test's own directory rather than the walked-up root.

**`test_stage_real_libraries_clones_into_sandbox` asserts a `.git` its helper stopped writing.**
`stage_real_libraries` moved to `shutil.copytree` plus a `STAGED_FROM` file naming the source and
HEAD when the library stopped being a separate repository, and `src/metasmith_libraries` is a
plain directory in the monorepo now. The assertion should read `STAGED_FROM`.

**A transform whose product extends the type it requires is a self-loop the solver walks.**
`kbase/filter_assembly/seqkit_filter_contigs` requires `sequences::assembly` and produces
`sequences::filtered_assembly`, which `extends assembly`; `kbase/polish_assembly/polypolish`
does the same with `polished_assembly`. So each satisfies its own requirement, and a plan
targeting a filtered assembly can chain them arbitrarily deep. Measured: the parity analysis
`a4_mags_from_metagenome` solves through an ELEVEN-long filter/polish/filter ladder before
`assembly_stats`, 19 steps where 13 do the job, and every product in the ladder is consumed by
the next step so it is a real chain and not dead branches. It was five long before curation
round 6; widening `alignment::bam` perturbed the search and lengthened it, which is how it was
found. The language has no negation, so "an assembly that has not already been filtered" cannot
be said as a requirement -- the first move is to decide whether these transforms should require
a narrower type than the one they extend, or whether the refiner should reject a candidate that
re-derives an ancestor of its own input.

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
Ships unfixed in 0.22.0. That release moves `CACHE_KEY_VERSION` to 5 for an unrelated reason —
the unit became one group member's invocation — so the epoch bump users pay for buys nothing
here, and the migration this entry wants still costs a second one.

**0.23.0 changes the shape of this rather than closing it.** A given is no longer a leaf: its
identity is assigned by an import and recorded, so no amount of stat'ing is involved and the
in-place edit does not move it either. The stale hit therefore survives, with a different cause
and a different fix. Under import the honest statement is that the pool records a declaration and
never re-reads the data, so changing the bytes under an entry is invisible until somebody imports
again — which is deliberate, and is what makes a 17 GB reference cost nothing to cite. What is
missing is any way to notice. The first move is a cheap change detector an operator can run
against a pool — size and top-level mtime per entry, compared with what the import recorded —
rather than making a citation re-read the data.

**Cancelling a run during its first minute silently does nothing.** `CancelWorkflow` keys on
`PID.lock`, which `start.sh` writes only once nextflow is up, while `RUN.token` lands as soon as
the launcher detaches. Measured on the docker lane: 22:40 for the token, 22:41 for the lock. A
cancel in that window returns `method: noop`, `status: not_running` — and the run then proceeds.
`ps` already reports the run during the same window, so the two disagree about whether anything
is running. The user-visible shape is a cancel button that reports nothing to stop and leaves the
run going. Either widen what cancel will act on to the run pgid `start.sh` already records, or
have it wait out the startup window rather than answer from a file that is not there yet.

**`check_launch` reads the config before `resource_overrides` is applied.** In
`agents/workflow_ops.py`, the preflight at the top of the launch runs against the transforms'
declared numbers, and the caller's per-step overrides land afterwards. So a caller who has
already capped every step to fit the host is still refused, and `METASMITH_SKIP_RESOURCE_CHECK=1`
is the only way through. The overrides do win once the run starts, confirmed from the generated
`workflow.config.nf` — this is ordering, not precedence.

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

**A one-ULP `log2` difference between the two solvers can still flip an `argmax` at a
near-tie.** The policy this entry used to ask for is now set: `SOLVER_VALUE_TOLERANCE_ULP` in
`models/solver_rng.py` states that the draw stream is exact and derived values carry libm's last
digit, and the differential test enforces exactly that. Measured against a staged binary that
post-dates the last change to `src/workflow_solver/`, so the stale-binary caveat this entry used
to carry does not apply: 8 of 36,290 values differ across the five seeds, all `log2`, all 1 ULP,
zero draw-count mismatches. What stays open is the consequence, not the divergence — nothing
establishes how near a tie has to be for that last digit to change a plan, and the corpus has
never been searched for one.

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

**A corrupt product is promoted into the cache and served as a hit.** A shard's manifest
attests that a file exists, never that it is whole. Measured on fir: metaSPAdes exited zero,
`reformat.sh` reported 86,744 records and 81,336,020 bases written, and the product on disk held
505,012 records and 59,091,548 bases, began mid-sequence, and carried 251 lines with embedded
nulls. The first null byte sits at offset 1,236,201, which is exactly the length of NODE_1 as
its own `.paths` product names it — a size-correct file with unflushed holes, read before the
writeback landed. The companion `.gfa` from the same task begins at `S 3` rather than a header.
The shard was promoted from the task's own work directory and survived a re-run byte-identical,
so the damage predates publishing and the copy is faithful. The transform's success predicate
was a non-emptiness test, which a file with a good first megabyte passes. The first move is to
decide what a shard attests: a record count the tool already prints costs one number and is the
only check here that would have caught it.

**Staging does not check that the agent's engine can run the library it is handed.** The
library and the agent image are version-locked by the tool-environment dispatch API, and nothing
verifies the lock. A library on the collapsed one-call API staged against a 0.22.1 agent solves
cleanly, stages cleanly, launches, submits, and then dies in every task with
`ExecutionContext.ExecWithEnv() got an unexpected keyword argument 'env'` — a Python
`TypeError` inside a container on a compute node, as far from the cause as a failure can get,
and reading as a library problem rather than an image problem. At campaign scale that is a whole
submission. `env/dispatch_scan.py` already knows both arm names; the first move is a
staging-time check that refuses at the client and names the image version required.

**An unsatisfiable requirement is reported against every target except the one that caused it.**
A driver that supplies only `resources/env` and never `resources/lib` leaves
`lib::cami_gold_standard.py` with no producer. The planner explores the whole library and then
dead-ends at unrelated targets — `ncbi::genome_name` and `sequences::background_genome` — so one
missing resource library reads as a comprehensively broken driver and sends the reader to edit
targets that were never wrong. Confidently wrong attribution costs more than no message. The
first move is to report the requirement that no transform produces, rather than the frontier the
search happened to end on.

**Publish is forced to copy when one directory is bind-mounted twice.** Metasmith reads a single
Lustre directory bound at two paths as two mounts, so it cannot hardlink and copies instead.
Measured: a cache hit on a cleaned-reads step took 45.6s rather than milliseconds, because a hit
still copies the shard's outputs into the task work directory and those outputs are gigabytes.
Reuse is near-free in compute and not free in I/O. The first move is to compare device and inode
rather than path when deciding whether two binds are the same filesystem.

**The dev overlay is a supported mechanism with nothing in the engine that populates it.**
`agent.py` renders both the `msm` wrapper and `lib/msm_bootstrap` with a conditional bind of
`$AGENT_HOME/dev/metasmith` over the image's installed package, and the bootstrap stages it
per node through a flock and a node-local tarball. It works: a 229-sample campaign ran a
pinned source tree, two API-breaking commits ahead of the image, inside the published 0.22.1
image, with the collapsed dispatch call executing on a compute node. But every user of it
hand-rolls the rsync and the tar, and the two can disagree. A tarball that does not match the
tree beside it fails open to the Lustre read that produced 93 incomplete copies out of 97. The
first move is a deploy-side verb that writes the tree and its tarball together, so the
integrity check has something that was built to satisfy it.

**A driver put on a compute node by `METASMITH_DRIVER_SLURM` finds no relay there, and the run
neither fails nor progresses.** `src/metasmith/bin/sbatch` submits through
`RemoteShell(AgentPaths.to_local_relay_coms())`, and that path is keyed on the node's own
hostname. On a compute node it looks for a relay socket named after that node while only the
login nodes' sockets exist, so every submission is dropped. Nextflow logs `Error submitting ...
Error is ignored`, the driver stays RUNNING, and nothing reports a failure -- the same silent
undispatch the resource-ceiling work was written to prevent, one level up. Measured on fir: two
single-sample calibration drivers sat submitting nothing, and both ran normally when relaunched
from a login node. The first move is to start `msm_relay` on the node inside `RenderLauncher`'s
foreground branch, before it calls `msm api run_workflow`.

**Compiling a resource library silently discards its pin.** `CompileUniqueLibrary` builds a
fresh `DataInstanceLibrary` over the directory instead of loading the one already there, so
`_pinned` is None on the new object and every guard that should stop this passes by
construction — `AddItem` and `Save` both check `is_pinned` and both see False. The write then
replaces `index.yml` and the `pinned:` block with it. Measured on 0.23.0: pinning
`resources/env` recorded 98 entries, and `metasmith build all` over the same directory exited 0
with no warning, left no `pinned:` block, and moved every member id. So the pin is futile
against exactly the churn it is prescribed for, because the rebuild that rewrites the mtimes is
also what drops the pin. The first move is for the compile to load the existing library and let
the guard fire, so a pinned library refuses the rebuild by name and the operator unpins on
purpose. Reported by the campaign, which reasoned it out from where the pinned block lives
before anyone measured it.

**A driver that overrides identity after a pipeline has imported leaves pool entries nobody
cites.** fabfos's pipelines now declare their givens through `Agent.PoolGivens`, which imports
whatever the pool lacks. A driver may then replace those identities through the `on_inputs` hook,
which is what `cyanoverse_gpr`, `scadc_gpr` and the `nostoc_*` drivers do: they keep
`pin_external_leaf_ids` so the benchmarks they record stay reproducible rather than having every
id move once. The import still happened, so the agent's pool accumulates an entry per input that
no plan ever references. Harmless and confusing. The first move is a `pool=False` on the
pipelines' `build_inputs`, so a driver that owns its identities says so before the write rather
than after it.

## Accepted risks

**An external mtime-touching event makes the next run cold, and one file is enough.** Stat
addressing bought the thing it was for — a 24 GB DIAMOND database or a 27k-file profiles tree
costs one stat instead of a full-tree hash — and the price is that a stat-keyed leaf moving
empties the whole hit set, including downstream steps that never read it. `rsync -a` preserves
mtimes, so staging does not itself re-key; exposure is any event that rewrites an mtime. `dvc
checkout` under a reference tree is one live case and **`git checkout` is the other**, because
git stamps every file it writes with the checkout time — so a commit that adds one env to
`resources/env/` re-keys every member of that library and moves the plan key of every plan that
requires an `env::` type. Measured on 0.23.0 in one directory: touching every file in
`resources/env` moved all 12 shipped templates' plan keys with the plan shape identical and only
the givens differing, while adding an unreferenced type to a shipped `.yml` and recompiling moved
none of them. `msm data pin` is the prescribed mitigation because `restat_leaf_ids` skips a
pinned library by design, **but it does not survive the rebuild** — see the entry below, so
today there is no mitigation for the git-checkout case. **CAUTION** Comparing two copies of a
library at different paths proves nothing here: a
stat-addressed id folds the absolute path, so a copied tree re-keys wholesale for a reason that
has nothing to do with the change under test.

**Closed for givens at 0.23.0, and still open everywhere else.** A given cited from a pool
carries an assigned identity, so a `dvc checkout` under a reference tree no longer moves it and
the run that follows is warm. What remains stat-keyed is a transform's own outputs and a library
the agent staged, where `restat_leaf_ids` still runs — and pinning a library is still the
mitigation there, because it skips pinned libraries by design.

**A mutable container tag can produce a false cache hit.** A container's leaf id addresses the
docker URL string, not the resolved image digest, so a pinned tag busts the cache on a version
bump and `:latest` does not. Inherent to tag-addressing; mitigated by the convention of
pinning biocontainer tags.

**A crash between a task's promote and the driver's `record_run` leaves a shard with no sqlite
row.** The rename is atomic and the probe reads the manifest, so the shard still serves. Only
`msm cache ls` and `gc` miss it, until a later run that hits it upserts the row. An orphaned
`<key>.<host>.<pid>.tmp` from a killed task is never reclaimed: nothing sweeps them, correctly,
since a dead writer on another host cannot be distinguished from a live one.

**A hit costs one helper subprocess per batch and one local task per hit batch.** The helper
(`python -m metasmith.caching.invocation`) takes about 0.07 s per call in the agent environment,
flat from 1 to 200 members, so a 200-batch step pays about 14 s of interpreter start-up on the
head node. Measured on this host only, not on a cluster submit node.

**A DVC-pinned artifact whose objects leave every cache is unrecoverable.** The live instance is
`data/fabfos/runs/aska/gpr`: no remote is configured, the pinned generation exists nowhere but a
shared local cache that no longer holds it, and the files on disk are hardlinks into that cache —
so `dvc checkout --force` would delete them with nothing to restore. The rule that manages this
is under *When a build artifact may be DVC-pinned* in `docs/metasmith/architecture.md`.

**A database transform fetches and indexes under one cache key, so an indexing failure discards
the archive.** `FetchCommand` makes a *retry in the same work directory* free — the transfer lands
on `<dest>.part` and is promoted only when it is whole — but nextflow gives a retry a fresh work
directory by default, so that only helps a re-run that reuses the directory. The durable fix is to
split each downloader into a fetch transform and an index transform, so the fetch is a cache entry
of its own. That is a library-shape change: `downloadUniRef50DB`, `downloadKofamDB` and
`downloadInterProScanDB` each grow an intermediate `ref::` type, and every plan that names them
re-solves.

## Deliberately rejected

**Deep-copying index value lists.** The lists are shared by reference across the DAG and the
no-writer rule is enforced only at the top-level map, but every production write was audited
and none mutates a value list. Copying changes the rendered index, which feeds
`file_instance_id`, which would orphan every existing cache shard. Reach for it the first time
a value-list mutation actually appears.

**A close-time "this stream contributed nothing" assertion.** A stream that delivers no items
is the legitimate optional-branch case, so asserting there fails runs that are behaving
correctly. The per-item guard catches the reported case earlier anyway.

## Validation gaps

**No transform body has been run under the MAMBA runtime since the execution arms were
collapsed.** Every check in curation round 6 was under DOCKER. The collapse means one command
now serves both routes, and `_ExecInEnv` was already the sole executor for both arms, so the
change is small -- but "small" is what the arms claimed too, and an arm that has never been run
answers wrongly. `dev/libraries.sh --create-envs` builds the conda environments to run one
against.

**The caching and reentrancy path has never been run live through the sockeye relay.** The
cross-host proof was assembled from separate hosts showing cache-key identity, not from one
relayed run.

**Deploy-and-run end to end on the HPC hosts was red at the last check** and has not been
re-verified since the bind failure was made fail-fast.

**No test stages one data library under two task keys**, in either the virtual runtime or the
`-stub` docker lane. 0.22.1 stopped a staged leaf's id depending on the task key, so this is a
regression guard rather than a way to see an open bug -- and the defect it would guard needed a
93-minute real run to find, twice. The same blind spot hid a record-path bug: the docker lane
promotes from the host, so a container-side path never enters a cache record.

**Closed at 0.22.0: the published artifacts are now driven as a user receives them.** The
release guards still only inspect the artifact as the builder sees it — `-ud` reads the locally
built image, `-uc` installs from `file://conda_build` — so the consumer check is a separate pass
and stays manual: `mamba create -c hallamlab -c bioconda -c conda-forge metasmith=<version>` from
outside every worktree with `env -u PYTHONPATH`, then `msm --help`, a rust solver report, a
`clone_stdlib` into an empty project and every shipped template solved; and the same drive inside
the image after `docker rmi` and a fresh `docker pull`, so the registry copy is what runs. Both
lanes were green for 0.22.0 (11/11 templates, `backend=rust`, library stamp `0.22.0+53d5540`
identical in both artifacts). Nothing automates this, so it is a gap again the moment a release
ships without somebody running it.

That pass proves the artifacts **plan**; it runs no workflow. The separate question of whether a
release *executes* correctly is answered by driving the annotation trio at two then three
samples: `/home/tony/msm.gate022/GATE.md` for 0.22.0, which failed it, and
`/home/tony/msm.gate0221/GATE.md` for 0.22.1, which passes. Worth repeating per release, since
planning green and running green are different claims and only the second one has found
anything.
