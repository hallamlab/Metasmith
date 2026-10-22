# Open followups

## What goes in this file

The punch list: bugs that are known and unfixed, risks that were accepted knowingly, and
designs that were rejected on purpose. One entry per item, naming what is wrong and the first
move. Nothing that is finished belongs here — git holds that — and nothing that a test or a
module already states belongs here either.

## Open bugs

**A run parameter that changes what a step produces does not change its cache key.** A member
key folds the transform key, the protocol source and the ids the member consumed. It folds no
value from `context.params`. So a download narrowed or redirected by a parameter is cached under
the same key as the default: `ganon2_organism_groups=archaea` caches an archaea-only index that
every later run on that agent home is served, and a `*_release` or `*_version` parameter is
served whichever release was fetched first. The dispatch scan already lists the keys each
transform reads (`EnvScan.param_keys`). The first move is to fold the values of those keys into
the step's signature, which moves the keys of only the transforms that read a parameter.

**A local task that reaches its wall can abort the whole run.** On fir, `downloadDramDB`
finished at 6 h 00 m, exactly its wall, and Nextflow's local executor enforced the wall in the
same poll. The kill found no process, Nextflow raised `IllegalThreadStateException: process
hasn't exited`, and the session aborted before the build step started. The fetched files had
already moved out of the staging dir into the unpromoted work dir, so a plain rerun fetches them
again. The race is in Nextflow. Give a local fetch a wall far above its measured time, as
`downloadDramDB`'s 24 h does. The first move is to reproduce the race on the pinned Nextflow and
report it upstream.

**A cancelled local task can keep running after the cancel.** On fir, a ganon2 fetch cancelled
at 425 of 1,582 genomes reported that its driver exited. Its protocol still went on to fetch
the taxdump files, which the next attempt then found already present. A relay job carries the
run token, so `workflow reap` should reach it. The first move is to find which process survived
— the relay job, a `LocalShell` child or `genome_updater` itself — and whether the token scan
sees it.

**A staging dir outlives its key, and nothing removes it.** `context.StagingDir()` sits under
`task_cache/staging/<transform>/<member token>`, and neither `gc` nor anything else reads that
tree. An edit to a fetch transform's source moves its member key, so the next run starts an
empty staging dir and the old one keeps everything it fetched. On fir a DRAM fix did this to
25 GB, recovered by hardlinking the fetched files into the new dir by hand. vConTACT3 also leaves
its intermediates there after it succeeds. The first move is a `cache` verb that lists the
staging dirs no current key names, with their sizes, for an operator to remove.

**Nothing checks inode headroom before a setup run, and a quota failure is not named.** A
database costs more inodes than its bytes suggest: InterProScan's unpack is about 42,000 files.
On fir a study's setup and run reached the project's hard limit of one million inodes. The Slurm
task that hit it reported `terminated for an unknown reason`, its retry could not create a work
dir, and the driver aborted. `lfs quota` lagged the limit, so a poller reading 996,928 never saw
the stop. The first move is a preflight that reads the inode quota and refuses a setup run whose
databases would not fit, with each database's file count recorded beside its size.

**A cache hit costs inodes, and nothing yet says which part of it.** Measured on fir: a resumed
run's 23 hits added about 31,000 inodes in 15 minutes. A twin hardlinks the files of a product,
and a hardlink costs no inode. A twin's directories, its work dir and any copy made by the
`cache hit copied, not linked` fallback do cost inodes. The first move is to count one twin's
work dir by entry type and read its stderr for that fallback line.

**A product damaged before promotion is promoted and served as a hit.** A hit needs every
regular file at the size its manifest recorded, which catches a file truncated after promotion
and nothing earlier. On fir, metaSPAdes exited zero and its contigs file held 505,012 records
where `reformat.sh` reported writing 86,744. The first null byte sat at offset 1,236,201, the
length of NODE_1: a size-correct file with unflushed holes, read before the writeback landed.
The manifest records such a file at its damaged size, and a directory product has no size check
at all. The first move is to decide what a shard attests beyond size. A record count the tool
already prints costs one number and is the only check here that would have caught it.

**A transform that folds a batch into one product caches the first member only.** The cache
keys each batch member alone, and the transform emits its one product from member 0. So the
other members record nothing, and the next run re-runs them. On fir, `gtdbtk_de_novo` re-ran
`de_novo_wf` on 99 of 100 bins beside the cached tree over all 100. It now gathers its bins with
`group_by` on the study. `kbase/build_tree/gtdbtk_tree.py` still folds a batch of 200 genomes
into one tree. The first move is the same change there, or a compile-time refusal of a batch
transform whose product is not per member.

**A full `/tmp` on a login node silences the relay, and the pool read hides the cause.** The
relay keeps its workspace in the node's `/tmp`. On fir login2, another user filled the 189 GB
tmpfs. The watcher then failed to save its status and stopped answering. `ReadPool` failed now
and then with "the agent returned no JSON", and that error shows the agent's stdout only, so the
stderr that named the cause was lost. The first move is to carry the stderr tail into that
error. The second is a relay that reports a failed state write instead of going silent.

**A one-ULP `log2` difference between the two solvers can still flip an `argmax` at a
near-tie.** The policy this entry used to ask for is now set: `SOLVER_VALUE_TOLERANCE_ULP` in
`models/solver_rng.py` states that the draw stream is exact and derived values carry libm's last
digit, and the differential test enforces exactly that. Measured against a staged binary that
post-dates the last change to `src/workflow_solver/`, so the stale-binary caveat this entry used
to carry does not apply: 8 of 36,290 values differ across the five seeds, all `log2`, all 1 ULP,
zero draw-count mismatches. What stays open is the consequence, not the divergence — nothing
establishes how near a tie has to be for that last digit to change a plan, and the corpus has
never been searched for one.

**The library suite's plan-only tests fail, and no gate runs them.** Tests such as
`TestLogisticsWorkflowGeneration` register their givens with `AddValue` and no pool import, so
planning refuses them with `GivenNotImportedError`. That has been true since 0.23.0 made a
given's identity an import. Port the fixtures to `PoolGivens`. Then run the suite's execution
tests once by name, since its `slow` and `network` marks now skip them by default.

**The research viromics driver no longer solves.**
`research/viromics/viromics_survey_from_paired_reads.py` targets `annotation::gpr_table`, whose
producer `gpr_4lane` requires `ref::lane_calibration` since the fabfos/dev merge. The driver
supplies no such given. Add it as a deferred input, as the four affected templates did.

**fabfos's GPR schema test fails on three committed tables.**
`test_the_schema_covers_every_gpr_table_in_the_tree` fails on the fang, fuhrer_clones and keio
GPR tables, which are pinned on the layout from before the schema. fabfos/dev fails it
identically, so the merge did not cause it. Re-run the research mappers that wrote those tables.
Do not edit the pins.

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

**A driver that overrides identity after a pipeline has imported leaves pool entries nobody
cites.** fabfos's pipelines declare their givens through `Agent.PoolGivens`, which imports
whatever the pool lacks. The `cyanoverse_gpr`, `scadc_gpr` and `nostoc_*` drivers then replace
those identities through the `on_inputs` hook and keep `pin_external_leaf_ids`, so the
benchmarks they record stay reproducible. The import still happened, so the pool gains an entry
per input that no plan references. The entries are harmless. A `pool=False` on `build_inputs` is
not a drop-in fix, because a build outside the pool cannot reproduce the agent's resolve of a
remote path, so a pinned benchmark id could move.

**Two builds of one version share a home's relay.** `Agent.Deploy` re-extracts a home relay only
when `msm_relay --version` differs from the engine's release version. `src/bash_relay/dev.sh`
bakes `version.txt` into the relay, so every build of 0.25.0 prints `0.25.0`. On fir a home
redeployed with 0.25.0+bb88d9e kept its 0.25.0+28ecc51 relay, which lacks the per-home
workspace. Pass `assertive=True` to `Deploy` to force the extraction. The first move is to
compare the relay's checksum with the image's copy instead of its version string.

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
pinned library by design, and a compile refuses a pinned library by name rather than dropping
the pin.

**CAUTION** A stat-addressed id folds the absolute path, so a second checkout of one commit
shares no cache with the first. Every `env::` leaf moves, and every member key moves with it.
On fir a second checkout made only to test one transform re-ran `downloadVirsorter2DB` and
re-keyed every step of the plan. Run a one-transform test from the checkout that ran the rest.
Comparing two copies of a library at different paths proves nothing about the change under test.

**Closed for givens at 0.23.0, and still open everywhere else.** A given cited from a pool
carries an assigned identity, so a `dvc checkout` under a reference tree no longer moves it and
the run that follows is warm. What remains stat-keyed is a transform's own outputs and a library
the agent staged, where `restat_leaf_ids` still runs — and pinning a library is still the
mitigation there, because it skips pinned libraries by design. A given whose bytes change under
its pool entry keeps its identity by design. `metasmith cache check` is how to notice.

**A mutable container tag can produce a false cache hit.** A container's leaf id addresses the
docker URL string, not the resolved image digest, so a pinned tag busts the cache on a version
bump and `:latest` does not. Inherent to tag-addressing; mitigated by the convention of
pinning biocontainer tags.

**A crash between a task's promote and the driver's `record_run` leaves a shard with no sqlite
row.** The rename is atomic and the probe reads the manifest, so the shard still serves. Only
`msm cache ls` and `gc` miss it, until a later run that hits it upserts the row. An orphaned
`<key>.<host>.<pid>.tmp` from a killed task is never reclaimed: nothing sweeps them, correctly,
since a dead writer on another host cannot be distinguished from a live one. A `record_run` that
cannot open the store, as on a full quota, does this to every member of the run and writes no
trace events either. Replaying `record_run` against the run's workspace restores both.

**A hit costs one helper subprocess per batch and one local task per hit batch.** The helper
(`python -m metasmith.caching.invocation`) takes about 0.07 s per call in the agent environment,
flat from 1 to 200 members, so a 200-batch step pays about 14 s of interpreter start-up on the
head node. Measured on this host only, not on a cluster submit node.

**A DVC-pinned artifact whose objects leave every cache is unrecoverable.** The live instance is
`data/fabfos/runs/aska/gpr`: no remote is configured, the pinned generation exists nowhere but a
shared local cache that no longer holds it, and the files on disk are hardlinks into that cache —
so `dvc checkout --force` would delete them with nothing to restore. The rule that manages this
is under *When a build artifact may be DVC-pinned* in `docs/metasmith/architecture.md`.

**Some small unpacks still run on the login node.** The large fetches are split into a `local`
fetch and a compute-node build. Smaller ones still unpack where they fetch: MetaPhlAn's tar and
`bunzip2` of about 20 GB is the largest, and SILVA, BacMet, VFDB, CARD, PathoFact, PredicTF,
CheckV, geNomad and VIBRANT's setup are the rest. ganon2's md5 check stays in its fetch on
purpose, because `genome_updater` fetches a bad file again, which only a networked step can do.
Split the next one that a cluster reaps.

## Deliberately rejected

**Deep-copying index value lists.** The lists are shared by reference across the DAG and the
no-writer rule is enforced only at the top-level map, but every production write was audited
and none mutates a value list. Copying changes the rendered index, which feeds
`file_instance_id`, which would orphan every existing cache shard. Reach for it the first time
a value-list mutation actually appears.

**A close-time "this stream contributed nothing" assertion.** A stream that delivers no items
is the legitimate optional-branch case, so asserting there fails runs that are behaving
correctly. The per-item guard catches the reported case earlier anyway.

**A solver prune for a transform that feeds itself.** `seqkit_filter_contigs` and `polypolish`
each produce a subtype of the type they require, so each can consume its own product. A prune
that refused such a re-application was built and changed no plan among the 12 templates, the 11
parity analyses or the solver corpus. It would add an untested incompleteness to the search, so
it is not landed. `tests/metasmith_libraries/test_self_loop_ladder.py` pins the plan that once
showed the ladder.

## Validation gaps

**iPHoP's `add_to_db` and `predict` have not run through the planner on the full database.**
Their exact steps ran by hand on fir against Jun25_rw with 42 CAMI marine MAGs. No host was
dropped, and 240 of 794 viruses got a host among the added MAGs. The planner's run of the same
steps failed on a relay workspace that another agent home shared. The per-home workspace in
0.25.0 fixes that. The host database is GTDB r226 and the de novo trees are r232. The graft
places each MAG beside its nearest base leaf in the r232 tree, and nothing checks that the two
releases agree.

**Most generated conda recipes have never built an env that ran its tool.**
`gen_tool_envs.py` writes one recipe per tool from its biocontainer tag. Of six run under MAMBA
for 0.25.0, three failed: one recipe lived outside the shipped tree, `instrain` solved to Python
3.6, and `antismash` solved to a MEME that `cassis` refuses. Each of the three is now
hand-written. The first move is a smoke per recipe: build the env and run the tool's
`--version` or an import.

**The caching and reentrancy path has never been run live through the sockeye relay.** The
cross-host proof was assembled from separate hosts showing cache-key identity, not from one
relayed run.

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
