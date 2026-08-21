# metasmith — the engine

## What goes in this file

The architectural brief for someone about to *change* metasmith: the concepts the code is
organised around, the invariants spanning more than one file, and the traps whose evidence
lives somewhere unreadable from here — a cluster, a scheduler, an upstream bug.

The test for a paragraph is: **would reading the code have told me this?** If yes, it does not
belong. Signatures, flag lists and command trees are recoverable by reading; why a shape was
chosen, which two files must agree, and what breaks silently when they stop agreeing are not.
Prefer the rule to the instance. Keep what a postmortem proved, drop the story.

Authoring a transform is the standard library's subject, not this file's — see
`src/metasmith_libraries/AGENTS.md`.

## What it is

A workflow generation system for bioinformatics. You declare what data you have, what result
you want, and which tools exist; the planner searches backwards for a chain connecting them,
compiles it to Nextflow, and runs it. Nobody writes Nextflow. Adding a tool means one Python
file declaring inputs, outputs and how to run it, after which the planner uses it wherever it
helps.

## The type system

The hardest part of the repo; everything else is downstream. Read
`src/metasmith/models/solver.py` alongside this section.

**A type is a set of strings.** `Node` — base of both `Endpoint` (a free-floating type) and
`Dependency` (a slot on a transform) — is a set of property strings plus a set of parent
Nodes. No class hierarchy, no name on the object, no runtime type beyond that. A keyed YAML
property becomes a compact JSON string in the set; an unkeyed one is the bare string.
Everything the type system does is set algebra over those strings.

`extends` resolves **at parse time by union**, not as a link: a subtype's property set is its
own plus every ancestor's, flattened. Two consequences bite. A type may only extend one
defined *earlier in the same file*, since resolution walks the YAML in order. And once loaded
there is no inheritance to inspect — "is X a subtype of Y" is a subset test and nothing more.

**The name is not part of the type.** `DataTypeLibrary` is a `{name: Endpoint}` map holding no
back-reference, which is why `DataInstance` carries `dtype_name` separately. Equality and hash
are over `Signature()`, the hash of the sorted property set plus sorted parent keys. So two
differently-named types with identical properties are **one node** — they satisfy each other's
requirements and collapse in any set or dict, which is the most common way a library goes
subtly wrong. Give near-twins a distinguishing property rather than trusting names. Likewise
two slots of one type on a transform are `==`, so any code resolving a declared `parents={...}`
back to a slot must match by **object identity first** or it answers with whichever came first.

**More properties means more specific.** `x.IsA(y)` is `y.properties ⊆ x.properties`, read as
"x can be used in place of y". A subtype satisfies a supertype's requirement; never the
reverse. That asymmetry is load-bearing in every consumer, and reversing it yields a planner
that appears to work while building wrong chains. It is also why a requirement should be
written as loosely as the tool genuinely tolerates. `ext` is the one magic property —
`GetPreferredFileExtension` scrapes it back out to name output files.

**Lineage is part of identity.** A Node's parents fold into its `Signature()`, so a type with
lineage is a different node from the same type without. That is the mechanism behind every
"which one did this come from" feature. Two ordering rules follow, and they are one rule twice:
a transform's requirement may only name parents already added, and a target may only name
targets declared before it — **declaration order is the reference space** in both.

Lineage must be acyclic, and not for tidiness: `AsSamples` walks up to a node's ancestors and
back down to their descendants, so over a cycle one mask becomes the whole library.
`ops.data._assert_acyclic` refuses it wherever parents are set. Note `show_item_lineage`
reports the **transitive closure**, not the declared parents — a consumer that does not
collapse it again offers a grandparent whose removal silently reverts on the next load.

**Products come in groups.** `Transform.produces` is a `list[list[Dependency]]`, advanced by
`NewProductGroup()`, and the call is **overloaded with opposite meanings**: for sample
alternatives it branches into separate timelines (A *or* B), for a multi-output transform the
products stay in one timeline (A *and* B). The solver discriminates on whether the
application's transform is the given one. Getting it wrong stops a multi-output tool's products
co-existing.

**What the solver does.** It works backwards from the target carrying `SolverState` of what it
has and which transforms remain candidates. An `Application` is one use of one transform — a
`{Dependency: Endpoint}` map of what filled each slot and what it produced — signed by the
transform key paired with each slot's filling endpoint. One transform on different inputs is a
different application; on the same inputs it dedupes.

When no complete plan exists, `WorkflowPlan.hints` carries structured `PlanHint` records with a
reverse-BFS chain, candidate transforms and near-misses ranked by property-Jaccard. Every
consumer is expected to surface them; a bare "no plan" is not an acceptable failure.
`too_general` leads because it is usually the whole answer: the direction rule above is the
most confusing thing the planner does, since registering a supertype of what a tool wants sends
the walk *past* the intended demand to dead-end hops later at something nobody mentioned. The
hint names both halves of the fix — the types that would satisfy the demand while still
describing the file, and any declared parent nothing registered could stand in for, which no
retyping supplies.

**Types are compiled once per library.** A namespace is the source YAML's filename stem, and a
duplicate namespace across directories raises. Each transform library carries its **own**
compiled `_metadata/types/`, so a new type must land in all of them (`metasmith build`).
Skipped, a plan becomes unreachable only from certain libraries — which reads like a solver bug
and is not.

**A library carries the types its own transforms declare, and `PruneTypes` is what establishes
that.** `Load` reads every file under `_metadata/types/` and takes each as a namespace, so the
prune only means something if it unlinks — a file left behind is a namespace nobody declared,
silently in force. Two consequences follow from the narrowing. A namespace present in a library
no longer implies the type is: resolve a name by asking for the *type*, never by finding the
first library holding its namespace. And a transform must reach every type it uses through
`AddRequirement`/`AddProduct` — a bare `GetType` the contract never mentions is invisible to the
prune and is gone on the next build.

## Data and transforms

**A `DataInstance`'s `instance_id` derives from path, dtype name and parent library — never
file bytes.** Bioinformatic inputs reach hundreds of GB, so content hashing is deliberately off
the table at this level. The consequence is that changing a path or a type re-registers the row
and costs cache reuse, and two different files at one path collide; the escape is an explicit
user-driven fork, not an automatic fix. The id is *stored* per path rather than re-derived, so
anything moving a manifest entry must move its `instance_meta` with it — the fallback derives
from the library key, a hash of the whole manifest, which would make one row's identity a
function of every other row's. The id survives `WithDType()` retyping and `Pack()`/`Unpack()`,
which is what makes `Load()` + `Trace()` the correct way to map results back to inputs, never
filename or work-directory parsing.

A transform is one Python file in three parts — contract, protocol, instance. Per-slot lineage
(`AddRequirement(..., parents={...})`) is why a transform's requirements must be indexed by
slot rather than as a set of types: bbduk does not want three files, it wants the reads
belonging to *this* metadata.

**A transform never learns which runtime it is on, and does not need to.** It declares an arm
per world (`ifContainerDo` / `ifVirtualEnvDo`, either omissible) and the `env` package owns
every per-runtime difference: bind dialect, whether a container boundary exists, GPU flags.
Code branching on the runtime is a bug — declare the need instead. That holds inside `env` too:
`MakeBindsParam` is the only place a mount is spelled, and rendered shell templates must
interpolate it rather than hand-write a flag, since a literal reads correctly under the runtime
its author had in mind and is silently wrong under the other.
`test_env_deploy_scripts.py::TestBindDialectPurity` holds that line by scanning each rendered
script for the other dialect's tokens. `ifContainerDo(args=[...])` appends verbatim runtime
flags just before the image, so a flag passed there beats the framework default of the same
name — but the dialect is the caller's problem, and mounts go through the typed `binds=`.

The protocol sees three path views (`.local`, `.container`, `.external`), identical under a
runtime with no container boundary.

**Write the product AT the output path, never beside it under a friendlier name.** The engine
names every output and the generated process collects exactly that glob, so a file written next
to it is invisible: the step does its work, reports success internally, and the task dies on a
failed `ls`. Nothing downstream can read a host or a sample out of those names, so attribution
must come from file *content*.

**A run publishes its targets and nothing else** (`WorkflowPlan.publish_intermediates`, off by
default). Intermediates stay in the work dir and the cache store, so what a collect copies back is
what was asked for — and a run that dies before its last step leaves an empty results folder, where
the per-step logs are the only record. `CollectResults` still registers an unpublished file: the
manifest entry is what lets a target name it as an ancestor, and it simply has no file behind it.

## Planning

**`group_by` and `batch_size` are two different axes and five files must agree on which is
which.** `group_by` partitions a step's inputs by what arrives on the named requirement's channel — one
item is one key, one key yields one task member holding every item matched to it, which is
what turns a fan-out back into a fan-in. `batch_size` folds N whole keys into one task and
never shards within a key, so a step runs `ceil(<keys> / batch_size)` tasks. Keys are items, not
plan instances: the plan carries one produce instance per dependency however many samples flow
through it, so a step grouped on another step's output has as many keys as that step made files.
`plan_oracle`, `cache_decisions`, `virtual_runtime`, the `TransformInstance` field and
`Orchestrator.groovy::group` each restate that count independently; when the runtime drifted
onto the other reading it did not fail — it silently handed a collecting transform one item,
and ppanggolin clustered a single genome. Both axes reach the protocol through
`context.AsBatch()`.

**The Dependency is the stable key, and every lookup that spans a stage uses it.** An Endpoint
mutates while the solver merges timelines and a DataInstance's hash changes when its dtype is
remapped, so either one keys a map that is correct when written and wrong when read; a
Dependency is created once with its Transform and never changes.

**A plan slot holds an archetype, not the runtime multiplicity.** A collecting step's
`step.dependency_map[dep]` carries *one* instance standing for however many the fan-out above
produces; only `group_by_instances` counts keys. Anything reading a slot's length as "how many
items this key receives" is wrong in the direction that hurts — it tells the runtime a key is
already whole and the group ships with one item. `grouping.expected_per_key` therefore answers
`None` for a one-instance slot, degrading everything downstream to the channel-close flush,
which is always safe.

**A cache hit must put the same lineage on the wire that a real run does.** The synthetic
channel replays each shard file with the index its producing task carried, captured at promote
time into the manifest's `index` field — compile time cannot reconstruct it, since which inputs
an output descends from is decided inside the task. A shard that cannot supply an index for
every matched file is demoted to a miss rather than replayed, and an index present but **empty**
counts as absent on both sides of that exchange: it renders to Groovy's `[:]`, which `_post`
stamps the produced key onto, so the replayed file reaches a downstream `o.group` carrying
exactly one key — its own — and is refused.

**That index is captured from a file beside the outputs, which survives only if it is copied
back.** Each task appends its lineage to `.command.metadata` in the current directory and
`promote` scans the work tree for it. Under `scratch` the current directory is node-local and
Nextflow copies back only the declared outputs plus `.command.{out,err,trace}` — the metadata
file is neither, so it dies with the scratch directory and every shard of that run is stored
with no ancestry. Twelve field runs produced 88 tasks and zero metadata files. The generated
script therefore resolves its own work directory from `$0` — absolute because of how the
launcher invokes it, and the only handle there is, since `task.workDir` is null at render time
and `NXF_TASK_WORKDIR` is exported *after* the chdir into scratch — and copies the file there
before the step runs. That copy is deliberately non-fatal: a miss costs a demoted shard and a
recompute, never a wrong result.

**A task's index arrives shared across every one of its output channels and must never be
written to.** A process declaring N output tuples binds the *same* map object to all N, and
each output channel is a separate dataflow operator on its own thread, so an in-place edit is N
threads writing one unsynchronised map — which does not fail loudly, it yields an emptied copy
and a product that reaches the next join with no ancestry at all. The sharing is Nextflow's,
decided before anything here loads, and cannot be fixed from this side; the writer can be.
`stripReserved` filters into a fresh map rather than deleting in place, and `_post`'s
`[:] + index` is where each stream stops sharing — so nothing inserted between the process call
and `_debatch` may write to an index, a safety that is positional rather than structural.
`test_the_streams_own_their_index_only_after_post` pins both halves on `identityHashCode`, so a
Nextflow that stops sharing announces itself there instead of leaving this paragraph quietly
false. The copy is shallow on purpose: value lists stay shared by reference across descendant
indexes, and no production path mutates one.

**A stream declared `DESCENDANT_OF_BY` must carry the by-key, and `group()` raises when one does
not.** Absent and empty-list are the same defect; the empty list is the worse one, because the
loop over it iterates zero times and used to leave no trace at all. The alternative to raising is
invisible: a dropped item makes the join emit nothing, an empty channel is not an error in
Nextflow, and the DAG simply ends early with every submitted task at exit 0 — a task that is
never created cannot fail, so no `errorStrategy` and no failure count can see it. Two production
runs lost days to exactly that, one truncating a nine-step workflow after seven, and in both the
only record was a dispatch-log row nothing reads. The producer-side guards each close one route
(the cache-hit index above, `promote._collect_output_indexes` for a replayed output); this is the
one that does not have to be re-derived for the next producer. `SIBLING` only logs, because
`_firstSharedAncestor` picks an arbitrary member of the ancestor intersection and an item may
legitimately relate through a different one.

**Two grouped slots are paired by ancestry, never by position.** A collecting step receives
each slot as an independently accumulated, independently deduped list in task-arrival order, so
`InputGroup(a)[i]` and `InputGroup(b)[i]` are related only by luck, and the lists can differ in
length when a producer is dropped. `context.SourceOf(path, dep)` answers which item of `dep` a
given item descends from. It returns `None` when uncaptured or unrelated and **raises** on more
than one match: a produced file's index is its producing *task's*, so the answer is exact only
when that task had one ancestor at that slot, and picking the first would be the mislabelling
this exists to remove. Two slots of one dtype share a channel and are refused outright, because
the wire genuinely cannot tell them apart — `Dependency.key` is a function of the property set,
so they have already collapsed in every dict by the time anything asks.

Three things make that work and are easy to break. The per-item maps ride the index under a
reserved key so channel tuple arity never changes; they are stripped where the batch is undone,
or they propagate into every descendant index forever; and the slot→channel mapping is
**emitted by the compiler, never re-derived**, since the name comes from `get_archetype` whose
merge decisions are compile-time state bootstrap does not have. A codegen gate drops parent-refs
from *every* row of an input unless all of them have an in-workflow parent, so one unparented
row silently zeroes provenance for the whole set — it warns, and that warning is the first thing
to check when `SourceOf` returns `None` for everything.

**The wire carries one lineage map per batch member, not one per task.** `LinPayload.entries`
is a list mirroring the indexes `Orchestrator._collateBatch` builds; collapsing it to a single
map costs every member after the first, silently, with the unread files still staged.
`LIN_PAYLOAD_VERSION` and the Groovy emitter must move together — the emitter interpolates the
constant rather than restating it, because as two literals they desynced and failed every
containerized task while the fast suite stayed green.

**Everything the solver needs before it runs is one serializable object.** `agents/spec.py`'s
`Spec` holds the input library, sample type, targets, transform and resource libraries and
shared input paths, and `Spec.Solve()` is the only door through to `WorkflowPlan.Generate`.
Two copies of those twenty lines existed and had already drifted on `shared_input_paths` and on
positional target lineage. Collapsing them gives a plan a *before* representation as well as an
after: the GUI's `request.yml` is the store envelope merged with a packed Spec, and a template
is nothing but a Spec whose input paths are deferred.

**`DEFERRED` is a path that is not known yet** (`models/paths.py`) — it plans, and refuses at
stage, which is what lets a template ship without its author's absolute paths. Three properties
are load-bearing. It is **absolute**, under a reserved root, because `ops.data.repoint_item`
reads `is_absolute()` as "not library-owned". It is **minted once and persisted**, because
identity derives from path and re-minting on load would give the same spec a different task key
every time. And it is a value, not a flag — nothing downstream tests for a sentinel.

**A sample type branches a plan; it is not a precondition for one.**
`plan_workflow(sample_type=None)` plans the library as it stands, one sample holding
everything; naming a type splits it into one run per item of that type. The GUI always passes
`None`, sheet or no sheet, so a non-null value only comes from a request written directly.
A sample's mask is one index item's lineage, so both directions of the shape matter: an index
item with a *parent* puts that parent's whole subtree in every mask and collapses all samples
into one view, while an item beside the index that nothing links to lands in no mask at all and
the planner never sees it though it is still staged. `ops.samples.validate` refuses the first;
`shared_input_paths` is the way out of the second.

**Planning is not reentrant, and the lock lives at the mutation.** `TransformInstance.Load`
imports by bare module name, mutates `sys.path`, calls `importlib.reload` and returns through a
*class* attribute — all process-global — so it takes a class-level lock and inserts/removes its
own `sys.path` entry rather than snapshotting the list. Callers still serialise around it, but
one import's correctness no longer depends on every caller remembering. Two failure modes taught
that: the loud one is `spec not found for the module`, and the quiet one is a
snapshot-and-restore putting a concurrent load's entry back **permanently** — nothing fails,
every later import scans more directories, and a day-old server plans an order of magnitude
slower. Pinned by
`tests/metasmith/unit/test_transform_load_is_serialised.py`.

Every transform file opens with `ResolveParentLibrary(__file__)`, so a library's load re-enters
it once per transform. Without the per-root cache behind that call, loading the standard library
re-read the manifest once per transform, which cost several times what planning did. The cache is keyed on a
`scandir` signature, so a transform edited between two plans in one process is not served stale.

## Execution

**`RunWorkflow` is fire-and-forget.** The agent shell launches Nextflow under `nohup … &` and
returns when the launch script exits. Any script going straight to `GetResultSource` races past
the run and crashes on a missing results directory. The contract is a sentinel line in the run's
agent log, which `metasmith workflow wait` blocks on. Poll for it; do not sleep and hope.

**A run is a process group and a token.** `start.sh` backgrounds the driver under `set -m`, so
the whole run descends from one process group, and exports `METASMITH_RUN=<task_key>.<timestamp>`,
which every descendant inherits, docker tool containers carry as the `msm.run` label and
apptainer's `--cleanenv` has put back explicitly. Both are written beside `PID.lock` as
`RUN.pgid` and `RUN.token`. The group is the cheap handle; the token is the backstop, because it
survives a `setsid` out of the group and cannot be shed. `metasmith workflow ps` reports what a
run still has running on its agent and `workflow reap` reclaims it.

**Cancel is a ladder, and reports what it did not achieve.** Removing `PID.lock` makes the
driver's supervisor TERM Nextflow's *own* process group — a second group, so the driver survives
to snapshot logs and promote the cache — and that TERM is given a real window, because Nextflow's
shutdown hook is what reaches `bin/scancel` for grid jobs. Then a group KILL, then a reap.
`CancelWorkflow` returns `{stopped, survived, rung}`; a run with survivors is recorded
`cancelling`, never `cancelled`. On SLURM the guarantee stops at Nextflow's own shutdown, which
is deliberate: a cancelled job dies with its allocation.

**`Source.Parse` must be a fixed point on its own output**, because anything storing an agent
home re-parses it on the next save. It was not: `SshSource` renders `ssh://host:path` while
`Parse` read the `:` as part of the host, so a remote home grew a colon per save until nothing
could reach it. Pinned by `tests/metasmith/unit/test_source_parse.py`.

**The remote dev overlay delivers CODE, not DEPENDENCIES.** It binds the pinned engine over the
agent image's `site-packages`, which is what lets an old base tag run a new engine — until the
engine gains a third-party import the image's env lacks. Adding `cbor2` was enough: on the older
tag every task died *after* staging, so the first sign was a queued job failing. A new dependency
in `envs/metasmith/base.yml` means the site's base tag has to move too.

**A stage sends the plan, not the library.** Each library ships as an image: its `_metadata/`
whole, plus the manifest entries the plan resolved against and every file the manifest does not
name. The manifest itself is never narrowed — the library key is a hash of it, and that key names
the staged directory, prefixes every packed `DataInstance` and appears in every step's transform
reference — so the prune is expressed as a *subtraction* of unused entries rather than a selection
of used ones. That is also what keeps it correct: `build` excludes `_`-prefixed files from the
manifest, and several of those are helper scripts their neighbours copy out by `__file__`, so a
selection would drop a runtime dependency with nothing to say so. The mask comes from
`plan.given`, the steps' transforms and the ancestor closure of both, which crosses libraries
because a parent entry names the library it lives in.

**Local transfers.** `Logistics` copies local→local in process — plain files, symlinks and trees
of those — and hands everything else to `rsync -auP`. The split is about **latency, not
throughput**: rsync's process spawn dwarfs the work itself on the metadata trees staging actually
moves, so a plan copying three of them spent most of its time waiting. Two rules
keep the fast path honest: it **surveys before it writes**, so a tree it will not claim is handed
over untouched rather than half-copied; and every file is written aside and `os.replace`d into
position, because an interrupted plain copy leaves a truncated file whose mtime is *newer* than
the source, which `-u` then skips forever.

**Bounding a hung agent is about silence, not elapsed time.** Everything metasmith does on an
agent goes through one bash subprocess returning on a marker line, so a host that stops answering
holds the calling thread for the life of the process. A large stage legitimately takes an hour and
a wedged one can fail in a minute, so the question is whether the far end is still emitting.
`LiveShell.Exec(idle_timeout=)` answers it and **raises**, naming the step, unlike the wall-clock
`timeout` which returns `None` that nobody checks. Two things make it work: the mark is taken on
the **raw read, before the line split**, because `rsync -P` redraws one line with carriage returns
and may not complete a line for minutes; and that output exists only because the shell is a
**pty** — route transfers through a plain pipe and the bound goes blind. The `curl` arm is exempt
for the inverse reason: under `--silent`, no output is not evidence of anything. The connect is
deliberately unbounded this way so someone can type a key passphrase, carrying
`-o ConnectTimeout` instead. A timed-out command still runs on the far end with a marker nobody
will claim, so dispose its shell rather than reusing it.

**Path translation** lives entirely in `src/metasmith/models/paths.py`. Never use raw
`str.replace` or a regex on a path root — those silently corrupt (`/msm_home_old_backup` is not
`/msm_home`) or misidentify; for shell-script content use `reroot_in_text`, which matches only at
segment boundaries. The container is **dual-bound**: the host scope dir lands at both `WORK_ROOT`
and `HOME_ROOT`, and Nextflow may resolve a work dir through either, so anything mapping a cwd
back to the host must check both prefixes.

**Anything codegen writes into the workflow graph must be in container coordinates**, because
channel values become the FILES manifest and are read back inside the per-step bootstrap
container, which mounts `WORK_ROOT`, `HOME_ROOT` and whatever `.command.binds` declares and
nothing else. The Nextflow head is not the reader and holds a bind the per-step container lacks,
so a host-spelled address stages fine and fails one process later, reported as a missing input.
Filesystem work at compile time stays in the host view; only the *emitted* literal goes through
`PathMap`. This holds for host-side targets too — `bin/sbatch` translates the two roots outward
before submitting. `publishDir` is the single deliberate exception.
`ContractRuntime.check_emitted_addresses` fails a test when a producer breaks this; every
`NextflowGenContext` in the suite except `tests/metasmith/cache/test_codegen.py` collapses
`external_home` onto `HOME_ROOT`, which is why the bug it pins stayed invisible so long.

**Apptainer: materialising is try-then-fall-back.** An unprivileged user cannot mount squashfs in
the kernel, so without the setuid `starter-suid` a single-file image is served by a userspace FUSE
reader apptainer spawns for itself. That works, and metasmith no longer probes the host to choose:
the old version-and-setuid probe predicted wrong in both directions. `MakeMaterialiseCommand` —
used by both the agent image and tool images, so the two cannot disagree — attempts
`apptainer pull`, then `apptainer build --mksquashfs-args "-no-fragments"`, then an unpacked
sandbox. **micb0 is why there is a third rung:** apptainer ships mksquashfs 4.7.5 in its private
libexec, which segfaults (exit 139) on a plain pull of the metasmith image, while the working 4.5
at system level is unreachable because apptainer always prepends its own libexec to the search
path. `-no-fragments` builds the same image for slightly more bytes, but only `build` accepts
mksquashfs arguments, so the retry is a different command rather than the same one with a flag. Each arm clears its own partial output first: a half-written SIF still satisfies the
existence test the run command checks. The unpacked sandbox is the genuinely FUSE-free option and by far
the most expensive, in both bytes and inodes.

`Rootfs` (`auto` | `sif` | `sandbox`) is the manual override, declared at `Agent.Deploy` for the
host's standing tendency and at `StageWorkflow` for one task's steps; precedence falls out of
absence. The stage-time override reaches *tool* images only — the agent's own image is settled at
deploy. A forced mode must be forced in **three** places or it is advisory: the fallback chain,
the already-materialised test, and the run command's choice of artifact. Missing the last two is
exactly how the old host-level override was inert — a leftover `.sandbox` satisfied an
either-test, materialising was skipped, and a "forced sif" run was quietly a sandbox run. Under
`auto` a sandbox on disk is **not** a leftover to tidy: it is the record that a pull and a build
both failed here, so it sticks until `Deploy(assertive=True)` clears the store. The store root is
one point of control, expanded on the *execution* host so fetch, build and exec agree.

**Docker materialises unconditionally.** `docker run`'s pull policy is "only if the tag is
absent", so without an explicit pull a stale or broken image already under a tag is trusted
forever with no freshness check. The pull falls back to whatever is cached locally only when the
registry is unreachable, so a never-pushed dev image still works. This is not gated behind
`assertive`, which means the narrower "redo relay extraction". `DeployFromContainer` separately
verifies the extracted relay binary's magic bytes and size, so a stub or corrupted relay fails
precisely instead of as a bare missing-file assertion later.

**Preparing a host is a verb, never part of a launch.** Tool images materialise inside the first
task that needs one — right on a connected cluster, impossible on a compute node with no route to
a registry. Moving the pulls onto every launch would make the connected case pay for the
disconnected one, so the two halves are split: `SetupEnvironment` fetches and is asked for
explicitly (`metasmith workflow setup-env`, the GUI's `setup environment`), and the launch path
only reports what is missing. It dispatches on the agent's own runtime — the image store for a
container agent, `mamba env create` from the library's per-tool recipes for a mamba or native one
— and reads the stage-time env manifest rather than a transform library, because the launching
host may hold neither the library nor the images. A tool whose env resource carries no `conda:`
entry is reported by name with the container it does have, rather than guessed at: a package spec
inferred from an image tag would produce a plausible env that is not the one the transform was
written against.

**Nextflow is pinned**, and the pinned line's strict syntax parser is on by default: generated `.nf`
and `Orchestrator.groovy` must avoid single-element parenthesized assignment and range-based for
loops. Multi-element destructures and `for (x : collection)` are fine. **Upstream
`nextflow-io/nextflow#6757` is open**: `Duration(long)` asserts non-negative, so under wall-clock
skew (WSL2, an NTP step) `invokeOnComplete()` throws and the JVM exits non-zero *after* the
workflow body succeeded and manifests are on disk. Production absorbs it; tests go through
`_assert_nxf_ok`, which detects it, warns and proceeds. A Groovy `metaClass` override was tried
and abandoned — the caller is `@CompileStatic`, so meta-dispatch is not intercepted. Test-side
only: interpolating a Map inside a `.view {}` closure makes Groovy's `formatMap` race concurrent
operators sharing that Map; render a non-Map field. `Orchestrator`'s shared maps are concurrent
types despite `synchronized` methods, because the collections leak to operator callbacks.

**GPUs are declared in the only unit a transform can honestly know — total VRAM — plus whether
the need is hard.** Device count and type are deliberately undeclarable there: whether 40 GB is
one A100, a MIG slice or two cards is a fact about the host, and a MIG profile name means nothing
on another cluster. Those live on the run side, said once by whoever launches. `extra=` applies to
GPU steps *only*, which is what distinguishes it from `clusterOptionsExtra`, since a site's
default partition usually has no cards. Metasmith derives devices per step and renders per-step
`withName` blocks through the same path `resource_overrides` uses, so a per-step override still
wins; resolving to more than one device warns loudly, since most tools cannot shard across cards.
`RunWorkflow` **refuses before launching** when a REQUIRED step has no device or exceeds the
declared devices. Inside a protocol, `DeclaredGpus()` and `DetectGpus()` answer different
questions — what was asked for, and what was got — and detection probes the execution host through
the relay so it stays honest under a partial allocation or a MIG slice. The runtime's GPU switch
is added automatically and in the right dialect **only when a device is actually detected**, not
merely declared, because `docker run --gpus all` fails outright on a CPU-only host and would turn
an OPTIONAL step's graceful fallback into a dead task. Two hosts verified live: **Sockeye**'s
`job_submit` plugin accepts *only* the untyped `--gpus-per-node=N`, so leave `type` unset;
**WSL2** needs `gpu_args` to bind `/usr/lib/wsl` and set `LD_LIBRARY_PATH`, since apptainer's
`--nv` injects `nvidia-smi` but misses the driver stack.

**Resource overrides at run time** retarget per-process cpus/memory/duration without re-staging.
The **key's type is the selector's scope**: an `int` is a step position, a `str` is a transform
name matching every step running it. Nothing about a key spelled `3` says which was meant, so
anything arriving over a wire with only string keys — JSON, hence the GUI — must cast before it
reaches ops. The precedence rule is not "exact beats regex", since Nextflow does not apply
specificity across config sources; it is **per-directive last-defined wins**, where last means the
later `-config` flag. Keeping overrides in the *second* file is also what keeps per-task hashing
stable across runs with different overrides. When an override seems dropped, check in order: the
regex actually matches the process name (**case-sensitive** Java regexes); no Groovy parse error
killed the include; no third config layer loaded after; and `sacct` `ReqCPUS`/`ReqMem`, since
`Alloc*` reflects partition rounding rather than the request. Local-executor caps reject asks
exceeding host memory *before* the run, which looks like a dropped override and is not.

## Task cache and lineage

**Cache identity is provenance, not bytes.** A step's `cache_key` is the transform key plus its
sorted input `instance_id`s, canonical-CBOR encoded and blake3-32 multihashed; nothing about the
output participates. On by default, with a per-transform opt-out and a global kill-switch. An
input produced by an earlier step names that step's slot id, so a key moves when its producer
does; `cache_decisions` stamps the slot id onto the consumer's instance as well as the producer's,
because the two start life sharing the transform archetype's id.

**Leaf ids are stat-addressed** — `multihash("stat" ‖ abspath ‖ mtime_ns)`, one stat whether the
leaf is a file or a 300k-file directory. The absolute path belongs to whichever host ran the stat,
so `StageWorkflow` re-derives every leaf id on the agent before compiling (`restat_leaf_ids`) and
writes the plan back to `task.yml`: the client that registered an input living on the agent's host
cannot stat it, and `CollectResults` later joins the trace against that same plan. Re-submitting
unmodified files at the same paths hits the same shards; two hosts holding identical bytes at
different paths do not agree, and a same-mtime in-place edit is invisible. A path nothing can stat
keeps a random per-call id and gets no reuse. `fabfos/refs.py` substitutes the DVC pin's own md5,
which survives the re-materialisation that moves an mtime. A change below the top node is
invisible by construction, and `msm data invalidate` is the lever for it: it moves the mtime
forward and re-mints through the same formula, so client and agent still agree.

**Nextflow will not publish a path outside its own work directory.** `PublishOp.collectFiles`
adds a path to the publish set only when `getTaskDir` resolves it under `session.workDir`, its
`tmp`, or `bucketDir`; anything else is dropped with no log and no error. A cache shard is outside
all three, so nothing the emitter puts on a channel reaches `results/` from a hit — the emitter
records the channel-to-directory spelling in `workflow.cache_publish.json` and the driver places
those products itself once nextflow has exited. This is invisible from here: the run reports
`completed` and the results directory is simply empty.

**A product is whatever carries the canonical `<batch>-<item>-<branch>.<hash>-<key><ext>` name** —
a directory as readily as a file. Nothing on the promote or the hit path may branch on the
declared extension to decide which: `GetPreferredFileExtension` answers `""` for plenty of file
types, and a directory type can carry one.

**A hit short-circuits the executor at compile time, not at run time.** The probe rewrites that
step's emission into a synthetic channel, and **every tuple must re-enter `o.post` before any
downstream `o.group` observes it**, or the orchestrator deadlocks — any change to cache-hit
codegen has to preserve that. The post-exec promote atomic-renames the staging dir onto the
shard; the reclaim sweep that follows is scoped to the promoting run's own keys, because an
unsealed `.tmp` is indistinguishable from one another run is still writing. Every on-disk name in
that sentence is defined once in `caching/layout.py`, since compile, promote, telemetry and ops
must agree and disagreeing reads as a cache miss rather than an error.

**`trace.jsonl` is the canonical event log, and it records banked work, not run work.** It
rotates on compile and is never truncated; a `SessionStart` sentinel leads every fresh file. Rows
are appended from inside the promote loop, so a lone sentinel after a hundred completed tasks
means promotion has not run yet — not that a buffer was lost. The dataclass in
`models/lineage.py` is the row spec.

**The cache-hit route and the promote route must emit the same event shape.** They are two
independent emitters of one record, and every field that diverged between them — an empty
output path, a slot id standing in for a file id, the consumer's dtype key instead of the
producer's — was invisible to a warm run and broke a lineage walk downstream.
`tests/audit/test_quadrant_probe.py` is what compares them field by field. A step that opts out of
caching promotes nothing but still emits its event: skipping it leaves every consumer downstream
naming a parent no row accounts for.

**A file's parents are recorded by what produced it, not derived from `consumes`.** `consumes`
names compile-time slot ids, which every fan-out sibling of a step shares; `ProducedFile.parents`
names the files a task actually read, taken off `PROV` positionally against `FILES` and stored in
the shard so a hit answers the same way a miss does. `CollectResults` is then a merge of those
rows — resolve each parent id to another produced file or to a given, register in topological
order — rather than a reconstruction in a third identity space. A producer that leaves `parents`
empty makes its outputs' ancestry unrecoverable.

**`index.yml` stores the transitively reduced parent graph and `Load` re-expands it.** `Pack`
drops a parent that is also a grandparent; `Unpack` walks the edges back into a closure. So a
caller writes direct parents and reads ancestors, and `Trace` — one hop over the in-memory
`parents` — answers multi-hop questions on any library that has been through a save.

**Nextflow shares one lineage-index object across a multi-product process's outputs.** The
sharing is decided by its output binding, before any code here runs, so the rule is that
nothing may mutate a rendered index in place — corruption needs both a shared object and a
writer, and the writer is the half this repo controls. Deep-copying instead is not the fix: it
changes the rendered index, which feeds `file_instance_id`, which would orphan every existing
cache shard.

**Two version constants, deliberately separate.** `CACHE_KEY_VERSION` is the cache-key epoch;
`LIN_PAYLOAD_VERSION` is the on-wire envelope the Groovy side parses. They were one constant
once, and bumping it for a key-epoch reason desynced the emitter and failed every containerized
step with a masked exit 1 that no fast test could see. `SHARD_LAYOUT_VERSION` separately tracks
the log capture that makes a shard's stdout resolve after `rm -rf work/`.

Open weaknesses in this subsystem — single-pass promotion, an orphan sweep that can reach another
run's staging — are tracked in `plans/consolidation-followups.md`.

## Re-exported packages

`models/libraries`, `models/workflow` and `agents` are packages whose `__init__.py` is nothing
but re-exports. They were single files until they grew past readable size; the dotted paths did
not change and are **not allowed to**, because `metasmith/__init__.py` is entirely
commented out, making those paths the public API. None declares `__all__`, since research
notebooks star-import them. `tests/metasmith/unit/test_module_surface.py` holds a snapshot of the
pre-split namespace and fails on any name that stops being reachable.

Two things a re-export does *not* give you, each of which cost a debugging session. **A
re-exported name is importable, not patchable** — `monkeypatch.setattr` on the package rebinds
the package's global while the code still reads the binding in the defining module. And
**`inspect.getsource(pkg)` resolves to the `__init__`**, so a test reading a module's source to
pin a literal must glob the package, and one pinning an *absence* passes trivially otherwise.

In `agents`, `RunWorkflow`/`StageWorkflow`/`CheckWorkflow` each name two things: an `Agent`
method (the client asking) and a free function in `agents.runner` (the agent host doing). Both
are public, so neither is renamed, and `coms/api.py` imports the free functions by bare name —
which makes the `__init__` import order load-bearing. `runner` is imported last and must stay
last.

## The three veneers

`metasmith.ops` is the one implementation; the CLI, the web GUI and the notebook API are veneers
over it. **Nothing shells out to the command line**, and a fix belongs in ops rather than in
whichever surface reported it. There is no server process and no persistent in-memory state —
each call loads what it needs by path and exits, and cold-load cost is small enough that this is
the right trade.

The CLI's surface is `--help` and `docs/metasmith/source/agentic/`. Two things about it are not
discoverable: `--json` routes progress logs to stderr so the stream stays clean, and errors exit
non-zero to stderr rather than being swallowed into a JSON error field. To run against this
checkout's own source rather than whatever an ambient `PYTHONPATH` resolves, use
`./dev/metasmith.sh -r` (and `--gui`) rather than a bare `msm`.

## Web GUI

`msm gui` serves a localhost page covering the same run path as the notebook — ssh host, agent,
inputs, plan, run, results — without writing Python. Transform *authoring* is deliberately
absent. Frontend source is `src/metasmith/frontend/` (Svelte 5 + Vite); the bundle it emits is
generated by `./dev/metasmith.sh --build-gui`, **never committed**, and node is a build
dependency deliberately kept out of the conda env. Brand marks are reached through a vite alias
rather than copied, and ship only because vite emits them into the bundle — an icon referenced by
neither `index.html` nor a component would not ship at all.

Test selection is by **directory**, not by hand-written marks: `tests/metasmith/conftest.py` maps
each directory to its markers at collection and fails loudly on a file under no axis. Handing
pytest those paths matters as much as `-m` does — `-m gui` alone still *collects* the whole tree,
and importing the e2e modules costs more than this suite takes to run. The two things that would
quietly undo its few-second runtime are per-test `create_app` (`bind_project` exists precisely so
one app can be re-pointed) and anything that shells out per item.

The invariants below are the ones that fail *silently*; the routes, components and their
behaviour are readable in `src/metasmith/gui/` and `src/metasmith/frontend/`.

- **Every editable object is saved by `PUT /<collection>/<id>` carrying the whole object**,
  identity included, so an id differing from the url is a rename applied as part of the save.
- **Incompleteness is reported, never refused, until launch** — you make an agent days before its
  cluster exists in your ssh config, and a half-filled recipe still solves because that is how you
  find out what a plan needs. So input problems are recorded into the result **at solve time**,
  and the run route refuses on that record rather than on what the page says at click time: the
  verdict must belong to the solve that produced the bundle a run would stage.
- **What a plan was solved from lives in the plan.** The request is rewritten on nearly every
  edit, so it describes the page as it is now and never the bundle beside it — asked whether a
  result is stale, it answers about the editor. The browser fingerprints the recipe it sends and
  the solve stores that string verbatim, minting nothing itself, so one implementation decides
  what a recipe serialises to. That is also why `rows.normalize` runs on *both* sides of the
  wire: a row just built and the same row just read off disk must serialise alike, or the
  fingerprint moves on a reload that changed nothing.
- **`ops.inputs.sync` is the one writer of an input library**, called unconditionally at the top
  of every solve — no gesture to remember, and no state between an edit and a solve that can go
  stale. **The sync is incremental, and that is not tidiness**: identity is a function of path and
  a task key is a function of identity, so a clear-and-rebuild would re-mint every id on every
  solve and cost every user their cache with nothing on screen to say so.
  `tests/metasmith/unit/test_input_rows.py` pins it directly, because a suite that only asserts
  "the library ends up right" passes a rebuild.
- **Which row owns which manifest entry is recorded, not re-derived**, server-side, since the
  browser rewrites the request wholesale on nearly every edit. That record carries the `adopted`
  mark, without which deleting a row could not be expressed — the item outlives the row until the
  next solve, and every read in between would put it back.
- **A value row states no path at all: the library mints a uuid**, once and recorded. A
  user-typed name only ever named a file nothing opens, and renaming it would re-mint identity
  and silently lose the cache.
- **What an array row expands into is keyed on identity, not on sheet position.** A file row
  registers its bound cell as the path; a value row, having none, keys its mint on the union of
  the cells its fields bind. Two sheet rows naming one pangenome are two samples of *one*
  pangenome, and a per-sheet-row key would turn that shared parent into three pangenomes holding
  one genome each — which plans fine, and is wrong.
- **Sharing is a string, not a file** — a versioned prefix so a later format is refused by name
  instead of misread, and a digest so a payload a mail client wrapped is refused instead of
  half-imported. Resolution on arrival is best effort by name, because refusing a whole import
  over one missing name is the worse failure. Lineage in the payload is stated in row **ids,
  never paths**: unbound, every row's path is the same string.
- **`Agent.Pack`'s optional block stringifies every value it writes** — right for the names beside
  it, silently fatal for a mapping, which reloads as a quoted Python literal, stays truthy and
  yields no params. A mapping field is packed on its own. A params *file* has nothing to merge
  into, so it wins whole and says so.
- **The workflow diagram's `<img src>` carries the plan's generation timestamp**, not just the
  theme: the route serves a file cached beside the bundle, so a url that never changes across a
  re-solve is one the browser's cache keeps answering from. The cache is keyed on nothing about
  the *renderer*, so changing how a node is drawn does not invalidate a drawing already on disk.
- **A step's controls sit level with its node, in the drawing's own pixels**, placed at the
  `dag_cy` the server measured off the same geometry `render_svg` is written in terms of. Scaling
  the image, or putting anything between it and the rows box, moves one origin and not the other —
  and since the displacement is about one row tall it reads as rows naming the *next* step.
- **A surface with rows of its own sends them up rather than applying them to what comes back.**
  The recipe's lineage rail posts its own order and each row's measured offset and receives the
  finished drawing, because the engine reorders rows freely when allowed to, and re-baking the
  curves in the browser means a second implementation of the pixel pass with nothing holding the
  two in step.
- **A type name is split in one place and it cuts where the engine cuts** — at the *first* `::`,
  matching `dag_draw.default_label`. The half the page prints is the *name*: a namespace is shared
  by every type in a library, so it never tells two rows apart.

## DAG rendering

Placement is metasmith's own, in four modules under `models/`: pure geometry, the text/SVG/DOT
backends, colour schemes over a finished layout, and the node/edge API. Graphviz is reached only
for raster formats and only as `neato -n2`, which honours our positions and lays nothing out.

- **A node's id is not its label.** `TransformInstance.name` is the definition file's stem, so a
  plan running one transform three times has three steps named alike; the step number in the id is
  the only thing keeping them apart. Shorten the id and the three fold into one node — after which
  the layout's cycle-breaker cuts edges to restore acyclicity, silently.
- **A repeated block is a shape, never a name.** Products are named per instance, so nothing
  matches as a string; a node's signature is its kind, its **fan-in**, and the sorted multiset of
  its children's signatures to a bounded depth. Fan-in is in there because without it three
  unrelated merge steps hash alike and get hoisted 14 rows from their readers. An instance's block
  is its descendants minus everything its siblings also reach — *not* its dominator subtree, which
  loses any node with a second parent.
- **Supply is emitted where it is consumed, not where it is declared.** A reference database is a
  root that owns nothing; drawn at either end it holds a rail across every module between and
  drags its consumers with it.
- **Colour is decoration and the layout must never see it.** Every scheme is a pure function of a
  finished `Layout`. Two opposite jobs share the word: `lane` and `module` are graph colouring
  (touching things differ), while `repeat` is the reverse — every instance of a motif in one hue.
  Only the second makes a repetition visible, and only because the layout already put the
  instances in the same shape.
- **`background=False` renders transparent**, for the GUI's card; node fills are untouched, so
  such a drawing is pixel-exact on a card of the theme's colour and only very close on any other.

**Where a pass has two defensible answers, both are drawn and measured.** `measure` returns
congruence, rail rows, lanes, crossings, detours and module contiguity, and `layout` picks
symmetry ahead of length. Congruence is *modal* — the largest set of instances arranged alike —
because mean agreement is too coarse to separate row orders. Prefer adding a candidate to tuning a
constant. Ceilings are pinned in `tests/metasmith/unit/test_dag_stress.py`; a tuning change is free to
improve one and has to say so out loud to make one worse.

Four things were measured and **rejected**, and the numbers are why they stay rejected: optimal
Sugiyama layer assignment as a row sort (ranks are right, but many nodes share one, so the branch
walk's grouping is lost and the drawing costs half again as much rail); sift-based local search on
that objective (lowers the cost, scatters every cluster to do it); marker-to-label distance as a
selection term (~11% more crossings across a 300-graph corpus, buys nothing on the metagenomics
plan); and detour as anything but the last tie-break (crossings cannot see one, and most detours
are forced). The first two are the general lesson: the objective is a proxy, and it stops agreeing
with the picture close to its optimum. The last two are kept as *measurements* so either case can
be re-argued in numbers.

`env` is in `blacklist_namespaces` alongside `lib` and `containers`: an environment is a declared
dependency like any other, so without it every plan DAG grows an `env::*` node per step. Three
defaults have to agree — `BuildDAG`, `RenderDAG`, and `ops.workflow.render_dag`.

## Versioning

Two files form the version: `version.txt` (bare PEP 440 release segment, source-controlled,
bumped by hand, no `+` or `-`) and `build_hash.txt` (7-char md5 over the `src/metasmith/` tree,
regenerated at build time, gitignored, absent in fresh checkouts — everything then degrades to
bare semver). `constants.py` derives the full version and container tag from that one chain, and
the wheel name, default agent container and dev-script tag all read it. Identical source ↔
identical hash ↔ identical image tag, which removes the chicken-and-egg of an embedded commit
hash. Pinned by `test_container_tag.py` and `test_dev_sh_tag.py`.

`RELEASE_PROTOCOL.md` is the followable sequence, and its order is load-bearing rather than
stylistic: relays before the image that bakes them, GUI bundle before the wheel whose hash covers
it, and nothing rebuilt between the pip and docker builds.

Note what the GUI bundle does to the hash: it sits inside the tree the hash walks, so **the
version is a function of the git tree *plus* the frontend build**, not of the commit alone. That
is correct — the bundle is part of what ships — and it is reproducible only because
`package-lock.json` pins the toolchain, making vite's output byte-identical across builds,
content-hashed asset names included. Measured, not assumed.

The two Rust products are built separately and shipped differently; see
`docs/bash_relay/architecture.md` and `docs/workflow_solver/architecture.md`.

### When a build artifact may be DVC-pinned

A generated artifact earns a pin when rebuilding it is expensive *and* the pin is genuinely how it
reaches consumers. An artifact that something regenerates on demand does not qualify, and
neither does one nothing reads; the GUI's
`scratch/gui-main` pin managed to be both, snapshotting 31 files of local run detritus that
`dev/metasmith.sh --gui` recreates with `mkdir -p`, and its objects had already left every cache
by the time it was removed.

The consumer-side rule matters more than the pin: **never require the pinned copy to be usable in
place.** A materialised DVC output is a read-only hardlink into a cache shared with every other
worktree, so it cannot be chmodded (that mutates their copy) or unprotected (that dirties the pin
over a permission bit that is not content). Code that needs to *run* a pinned file must copy it
somewhere writable first — `solver_engine._runnable_engine_path` is the worked example, and the
reason it exists is that the alternative failed silently for a long time.

The residual risk is unchanged by any of this and worth stating plainly: a pin whose objects leave
every cache is unrecoverable, because the pin records a hash rather than the bytes. The engine
pin is one careless collection away from being exactly that.

## `src/metasmith/examples/`

The smallest valid metasmith library — agnostic (no host or runtime references) and reusable for
any deploy or runtime smoke test: one namespace, one container pointing at the metasmith image
itself, one transform, and a committed `_metadata/`. It is the one library whose metadata *is*
tracked, precisely so a smoke test needs no build step.
