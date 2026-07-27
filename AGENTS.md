# Agent Notes for Metasmith

## What goes in this file

This is the architectural brief for someone about to *change* metasmith: the concepts
the code is organised around, the invariants that span more than one file, and the traps
whose evidence lives somewhere you cannot read from here — a cluster, a scheduler, an
upstream bug, a decision made once and never restated.

The test for a paragraph is: **would reading the code have told me this?** If yes, it does
not belong here. Per-module behaviour, function signatures, flag lists and command trees,
and tool-specific quirks are all recoverable by reading — they go in a docstring, in
`docs/`, or nowhere. What is *not* recoverable is why a shape was chosen, which two files
must agree, what breaks silently when they stop agreeing, and what some other host did to
us once.

Corollaries:

- **Prefer the rule to the instance.** One sentence naming the invariant beats five
  recounting the bugs that taught it.
- **A postmortem is not documentation.** Keep what it proved; drop the story.
- **Nothing enumerable.** A command list, a type roster, a file inventory — all go stale
  without this file changing. Name the source of truth instead (`--help`, `docs/`, the
  directory itself).
- **If it only mattered to one session**, it belongs in the scope journal, not here.

## Environment

Use the `msm` mamba environment: `mamba run -n msm <command>`.

**Pin `PYTHONPATH` to this worktree's `src/`; do not merely unset it.** `metasmith` is not
installed into `msm` at all — `tests/conftest.py` inserts `src/` for in-process imports, so
an unset `PYTHONPATH` looks fine until a subprocess test spawns `python -m metasmith` and
fails on its own. An ambient workspace `PYTHONPATH` is the opposite trap: it resolves the
import to some other checkout. `PYTHONPATH="$PWD/src" mamba run -n msm …` is the form that
is right under both.

---

## What Metasmith is

A workflow generation system for bioinformatics. You say what data you have, what result
you want, and which tools exist; it finds a chain of tools connecting them and compiles
that chain into a Nextflow pipeline, then runs it.

The core idea is a **type system for bioinformatics data**. You describe data by what it
*is* — an NCBI accession, a FASTA, a pangenome heatmap — rather than writing "run A, pipe
to B", and the planner works backwards from the target. You never write Nextflow; adding a
tool means writing one Python file declaring its inputs, outputs, and how to run it, and
the planner incorporates it into every workflow where it helps.

## The type system

The hardest part of the repo, and everything else is downstream of it. Read
`src/metasmith/models/solver.py` alongside this section.

### A type is a set of strings

`Node` — the base of both `Endpoint` (a free-floating data type) and `Dependency` (a slot
on a transform) — is a set of **property strings** plus a set of parent Nodes. There is no
class hierarchy, no name on the object, and no runtime type beyond that. A keyed property
from YAML becomes a compact JSON string in the set (`database: assemblies` →
`{"database":"assemblies"}`); an unkeyed one is the bare string. Everything the type system
does is set algebra over those strings.

`extends` is resolved **at parse time by union**, not kept as a link: a subtype's property
set is its own properties plus every ancestor's, flattened. Two consequences bite. A type
may only extend one defined **earlier in the same file**, since resolution walks the YAML
in order. And once loaded there is no inheritance to inspect — "is X a subtype of Y" is a
subset test, nothing more.

### The name is not part of the type

`DataTypeLibrary` is a `{name: Endpoint}` map and the Endpoint holds no back-reference —
`DataInstance` has to carry `dtype_name` as a separate field for exactly this reason.
`__hash__`/`__eq__` are over `Signature()`, the hash of the sorted property set (plus
sorted parent keys, when there are parents). So:

- **Two differently-named types with identical properties are one node.** They satisfy each
  other's requirements and collapse in any set or dict. This is the most common way a
  library goes subtly wrong: give near-twins a distinguishing property
  (`content: metagenomic bins`) rather than trusting the names to keep them apart.
- **Two slots of one type on a transform are `==`.** `Dependency` inherits that equality, so
  any code resolving a declared `parents={...}` back to a slot must match by **object
  identity first**, or it answers with whichever slot came first.
- Names exist for humans and for the index. The solver never sees one.

### Direction: more properties means more specific

`x.IsA(y)` is `y.properties ⊆ x.properties`, read as *"x can be used in place of y."* A
subtype satisfies a supertype's requirement; a supertype never satisfies a subtype's. That
asymmetry is load-bearing in every consumer, and reversing it yields a planner that appears
to work while quietly building wrong chains. It is also why a requirement should be written
as loosely as the tool genuinely tolerates — every property added is one more thing an input
must carry. `ext` is the one magic property: `GetPreferredFileExtension` scrapes it back out
of the set to name output files.

### Lineage is part of identity

A Node's `parents` fold into its `Signature()`, so a type *with* lineage is a different node
from the same type without. That is the mechanism behind every "which one did this come
from" feature: per-slot `parents=` on a transform, `parents=` on a target, item parents in a
data library, and `AsSamples` masking a library to one item plus its ancestors.
`WithLineage` produces the re-parented image.

Two ordering rules fall out, and they are the same rule twice: a transform's requirement may
only name parents already added (`_add_dependency` asserts it), and a target may only name
targets declared before it. **Declaration order is the reference space** in both.

Lineage must also be acyclic, and not merely for tidiness: `AsSamples` walks up to a node's
ancestors and back down to their descendants, so over a cycle one mask becomes the whole
library, and the expand-on-load / collapse-on-save pair the data library performs has no
meaning. `ops.data._assert_acyclic` refuses it wherever parents are set. Note that
`show_item_lineage` reports the **transitive closure**, not the declared parents — any
consumer that does not collapse it again will offer a grandparent whose removal silently
reverts on the next load.

### Products come in groups

`Transform.produces` is a `list[list[Dependency]]` — product *groups*, advanced by
`NewProductGroup()`. The call is **overloaded, and the two meanings are opposite**: for
sample alternatives it branches into separate timelines (this run produces A *or* B), while
for a user's multi-output transform the products must stay in one timeline (A *and* B). The
solver discriminates on whether the application's transform is the given one. Getting it
wrong makes a multi-output tool's products stop co-existing.

### What the solver does with it

The solver works backwards from the target, carrying `SolverState` of what it has and which
transforms remain candidates. An `Application` is one use of one transform — a
`{Dependency: Endpoint}` map of what filled each slot, plus the same for what it produced —
and its signature is the transform key with each slot's key paired to the endpoint that
filled it. So one transform applied to different inputs is a different application, and
applied to the same inputs it dedupes.

When no complete plan exists, `WorkflowPlan.hints` carries structured `PlanHint` records
(`unreachable_target`, `missing_input`, `lineage_mismatch`) with a reverse-BFS chain,
candidate transforms, and near-misses ranked by property-Jaccard against the givens. Every
consumer is expected to surface them — a bare "no plan" is not an acceptable failure.

### Types are compiled once per library

A namespace is the source YAML's filename stem, and a duplicate namespace across directories
raises. Each transform library then carries its **own** `_metadata/types/` in a compiled form
distinct from the source, so a new type must land in all of them; `metasmith build` does it.
Skipped, a plan becomes unreachable only from certain libraries — which reads like a solver
bug and is not.

## The building blocks

### DataInstanceLibraries

`.xgdb` directories holding actual files and values, each tagged with a DataType. Items
carry parent/child links (genomes under a pangenome); `AsSamples("type")` splits the
library into per-item views for parallel processing, each sample carrying the matched item
plus its ancestors.

Each `DataInstance` has a stable `instance_id` derived from **path, dtype name, and parent
library — never file bytes**. Bioinformatic inputs reach hundreds of GB, so content
hashing is deliberately off the table; the consequence is that changing a path or a type
re-registers the row and costs cache reuse downstream, and two different files at one path
collide. That is worked around by an explicit user-driven fork, not by an automatic fix.
The id survives `WithDType()` retyping and `Pack()`/`Unpack()`, which is what makes
`Load()` + `Trace()` the correct way to map results back to inputs — never filename or
work-directory parsing.

### Transforms

One Python file per tool, in three parts — contract, protocol, instance:

```python
lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("containers::tool.oci"))
input = model.AddRequirement(lib.GetType("namespace::type"))
out   = model.AddProduct(lib.GetType("namespace::output_type"))

def protocol(context: ExecutionContext):
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="...") \
        .ifVirtualEnvDo(env=image, cmd="...")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=input,
                  resources=Resources(cpus=N, memory=Size.GB(M)))
```

`AddRequirement(..., parents={...})` declares **per-slot lineage** — bbduk does not want
three files, it wants the reads belonging to *this* metadata. A third of the standard
library uses it, and it is why a transform's requirements must be indexed by slot rather
than as a set of types.

**A transform never learns which runtime it is on, and does not need to.** It declares an
arm per world (`ifContainerDo` / `ifVirtualEnvDo`, either omissible — omitting the venv arm
is how a container-only tool says so) and the `env` package owns every per-runtime
difference: bind dialect, whether a container boundary exists at all, GPU flags. Code that
branches on the runtime is a bug; declare the need instead. `ifContainerDo(args=[...])`
appends verbatim runtime flags just before the image, so a flag passed there beats the
framework default of the same name — but the dialect is the caller's problem, and mounts go
through the typed `binds=`, never `args=`.

The protocol sees three path views — `.local`, `.container`, `.external` — handed to it by
the framework (`context.Input/InputGroup/Output`). Under a runtime with no container
boundary all three are the same host path.

`TransformInstanceLibrary.AsView(mask, invert=)` hides transforms by file path without
rebuilding the library on disk (mirrors `DataInstanceLibrary.AsView`); `virtual_runtime`
validates contracts without pulling images.

### Planning

Given typed inputs, transform libraries, resources, and a target type, the solver searches
for a chain connecting them and emits a DAG. Parallelism is automatic — three accessions
produce three parallel jobs feeding one collecting step:

```
ncbi::assembly_accession → [getNcbiAssembly] → sequences::gbk
sequences::gbk (grouped)  → [ppanggolin]     → pangenome::ppanggolin_matrix
                          → [heatmap]        → pangenome::heatmap
```

`TargetBuilder.Add(target_type, parents=None)` returns an opaque handle; passing handles in
`parents=` is what makes two targets of the same type **distinct requests** rather than the
duplicate `Add` rejects. Same type *and* same parents still raises.

**A sample type is a way of branching a plan, not a precondition for one.**
`plan_workflow(sample_type=None)` plans the library as it stands — one sample holding
everything in it — and naming a type splits it into one run per item of that type
(`AsSamples`). The GUI does not offer sampling and always passes `None`; the CLI's
`--sample-type` is optional for the same reason.

**Planning is not reentrant.** `TransformInstance.Load` imports each transform by bare module
name, mutates `sys.path`, calls `importlib.reload`, and returns through a *class* attribute —
all process-global. Two concurrent plans in one process clobber each other and fail with a
bare `spec not found for the module`. The CLI never hits this (one process, one plan);
anything long-lived must serialise generates, and anything walking the same import path must
hold the same lock — which is **every** caller that imports a transform, not only the ones
that plan: building a type index, and reading a staged bundle back with `load_task`, both do
it. The second failure mode is quieter than the `spec not found` one — `Load` returns through
the class attribute a concurrent load already reset, so a transform arrives as `None` — and it
surfaces as an assertion two frames away in `GetTransform`.

### Agents and execution

An Agent is a deployment target: it owns a home directory, orchestrates containers, and
compiles the DAG into Nextflow.

```python
smith = Agent(home=Source.FromLocal(path), runtime=Runtime.DOCKER)
smith.Deploy(); smith.StageWorkflow(task); smith.RunWorkflow(task)
```

**`RunWorkflow` is fire-and-forget.** The agent shell launches Nextflow under `nohup … &`
and returns when the launch script exits. This is fine in a notebook, where the human
advances the cell; any script that goes straight to `GetResultSource`/`CheckWorkflow` races
past the run and crashes on a missing results directory. The contract is a sentinel line —
`run completed at [<ts>]` in `runs/<key>/_metasmith/logs.<latest>/agent.log` — which
`metasmith workflow wait` blocks on. Poll for it; do not sleep and hope.

**`Source.Parse` must be a fixed point on its own output**, because anything that stores an
agent home re-parses it on the next save. It was not: `SshSource` renders `ssh://host:path`
while `Parse` split on `/` and read the `:` as part of the host, so a remote home grew a
colon per save until nothing could reach it. Pinned by
`tests/models/test_source_parse.py::TestSshRoundTrip`.

---

## Adding a transform

1. Data types in `data_types/<namespace>.yml`; container type in `data_types/containers.yml`
   with a `provides:` list; a `.oci` file in `resources/containers/` holding the docker URL.
2. The transform in `transforms/<category>/`, and both `_metadata/index.yml` files updated.
3. `metasmith build`, to compile the new types into **every** library's `_metadata/types/`
   (see § *Types are compiled once per library* for why skipping it looks like a solver bug).

**Container tags** come from `mamba search -c bioconda <tool>`; biocontainers are
`quay.io/biocontainers/<tool>:<version>--<hash>` and the hash suffix is not guessable.

Reference implementations: `checkm.py` (single assembly input), `gtdbtk.py` (external
database binding), `getNcbiAssembly.py` (multiple outputs).

---

## Execution invariants

### Local transfers

`Logistics` copies local→local in process — plain files, symlinks, and trees of those two —
and hands everything else to `rsync -auP`, which keeps ownership of special files, deletion,
and `-L`. The split is about **latency, not throughput**: an rsync is ~45ms of process spawn
wherever it runs and ~0.1ms of work on the metadata trees staging actually moves, so a plan
copying a task bundle, a data library and a transform library spent 145ms of its 170ms
waiting for three of them.

Two rules keep the fast path honest. It **surveys before it writes**, so a tree it will not
claim is handed over untouched rather than half-copied. And every file is written aside and
`os.replace`d into position, because an interrupted plain copy leaves a truncated file whose
mtime is *newer* than the source — which `-u` then skips forever.

### Bounding a hung agent

Everything metasmith does on an agent goes through one bash subprocess and returns when a
marker line comes back, so a host that stops answering mid-command holds the calling thread
for the life of the process. The bound is **silence, not elapsed time**: a stage of a large
library legitimately takes an hour, and a wedged one can fail in a minute, so the question
worth asking is whether the far end is still emitting anything. `LiveShell.Exec(idle_timeout=)`
answers it, and unlike the wall-clock `timeout` — which returns `None`, which nobody checks —
it **raises**, naming the step.

Two things make it work and would silently break it. The mark is taken on the **raw read,
before the line split**, because `rsync -P` redraws one line with carriage returns and may
not complete a line for minutes on a large file. And that output exists at all only because
the shell is a **pty**; route transfers through a plain pipe and the bound goes blind. The
`curl` arm of `Logistics` is exempt for the same reason inverted — `--silent` means no
output is not evidence of anything.

The connect is deliberately *not* bounded this way: `Exec("ssh host", inherit_stdin=True)`
lets someone type a key passphrase, and a shell bound would cut that off. It carries
`-o ConnectTimeout` instead, which bounds only the handshake.

A timed-out command is still running on the far end with a marker nobody will claim, so its
shell is spent — dispose it rather than reusing it, and skip the polite `exit`.

### Path translation

All local ↔ external ↔ container conversion lives in `src/metasmith/models/paths.py`:
`PathMap` (per-execution context, built from an agent or an external cwd) does the typed
reroots and the `Parse`/`Render` consolidation; `ContextPath` is the frozen value type,
enforcing that all three views are absolute, `..`-free, and mutually consistent.

**Never use raw `str.replace` or a regex on a path root.** Those shapes silently corrupt
(`/msm_home_old_backup` is not `/msm_home`) or misidentify. For shell-script content use
`reroot_in_text`, which matches only at segment boundaries.

The container is **dual-bound**: the host scope dir lands at both `/ws` (`WORK_ROOT`) and
`/msm_home` (`HOME_ROOT`). Nextflow may resolve a work dir through either, so anything
mapping a cwd back to the host must check both prefixes and route HOME_ROOT cwds through
`agent.real_path`. Pinned by `tests/path_overhaul/test_sbatch_home_root_cwd.py`.

### DAG rendering

Placement is metasmith's own, in four modules under `src/metasmith/models/`: `dag_layout`
(pure geometry — rows, lanes, routed polylines), `dag_draw` (the text / SVG / DOT backends),
`dag_colour` (schemes over a finished layout, off by default), `dag_renderer` (the node/edge
API callers use). Graphviz is reached only for raster formats, and only as `neato -n2`, which
honours our `pos` and lays nothing out; `to_dot()` stays a plain, positionless description
for consumers running their own.

- **A node's id is not its label.** `TransformInstance.name` is the definition file's stem, so
  a plan running one transform three times has three steps all named `checkm`; the step number
  in the id is the only thing keeping them apart. Shorten the id and the three fold into one
  node — and the layout's cycle-breaker then cuts edges to restore acyclicity, silently.
- **The `target` sink is expensive.** Collecting every requested output into one node holds a
  rail from each target's row to the last; on the spanish-lakes metagenomics plan that is 10
  of 24 lanes. Targets are marked on the node instead; `RenderDAG(target_sink=True)` restores
  the old shape.
- **Corners are rounded in pixel space, not in the grid.** `_polyline` stays axis-aligned and
  `_pixel_path` trims corners afterwards, so the arcs cost the layout invariants nothing. Text
  gets no diagonals on purpose: a lane is two character columns and a diagonal glyph is one
  cell, so every lane of travel would cost two rows.
- **Supply is emitted where it is consumed, not where it is declared.** A reference database is
  a root that owns nothing; drawn at the top or the bottom it holds a rail across every module
  between, and drags its consumers with it. `_row_order` seeds only the root owning most of the
  graph and pulls each other root's chain in immediately above the first step that stalls on it.
- **A repeated block is a shape, never a name.** Products are named per instance, so nothing
  matches as a string; a node's signature is its kind, its **fan-in**, and the sorted multiset
  of its children's signatures to a bounded depth. Fan-in is in there because without it three
  unrelated merge steps hash alike and get hoisted 14 rows from their readers. An instance's
  block is its descendants minus everything its siblings also reach — *not* its dominator
  subtree, which loses any node with a second parent and leaves a stub the hoist acts on wrongly.
- **Where a pass has two defensible answers, both are drawn and measured.** `measure` returns
  congruence, rail rows, lanes, crossings and module contiguity; `layout` picks on
  `(congruence, rail, lanes, crossings)` — symmetry ahead of length, which costs ~1% on graphs
  that have none. Congruence is *modal*, the largest set of instances arranged alike: mean
  agreement is too coarse to separate row orders, and offsets are measured against the previous
  instance because instances fanning out of one node cannot share absolute lanes. Prefer adding
  a candidate to tuning a constant. Ceilings are pinned in `test_dag_stress.py` with the
  pre-change numbers in the docstring; making one worse has to be said out loud.
- **Colour is decoration and the layout must never see it.** Every scheme is a pure function of
  a finished `Layout`. Two opposite jobs share the word: `lane` and `module` are graph colouring
  — touching things differ, good for tracing one rail — while `repeat` is the reverse, every
  instance of a motif in one hue. Only the second makes a repetition visible, and only because
  the layout already put the instances in the same shape.

Two things tried on the row order and measured *worse*: optimal Sugiyama layer assignment
(Gansner et al. 1993) used as a row sort — the ranks are right but many nodes share one, so the
branch walk's grouping is lost and the drawing costs half again as much rail; and sift-based
local search on that same objective, which lowers the cost and scatters every cluster to do it.
The objective is a proxy, and it stops agreeing with the picture close to its optimum.

`env` is in `blacklist_namespaces` alongside `lib` and `containers`: an environment is a
declared dependency like any other, so without it every plan DAG grows an `env::*` node per
step. Three defaults have to agree — `BuildDAG`, `RenderDAG`, and `ops.workflow.render_dag`.

### GPUs

A transform declares GPU need in the only unit it can honestly know — **total VRAM** —
plus whether the need is hard: `Resources(gpus=Gpus.REQUIRED, gpu_memory=Size.GB(40))`.
Device *count* and *type* are deliberately undeclarable there: whether 40 GB is one A100,
a MIG slice, or two cards is a fact about the host, and a MIG profile name means nothing
on another cluster. Those live on the run side, said once by whoever launches:

```python
smith.RunWorkflow(task, gpus=Gpu(memory=Size.GB(80), type="a100", count=4,
                                 flag="--gres=gpu:", extra=["--partition=gpu"]))
```

`extra=` applies to GPU steps *only* — that is what distinguishes it from
`params.process.clusterOptionsExtra`, since a site's default partition usually has no
cards and sending CPU work there would be wrong. Sites charging GPU work to a separate
allocation set `params={"slurmGpuAccount": ...}`.

Metasmith derives `ceil(gpu_memory / device.memory)` per step and renders per-step
`withName` blocks into `workflow.config.nf` — the same path `resource_overrides` uses, so a
per-step override still wins. Resolving to more than one device warns loudly, since most
tools cannot shard across cards. `RunWorkflow` **refuses before launching** when a REQUIRED
step has no device or a step's ask exceeds the declared devices; OPTIONAL steps never fail
and take their own CPU branch.

Inside a protocol, `context.DeclaredGpus()` and `context.DetectGpus()` answer two different
questions — what was asked for, and what was actually got. Detection probes the execution
host through the relay, so it stays honest under a partial allocation or a MIG slice, where
`CUDA_VISIBLE_DEVICES` is a `MIG-<uuid>` rather than an index.

The runtime's GPU switch (`--nv` / `--gpus all`) is added automatically, in the right
dialect, **only when a device is actually detected** — not merely declared. `docker run
--gpus all` fails outright on a CPU-only host, which would turn an OPTIONAL step's graceful
fallback into a dead task.

Hosts needing more than the runtime's own switch declare it once on the agent,
`Agent(gpu_args=[...])`. **Sockeye**, verified live: `Gpu(memory=Size.GB(32),
extra=["--partition=gpu"])` with `slurmAccount`/`slurmGpuAccount` set — its `job_submit`
plugin accepts *only* the untyped `--gpus-per-node=N`, rejecting both `--gres=gpu:v100:N`
and the typed form with `requested_gpus 0`, so leave `type` unset. **WSL2**: apptainer's
`--nv` injects `nvidia-smi` but misses the driver stack under `/usr/lib/wsl`, so NVML
reports "GPU access blocked by the operating system" until `gpu_args` binds it and sets
`LD_LIBRARY_PATH`.

### Apptainer: SIF vs sandbox

`Agent.Deploy()` runs a two-axis static probe on the target host — is `starter-suid`
present, and is apptainer ≥1.4 — and acts on the verdict, because both arms have a failure
mode the other avoids:

- **`use-sif`** when setuid exists (kernel squashfs mount; HPC), *or* when apptainer <1.4
  lacks it — the sandbox path then falls back to fuse-overlayfs, which races SIGBUS under
  SLURM array contention. Deploy removes any stale sandbox dir.
- **`use-sandbox`** for apptainer ≥1.4 without setuid (WSL2): SIF would engage
  `squashfuse_ll` and wedge under the relay daemon's fork chain. The sandbox rootfs is read
  through unprivileged kernel overlayfs, never FUSE.

**A use-sandbox host never runs `mksquashfs`, and that is the point, not an optimisation.**
`MakeBuildSandboxCommand(from_image=True)` builds the sandbox straight from the registry,
so no SIF is pulled and no squashfs is packed. Some hosts' `mksquashfs` aborts on large
images (`malloc(): corrupted top size` on micb0, and the 4.7 series fails most builds of
anything sizeable), and on those hosts the SIF was only ever a throwaway intermediate.
Transform containers get the same treatment at execute time: `_ExecInEnv` builds a
not-yet-cached image as a flock-guarded sandbox rather than falling back to
`apptainer exec docker://…`, which would convert to SIF and crash on the big ones. So each
host holds exactly one artifact — a SIF or a sandbox dir, not both.

`MakeRunCommand` emits a run-time ternary picking whichever exists, so Deploy controls the
choice by controlling the directory's presence; the verdict is re-evaluated every deploy,
so an apptainer upgrade flips it. The store root is one point of control
(`Environment._store_root()` → `${APPTAINER_CACHEDIR:-<home>/container_images}`), expanded
on the *execution* host so the pull, the sandbox build, and the exec all agree.

### Nextflow

**Pinned to `nextflow=26.04.1`**, whose strict syntax parser is on by default. Generated
`.nf` and `Orchestrator.groovy` must avoid single-element parenthesized assignment
(`(_x) = expr` → use `_x = (expr)[0]`) and range-based for loops (`for (i in 0..N)` → use
`.each`). Multi-element destructures and `for (x : collection)` are fine.

**Upstream `nextflow-io/nextflow#6757`** (open): `Duration(long)` asserts non-negative, so
under wall-clock skew (WSL2, an NTP step) `invokeOnComplete()` throws and the JVM exits
non-zero *after* the workflow body succeeded and manifests are on disk. Production absorbs
it (`trap stop EXIT; exit 0` around `nextflow run`); tests go through `_assert_nxf_ok`,
which detects it, warns, and proceeds. A Groovy `metaClass` override was tried and
abandoned — the caller is `@CompileStatic`, so meta-dispatch is not intercepted.

**Test-side only:** interpolating a Map inside a `.view {}` closure makes Groovy's
`formatMap` iterate entries and race concurrent operators sharing that Map, surfacing as
`ConcurrentModificationException`. Render a non-Map field. Production workflows use no
`view`.

`Orchestrator`'s shared maps and the collections they hand out are concurrent types even
though the methods are `synchronized` — the collections leak to operator callbacks.

### Resource overrides at run time

`RunWorkflow(resource_overrides={key: Resources(...)})` retargets per-process cpus/memory/
duration without re-staging. Stage time emits `workflow.resources.nf` with an **exact**
selector per step (`pNN__<transform.name>`); run time appends a **regex** selector block to
`workflow.config.nf`, and the runner passes them in that order.

The **key's type is the selector's scope**: an `int` is a step position and addresses that
step alone, a `str` is a transform name and matches every step running it. Nothing about a
key spelled `3` says which was meant, so anything arriving over a wire that has only string
keys — JSON, hence the GUI — must cast before it reaches `ops.runtime.run`. The prefix that
makes the int form work is minted by `models.workflow.NextflowProcessName`, which the
compiler and every selector-builder share for the obvious reason.

The precedence rule that makes this work is not "exact beats regex" — Nextflow does not
apply specificity across config sources. It is **per-directive last-defined wins**, where
last means the later `-config` flag. So the run-time regex block wins any directive it
sets, and directives only in the stage file (the retry-scaling `memory` closure) fall
through untouched. Keeping the overrides in the *second* file is also what keeps
per-task hashing stable across runs with different overrides.

When an override seems dropped, check in order: the regex actually matches `pNN__<name>`
(these are **case-sensitive** Java regexes — `.*ncbi.*` never matches `p01__getNcbiAssembly`);
no Groovy parse error killed the include; no third config layer loaded after; and `sacct`
`ReqCPUS`/`ReqMem` — `Alloc*` reflects partition rounding, not the request. Local-executor
caps in `local.nf` reject asks exceeding host memory *before* the run, which looks like a
dropped override and is not.

---

## Task cache and lineage

**Cache identity is provenance, not bytes.** A step's `cache_key` is the transform key plus
its sorted input `instance_id`s, canonical-CBOR encoded and blake3-32 multihashed. Nothing
about the output participates. The cache lives at `<agent_home>/task_cache/` and is on by
default — per-transform opt-out is `TransformInstance(..., cacheable=False)`, the global
kill-switch is `METASMITH_CACHE=0`.

**Leaf ids are content-addressed, with the relative path folded in.** A leaf's identity is
`multihash(blake3(file_bytes) ‖ relpath)` when the file is present at `AddItem` time, which
is what makes two independent runs over identical inputs hit the same shards — cross-run
reentrancy comes free, with no import step. Folding `relpath` in is not decoration: pure
content-addressing collapses every degenerate-but-distinct input (N empty files, byte-identical
samples) onto one id, which flattens fan-out and trips the solver's O(n²) collision path.
Absent or remote inputs fall back to a random per-call id and get no reuse.
`METASMITH_LEAF_RANDOM=1` opts the whole mechanism out.

**A hit short-circuits the executor at compile time, not at run time.** The probe rewrites
that step's emission in `workflow.nf` into a synthetic channel routed through
`o.post(o.asStreams(...), k)` — every tuple must re-enter `o.post` before any downstream
`o.group` observes it, or the orchestrator deadlocks. Any change to cache-hit codegen has to
preserve that. The post-exec promote atomic-renames `<key>.tmp/` into `<key[:2]>/<key[2:]>/`;
the reclaim sweep that follows is scoped to the promoting run's own keys, because an unsealed
`.tmp` is indistinguishable from one another run is still writing.

**`trace.jsonl` is the canonical event log, and it records banked work, not run work.**
`<run_dir>/_metasmith/trace.jsonl` rotates on compile and is never truncated; a `SessionStart`
sentinel leads every fresh file and tags subsequent rows with a monotonic `session_id`. One
row schema covers every status; the dataclass docstring in `models/lineage.py` is the spec.
Rows are appended from inside the promote loop, so a lone sentinel after a hundred completed
tasks means promotion has not run yet — not that a buffer was lost.

**Two version constants, deliberately separate.** `CACHE_KEY_VERSION` is the cache-key epoch;
`LIN_PAYLOAD_VERSION` is the on-wire lineage envelope the Groovy side parses. They were one
constant once, and bumping it for a key-epoch reason desynced the emitter and failed every
containerized step with a masked exit 1 that no fast test could see. `SHARD_LAYOUT_VERSION`
separately tracks the `<shard>/logs/` capture that makes `get_logs_of(...).stdout` resolve
after `rm -rf work/`.

## The three veneers

`metasmith.ops` is the one implementation. The CLI (`metasmith` / `msm`), the web GUI, and
the notebook API are all veneers over it — **nothing shells out to the command line**, and
a fix belongs in ops, not in whichever surface reported it.

There is no server process and no persistent in-memory state: each call loads what it needs
by path and exits. Cold-load cost for parsing type/data/transform manifests is small enough
that this is the right trade; the caching an earlier MCP server held in `ServerState` is now
just disk reads.

The CLI's full surface is documented in `docs/source/agentic/` (`tool_reference.rst`); the
command tree is discoverable with `--help` and is not restated here. Two things about it are
not discoverable: `--json` routes progress logs to stderr so the stream stays clean, and
errors exit non-zero to stderr rather than being swallowed into `{"error": ...}`.

## Web GUI

`msm gui` serves a localhost page covering the same run path as the notebook — ssh host,
agent, inputs, plan, run, results — without writing Python. Transform *authoring* is
deliberately absent; it stays in the notebook and CLI.

The wiring: `store.py` owns the project directory (`agents/`, `workflows/<name>/`,
`runs/<name>/`); `stdlib.py` owns the standard-library clone and builds the whole-type-system
index the browser is shipped in one fetch, so library toggles and produced-by/consumed-by
readouts cost no round trip; `sshconfig.py` owns the marked block metasmith writes into
`~/.ssh/config`; `jobs.py` runs background work and SSE log streams; `watcher.py` rediscovers
live runs from disk; `api.py`/`app.py` are the routes and the Flask app. Frontend source is
`frontend/` (Svelte 5 + Vite) — the bundle under `src/metasmith/gui/static/` is generated by
`./dev.sh --build-gui` and never committed, and node is a build dependency deliberately kept
out of `envs/base.yml`.

The GUI's own tests carry `pytestmark = pytest.mark.gui`, and `./dev.sh -tg` derives the file
list from that mark rather than repeating it — so marking a new file is all it takes to join
the set. Handing pytest those paths matters as much as the `-m` does: `-m gui` alone still
*collects* the whole tree, and importing the e2e modules costs more than this suite takes to
run. It is meant to be run every minute, so the number to hold is a few seconds; the two
things that would quietly undo that are per-test `create_app` (`bind_project` is split out
precisely so one app can be re-pointed instead — werkzeug compiles a builder per route) and
anything that shells out per item.

The brand marks live in `src/metasmith/gui/icon/` and are reached from the frontend through
vite's `$icon` alias rather than copied into it, so there is one of each. They are build-time
inputs despite sitting in the python package: `setup.py` ships `gui/static/**` and nothing
else under `gui/`, so what actually packages them is vite emitting them into the bundle, and
an icon referenced by neither `index.html` nor a component would not ship at all. The dir is
outside the vite root, which is why `server.fs.allow` has to name it for the dev server.

Two conventions shape the routes. **Every editable object is saved by `PUT /<collection>/<id>`
carrying the whole object**, identity field included, so an id differing from the url is a
rename applied as part of the save — what that costs differs by collection, since a
workflow's directory becomes the task bundle a run stages from while an agent is one yaml
nothing points into. And **incompleteness is reported, never refused, until launch**: you
make an agent days before its cluster exists in your ssh config, so `problems`/`valid` ride
on the payload and only the launch route enforces them.

An agent carries a **default preset and default params**, and a run layers its own over them
per key — the person clicking launch is the one least placed to know their login node needs
`slurm` and an account, so those are properties of the machine. Two traps sit under that.
`Agent.Pack`'s optional block **stringifies every value it writes**, which is right for the
names beside it and silently fatal for a mapping: it reloads as a quoted Python literal,
stays truthy, and yields no params at all — so a mapping field is packed on its own. And a
params *file* has nothing to merge into, so it wins whole and says so.

A value typed into a box is given **the type it looks like** — a JSON scalar becomes that
scalar, anything else stays a string, and a quoted number is the escape hatch. That rule
lives on the server so the CLI and the notebook get the same answer.

An agent's home field is **empty when the home is still the default**, with the default as
its placeholder — so the home follows a rename for as long as nobody has chosen one. Whether
it is still the default is the server's `home_is_default` to say, since `Source.Parse`
expands `~` and only that side knows what it expanded to. The test has to be exact on both
spellings: an emptied box saves as the default, so a suffix match would answer `True` for
someone's `/scratch/.../msm.<name>` and replace it with `~/msm.<name>` on their next save.

An agent created without a name is named after its **host**, not a bare adjective-noun slug —
`gui/names.py` composes `<word>-<host>` and stores the word as `agent_naming`, which is how
renaming an ssh alias can re-derive and rename the agent to match (`_repoint_agents`) while
leaving its home path untouched. A hand-typed name carries no such record, so it does not
follow a host rename.

---

## Release versioning

Two files under `src/metasmith/` form the version: **`version.txt`** (bare PEP 440 release
segment, source-controlled, bumped by hand, must not contain `+` or `-`) and
**`build_hash.txt`** (7-char md5 over the `src/metasmith/` tree, regenerated at build time,
gitignored, absent in fresh checkouts — everything then degrades to bare semver).
`constants.py` derives `FULL_VERSION = f"{VERSION}+{BUILD_HASH}"` and `CONTAINER_TAG` with
the single `+`→`-` substitution; the wheel name, the default agent container, `dev.sh`'s
`DOCKER_TAG`, and `testing/docker_builder` all read that one chain. Identical source ↔
identical hash ↔ identical image tag, which is what removes the chicken-and-egg of an
embedded commit hash. Pinned by `tests/test_container_tag.py`, `tests/test_dev_sh_tag.py`.

Bump: edit `version.txt`, commit, then build+publish (`./dev.sh --build-gui`, `-bp`, `-br`,
`-bd`, `-ud`, `-bs`), then tag. A release ships **both** a quay image and a conda package.

Two steps are load-bearing because their output is generated and never committed, so
skipping them ships something empty that nobody notices for a while:

- `--build-gui` — without it `src/metasmith/gui/static/` is empty until someone opens the
  page. `-bp`/`-bd` run `_assert_gui_bundle` and refuse; override `MSM_SKIP_GUI_CHECK=1`.
- `-br` — `--update_container` skips it, which is how one release shipped with 3 of 4 relay
  binaries replaced by 28-byte `echo 'stub relay'` stubs. `-ud`/`-bs` now run
  `_assert_real_relays` against the tagged image and refuse on a wrong-magic or <100 KB slot;
  override `MSM_SKIP_RELAY_CHECK=1`.

## `examples/` — minimal regression library

Top-level `examples/` is the smallest valid metasmith library — agnostic (no host or runtime
references) and reusable for any deploy/runtime smoke test: one namespace, one container
`.oci` pointing at the metasmith image itself, one transform (name → greeting), and a
committed `_metadata/`. Rebuild after editing with
`python -m metasmith build -t examples/data_types -r examples`. Used from smoke scripts such
as `main/local_mock/smoke_hpc_deploy.py`.
