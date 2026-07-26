# Agent Notes for Metasmith

## Environment

Use the `msm` mamba environment to run Python, tests, and CLI commands:
```
mamba run -n msm <command>
```

## What is Metasmith?

Metasmith is a workflow generation system for bioinformatics. You tell it what data
you have, what result you want, and which tools are available — and it figures out
how to chain those tools together into a Nextflow pipeline, then runs it.

The core idea is a **type system for bioinformatics data**. Instead of manually
writing pipelines that say "run tool A, pipe its output to tool B", you describe
data in terms of what it *is* (an NCBI assembly accession, a FASTA file, a
pangenome heatmap), and Metasmith works backwards from your target to find a
valid chain of transforms.

---

## The Five Building Blocks

### 1. DataTypes

DataTypes are the vocabulary. They're defined in YAML and describe data products
by their properties:

```yaml
# ncbi.yml
types:
    assembly_accession:
        extends: accession
        properties:
            database: assemblies
```

Types can extend other types (inheritance), and they live in namespaces like
`ncbi::assembly_accession` or `sequences::gbk`. The type system is structural —
matching is based on properties, not just names. A supertype cannot satisfy a
requirement for a more specific subtype.

### 2. DataInstanceLibraries

These are directories (`.xgdb`) that hold actual data — files and values — each
tagged with a DataType. You register your inputs here:

```python
inputs = DataInstanceLibrary(path)
group = inputs.AddValue("pangenome", "e coli", "pangenome::pangenome")
inputs.AddValue("K12", "GCF_000005845.2", "ncbi::assembly_accession", parents={group})
inputs.AddItem(Path("genome.gbk"), "sequences::gbk", parents={group})
```

Items can have parent-child relationships (genomes grouped under a pangenome).
The `AsSamples("type")` method splits the library into per-item views for
parallel processing — each sample includes the matched item plus its ancestors.

### 3. Transforms

Transforms are the tools. Each is a Python file with three parts:

**The contract** declares what types it consumes and produces:
```python
model = Transform()
dep   = model.AddRequirement(lib.GetType("ncbi::assembly_accession"))
gbk   = model.AddProduct(lib.GetType("sequences::gbk"))
```

**The protocol** is a function that actually runs the tool:
```python
def protocol(context: ExecutionContext):
    context.ExecWithContainer(image=image, cmd="datasets download ...")
    return ExecutionResult(manifest=[{gbk: out_path}], success=True)
```

**The instance** ties them together with resource requirements:
```python
TransformInstance(protocol=protocol, model=model, group_by=dep,
    resources=Resources(cpus=1, memory=Size.GB(1)))
```

Transforms run inside containers. The protocol has access to three path views:
`.local` (protocol working dir), `.container` (inside the container), and
`.external` (absolute host path). `ContextPath` enforces three invariants
post-construction: all three views are absolute, none contain `..`
segments, and they are mutually consistent — violations raise
`ValueError`. Protocols rarely build a `ContextPath` directly; the
framework hands them ready-made via `context.Input(dep)`,
`context.InputGroup(dep)`, and `context.Output(dep)`.

#### Extra container args

`context.ExecWithContainer(...)` accepts `args: list[str]` for arbitrary
runtime flags (e.g. `["--gpus", "all", "--shm-size=8g", "-e", "FOO=bar"]`).
Tokens are appended verbatim after the framework's default flags and binds,
just before the image — so a flag passed in `args=` wins over the default of
the same name (e.g. `--network=none` overriding the Docker default
`--network=host`). The caller is responsible for using the right dialect:
flag syntax differs between Docker and Apptainer.

The active runtime is readable on the context as
`context.container_runtime` (`ContainerRuntime.DOCKER` or
`ContainerRuntime.APPTAINER`), so a protocol can branch:

```python
from metasmith.coms.containers import ContainerRuntime

if context.container_runtime is ContainerRuntime.DOCKER:
    gpu_args = ["--gpus", "all"]
else:
    gpu_args = ["--nv"]
context.ExecWithContainer(image=image, cmd="...", args=gpu_args)
```

`binds=` remains a separate, typed parameter — do not pass mounts through
`args=`.

### 4. Workflow Generation

This is where Metasmith earns its keep. Given:
- Input data (DataInstanceLibrary with typed items)
- Available tools (TransformInstanceLibraries)
- Available resources (container images, shared libraries)
- A target type (e.g., `pangenome::heatmap`)

...it searches for a valid chain of transforms whose inputs and outputs connect
your starting data to the target. The result is a DAG (directed acyclic graph)
of steps.

For example, targeting `pangenome::heatmap` from NCBI accessions:
```
ncbi::assembly_accession → [getNcbiAssembly] → sequences::gbk
sequences::gbk (grouped) → [ppanggolin]      → pangenome::ppanggolin_matrix
pangenome::ppanggolin_matrix → [heatmap]      → pangenome::heatmap
```

The planner handles parallelism automatically — if you have 3 accessions, it
generates 3 parallel getNcbiAssembly jobs, then one ppanggolin that collects all
the resulting gbk files.

#### Declaring targets

`TargetBuilder.Add(target_type, parents=None)` returns an opaque `TargetSpec`
handle. Pass handles in `parents=` to link lineage-distinct forks:

```python
targets = TargetBuilder()
asm     = targets.Add("sequences::assembly")
mb_bins = targets.Add("binning::metabat2_bin_table", parents={asm})
sb_bins = targets.Add("binning::semibin2_bin_table", parents={asm})
# duplicate-type targets are allowed when lineage parents differ:
targets.Add("taxonomy::gtdbtk", parents={mb_bins})
targets.Add("taxonomy::gtdbtk", parents={sb_bins})
```

Two `Add` calls with the same `target_type` *and* the same `parents=` set raise
— structurally identical requests are still rejected. `WorkflowPlan.Generate`
takes `target_names: list[str]` aligned positionally with `target_model.requires`
(no Endpoint-keyed dict).

#### Diagnosing failed plans

When the solver can't produce a complete plan, `WorkflowPlan.hints` carries
structured `PlanHint` records (kinds: `unreachable_target`, `missing_input`,
`lineage_mismatch`) describing why. Each hint has a `target`, human-readable
`message`, optional reverse-BFS `chain` of requirements, candidate transforms,
and "did you mean ..." near-misses ranked by property-Jaccard to the givens.
`missing_input` hints are de-duped by demand shape and sorted by similarity to
givens so the most actionable suggestion is first. Consumers (the CLI,
agents) surface these to the user as diagnostic output on failure.

### 5. Agents and Execution

An Agent is a deployment target. It manages a home directory, handles container
orchestration, and compiles the generated workflow into Nextflow syntax.

```python
smith = Agent(home=Source.FromLocal(path), runtime=ContainerRuntime.DOCKER)
smith.Deploy()              # one-time setup, pulls metasmith container
smith.StageWorkflow(task)   # compiles DAG → Nextflow scripts
smith.RunWorkflow(task)     # launches Nextflow (async!)
```

#### Apptainer SIF ↔ sandbox decision

`Agent.Deploy()` runs `Container.MakeSandboxDecisionProbe()` against the
target host (login node for HPC, locally for WSL2) and acts on the
verdict it prints:

- **`use-sif`** — setuid `starter-suid` is present (kernel squashfs
  mount; HPC like Sockeye), **or** apptainer is older than 1.4 without
  setuid (sandbox path falls back to fuse-overlayfs, which races SIGBUS
  under SLURM array contention — Bug E.4 on fir 1.3.5). Deploy removes
  any stale `<name>.sandbox/` so the run-time ternary picks SIF.

- **`use-sandbox`** — apptainer ≥1.4 without setuid (the Bug E.2 surface
  on WSL2: SIF would engage squashfuse_ll and wedge under msm_relay's
  fork chain). Deploy runs `apptainer build --force --sandbox` if the
  directory doesn't already exist. The sandbox rootfs is read through
  unprivileged kernel overlayfs, never FUSE.

The probe is a two-axis static check (no `apptainer exec` at deploy
time): `[ -u .../starter-suid ]` first, then `apptainer --version` major
and minor compared against `1.4`. Verdict is re-evaluated on every
`Deploy()` call, so an apptainer upgrade flips the on-disk state on next
deploy. `assertive=True` prepends `rm -rf <sandbox>` so a forced
redeploy unconditionally re-probes and rebuilds.

`Container.MakeRunCommand(local=True)` emits the run-time ternary
`"$(if [ -d <sandbox> ]; then echo <sandbox>; else echo <sif>; fi)"` —
unchanged. Deploy controls which arm fires by controlling the directory's
presence on the target host.

Cache layout: `<store>/<name>.sif` (always retained) alongside
`<name>.sandbox/` (present iff verdict is `use-sandbox`). The store root
`<store>` is the single point of control `Container._store_root()`:
`${APPTAINER_CACHEDIR:-<home>/container_images}` — i.e. the host's
`APPTAINER_CACHEDIR` when set, else `<home>/container_images`. It is a shell
expression expanded on the *execution host* (like `$AGENT_HOME` in the same
strings), so the pull (write), sandbox build, and `exec` (read) sides always
agree. Both `GetLocalPath` and `GetSandboxPath` derive from it, keeping the
`.sif` and `.sandbox` siblings. Helpers in
`src/metasmith/coms/containers.py`: `_store_root / GetLocalPath /
GetSandboxPath / MakeSandboxDecisionProbe / MakeBuildSandboxCommand`.

RunWorkflow fires and returns immediately. The actual execution happens in a
Nextflow process that manages container pulls, job scheduling, and data staging.
You poll for completion by checking if the results metadata directory appears.

#### Waiting for a detached run

`RunWorkflow` is fire-and-forget: the agent shell launches Nextflow under
`nohup ... &` and returns as soon as the launch script exits. This is fine in
Jupyter (the user advances the cell manually) but a plain-Python caller that
immediately calls `GetResultSource` / `CheckWorkflow` will race past the run
and crash on a missing `runs/<key>/results` or `logs.<ts>/main.log`.

The agent writes a single sentinel line to `agent.log` when the Nextflow
process exits cleanly:

```
runs/<key>/_metasmith/logs.<latest>/agent.log:
  ...
  run completed at [<timestamp>]
```

Script callers should poll for that sentinel before reading results. A minimal
helper (works against either a local or remote home):

```python
import time
from pathlib import Path

def wait_for_run(task_dir: Path, poll_s: float = 10.0, timeout: float | None = None):
    """Block until the agent writes 'run completed at' to the latest agent.log."""
    deadline = None if timeout is None else time.monotonic() + timeout
    while deadline is None or time.monotonic() < deadline:
        logs = sorted((task_dir / "_metasmith").glob("logs.*"))
        if logs:
            agent_log = logs[-1] / "agent.log"
            if agent_log.exists() and "run completed at" in agent_log.read_text():
                return True
        time.sleep(poll_s)
    return False

# usage:
smith.RunWorkflow(task, ...)
wait_for_run(Path(agent_home_path) / "runs" / task.GetKey())
results_path = smith.GetResultSource(task).GetPath()
```

A first-class `wait`/`WaitForRun` API is tracked for 0.18.

#### Resource overrides at run time

`RunWorkflow(resource_overrides={key: Resources(...), ...})` overrides
per-process `cpus` / `memory` / `duration` for the run without re-staging.
The mechanism is selector-based and relies on a specific Nextflow precedence
rule — load it before adding new override shapes or debugging "the override
didn't apply."

**Two files, two selectors.** Stage time emits `workflow.resources.nf` with
one block per step using an **exact** selector (`step.transform.name`
prefixed by `pNN__`); run time emits an appended block in
`workflow.config.nf` with a **regex** selector derived from the key:

| Key shape | Emitted run-time selector |
|---|---|
| `"all"` / `"*"` | `withName: '.*'` |
| `int` (step index) | `withName: 'pNN__.*'` |
| `str` (transform name) | `withName: '.*__<str>'` |
| `Transform` / `TransformInstance` | `withName: '.*__<.name>'` |

The runner passes the two files in order: `nextflow -config
./workflow.resources.nf -config ./workflow.config.nf ...` (see
`agents.py:1344-1346`).

**Precedence rule that makes it work.** Nextflow does **not** apply "exact >
regex specificity" across selectors from different config sources. The
rule that actually applies is: **per-directive last-defined wins**, where
"last" = the later of the two `-config` flags. The exact `pNN__<name>`
block from `workflow.resources.nf` is loaded first; the regex block from
`workflow.config.nf` is loaded second, so any directive set in both files
takes the run-time value. Directives set only in the stage file (e.g.
`memory = { (2**(task.attempt-1)) * (... as MemoryUnit) }` retry-scaling
closure) fall through unchanged. `nextflow config -config A -config B` is
the quickest way to inspect the merged tree when troubleshooting.

**Caveats.**

- The regex selectors are **case-sensitive** Java regexes. `'.*ncbi.*'`
  will not match `p01__getNcbiAssembly`; the user's regex has to handle
  case explicitly or use the transform's literal name.
- Local-executor caps (`params.executor.memory` in `local.nf`) reject
  asks that exceed available host memory before the run starts — the
  override is honored; Nextflow is just rejecting the resulting request.
  Bump the executor cap or use a smaller override for local smoke tests.
- The "won't mess with caching" comment at
  `src/metasmith/models/workflow.py:1265` is load-bearing:
  `workflow.resources.nf` content stays stable across runs with
  different overrides (overrides live in `workflow.config.nf`), so
  Nextflow's per-task hashing is unaffected by override differences.

If an override appears to be silently dropped, check in this order: the
regex actually matches the staged process name (`pNN__<transform.name>`),
no Groovy parse error in `.nextflow.log` killed the include, no third
config layer was loaded after `workflow.config.nf`, and `sacct` request
columns (`ReqCPUS`/`ReqMem`) match the override — `AllocCPUS`/`AllocMem`
reflect SLURM partition rounding, not what Nextflow requested.

---

## How It All Fits Together

```
  You have:                    You want:
  ┌─────────────┐              ┌─────────────────┐
  │ 3 NCBI      │              │ pangenome        │
  │ accessions  │─────────────▶│ heatmap (SVG)    │
  │ + 1 gbk     │              └─────────────────┘
  └─────────────┘
         │
         ▼
  ┌──────────────────────────────────────┐
  │ Metasmith workflow planner           │
  │                                      │
  │ Searches: accession → ??? → heatmap  │
  │ Finds:    getNcbiAssembly            │
  │           → ppanggolin               │
  │           → heatmap                  │
  └──────────────────────────────────────┘
         │
         ▼
  ┌──────────────────────────────────────┐
  │ Nextflow pipeline (auto-generated)   │
  │                                      │
  │ p01: getNcbiAssembly (x2, parallel)  │
  │ p02: ppanggolin (x1, collects gbks)  │
  │ p03: heatmap (x1)                    │
  └──────────────────────────────────────┘
         │
         ▼
  results/pangenome-heatmap/*.svg
```

The value proposition: you never write Nextflow. You define types, write
transform contracts, and Metasmith handles the plumbing. Adding a new tool means
writing one Python file that declares its inputs/outputs and how to run it.
The planner automatically incorporates it into any workflow where it's useful.

---

## Instance Identity & Serialization

Each `DataInstance` has a stable `instance_id` (10-char hash derived from path, dtype name, and parent library). This ID persists across:
- Type retyping via `WithDType()` — the instance keeps its identity even when viewed as a different type
- Serialization/deserialization — `Pack()`/`Unpack()` explicitly preserve `instance_id`
- Workflow steps — `WorkflowStep.Pack()` uses a v2 schema that stores instances by `instance_id`

This enables reliable lineage tracking: use `DataInstanceLibrary.Load()` + `Trace()` to map results back to inputs rather than parsing filenames or work directories.

### Live-masking transforms

`TransformInstanceLibrary.AsView(mask: set[Path], invert=False)` returns a
`TransformInstanceLibraryView` that filters `IterateTransforms` to (or away
from, with `invert=True`) the given `.py` paths. Pass the view into
`Agent.GenerateWorkflow(transforms=[...])` or `WorkflowPlan.Generate(...)` in
place of the underlying library to hide transforms by file path without
rebuilding the library on disk. Mirrors `DataInstanceLibrary.AsView`.

### Testing Without Containers

The `virtual_runtime` module provides a test harness for transforms that doesn't require container setup:
- `TransformHarness` tracks instance_id in test scenarios
- Useful for validating transform contracts (inputs/outputs/types) without pulling images

### Path translation

All conversion between the local / external / container path views
lives in `src/metasmith/models/paths.py`. The two classes:

- **`PathMap`** — per-execution context (carries `extern_home`,
  `task_key`, optional `extern_cwd`). Built via `PathMap.FromAgent(agent,
  task_key)` from `ExecuteStep`, or `PathMap.FromExternalCwd(cwd, agent)`
  from `bin/sbatch`. Exposes `LocalToExternal` / `ExternalToLocal` /
  `LocalToContainer` / `ContainerToLocal` for typed reroots using
  `relative_to`, plus `Parse(p)` (the consolidator that handles
  `/ws/<tail>` absolute, `../ws/<tail>` relative, HOME_ROOT-rooted
  symlinks, and foreign symlinks uniformly) and `Render(p, dialect)`
  (prefix-aware token substitution for `$AGENT_HOME` / `{agent_home}` /
  `${params.home}`).
- **`ContextPath`** — value type. Frozen, three Path fields, invariants
  enforced in `__post_init__`. Build via classmethods (`FromLocal`,
  `FromExternal`, `ForOutput`) when a `PathMap` is in scope.

For shell-script content rewrites (e.g. `bin/sbatch.fix_paths` patching
a `.command.run` body), use the `reroot_in_text(content, old_root,
new_root)` helper: it matches the root only at path-segment boundaries
so inner occurrences like `/msm_home_old_backup` or `/wsadm/ws/` are
not corrupted.

The container is dual-bound: the host scope dir lands at both `/ws` and
`/msm_home`. When Nextflow resolves a process work-dir through the
home-bind rather than the work-bind, `bin/sbatch` sees `cwd` under
`AgentPaths.HOME_ROOT` (`/msm_home`) instead of `WORK_ROOT` (`/ws`) —
the cwd-to-host mapping must check both prefixes and route HOME_ROOT
cwds through `agent.real_path`. Pinned by
`tests/path_overhaul/test_sbatch_home_root_cwd.py`.

Never use raw `str.replace(extern_home, ...)`, `str.replace(HOME_ROOT,
...)`, or regex like `r"/\w*/nxf_work/.*"` for path translation —
those shapes silently corrupt or misidentify; the helpers above are the
prefix-aware replacements.

---

## Key Patterns

### Transform Structure (Template)
```python
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("containers::tool.oci"))
input = model.AddRequirement(lib.GetType("namespace::type"))
out   = model.AddProduct(lib.GetType("namespace::output_type"))

def protocol(context: ExecutionContext):
    # context.Input(slot) / context.Output(slot) for paths
    # context.ExecWithContainer(image=image, cmd="...")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=input,
    resources=Resources(cpus=N, memory=Size.GB(M), duration=Duration(hours=H))
)
```

### Adding New Transforms Checklist
1. Create data types in `data_types/<namespace>.yml`
2. Add container type to `data_types/containers.yml` with `provides:` list
3. Create `.oci` file in `resources/containers/` with docker URL
4. Update `resources/containers/_metadata/index.yml`
5. Write transform in `transforms/<category>/<transform>.py`
6. Update `transforms/<category>/_metadata/index.yml`
7. **Propagate types**: Copy compiled types to ALL `_metadata/types/` directories

### Type Propagation (Critical!)
Every transform library has its own `_metadata/types/` directory. New types must be copied to ALL of them:
- `transforms/amplicon/_metadata/types/`
- `transforms/assembly/_metadata/types/`
- `transforms/functionalAnnotation/_metadata/types/`
- `transforms/logistics/_metadata/types/`
- `transforms/metabolicModelling/_metadata/types/`
- `transforms/metagenomics/_metadata/types/`
- `transforms/pangenome/_metadata/types/`
- `resources/containers/_metadata/types/`

Compiled type format (different from source):
```yaml
ontology:
  doi: https://doi.org/10.1093/bioinformatics/btt113
  name: EDAM
  strict: false
  version: '1.25'
schema: v1
types:
  typename:
    properties:
      _: description
      ext: file_extension
```

### Avoiding Type Collisions
Use the `content:` property to distinguish types that might otherwise structurally match:
```yaml
bin_directory:
  properties:
    content: metagenomic bins  # Distinguishes from other directories
```

## CLI

Metasmith exposes its **full** Python API as a CLI under `metasmith` (alias `msm`). The same surface is used by humans typing into a shell and by LLM agents shelling out with `--json` for machine-readable output. There is no server process; each invocation loads what it needs from disk and exits.

The canonical reference is **`docs/source/agentic/`** (see `tool_reference.rst` for the full catalog).

### Global flags

```bash
metasmith [--json] [--quiet] [--workspace PATH] COMMAND ...
```

- `--json` — emit machine-readable JSON on stdout (progress logs are routed to stderr so they don't corrupt the stream)
- `--workspace PATH` — workspace for cached workflow tasks (default `~/.metasmith/workspace`; env `METASMITH_WORKSPACE`)
- `--quiet` — suppress non-essential output

Errors print to stderr and exit non-zero — they are not swallowed into `{"error": ...}` dicts.

### Command tree

| Group | Subcommands |
|-------|-------------|
| `metasmith type` | `list`, `show`, `compat`, `create`, `add` |
| `metasmith data` | `inspect`, `list`, `create`, `attach-types`, `add-item`, `add-value`, `set-parents`, `remove`, `rename`, `rename-by-parent`, `prune-types`, `consolidate`, `save`, `trace`, `load-remote`, `lineage` |
| `metasmith transform` | `list`, `libraries`, `show`, `read`, `write`, `scaffold`, `validate`, `propagate-types` |
| `metasmith plan` | one-shot planner (`--data-library`, `--sample-type`, `--target-type ...`, `--transform-library ...`) |
| `metasmith workflow` | `stage`, `run`, `wait`, `tail`, `cancel`, `runs`, `check`, `collect`, `result-source`, `presets` |
| `metasmith agent` | `list`, `info`, `save`, `ping`, `deploy` |
| `metasmith source` | `parse`, `exists`, `transfer` |
| `metasmith task` | `list`, `show`, `hints`, `dag`, `delete` |
| `metasmith build` | `all` (default), `types`, `uniques`, `transforms` — compile data type, unique, and transform libraries |
| top-level legacy | `get`, `lab`, `gui`, `api`, `help` |

### Workflow via CLI

The full lifecycle is:

```
metasmith plan ...                     → task_key (cached to workspace)
metasmith workflow stage AGENT TASK    → compile DAG → Nextflow → transfer
metasmith workflow run AGENT TASK      → detached launch under nohup
metasmith workflow wait AGENT TASK     → blocks on `run completed at` sentinel
metasmith workflow tail AGENT TASK     → last N lines of agent.log / main.log
metasmith workflow collect AGENT TASK --dest URI
```

`workflow run` is detached (the launcher exits as soon as `nohup nextflow … &` starts). Script callers MUST follow with `workflow wait`, which blocks on the `run completed at` sentinel in `runs/<task_key>/_metasmith/logs.latest/agent.log`. `workflow cancel` cleanly stops a run by removing `workspace/PID.lock` (the in-container loop catches the absence and gracefully kills nextflow).

Tasks are cached to disk under `--workspace` and can be re-fetched via `metasmith task show <key>` or `metasmith task list`.

## Web GUI

`msm gui` serves a localhost page covering the same run path as the notebook —
host, agent, inputs, plan, run, results — without writing Python. Transform
*authoring* is deliberately absent; that stays in the notebook and CLI.

`src/metasmith/gui/` is a **second veneer over `metasmith.ops`, exactly as the CLI
is**. Routes call ops functions directly; nothing shells out to the command line.

| module | holds |
|---|---|
| `store.py` | the project directory: `agents/`, `workflows/<name>/`, `runs/<name>/` |
| `names.py` | readable names — `blazing-ape`, run `blazing-ape-0XwE9` |
| `stdlib.py` | the `MetasmithLibraries` clone + bootstrap, shared with `msm lab` |
| `sshconfig.py` | the marked block metasmith owns in `~/.ssh/config`, plus generated identity keys |
| `jobs.py` | background work (deploy/generate/stage/collect) and SSE log streams |
| `watcher.py` | rediscovers live runs from disk each cycle |
| `api.py` / `app.py` | routes and the Flask app |
| `static/` | built bundle — generated by `./dev.sh --build-gui`, never committed |

These are load-bearing:

- **The task bundle sits at the root of the workflow directory.** That is what makes
  `metasmith workflow stage AGENT workflows/blazing-ape` work: `ops.workspace`
  resolution accepts a bundle directory as well as a workspace key. `input.xgdb`
  stays live and editable beside the bundle's frozen copy under `data/`.
- **The failure case has an on-disk form.** `plan_workflow` returns early on an
  unsolvable target without building a task, so `request.yml` + `result.yml` are the
  only record of a failed generate; without them a browser reload would lose it.
- **Planning is not reentrant.** `TransformInstance.Load` imports each transform by
  bare module name, mutates `sys.path`, calls `importlib.reload`, and returns the
  result through a *class* attribute — all process-global. Two `plan_workflow` calls
  in one process clobber each other and fail with a bare
  `spec not found for the module`. The CLI never hits this (one process, one plan);
  the GUI can, so `api.py` serialises generates behind `_plan_lock`. **`stdlib.type_index`
  goes through the same import path** and so runs under that same lock — without it,
  opening a workflow while another one plans breaks both.

- **The type index is built whole and filtered in the browser.** `stdlib.type_index`
  walks *every* transform library found, not the enabled subset, and returns one
  `transforms` list plus a `by_type` map of entries pointing into it. Toggling a library
  is then instant and never refetches, which is what lets a row of the recipe show live
  produced-by / consumed-by counts while a type is being typed. It is cached against
  the stdlib commit; one unloadable library records its error and costs only itself.

- **The index matches on properties, not on names.** `Endpoint.IsA` is a property-subset
  test (`x.IsA(y)` iff `y.properties ⊆ x.properties`), so `produced_by[T]` is every
  transform with a product `P` where `P.IsA(T)`, and `consumed_by[T]` every transform with
  a requirement `R` where `T.IsA(R)`. Keying on names alone told the user
  "nothing can make this — the plan will not solve" about `sequences::assembly`, which six
  transforms produce a narrower assembly for, and "nothing takes this" about a
  `sequences::flye_assembly` twenty transforms accept — 34 produce matches, 159
  non-plumbing consume matches, and 42 cross-namespace aliases, all invisible. Entries are
  `{i, as, match}`: which transform, the type it actually *declared*, and how that relates
  to the one asked about (`exact` | `alias` | `narrower` | `broader`), because
  "takes it as something more general" is a different thing to know than "takes exactly
  this" — and without `as` the transform looks like it named your type and did not. Exact
  matches sort first; a transform appears once per side, under its closest relation. The
  asymmetry is the point and is pinned by test: a supertype never satisfies a subtype's
  requirement. `by_type` is keyed over *every* named type, from the standalone type files
  and each transform library's own `_metadata/types/` — a polymorphic match can exist for
  a type no transform names, and a key with two empty lists is a different answer to a
  type the index has never heard of.

- **A target is a type *and* the targets it descends from.** `target_types` entries are
  either a bare type name or `{"type": ..., "parents": [i, ...]}`, where each `i` indexes
  an **earlier** entry in the same list — `ops.workflow._add_targets` refuses a forward
  reference by position. That is what makes two targets of one type distinct requests
  rather than the duplicate `TargetBuilder.Add` rejects. Both spellings are read from
  `request.yml`, so workflows written before lineage existed still load. Because the
  link is positional, removing a target renumbers the rest and drops any link *into*
  the removed one — the GUI says so out loud when it happens.
- **The ssh block is written first in the file.** ssh takes the first value it finds
  per keyword, so a `Host *` above it would set User or IdentityFile for a brand-new
  alias — an entry that parses cleanly, displays correctly, and connects as the wrong
  user. Collision detection follows `Include`; wildcard patterns are defaults, not
  destinations. An alias declared in more than one block is **one** host: `resolved()`
  collapses the parse to one entry per pattern, merging keywords first-wins the way ssh
  reads them. That is not cosmetic — the rail keys its rows by alias, and a duplicate
  key aborts the Svelte render for the *whole page*, not just the list.
  Both halves of the file are editable (`write_all`), but only the *layout* is fixed:
  the block is always reassembled from its marker and written first, and markers in the
  native half are refused. Generated keys are ed25519 under `~/.ssh/metasmith/`, are
  never overwritten (an existing pair is returned with `created: False`), and only the
  `.pub` is ever read — the private half never reaches a response body. Deletion is
  scoped to that directory for the same reason ownership is scoped everywhere else here:
  removing the wrong key locks someone out of a machine with no undo.

- **An agent's `home` is re-parsed every time it is saved.** `PUT /agents/<name>` feeds
  the stored address straight back through `Source.Parse`, so that function has to be a
  fixed point on its own output. It was not: `SshSource` renders `ssh://host:path` while
  `Parse` split host from path on `/`, read the `:` as part of the host, and emitted a
  second one — a remote agent's home grew a colon per save until nothing could reach it.
  `Parse` now delegates the `:` form to `SshSource.Parse` and expands `~` for local
  paths. Pinned by `tests/models/test_source_parse.py::TestSshRoundTrip`.

Frontend source is `frontend/` (Svelte 5 + Vite). Node is a **build** dependency
only and is deliberately absent from `envs/base.yml`; flask and coolname are runtime
dependencies and are in it.

**A workflow is created the moment it is asked for.** There is no form in front of it:
`+ workflow` POSTs and selects, and the generated name is editable on the page you land
on — the heading *is* the field. `store.rename_workflow` allows that only before a
generate, because the workflow directory becomes the task bundle a run stages from, and
moving it afterwards would strand the bundle under a name nothing points at; `fork` is the
deliberate way to get a new name later, and it says out loud that it discards cache reuse.
The archive mark is keyed by name and moves with the directory. Two things bite here: the
existence check is not a lock, so a losing race must surface as a refusal rather than an
`OSError` out of the route (`Path.rename` raises on a non-empty target); and Enter closes
the field, which unmounts the input, which fires `blur` — so the view has to refuse the
second commit or the rename is sent twice and races itself.

The workflow pane is one column plus a panel. The recipe card holds inputs and outputs
as one flat list under two headings, and that list *is* the form — `+ a file` /
`+ a value` / `+ an output` append an empty row you fill in place, rather than a builder
card below the list whose result appears somewhere else. The generate button sits between
the recipe and the result, which renders the DAG on success and the planner's hints on
failure in the same slot. The right-hand `SidePanel` is the rail's mirror — same grip,
same remembered width, collapsing to a strip — and shows what sits on either side of
whichever type is in focus. Container and `lib::` requirements are hidden from that
readout (counted as "supplied"), the same namespaces `render_dag` blacklists: they are
never a user's to register, and listing them buries the requirement that is.

- **The panel is outside the scroll, not inside it.** A panel within the scrolling box is
  not a panel: the page's scrollbar ends up to the *right* of it and it slides under the
  header, which is why it used to fake staying put with `position: sticky` and a `100vh`
  guess. So `main` drops out of the scrolling business for this one view — `App.svelte`
  gives it a `flush` modifier (no padding, no overflow, a flex row) whenever a workflow is
  selected — and `WorkflowView` scrolls its own column instead. Left to right that reads:
  the page, the page's scrollbar, the panel, the panel's scrollbar. The modifier and the
  panel have to appear together; a `main` left flush under another view would silently kill
  that view's scrolling. The panel is furniture like the rail (full height, one border on
  the inner edge, no radius) and is split across as well as down: a fixed upper section
  holding the libraries and the graph, a horizontal grip, and the lower list that scrolls.
  All three remembered widths/heights live in `lib/state.svelte.js` beside `railWidth`.

- **The graph is laid out in the browser, from the index already in it.** No route and no
  fetch — the index is shipped whole precisely so a library toggle costs nothing, and a
  server-rendered picture would put a round trip back in front of every click. `lib/graphs.js`
  turns the index into `{nodes, edges}` for three cases (a tool, a library, a type's
  neighbourhood, capped per side); `lib/dagLayout.js` is a Sugiyama pipeline in miniature —
  break cycles, longest-path rows, thread long edges through invisible lane nodes, median
  ordering, bounded straightening; `components/MiniGraph.svelte` draws edges in one SVG layer
  with the nodes as ordinary buttons over it, so clicking one moves the panel onto it.
  Four things there are load-bearing. **Cycles must be broken first** — a library graph is not
  guaranteed acyclic (one tool consuming and producing the same type is enough) and a
  longest-path walk over a cycle does not terminate. **Straightening is bounded by the widest
  row**: pulling a node toward its parents only enforces a *minimum* gap, so without a ceiling
  rows drift apart and the assembly library lays out at 1269px instead of 613px. **Lanes are
  charged a sliver of clearance, not a column**, or routing costs more width than it saves.
  And boxes are a **fixed width**, which is what removes the measure pass: layout is pure data.
  Two of the three cases match on properties, not names — a library's chain is only continuous
  because the `as`/`match` entries bridge a narrower product to a broader requirement.

- **A transform's requirements are indexed per slot, not just as a set of names.** A third of
  the standard library declares `AddRequirement(..., parents={...})` — bbduk does not want
  three files, it wants the reads belonging to the metadata and the stats belonging to those
  reads — and `inputs`/`outputs` cannot carry that, because they de-dupe and a slot has no
  stable position in them. So `type_index` also emits `requires`: one entry per slot in
  declaration order, `{as, parents}` with `parents` holding positions in that same list.
  Plumbing slots stay in it so the positions are the transform's own; the consumer drops them
  by namespace. **Parents are resolved by object identity first**: `Dependency` inherits
  `Node.__eq__`, which compares property *signatures*, so two slots of one type are equal and
  `==` alone would answer with whichever came first. `transformGraph` turns those into
  `kind: 'lineage'` edges between the input boxes, styled apart from the grey flow edges
  because "this input must descend from that one" is not the same statement as "this tool
  takes that type"; two slots of one type collapse onto one node and are dropped rather than
  drawn as a self-loop.

- **A half-built input row is a draft, not a library item.** `add_item` needs a type *and* a
  path, so a row you have only started cannot live in `input.xgdb`. It lives in `request.yml`
  under `input_drafts`, beside the targets that have always been request-only, and it POSTs
  itself the moment it has a type, an identity, and every parent it names is registered — so
  a chain of drafts commits in cascade as the top of it is filled in, each commit rewriting
  the `#<draft-id>` references below it to the path that just landed. Two consequences.
  Editing is written back **when a field is left**, not per keystroke, and `persist()` is
  serialised: it is one file written by write-then-rename, so two overlapping writes raced
  for one `.tmp` path (`_write_yaml` now names it per pid/thread as well).

- **A registered row is corrected in place, and correcting it moves no file.** An input row is
  two lines — what it points at, then what it is and what it came from — and clicking either
  the path or the type on a *registered* row opens the same field a draft row has. Both go
  through new ops beside `rename_item`: `retype_item` (validate through the library's own
  `GetType`, swap the manifest value) and `repoint_item`, which branches on one thing only.
  An **absolute** entry is a pointer at the user's file, so re-pointing it is a manifest
  re-key and the filesystem is never touched; a **relative** entry is library-owned (what
  `AddValue` writes) and delegates to `DataInstanceLibrary.Rename`, where moving the file is
  the correct behaviour. `PUT .../inputs/items/{type,path}`, PUT for the same reason the
  parents route is. Three traps. A parent is stored as metadata carrying the parent's path
  *and* type, so a re-keyed or retyped row leaves its children describing something the
  manifest no longer holds — `_relink_children` rebuilds each affected child through
  `SetParentsOf` rather than reaching into the metadata objects (`Rename` does *not* do this,
  which is a latent bug, not a licence). `AddItem`/`Rename` assert against a taken key, so a
  collision is asserted for up front and comes back as a notice. And identity is derived from
  path and type, so either edit changes the row's `instance_id` and costs cache reuse
  downstream — which the row says, in place of the old "remove and add it again".

- **The parents are the control; the menu is the add button.** `components/ParentPicker.svelte`
  states what a row descends from one line per parent, each removable on its own `×`, under a
  `+ parent` dropdown that only ever *adds*. It replaced a menu of every candidate with ticks,
  which put what a row *does* descend from behind a click inside a list of what it does not —
  and those are not equally interesting: the parents are part of reading the row, the
  candidates are wanted only while you are adding one. What descends from a row is not shown
  at all; it is stated on those rows, and offering it twice gives one link two places to be
  edited from. Hovering a parent line — or an option in the menu — marks the row it names,
  because the label is a path on an input and a type name on an output and neither is unique
  enough to find by eye. Three traps. `set_item_parents` → `AddParentsTo` is a **union**, so
  it can add a parent and never take one away; the route goes through `replace_item_parents` →
  `SetParentsOf`, and PUT was already the right verb for it. `show_item_lineage` reports the
  **transitive closure**, because the library expands the chain on `Load` and collapses it
  again on `Save` — left alone, the menu offers a grandparent whose removal silently reverts
  on the next load, so the browser collapses it the same way. And a target's parents are
  stored as **numbers** while every row on the page is keyed by string, so `targetRows`
  converts at the row model and `setParents` converts back: left unconverted the two key
  spaces never met, and the tick never rendered, the children readout was always empty, and
  the summary fell through to printing the raw 0-based position — which is the one place a
  person ever saw an output's index. Removing an item clears the links into it in both halves
  and says how many, since the library leaves them dangling.

- **A cycle is filtered out of the menu, and refused by the route.** A row cannot descend from
  something that descends from it: nothing downstream is defined over a loop — `AsSamples`
  walks up to the ancestors and then back down to their descendants, so one mask becomes the
  whole library, and the expand-on-load / collapse-on-save pair has no meaning over a cycle.
  The browser excludes self, existing parents, and the whole descendant closure (a fixpoint
  over the collapsed links, since items arrive as a closure and drafts state one level);
  `ops.data._assert_acyclic` refuses the same thing on `set_item_parents` and
  `replace_item_parents`, because the route is reachable without the page. Outputs need none
  of that: they may only name a target declared *before* them, which `_add_targets` already
  asserts, so ordering does the check for free — and the menu offers only earlier rows rather
  than offering a link the generate then throws out.

- **An output row is an input row with the path line taken off.** Not a resemblance to be
  re-derived in two places: `RecipeCard` renders one `detail` snippet for both, so the type
  field, the parents and the trailing `×` cannot drift apart by a column (the output's
  typecell was `max-width: none` while the input's capped at 360px, which is exactly how they
  did). The trailing cell is fixed-width whether or not it holds a delete, which is what puts
  an output's `×` over the one on an input's first line. An output carries no ordinal: it is
  named by its **type** everywhere a person reads it, with the 1-based position appended only
  when two outputs share a type and the name alone would point at either.

- **A type is a word until it is clicked, on every row.** A registered input, a draft and an
  output all name a type, and one of them rendering a permanently-open combobox while the
  others read as a word made the list look like three kinds of thing. Clicking the word moves
  the panel onto that type *and* opens the field — which is the whole reason the type is the
  thing you click — so `TypeSelect` takes an `autofocus`, or the click lands in a field that
  is not listening. Nothing is highlighted by that click: the row mark used to key on the
  focused *type*, so touching an input lit up every row sharing its name, in both halves. The
  only mark left comes from a pointer resting on a parent line somewhere else.

- **`apply` stamps a transform's input shape into the recipe.** Beside each card in
  `TypeInspector`, and in the panel header while a tool is drawn: one draft per non-plumbing
  requirement, typed as declared and wired per the declared lineage, with the paths left for
  the user. A requirement an existing input already satisfies is skipped, and "satisfies" is
  read off `by_type[T].consumed_by` — property matching that the index already did, not name
  equality, so a registered `flye_assembly` counts for a slot wanting an `assembly`. A draft
  of the same type occupies the requirement too, or applying twice would build a second copy
  of everything — but a draft only *satisfies* it once it has an identity: a blank row this
  same button made is a row you still have to fill in, and counting it is what let a second
  press claim every input was already here. A filler is consumed, so one file cannot answer
  three slots. What it skipped is reported per requirement, naming what stands in for each,
  because "every input is already here" is a claim you cannot check from where you read it —
  and the notice has to be written *after* `persist()` resolves, since `attempt` clears the
  notice on its way in. The index keeps only the *best* match per transform per side, so a
  tool wanting both a broad and a narrow flavour of one type gets one extra row; that is a
  spare row to delete, not a wrong plan.

- **A transform card in the panel is two plain lists.** `inputs` over one de-duped type name
  per line with `+N supplied` as the last line of that list, then `outputs` the same way —
  not prose fragments ("takes" / "also needs" / "produces") with the types as chips wrapped
  across a line, which reads as a sentence rather than as what goes in and what comes out.
  The type in focus stays in the list like any other: filtering it out made a card look like
  it named a type it did not. Two consequences — the supplied count is over the transform's
  *whole* requirement list (excluding the focus left it one short on the cards where the focus
  is itself plumbing), and a line has to `word-break`, since the card is the narrowest column
  on the page.

- **Which libraries are enabled is not part of adding a row.** It is what the planner may
  reach for, so `views/LibraryList.svelte` holds it in the panel — beside the counts and the
  graph it narrows — rather than in a fold in the middle of the recipe. Each row carries an
  eye and a checkbox, and they are deliberately independent: the eye draws a library whether
  or not it is enabled, and switching one off does not yank the graph out from under you. A
  refused toggle has to be undone by hand: the last library may not be switched off, but a
  native checkbox has already flipped itself by then and nothing re-renders it, so the row
  would show unticked over a library that is still enabled.

- **Fuzzy matching is ranking, not membership.** `lib/fuzzy.js` scores every type name against
  what was typed — exact, prefix, the bare name under a namespace, a whole word, a word start,
  a substring, then a subsequence, with ties broken on how tightly the matched characters sit
  and how early they start. Subsequence matching alone is far too generous (`gbk` is a
  subsequence of half the library), so the ranking *is* the feature. It only decides what the
  list offers: whether a type exists is still the exact `typeNames.has(...)` test on the
  row, and a name that merely ranked well is not a name you can register.

- **Every call goes through one api service, which is what pays for the header's dot.**
  `lib/api.svelte.js` is a class holding `$state` and exported as a single instance; every
  component imports that one, and `components/StatusDot.svelte` reads `api.status` — nothing
  subscribes by hand and nothing else keeps a copy. Green means the server answered, yellow
  that a mutating request is in flight, red that one never arrived. Four things there are
  load-bearing. A reply of **any** status is proof of life — a 409 refusal and a 500 are both
  the server talking — so only a `fetch` rejection turns the dot red. A save is held yellow for
  a floor of **200ms**: most of them return inside a frame, and a light that lasts one frame is
  a light nobody sees; overlapping saves share one window, ending when the last has landed and
  the newest has had its 200ms. The dot cannot be only as fresh as the last click — a server
  killed in its terminal would read green until someone tried to save into it — so `api.watch()`
  polls `GET /api/health` every 5s, skipping a hidden tab and re-pulsing the moment it is looked
  at again; that route is deliberately the cheapest in `api.py` (no project, no disk), because
  `/project` answers the same question but re-walks the standard library each time. And the
  label renders **all three words stacked in one grid cell** with the inactive ones
  `visibility: hidden`, so the box is as wide as the widest word: sizing it to the live text
  would shove the path and the four links sideways every time a save started. `visibility`
  rather than opacity, so the hidden words stay out of the accessibility tree.

The page has no network of its own — it is served from a bundle and never reaches a CDN.
So anything that would normally be a small dependency is inlined instead:
`components/Icon.svelte` carries the header glyphs as SVG paths (the docs site's own set),
and `components/ConfigEditor.svelte` does syntax highlighting with a transparent textarea
over a painted underlay rather than pulling in an editor. Both layers of that editor must
keep identical font, padding and wrapping; they are what put the caret on its glyph.
`components/TypeSelect.svelte` is the third: a combobox written out rather than installed.
It replaced `<input list>` + `<datalist>`, which was the right behaviour in the wrong shape
— the browser renders a datalist as its own pale bubble, sized to its taste, with the
option labels as a dim aside, so the produce/consume counts that are the whole reason to
open the list read as a hover hint. It is also un-styleable and un-scriptable: there is no
hook for an active row, a warned row, or a count that narrows as libraries are toggled. The
caret is a drawn SVG chevron because `▾` renders as a faint speck at that size, and that
glyph is the affordance saying a list opens at all.

---

### Library architecture

Each call loads its inputs by path; there is no persistent in-memory state. Cold-load cost for a parse of types / data / transform manifests is small enough that re-loading per invocation is acceptable. Caching that the old MCP server held in `ServerState` is now just disk reads of `.xgdb` manifests, type YAMLs, and workspace task files.

---

## Lessons Learned

### Metagenomic Binning (Feb 2026)

**Tools implemented:**
- **COMEBin** (`comebin.py`): Highest accuracy, slow, 32GB RAM, short-read focused
- **SemiBin2** (`semibin2.py`): Fast, 16GB RAM, supports both short AND long reads

**Container discovery:**
- Use `mamba search -c bioconda <tool>` to find correct container tags
- Biocontainers format: `quay.io/biocontainers/<tool>:<version>--<hash>`
- Example working tags:
  - `quay.io/biocontainers/comebin:1.0.4--hdfd78af_1`
  - `quay.io/biocontainers/semibin:2.1.0--pyhdfd78af_0`

**COMEBin gotchas:**
- `run_comebin.sh -h` returns "illegal option" (not standard help)
- Expects BAM files in a directory (`-p` flag), not individual files
- Output structure: `{workdir}/comebin_res/comebin_res_bins/` and `comebin_res_contig_bin.tsv`

**SemiBin2 notes:**
- Auto-detects read type but can set `--sequencing-type short_reads|long_reads`
- Uses different clustering: community detection (short) vs DBSCAN (long)
- Output: `{workdir}/output_bins/` and `contig_bins.tsv`
- Optional `--environment` for pre-trained habitat models (global, human_gut, etc.)

**BAM inputs:**
- Both binners need reads aligned back to the assembly for coverage information
- Coverage patterns help cluster contigs from the same organism
- Workflow: reads -> assembly -> align reads to contigs -> BAM -> binner

**Resource requirements:**
- COMEBin: 8 CPUs, 32GB RAM, 12h (heavy)
- SemiBin2: 8 CPUs, 16GB RAM, 4h (lighter)
- Test on a machine with sufficient RAM before production runs

### Nextflow Quirks (May 2026)

**Version: pinned to `nextflow=26.04.1`** in `envs/base.yml`. Bumped from 25.10.0; the codebase is forward-compatible with both. The 26.x line enables the strict syntax parser by default — keep generated `.nf` and our `Orchestrator.groovy` clean of:

- **Single-element parenthesized assignment** `(_x) = expr` — strict mode rejects it. Use `_x = (expr)[0]` instead. The workflow generator at `src/metasmith/models/workflow.py` emits the indexed form (`_v = (o.postIn(...))[0]`, `_x = (o.post(...))[0]` for single-output processes). Multi-element destructures `(a, b) = expr` still work.
- **Range-based for loops** `for (i in 0..N)` — strict mode rejects. Use `(0..N).each { i -> ... }`. (Multi-element `for (x : collection)` is fine; that's what `Orchestrator.groovy` uses.)

**Don't render index Maps via `.view {}` in test scripts.** When a `.view {}` closure interpolates a `Map` (e.g. `view { "P01: ${it[0]}" }` where `it[0]` is an index Map), Groovy's `FormatHelper.formatMap` iterates entries and races with concurrent operators that share the Map reference. This surfaces as a `ConcurrentModificationException` from inside `view`. Production-generated workflows don't use `view` at all, so this is a test-side hazard only. Pattern: render a non-Map field (`view { "P01: ${it[1].name}" }`) or skip the view.

**Upstream bug `nextflow-io/nextflow#6757` (open).** `nextflow.util.Duration(long)` asserts `duration >= 0`; under wall-clock skew (WSL2, NTP step) `WorkflowMetadata.invokeOnComplete()` throws and the JVM exits non-zero. The workflow body has already completed and `publish` manifests are on disk — only the optional `nxf_report.html` / `timeline.html` / `trace.tsv` artifacts are lost.
- **Production path** (`src/metasmith/agents.py`): the shell heredoc wrapping `nextflow run` has `trap stop EXIT; exit 0`, so the non-zero JVM exit is absorbed; `CollectResults` runs unconditionally and report parsing has graceful fallbacks. No code change needed.
- **Test path**: use `_assert_nxf_ok` / `NxfTestRunner.assert_nxf_ok` — they detect the assertion in `stdout`/`stderr`, print `WARN: tolerated upstream nextflow-io/nextflow#6757 …`, and return so downstream parsing proceeds. If `CollectResults` then fails on missing manifests, the test surfaces a clear error blaming #6757.
- A Groovy `metaClass` override on `Duration.between` was tried and abandoned: Nextflow's caller is `@CompileStatic`, so meta-dispatch isn't intercepted.

**Orchestrator concurrency hygiene.** `pending_tasks` / `index_history` / `child2parent` and the value Sets/Lists they hold are `ConcurrentHashMap` + `ConcurrentHashMap.newKeySet()` + `Collections.synchronizedList`. Defensive — the methods are `synchronized` but the collections they hand out leak to operator callbacks.

## Release versioning

Two files form the version. Both live under `src/metasmith/`:

- **`version.txt`** — bare PEP 440 release segment, e.g. `0.18.2`. Source-controlled. Bumped by hand when shipping. **Must not** contain `+` or `-`.
- **`build_hash.txt`** — 7-char md5 over the `src/metasmith/` tree, regenerated by `_build_hash.py` at build time. Gitignored. Absent in fresh dev checkouts (then everything degrades to bare semver).

Derivations in `constants.py`:

```
VERSION       = "0.18.2"                              # from version.txt
BUILD_HASH    = "abc1234"                             # from build_hash.txt (or "" if absent)
FULL_VERSION  = f"{VERSION}+{BUILD_HASH}"             # canonical, PEP 440 local form
CONTAINER_TAG = FULL_VERSION.replace('+', '-')        # single +→- site
```

Downstream consumers:

- `setup.py` ships the wheel as `metasmith-{FULL_VERSION}-py3-none-any.whl`.
- `Agent.container` default = `docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}` — fresh deploys pull the exact image the maintainer pushed.
- `dev.sh` reads both files and renders `DOCKER_TAG` identically (`test_dev_sh_tag.py` keeps it in lockstep).
- `testing/docker_builder.{get_full_version,get_docker_tag}()` mirror the same chain.

### Bump procedure

The build hash decouples the chicken-and-egg of self-referential commit hashes — there is no embedded git hash to manage.

```bash
# 1. Bump semver
echo 0.18.3 > src/metasmith/version.txt
git commit -am "Bump version to 0.18.3"

# 2. Build & publish (these stamp build_hash.txt as a side effect)
./dev.sh --build-gui   # web GUI bundle → src/metasmith/gui/static/ (needs node)
./dev.sh -bp           # pip wheel → metasmith-0.18.3+<hash>-py3-none-any.whl
./dev.sh -brc          # one-time: build the rust cross-compile container
./dev.sh -br           # build all 4 relay binaries (x86_64/arm64 × linux/darwin)
./dev.sh -bd && -ud    # docker → quay.io/hallamlab/metasmith:0.18.3-<hash>
./dev.sh -bs           # apptainer .sif (matching tag)

# 3. Tag
git tag v0.18.3 && git push upstream v0.18.3
```

`--build-gui` is load-bearing for the same reason `-br` is: the bundle is generated, never committed, and skipping it ships an empty `src/metasmith/gui/static/` that nobody notices until someone opens the page. `-bp` and `-bd` run `_assert_gui_bundle` and refuse without it. Override only for emergencies: `MSM_SKIP_GUI_CHECK=1`.

`-br` is load-bearing — `--update_container` skips it, which is how 0.18.4 shipped with 3 of 4 `/app/msm_relay.*` slots replaced by 28-byte `#!/bin/sh\necho 'stub relay'` stubs left over in `main/relay_agent/target/`. `dev.sh -ud` and `-bs` now invoke `_assert_real_relays` against the just-tagged image and refuse to publish or convert if any slot is a stub (wrong magic for its arch, or <100 KB). Override only for emergencies: `MSM_SKIP_RELAY_CHECK=1 ./dev.sh -ud`.

The hash captures the source state; identical source ↔ identical hash ↔ identical image tag. Two builds from the same commit produce the same tag; a one-line edit produces a new tag (and a new image).

Regression tests pinning the chain: `tests/test_container_tag.py`, `tests/test_dev_sh_tag.py`.

## Reference Transforms
- `transforms/metagenomics/binning/checkm.py` - single assembly input pattern
- `transforms/metagenomics/taxonomy/gtdbtk.py` - external database binding pattern
- `transforms/logistics/getNcbiAssembly.py` - multiple outputs pattern

## `examples/` — minimal regression library

Top-level `examples/` is the smallest valid metasmith library — agnostic
(no host/runtime references) and reusable for any deploy/runtime smoke.
Layout:

```
examples/
├── data_types/{examples,containers}.yml   # source types: examples::name, examples::greeting, containers::metasmith.oci
├── metasmith.oci                          # docker:// URL for the metasmith image itself
├── echo_greeting.py                       # 1 transform: name → echo "hello $name" > greeting.txt
└── _metadata/                             # generated by `metasmith build -t data_types -r .`, committed
```

Use it from a smoke script like `main/local_mock/smoke_hpc_deploy.py`:
`DataInstanceLibrary` for the input value + a containers `DataInstanceLibrary`
with `metasmith.oci` `AddItem`'d in, then `TransformInstanceLibrary.Load(examples)`
and `TargetBuilder().Add("examples::greeting")`.

Rebuild after editing yamls or the transform:
`python -m metasmith build -t examples/data_types -r examples`.
