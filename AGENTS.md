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

Cache layout: `<home>/container_images/<name>.sif` (always retained)
alongside `<name>.sandbox/` (present iff verdict is `use-sandbox`).
Helpers in `src/metasmith/coms/containers.py`:
`GetSandboxPath / MakeSandboxDecisionProbe / MakeBuildSandboxCommand`.

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
| `metasmith data` | `inspect`, `list`, `create`, `attach-types`, `add-item`, `add-value`, `set-parents`, `remove`, `rename`, `rename-by-parent`, `prune-types`, `consolidate`, `save`, `trace`, `load-remote`, `import-library`, `lineage` |
| `metasmith transform` | `list`, `libraries`, `show`, `read`, `write`, `scaffold`, `validate`, `propagate-types` |
| `metasmith plan` | one-shot planner (`--data-library`, `--sample-type`, `--target-type ...`, `--transform-library ...`) |
| `metasmith workflow` | `stage`, `run`, `wait`, `tail`, `cancel`, `runs`, `check`, `collect`, `result-source`, `presets` |
| `metasmith agent` | `list`, `info`, `save`, `ping`, `deploy` |
| `metasmith source` | `parse`, `exists`, `transfer` |
| `metasmith task` | `list`, `show`, `hints`, `dag`, `delete` |
| `metasmith build` | `all` (default), `types`, `uniques`, `transforms` — compile data type, unique, and transform libraries |
| `metasmith cache` | `list`, `gc`, `explain` — lineage-addressed task-cache operations |
| `metasmith status` | `<run_dir>` — render per-task hit/run status from `_metasmith/trace.jsonl` + `workflow.step_N.meta` |
| top-level legacy | `get`, `lab`, `api`, `help` |

### Task cache (feat/caching)

A lineage-addressed cache lives at `<agent_home>/task_cache/`. Identity is provenance (transform key + sorted input instance_ids encoded as canonical CBOR + blake3-32 multihash), not bytes. Defaults: cache is **ON**; per-transform opt-out via `TransformInstance(..., cacheable=False)`; global kill-switch via `METASMITH_CACHE=0` env. Cache hits short-circuit the executor — compile-time probe rewrites the per-step emission in `workflow.nf` to a synthetic `Channel.of(...)` routed through `o.post(o.asStreams(...), k)` (Critic E#1 invariant preserved); the post-exec promote (`promote_run`) atomic-renames `<key>.tmp/` → `<key[:2]>/<key[2:]>/`. Per G8 leaf ids are unique per `AddItem`, so cross-build hits require `metasmith data import-library <src> <dest>` (upserts `origin in {"lineage","imported"}` entries into the destination cache). See `docs/source/usage/nextflow.rst` for the full surface.

**trace.jsonl is the canonical event log.** `<run_dir>/_metasmith/trace.jsonl` is rotate-on-compile (never truncated); a `SessionStart` sentinel leads every fresh file and a monotonic `session_id` (from the cache sqlite's `trace_session_counter` row) tags every subsequent row. One row schema covers every status — `InvocationEvent.status ∈ {"hit","miss","promoted","fail"}` — and the canonical dataclass with field-by-field semantics lives in `src/metasmith/models/lineage.py` (docstring is the spec). Compile-time emits the `hit` rows; `promote_run` appends the post-exec rows. Legacy v1 rows (no `schema_version`) are tolerated by `InvocationEvent.from_jsonl` for `msm status <old_run_dir>`. Bumping `LIN_PAYLOAD_VERSION` in `caching/keys.py` (currently 2) renders pre-v2 shards unreachable; `SHARD_LAYOUT_VERSION` (currently 2) tracks the `<shard>/logs/.command.{sh,out,err,log}` directory captured by `promote.py:_find_step_logs`. `get_logs_of(any_output).stdout` resolves there after `rm -rf work/` + resume.

**User-facing telemetry API.** On a loaded `DataInstanceLibrary`, the trace is auto-attached via `Load(attach_trace=True)` (default). Methods: `get_lineage_of`, `get_logs_of`, `get_transform_of`, `get_siblings_of(scope="slot"|"task")`, `walk_ancestors`, `find_by`, `list_dtypes`, `list_transforms`, `summary`, `get_invocation`, `try_get_invocation`, `find_invocations`, `get_outputs_of`, `find_failures`. Re-exported from `metasmith.python_api`. Each method's docstring names its closed `Literal` values, exception classes, and AND/OR filter semantics; treat those as the contract.

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

**Orchestrator concurrency hygiene.** `index_history` / `child2parent` and the value Sets/Lists they hold are `ConcurrentHashMap` + `ConcurrentHashMap.newKeySet()` + `Collections.synchronizedList`. Defensive — the methods are `synchronized` but the collections they hand out leak to operator callbacks.

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
./dev.sh -bp           # pip wheel → metasmith-0.18.3+<hash>-py3-none-any.whl
./dev.sh -bd && -ud    # docker → quay.io/hallamlab/metasmith:0.18.3-<hash>
./dev.sh -bs           # apptainer .sif (matching tag)

# 3. Tag
git tag v0.18.3 && git push upstream v0.18.3
```

The hash captures the source state; identical source ↔ identical hash ↔ identical image tag. Two builds from the same commit produce the same tag; a one-line edit produces a new tag (and a new image).

Regression tests pinning the chain: `tests/test_container_tag.py`, `tests/test_dev_sh_tag.py`.

## Reference Transforms
- `transforms/metagenomics/binning/checkm.py` - single assembly input pattern
- `transforms/metagenomics/taxonomy/gtdbtk.py` - external database binding pattern
- `transforms/logistics/getNcbiAssembly.py` - multiple outputs pattern
