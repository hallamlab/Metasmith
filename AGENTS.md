# Agent Notes for Metasmith

## Environment

Use the `msm_env` mamba environment to run Python, tests, and CLI commands:
```
mamba run -n msm_env <command>
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
`.external` (absolute host path).

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
givens so the most actionable suggestion is first. Consumers (the MCP server,
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

## MCP Server

Metasmith exposes its **full** API via [Model Context Protocol](https://modelcontextprotocol.io) through `metasmith-mcp`. An LLM client (Claude, etc.) can register inputs, author transforms, plan workflows, run them, wait on detached runs, and collect results — all without dropping into Python.

The canonical reference is **`docs/source/agentic/`** (see `tool_reference.rst` for the full catalog).

### Running

```bash
metasmith-mcp \
  --types data_types/ncbi.yml data_types/sequences.yml \
  --data inputs.xgdb \
  --transforms transforms/amplicon transforms/pangenome \
  --agents agents/local.yml \
  --workspace ~/.metasmith/mcp_workspace
```

All flags also accept env vars: `METASMITH_TYPE_LIBS`, `METASMITH_DATA_LIBS`, `METASMITH_TRANSFORM_LIBS`, `METASMITH_AGENTS`, `METASMITH_WORKSPACE` (colon-separated paths).

Inputs can also be added at runtime via `register_type_library`, `register_data_library`, `register_transform_library`, `register_agent`, so the server need not be restarted as work expands.

### Tools (62 total)

| Category | Tools |
|----------|-------|
| **Server** | `server_status`, `register_type_library`, `register_data_library`, `register_transform_library`, `register_agent`, `reload_libraries` |
| **Types** | `list_types`, `get_type`, `check_type_compatibility`, `create_type_library`, `add_type` |
| **Data libraries** | `list_data_libraries`, `inspect_data_library`, `list_data_items`, `show_item_lineage`, `create_data_library`, `attach_type_library`, `add_data_item`, `add_data_value`, `set_item_parents`, `remove_data_item`, `rename_data_item`, `rename_by_parent`, `prune_types`, `consolidate_library`, `save_library`, `trace_lineage`, `load_remote_library` |
| **Transforms** | `list_transform_libraries`, `list_transforms`, `show_transform_contract`, `read_transform_source`, `write_transform`, `scaffold_transform`, `validate_transform_contract`, `propagate_types` |
| **Workflow planning** | `plan_workflow`, `get_workflow_plan`, `get_plan_hints`, `render_plan_dag`, `list_workflow_tasks`, `delete_workflow_task` |
| **Agents** | `list_agents`, `load_agent`, `save_agent`, `get_agent_info`, `agent_ping`, `deploy_agent` |
| **Lifecycle** | `stage_workflow`, `run_workflow`, `wait_for_workflow`, `tail_workflow_log`, `cancel_workflow`, `list_workflow_runs`, `check_workflow`, `get_result_source`, `collect_results`, `list_config_presets` |
| **Source / Logistics** | `parse_source`, `source_exists`, `transfer_source` |
| **Build** | `build_libraries` |

### Workflow via MCP

The full lifecycle is:

```
plan_workflow → stage_workflow → run_workflow → wait_for_workflow → tail_workflow_log → collect_results
```

`run_workflow` is detached (the launcher exits as soon as `nohup nextflow … &` starts). Script callers MUST follow with `wait_for_workflow`, which blocks on the `run completed at` sentinel in `runs/<task_key>/_metasmith/logs.latest/agent.log`. `tail_workflow_log` returns the last N lines of `agent.log` or `main.log` and `cancel_workflow` cleanly stops a run by removing `workspace/PID.lock` (the in-container loop catches the absence and gracefully kills nextflow).

Tasks are cached to disk in the workspace directory and can be re-fetched via `get_workflow_plan(task_key)` or `list_workflow_tasks()`.

### Resources (12)

URI-based read-only views:

- `metasmith://server/status`
- `metasmith://types`, `types/{ns}`, `types/{ns}/{name}`
- `metasmith://data/{lib}`
- `metasmith://transforms/{lib}`, `transforms/{lib}/{transform}`
- `metasmith://agents`, `agents/{name}`
- `metasmith://tasks`, `tasks/{key}`, `tasks/{key}/dag`

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

`src/metasmith/version.txt` holds the **PEP 440 local version** including the build's git short hash, e.g. `0.17.1+a8d676a`. This is the single source of truth — bump it by hand in the same commit that ships the new image. From it:

- `constants.VERSION` = file content verbatim (`0.17.1+a8d676a`).
- `constants.CONTAINER_TAG` = `VERSION.replace('+', '-')` (`0.17.1-a8d676a`) — Docker tags reject `+`.
- `Agent.container` default = `docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}`, so fresh deploys pull the exact image the maintainer pushed.
- `dev.sh` derives `DOCKER_TAG` from `version.txt` the same way; `dev.sh -bd && -ud` build/push the matching tag.
- `testing/docker_builder.get_git_version()` returns `version.txt` verbatim (no git rev-parse).

Regression tests pinning this wiring: `tests/test_container_tag.py`, `tests/test_dev_sh_tag.py`.

## Reference Transforms
- `transforms/metagenomics/binning/checkm.py` - single assembly input pattern
- `transforms/metagenomics/taxonomy/gtdbtk.py` - external database binding pattern
- `transforms/logistics/getNcbiAssembly.py` - multiple outputs pattern
