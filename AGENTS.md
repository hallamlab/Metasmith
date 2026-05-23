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
| top-level legacy | `get`, `lab`, `api`, `help` |

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

## Reference Transforms
- `transforms/metagenomics/binning/checkm.py` - single assembly input pattern
- `transforms/metagenomics/taxonomy/gtdbtk.py` - external database binding pattern
- `transforms/logistics/getNcbiAssembly.py` - multiple outputs pattern
