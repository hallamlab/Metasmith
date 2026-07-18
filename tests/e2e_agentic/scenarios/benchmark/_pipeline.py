"""Shared pipeline definition + start-state builders for the 7 benchmark tests.

The token benchmark drives ONE science task across 10 arms (env × orchestrator,
see ``scenarios/arms.py``): an *E. coli* isolate functional-genomics pipeline

    fastp → SPAdes → bakta → eggNOG-mapper → clusterProfiler(R)

from paired-end short reads to a KEGG/GO functional-enrichment plot + table.

Every benchmark scenario builds its per-iteration prompt as

    compose_benchmark_prompt(shared_goal_block(<test>, sandbox=...), ctx.arm)

so the GOAL / DATA / DONE block is **byte-identical across arms** and only the
arm's additive ``preamble`` (its native env + tools + reference docs) differs.
That prompt symmetry is the study's integrity invariant — keep it.

--------------------------------------------------------------------------------
RECONCILE-ME  (pipeline-specific constants — settle these against the
transforms agent's ``main/transforms/std`` output before any live run)
--------------------------------------------------------------------------------
The constants immediately below are the single place the pipeline's type / glob
names live. They were read off ``main/transforms/std/dtypes.yml`` +
``.../materials/manifest.md`` as of authoring; if the transforms agent renames a
type or an output format, change it HERE and every scenario follows.
"""
from __future__ import annotations

import dataclasses
import os
import shutil
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from ..arms import Arm
from ..base import compose_prompt
from .._fixture_utils import (
    DataItemSpec,
    build_data_lib,
    run_msm,
    _msm_bin,
)
from ...harness.sandbox import SandboxLayout

# ===========================================================================
# >>> RECONCILE-ME: pipeline type + artifact constants <<<
# ===========================================================================

#: Metasmith type namespace the std transform library publishes under.
TYPE_NAMESPACE = "std"

#: The two input datatypes the pipeline consumes (paired-end short reads).
#: Confirmed present in main/transforms/std/dtypes.yml.
INPUT_TYPES: tuple[str, str] = (
    "std::paired_reads_forward",
    "std::paired_reads_reverse",
)

#: The final science artifact the shared oracle checks. clusterProfiler emits
#: BOTH a dotplot (PNG) and a results table (TSV). ``enrichment_plot`` /
#: ``enrichment_table`` are present in dtypes.yml today.
FINAL_ARTIFACT_TYPE = "std::enrichment_plot"          # KEGG/GO dotplot (PNG)
FINAL_ARTIFACT_TABLE_TYPE = "std::enrichment_table"   # KEGG/GO table (TSV)

#: Arm-independent canonical location the agent is instructed to deposit the
#: final artifacts into, relative to the sandbox. Making the collect target a
#: fixed path keeps the success oracle identical for every arm (metasmith arms
#: `workflow collect --dest` here; baseline arms write here directly).
FINAL_RESULTS_REL = "workspace/results"

#: Success glob (relative to sandbox root). ALL scenarios reuse this so the
#: oracle is arm-independent. clusterProfiler's PNG dotplot is the load-bearing
#: artifact; the TSV table rides alongside it. Kept as a single required glob
#: because ``standard_verify`` treats each listed glob as a hard requirement.
FINAL_ARTIFACT_GLOB = "workspace/results/**/*.png"

#: Metasmith-only lineage trace the oracle runs (`metasmith data trace`). Skipped
#: for non-metasmith arms by ``standard_verify`` (they have no results.xgdb).
EXPECTED_TRACE: tuple[str, str] = (INPUT_TYPES[0], FINAL_ARTIFACT_TYPE)

# ===========================================================================
# >>> RECONCILE-ME: on-disk materials (staged by the parallel materials agent)
# ===========================================================================

#: Root of the pre-staged benchmark materials (reads, DBs, container sifs,
#: nf-core checkout). See .../token-benchmark/materials/manifest.md.
#: Host-overridable via ``BENCHMARK_MATERIALS_ROOT`` so the benchmark can run on
#: hosts other than the Cosmos dev box (e.g. micb0, where materials live under
#: ``~/token-benchmark/materials``). Defaults to the Cosmos dev location.
MATERIALS_ROOT = Path(
    os.environ.get(
        "BENCHMARK_MATERIALS_ROOT",
        "/home/tony/agentic_workspace/data/metasmith/token-benchmark/materials",
    )
)

READS_R1_SRC = MATERIALS_ROOT / "reads" / "ecoli_R1.fastq.gz"
READS_R2_SRC = MATERIALS_ROOT / "reads" / "ecoli_R2.fastq.gz"

#: Reference databases (pre-provisioned fixtures, identical across arms).
DB_BAKTA = MATERIALS_ROOT / "dbs" / "bakta-light" / "db-light"
DB_EGGNOG = MATERIALS_ROOT / "dbs" / "eggnog" / "data"

#: Tool container images (sif) + their quay tags. Source of truth:
#: materials/images/container_tags.txt.
CONTAINER_IMAGES: dict[str, tuple[Path, str]] = {
    "fastp": (MATERIALS_ROOT / "images" / "fastp.sif",
              "quay.io/biocontainers/fastp:0.23.4--h5f740d0_0"),
    "spades": (MATERIALS_ROOT / "images" / "spades.sif",
               "quay.io/biocontainers/spades:3.15.5--h95f258a_1"),
    "bakta": (MATERIALS_ROOT / "images" / "bakta.sif",
              "quay.io/biocontainers/bakta:1.9.3--pyhdfd78af_0"),  # RECONCILE: sif not yet staged
    "eggnog-mapper": (MATERIALS_ROOT / "images" / "eggnog-mapper.sif",
                      "quay.io/biocontainers/eggnog-mapper:2.1.12--pyhdfd78af_0"),
    "clusterprofiler": (MATERIALS_ROOT / "images" / "clusterProfiler.sif",
                        "quay.io/biocontainers/bioconductor-clusterprofiler:4.6.0--r42hdfd78af_0"),
    "abricate": (MATERIALS_ROOT / "images" / "abricate.sif",
                 "quay.io/biocontainers/abricate:1.0.1--ha8f3691_2"),
}

#: nf-core/bacass provenance checkout (tests 6 & 7 nf-core sub-study).
NFCORE_BACASS = MATERIALS_ROOT / "pipelines" / "nf-core-bacass"

#: Ordered pipeline stages (toy provenance).
PIPELINE_STAGES = ("fastp", "spades", "bakta", "eggnog-mapper", "clusterprofiler")

#: In-sandbox path where reads land (workspace-relative), used by DATA text.
READS_REL_DIR = "workspace/reads"
READS_R1_REL = f"{READS_REL_DIR}/ecoli_R1.fastq.gz"
READS_R2_REL = f"{READS_REL_DIR}/ecoli_R2.fastq.gz"


# ---------------------------------------------------------------------------
# Tool roster (install test t1) — E. coli isolate functional genomics
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolSpec:
    key: str                 # canonical tool key (matches CONTAINER_IMAGES)
    binary: str              # the executable the agent must be able to run
    role: str                # pipeline role
    probe: str               # one-line description of the probe that proves it runs


TOOLS: dict[str, ToolSpec] = {
    "fastp": ToolSpec("fastp", "fastp", "QC / adapter trim",
                      "trim the paired reads and emit trimmed FASTQ + an HTML/JSON report"),
    "spades": ToolSpec("spades", "spades.py", "de-novo assembly",
                       "assemble the (trimmed) reads and emit contigs.fasta"),
    "bakta": ToolSpec("bakta", "bakta", "structural + basic annotation",
                      "annotate a small contigs FASTA and emit GFF3/FAA (uses the bakta-light DB fixture)"),
    "eggnog-mapper": ToolSpec("eggnog-mapper", "emapper.py", "functional annotation (COG/KEGG/GO)",
                              "annotate a small protein FASTA and emit the emapper annotations TSV (uses the eggNOG DB fixture)"),
    "clusterprofiler": ToolSpec("clusterprofiler", "Rscript", "R functional enrichment + plot",
                                "run a clusterProfiler KEGG/GO enrichment in R and emit a PNG dotplot + TSV table"),
    "abricate": ToolSpec("abricate", "abricate", "AMR / virulence screen (test-6 added tool)",
                         "screen a small contigs FASTA against a bundled DB and emit the abricate TSV report"),
}

#: Which env channels the install test sweeps (orchestration collapses for a lone tool).
INSTALL_ENV_CHANNELS = ("ad-hoc", "mamba", "container", "metasmith")


# ===========================================================================
# Shared GOAL / DATA / DONE block — byte-identical across arms
# ===========================================================================

# Per-test goal narrative. Depends ONLY on the test (never on the arm), so the
# composed shared block stays byte-identical across arms.
_GOALS: dict[str, str] = {
    "t2_pipeline": (
        "Define the full E. coli functional-genomics pipeline\n"
        "    fastp -> SPAdes -> bakta -> eggNOG-mapper -> clusterProfiler\n"
        "and DRY-VALIDATE it end-to-end. Do NOT run the pipeline to completion —\n"
        "the goal is a defined pipeline that passes a dry-run / plan validation\n"
        "showing every stage wires up from the paired reads to the final\n"
        "clusterProfiler enrichment artifact."
    ),
    "t3_run": (
        "Run the full E. coli functional-genomics pipeline\n"
        "    fastp -> SPAdes -> bakta -> eggNOG-mapper -> clusterProfiler\n"
        "end-to-end on the paired-end reads, producing the final clusterProfiler\n"
        "KEGG/GO functional-enrichment artifact (a PNG dotplot + a TSV table)."
    ),
    "t4_adapt_new_host": (
        "This pipeline already runs on the origin host. ADAPT it to run on the\n"
        "new host and execute it there end-to-end, producing the final\n"
        "clusterProfiler enrichment artifact. A successful run on the new host\n"
        "IS the objective."
    ),
    "t5_adapt_hpc": (
        "This pipeline already runs locally. ADAPT it to run on the HPC cluster\n"
        "under the SLURM scheduler and execute it there end-to-end, producing the\n"
        "final clusterProfiler enrichment artifact. A successful scheduler run IS\n"
        "the objective."
    ),
    "t6_adapt_add_tool": (
        "The E. coli pipeline already runs. SPLICE the pre-installed `abricate`\n"
        "AMR/virulence screen into the pipeline (it consumes the assembled\n"
        "contigs), then run the pipeline so BOTH the abricate report AND the final\n"
        "clusterProfiler enrichment artifact are produced."
    ),
    "t7_adapt_from_middle": (
        "The E. coli pipeline already runs. Pre-computed assembly contigs are\n"
        "provided. RESUME the pipeline from the assembly stage — reuse the given\n"
        "contigs (do NOT re-run fastp or SPAdes) and run the downstream stages\n"
        "(bakta -> eggNOG-mapper -> clusterProfiler) to produce the final\n"
        "clusterProfiler enrichment artifact."
    ),
}


def goal_for(test_name: str) -> str:
    if test_name not in _GOALS:
        raise KeyError(f"no benchmark goal registered for {test_name!r}; "
                       f"known: {sorted(_GOALS)}")
    return _GOALS[test_name]


def shared_goal_block(
    test_name: str,
    *,
    sandbox: Path,
    goal: str | None = None,
    done_key: str | None = None,
    data_lines: list[str] | None = None,
) -> str:
    """Assemble the GOAL / DATA / DONE block shared byte-identically across arms.

    Args:
      test_name  benchmark test id (selects the default goal + done key).
      sandbox    the sandbox root; interpolated into paths.
      goal       override the registered goal narrative (default: ``goal_for``).
      done_key   the checkpoint key (default: ``test_name``).
      data_lines extra scenario-specific DATA bullet lines (e.g. pre-staged
                 contigs for t7). Depend only on the test, never the arm.

    The returned text carries NO arm-specific content — the arm's environment /
    tools / reference material live entirely in ``arm.preamble``, prepended by
    ``compose_prompt`` / ``compose_benchmark_prompt``.
    """
    goal = goal if goal is not None else goal_for(test_name)
    done_key = done_key if done_key is not None else test_name
    sb = str(sandbox)

    data_block = [
        f"  paired reads (forward):  {sb}/{READS_R1_REL}",
        f"  paired reads (reverse):  {sb}/{READS_R2_REL}",
        f"  reference DBs:           pre-provisioned host fixtures "
        f"(bakta-light, eggNOG) — identical across environments",
    ]
    for extra in (data_lines or []):
        data_block.append(f"  {extra}")

    return textwrap.dedent(f"""\
        # E. coli isolate — functional-genomics benchmark

        ## Goal
        {goal}

        ## Data
        The following inputs are staged for you:
        {chr(10).join(data_block)}

        ## Deliverable
        The final artifact is the clusterProfiler KEGG/GO functional-enrichment
        result: a PNG dotplot (and its TSV results table). Collect / write the
        final artifact(s) into:

          {sb}/{FINAL_RESULTS_REL}/

        so a `.png` appears under that directory when you are done.

        ## Done protocol
        If any step fails or produces unexpected output, stop immediately and run:

        ```bash
        metasmith e2e report_issue --cwd "{sb}" --reason "<one line describing what you saw>"
        ```

        When the final clusterProfiler artifact is present under
        `{sb}/{FINAL_RESULTS_REL}/`, run:

        ```bash
        metasmith e2e checkpoint done --cwd "{sb}" --key {done_key}
        ```

        The `--cwd "{sb}"` flag writes `CONTROL.json` to the sandbox root, where
        the harness loop watches for it.
        """)


# ===========================================================================
# Arm preamble + reference-doc bundles (the ONLY per-arm prompt variation)
# ===========================================================================

_ENV_PREAMBLE: dict[str, str] = {
    "ad-hoc": (
        "Your environment: tool binaries are expected on the system PATH (or you "
        "build them from source). There is no environment manager and no "
        "containers — glue the tools together with shell."
    ),
    "mamba": (
        "Your environment: use conda/mamba to create and manage tool "
        "environments. A conda environment spec has been staged for you at "
        "`workspace/env/environment.yml`; the tools are resolved from "
        "conda-forge/bioconda."
    ),
    "container": (
        "Your environment: each tool ships as a container image. Docker→Apptainer "
        "conversion is done BY HAND. The tool sif images are pre-staged (see "
        "`workspace/env/images.txt`); invoke each tool through its container."
    ),
    "metasmith": "",  # A10 — empty preamble; prompt is the shared block verbatim.
}

_ORCH_PREAMBLE: dict[str, str] = {
    "ad-hoc": "Orchestration: glue the stages together with a shell script.",
    "snakemake": (
        "Orchestration: use Snakemake. A starter Snakefile is staged at "
        "`workspace/Snakefile`."
    ),
    "nextflow": (
        "Orchestration: use Nextflow. A starter `workspace/main.nf` + "
        "`workspace/nextflow.config` are staged."
    ),
    "metasmith": "",  # A10 — metasmith is both env and orchestrator.
}

_ENV_REFDOCS: dict[str, list[str]] = {
    "ad-hoc": ["tool man-pages (fastp, spades.py, bakta, emapper.py, clusterProfiler/Rscript)"],
    "mamba": ["conda/mamba docs", "bioconda tool pages"],
    "container": ["docker + apptainer CLI docs", "biocontainers registry pages"],
    "metasmith": ["metasmith agentic docs (docs/source/agentic/)"],
}

_ORCH_REFDOCS: dict[str, list[str]] = {
    "ad-hoc": [],
    "snakemake": ["Snakemake docs (snakemake.readthedocs.io)"],
    "nextflow": ["Nextflow docs (nextflow.io/docs)"],
    "metasmith": [],
}


def preamble_for(arm: Arm) -> str:
    """The arm's additive prompt preamble (env + orchestrator + reference docs).

    Empty for the metasmith arm (A10) so its composed prompt is the shared goal
    block verbatim — preserving current metasmith-scenario behavior.
    """
    if arm.is_metasmith:
        return ""
    parts = [_ENV_PREAMBLE.get(arm.env, ""), _ORCH_PREAMBLE.get(arm.orchestrator, "")]
    refs = reference_docs_for(arm)
    body = "\n".join(p for p in parts if p)
    if refs:
        body += "\n\nReference material available to you:\n" + "\n".join(
            f"  - {r}" for r in refs
        )
    return body.strip()


def reference_docs_for(arm: Arm) -> list[str]:
    if arm.is_metasmith:
        return list(_ENV_REFDOCS["metasmith"])
    return _ENV_REFDOCS.get(arm.env, []) + _ORCH_REFDOCS.get(arm.orchestrator, [])


def arm_with_preamble(arm: Arm) -> Arm:
    """Return ``arm`` carrying its benchmark preamble + reference docs.

    If the arm already carries a non-empty preamble (e.g. a future P-workstream
    populated ``ARMS`` directly), it is left untouched. Otherwise the
    pipeline-specific preamble is attached via ``dataclasses.replace`` so the
    canonical ``ARMS`` objects stay immutable and generic.
    """
    if arm.preamble.strip():
        return arm
    return dataclasses.replace(
        arm,
        preamble=preamble_for(arm),
        reference_docs=reference_docs_for(arm),
    )


def compose_benchmark_prompt(shared_block: str, arm: Arm) -> str:
    """Shared block + the arm's benchmark preamble. The one prompt seam."""
    return compose_prompt(shared_block, arm_with_preamble(arm))


# ===========================================================================
# Start-state builders (baseline arm provisioning)
# ===========================================================================


def stage_reads(layout: SandboxLayout) -> tuple[Path, Path]:
    """Copy the paired-end reads into ``<sandbox>/workspace/reads/``.

    Fail-fast (per project preference) if the materials agent has not staged the
    canonical ``ecoli_R1/R2.fastq.gz`` yet — no silent placeholder.
    """
    dest_dir = layout.workspace / "reads"
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = []
    for src in (READS_R1_SRC, READS_R2_SRC):
        if not src.exists():
            raise RuntimeError(
                f"benchmark reads not staged: {src} is missing. The materials "
                f"agent stages ecoli_R1/R2.fastq.gz under {MATERIALS_ROOT}/reads/. "
                f"Stage them (or point MATERIALS_ROOT at the real location) before "
                f"running this scenario live."
            )
        dst = dest_dir / src.name
        shutil.copy2(src, dst)
        out.append(dst)
    return out[0], out[1]


def _reads_present() -> bool:
    return READS_R1_SRC.exists() and READS_R2_SRC.exists()


def build_scripts_repo(layout: SandboxLayout) -> Path:
    """ad-hoc start-state: a shell-glue pipeline repo.

    Drops ``<workspace>/pipeline/run.sh`` — an ordered shell scaffold naming each
    stage, plus a per-stage stub the agent fleshes out. This is the *start-state*
    (an empty-but-structured repo), not a working pipeline.
    """
    repo = layout.workspace / "pipeline"
    repo.mkdir(parents=True, exist_ok=True)
    stages = " -> ".join(PIPELINE_STAGES)
    run_sh = repo / "run.sh"
    run_sh.write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        # E. coli functional-genomics pipeline (shell glue).
        # Stages: {stages}
        # Inputs:  $READS_R1 / $READS_R2   Final artifact -> $RESULTS/*.png
        set -euo pipefail

        READS_R1="${{READS_R1:?set READS_R1 to the forward reads}}"
        READS_R2="${{READS_R2:?set READS_R2 to the reverse reads}}"
        RESULTS="${{RESULTS:?set RESULTS to the output dir}}"
        mkdir -p "$RESULTS"

        # TODO(agent): implement each stage below.
        # 1. fastp           : QC / adapter trim
        # 2. spades.py       : de-novo assembly -> contigs.fasta
        # 3. bakta           : structural + basic annotation (bakta-light DB)
        # 4. emapper.py      : functional annotation (eggNOG DB) -> annotations TSV
        # 5. clusterProfiler : KEGG/GO enrichment in R -> $RESULTS/enrichment.png + .tsv
        echo "pipeline scaffold: implement the stages above" >&2
        exit 1
    """))
    run_sh.chmod(0o755)
    (repo / "README.md").write_text(
        f"# E. coli functional-genomics pipeline (ad-hoc shell glue)\n\n"
        f"Stages: {stages}\n\nEdit `run.sh` to implement each stage.\n"
    )
    return repo


def build_snakefile(layout: SandboxLayout) -> Path:
    """snakemake start-state: a starter Snakefile skeleton."""
    sf = layout.workspace / "Snakefile"
    stages = ", ".join(PIPELINE_STAGES)
    sf.write_text(textwrap.dedent(f'''\
        # E. coli functional-genomics pipeline (Snakemake skeleton).
        # Stages: {stages}
        # Wire fastp -> spades -> bakta -> eggnog-mapper -> clusterProfiler.

        RESULTS = "{FINAL_RESULTS_REL.split("/", 1)[-1]}"

        rule all:
            input:
                RESULTS + "/enrichment.png"

        # TODO(agent): add one rule per stage. Final rule must produce
        # RESULTS/enrichment.png (clusterProfiler KEGG/GO dotplot).
    '''))
    return sf


def build_nextflow(layout: SandboxLayout) -> tuple[Path, Path]:
    """nextflow start-state: a starter main.nf + nextflow.config skeleton."""
    nf = layout.workspace / "main.nf"
    cfg = layout.workspace / "nextflow.config"
    stages = " -> ".join(PIPELINE_STAGES)
    nf.write_text(textwrap.dedent(f"""\
        // E. coli functional-genomics pipeline (Nextflow skeleton).
        // Stages: {stages}
        nextflow.enable.dsl = 2

        params.reads_r1 = null
        params.reads_r2 = null
        params.outdir   = 'results'

        // TODO(agent): define one process per stage
        //   FASTP -> SPADES -> BAKTA -> EGGNOG -> CLUSTERPROFILER
        // and a workflow that chains them; publish the final PNG to params.outdir.

        workflow {{
            // fill in
        }}
    """))
    cfg.write_text(textwrap.dedent("""\
        // Fill in executor / container / conda directives per arm.
        params.outdir = 'results'
    """))
    return nf, cfg


def build_env_spec(layout: SandboxLayout, env: str) -> Path:
    """Write the arm's environment start-state spec.

    env == 'mamba'      -> workspace/env/environment.yml (conda spec)
    env == 'container'  -> workspace/env/images.txt      (sif path + quay tag list)

    Writing the *spec* is the real start-state. Actually materializing the
    environment (running ``mamba env create`` / pulling+converting every image)
    is the agent's job during the run, or the team-fill hooks below for a host
    that must be pre-provisioned.
    """
    env_dir = layout.workspace / "env"
    env_dir.mkdir(parents=True, exist_ok=True)
    if env == "mamba":
        spec = env_dir / "environment.yml"
        spec.write_text(textwrap.dedent("""\
            name: ecoli-pipeline
            channels:
              - conda-forge
              - bioconda
            dependencies:
              - fastp=0.23.4
              - spades=3.15.5
              - bakta=1.9.3
              - eggnog-mapper=2.1.12
              - bioconductor-clusterprofiler=4.6.0
              - abricate=1.0.1
        """))
        return spec
    if env == "container":
        spec = env_dir / "images.txt"
        lines = [
            f"{key}\t{tag}\t{sif}"
            for key, (sif, tag) in CONTAINER_IMAGES.items()
        ]
        spec.write_text(
            "# tool\tquay_tag\tstaged_sif_path\n" + "\n".join(lines) + "\n"
        )
        return spec
    raise ValueError(f"build_env_spec: no spec for env={env!r} "
                     f"(only 'mamba' / 'container' carry an env spec)")


def build_intermediate_contigs(layout: SandboxLayout) -> Path:
    """t7 (from-middle) start-state: pre-computed assembly contigs.

    Copies the staged toy contigs if the materials agent produced them, else
    synthesises a tiny valid multi-FASTA so the from-middle scenario has a real
    assembly to resume from without re-running fastp/SPAdes.
    """
    dest = layout.workspace / "precomputed" / "contigs.fasta"
    dest.parent.mkdir(parents=True, exist_ok=True)
    staged = MATERIALS_ROOT / "intermediates" / "contigs.fasta"  # RECONCILE if renamed
    if staged.exists():
        shutil.copy2(staged, dest)
        return dest
    # Synthetic fallback: two short contigs (valid FASTA; downstream tools accept it).
    dest.write_text(
        ">contig_1 synthetic_toy\n"
        + "ATGCGATCGATTACGGCTAGCTAGGCTAACGGATCGGCTAGCTAGCATCGATCGTAGCTAG\n" * 4
        + ">contig_2 synthetic_toy\n"
        + "TTGACAGGCTAACGTTAGCGGCTAGCTAGGCATCGGCTAGCATCGGCTAACGATCGGATCG\n" * 4
    )
    return dest


# ---------------------------------------------------------------------------
# Metasmith arm start-state (A10)
# ---------------------------------------------------------------------------

#: Source of the std transform library authored in parallel.
_STD_LIB_SRC = Path(__file__).resolve().parents[4] / "main" / "transforms" / "std"


def provision_metasmith(layout: SandboxLayout, ctx) -> Path:
    """Stage the metasmith std transform library + a typed input data library.

    Copies ``main/transforms/std`` into ``<workspace>/std/`` and registers the
    paired reads into ``<workspace>/inputs.xgdb`` typed as the INPUT_TYPES, so
    the planner has both a transform library and a typed starting point.

    NOTE: the std transforms (fastp/SPAdes/eggNOG/clusterProfiler) are authored
    in parallel; if the compiled library is incomplete this stages what exists
    and leaves compilation to reconcile-time. The data-library step needs reads
    on disk, so it is skipped (with the tree still staged) when reads are absent.
    """
    dest = layout.workspace / "std"
    if dest.exists():
        shutil.rmtree(dest)
    if not _STD_LIB_SRC.exists():
        raise RuntimeError(
            f"metasmith std transform library not found at {_STD_LIB_SRC}; "
            f"cannot provision the metasmith arm."
        )
    shutil.copytree(_STD_LIB_SRC, dest)

    if not _reads_present():
        # Structure staged; the typed data library needs the real reads, which
        # the materials agent stages separately. Live runs will have them.
        return dest

    if not _msm_bin(layout).exists():
        # The typed data library is built via the sandbox-local `metasmith data`
        # CLI, which needs metasmith pre-installed. Wiring/dry checks skip the
        # install, so stage the transform tree only and leave the data-lib step
        # to the live run (same degradation as the reads-absent case above).
        return dest

    r1, r2 = stage_reads(layout)
    # The type-lib's NAMESPACE is derived from its filename stem
    # (DataInstanceLibrary.AddTypeLibrary → namespace = path.name), and the
    # transforms reference `std::` types — so the file must be named `std.yml`,
    # not `dtypes.yml` (which would attach under namespace "dtypes" and make
    # `data add-item --dtype std::...` fail with "namespace [std] not found").
    type_lib = dest / "std.yml"
    if not type_lib.exists():
        shutil.copy(dest / "dtypes.yml", type_lib)
    specs = [
        DataItemSpec(name="ecoli_R1", dtype=INPUT_TYPES[0], host_path=r1),
        DataItemSpec(name="ecoli_R2", dtype=INPUT_TYPES[1], host_path=r2),
    ]
    # Pre-register the reference DBs as typed directory DataInstances so the
    # metasmith arm starts from a prepared world (its reuse advantage): the
    # planner sees the bakta/eggNOG DB givens without the agent staging them.
    # NOTE: registering a large dir may hash its contents (eggNOG ~41G) — for the
    # full sweep, register once into a shared .xgdb and copy it per run rather
    # than re-registering each time (see SOP-05).
    if DB_BAKTA.exists():
        specs.append(DataItemSpec(name="bakta_db", dtype="std::bakta_database", host_path=DB_BAKTA))
    if DB_EGGNOG.exists():
        specs.append(DataItemSpec(name="eggnog_db", dtype="std::eggnog_database", host_path=DB_EGGNOG))
    build_data_lib(layout, layout.workspace / "inputs.xgdb", type_lib, specs)
    return dest


# ---------------------------------------------------------------------------
# Team-fill hooks (documented NotImplementedError stubs — NOT on A7/A10 path)
# ---------------------------------------------------------------------------


def install_conda_pipeline_env(layout: SandboxLayout, ctx) -> Path:
    """[TEAM-FILL] Harness-side ``mamba env create`` from the staged spec.

    The mamba arms' *start-state* (the environment.yml spec) is provisioned by
    ``build_env_spec``; actually building the env host-side (rather than letting
    the agent do it during the run) is deferred. Wire this in if a mamba-arm run
    should start from an already-materialized env.
    """
    raise NotImplementedError(
        "install_conda_pipeline_env is a team-fill hook: `mamba env create -f "
        f"{layout.workspace}/env/environment.yml`. The spec is staged by "
        "build_env_spec(layout, 'mamba'); implement host-side creation here if "
        "the arm should not create it during the run."
    )


def prefetch_images_remote(layout: SandboxLayout, ctx, host: str) -> None:
    """[TEAM-FILL] Pre-stage tool sifs + DBs onto a remote host (chamois / HPC).

    Container arms on a remote host need the sifs + reference DBs mirrored ahead
    of time (compute nodes have no internet). t4/t5 depend on this once they run
    against real remote hosts.
    """
    raise NotImplementedError(
        f"prefetch_images_remote({host!r}) is a team-fill hook: mirror "
        f"{MATERIALS_ROOT}/images/*.sif and the reference DBs onto {host} before "
        f"the remote run (see materials/manifest.md § Mirroring)."
    )


def stage_nfcore_bacass(layout: SandboxLayout, ctx) -> Path:
    """[TEAM-FILL] nf-core/bacass provenance variant (tests 6 & 7, NF arms + A10).

    The toy provenance is fully real; the nf-core sub-study (starting from the
    pinned bacass checkout at NFCORE_BACASS) is an additive documented hook.
    """
    raise NotImplementedError(
        "stage_nfcore_bacass is a team-fill hook for the nf-core sub-study "
        f"(tests 6 & 7): copy the pinned checkout from {NFCORE_BACASS} and, for "
        "A10, stage the functionally-matched metasmith pipeline. Toy provenance "
        "is the default and is fully provisioned."
    )


# ---------------------------------------------------------------------------
# Provision dispatcher
# ---------------------------------------------------------------------------


ProvisionFn = Callable[[SandboxLayout, object], None]


def provision_for(arm: Arm) -> ProvisionFn:
    """Return a ``provision(layout, ctx)`` callable staging ``arm``'s start-state.

    Composition: an env-channel builder + an orchestrator builder. Reads are
    staged for every arm first (fail-fast if absent). The returned callable is
    always safe to call for the reference arms A7 (container/ad-hoc) and A10
    (metasmith); mamba / snakemake / nextflow arms get their real start-state
    files too. The deeper host-materialization steps are the team-fill hooks
    above.
    """
    def _provision(layout: SandboxLayout, ctx) -> None:
        # Common: ensure the results dir exists and reads are staged.
        (layout.workspace / "results").mkdir(parents=True, exist_ok=True)

        if arm.is_metasmith:
            provision_metasmith(layout, ctx)
            return

        # Reads are needed by every baseline arm.
        if _reads_present():
            stage_reads(layout)

        # --- env-channel start-state
        if arm.env == "ad-hoc":
            build_scripts_repo(layout)
        elif arm.env == "container":
            build_env_spec(layout, "container")
            build_scripts_repo(layout)  # container arms still glue via shell unless orch overrides
        elif arm.env == "mamba":
            build_env_spec(layout, "mamba")
        else:
            raise ValueError(f"provision_for: unknown env {arm.env!r}")

        # --- orchestrator start-state
        if arm.orchestrator == "ad-hoc":
            pass  # scripts repo (or images list) already staged
        elif arm.orchestrator == "snakemake":
            build_snakefile(layout)
        elif arm.orchestrator == "nextflow":
            build_nextflow(layout)
        else:
            raise ValueError(f"provision_for: unknown orchestrator {arm.orchestrator!r}")

    return _provision
