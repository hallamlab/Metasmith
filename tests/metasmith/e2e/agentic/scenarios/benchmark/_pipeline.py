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

# >>> RECONCILE-ME: pipeline type + artifact constants <<<

TYPE_NAMESPACE = "std"

INPUT_TYPES: tuple[str, str] = (
    "std::paired_reads_forward",
    "std::paired_reads_reverse",
)

FINAL_ARTIFACT_TYPE = "std::enrichment_plot"
FINAL_ARTIFACT_TABLE_TYPE = "std::enrichment_table"

FINAL_RESULTS_REL = "workspace/results"

FINAL_ARTIFACT_GLOB = "workspace/results/**/*.png"

FINAL_TABLE_GLOB = "workspace/results/**/*.tsv"


GOLDEN_MIN_PNG_BYTES = 1024
GOLDEN_TSV_REQUIRED_COLUMNS: tuple[str, ...] = (
    "ID", "Description", "GeneRatio", "BgRatio",
    "pvalue", "p.adjust", "qvalue", "geneID", "Count",
)
GOLDEN_MIN_TSV_ROWS = 1

EXPECTED_TRACE: tuple[str, str] = (INPUT_TYPES[0], FINAL_ARTIFACT_TYPE)

# >>> RECONCILE-ME: on-disk materials (staged by the parallel materials agent)

MATERIALS_ROOT = Path(
    os.environ.get(
        "BENCHMARK_MATERIALS_ROOT",
        "/home/tony/agentic_workspace/data/metasmith/token-benchmark/materials",
    )
)

READS_R1_SRC = MATERIALS_ROOT / "micro" / "reads" / "ecoli_R1.fastq.gz"
READS_R2_SRC = MATERIALS_ROOT / "micro" / "reads" / "ecoli_R2.fastq.gz"

DB_BAKTA = MATERIALS_ROOT / "dbs" / "bakta-light" / "db-light"
DB_EGGNOG = MATERIALS_ROOT / "micro" / "dbs" / "eggnog" / "data"

GOLDEN_DIR = MATERIALS_ROOT / "golden"
GOLDEN_CONTIGS = GOLDEN_DIR / "contigs.fasta"

CONTAINER_IMAGES: dict[str, tuple[Path, str]] = {
    "fastp": (MATERIALS_ROOT / "images" / "fastp.sif",
              "quay.io/biocontainers/fastp:0.23.4--h5f740d0_0"),
    "spades": (MATERIALS_ROOT / "images" / "spades.sif",
               "quay.io/biocontainers/spades:3.15.5--h95f258a_1"),
    "bakta": (MATERIALS_ROOT / "images" / "bakta.sif",
              "quay.io/biocontainers/bakta:1.11.0--pyhdfd78af_0"),
    "eggnog-mapper": (MATERIALS_ROOT / "images" / "eggnog-mapper.sif",
                      "quay.io/biocontainers/eggnog-mapper:2.1.12--pyhdfd78af_0"),
    "clusterprofiler": (MATERIALS_ROOT / "images" / "clusterProfiler.sif",
                        "quay.io/biocontainers/bioconductor-clusterprofiler:4.6.0--r42hdfd78af_0"),
    "abricate": (MATERIALS_ROOT / "images" / "abricate.sif",
                 "quay.io/biocontainers/abricate:1.0.1--ha8f3691_2"),
}

NFCORE_BACASS = MATERIALS_ROOT / "pipelines" / "nf-core-bacass"

PIPELINE_STAGES = ("fastp", "spades", "bakta", "eggnog-mapper", "clusterprofiler")

READS_REL_DIR = "workspace/reads"
READS_R1_REL = f"{READS_REL_DIR}/ecoli_R1.fastq.gz"
READS_R2_REL = f"{READS_REL_DIR}/ecoli_R2.fastq.gz"


@dataclass(frozen=True)
class ToolSpec:
    key: str
    binary: str
    role: str
    probe: str


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

INSTALL_ENV_CHANNELS = ("ad-hoc", "mamba", "container", "metasmith")


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
        Your job is to BUILD the pipeline implementation, then SUBMIT it — a
        separate non-agentic checker runs your submission to produce the final
        artifact: the clusterProfiler KEGG/GO functional-enrichment result (a PNG
        dotplot + its TSV results table) under

          {sb}/{FINAL_RESULTS_REL}/

        Validate your implementation cheaply before submitting (e.g. a metasmith
        `plan`, `nextflow -stub-run`, `snakemake -n`, or a dry run). Do NOT block
        on the full, long-running pipeline yourself — the checker executes it.

        ## Submit protocol
        If any step fails or produces unexpected output, stop immediately and run:

        ```bash
        metasmith e2e report_issue --cwd "{sb}" --reason "<one line describing what you saw>"
        ```

        When your implementation is ready and cheaply validated, submit it with
        the form matching what you built:

        ```bash
        # a metasmith workflow you staged (metasmith plan + `metasmith workflow stage`):
        metasmith e2e submit --cwd "{sb}" --key <task_key> --agent <agent>

        # OR a runnable script/pipeline you wrote:
        metasmith e2e submit --cwd "{sb}" --entrypoint <path to run.sh|Snakefile|main.nf>
        ```

        The `--cwd "{sb}"` flag writes `CONTROL.json` to the sandbox root, where
        the harness loop watches for it. After you submit, the checker runs your
        implementation and materializes the final artifact.
        """)


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
    "metasmith": "",
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
    "metasmith": "",
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
    if arm.preamble.strip():
        return arm
    return dataclasses.replace(
        arm,
        preamble=preamble_for(arm),
        reference_docs=reference_docs_for(arm),
    )


def compose_benchmark_prompt(shared_block: str, arm: Arm) -> str:
    return compose_prompt(shared_block, arm_with_preamble(arm))


def stage_reads(layout: SandboxLayout) -> tuple[Path, Path]:
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
              - bakta=1.11.0
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
    dest = layout.workspace / "precomputed" / "contigs.fasta"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if GOLDEN_CONTIGS.exists():
        shutil.copy2(GOLDEN_CONTIGS, dest)
        return dest
    staged = MATERIALS_ROOT / "intermediates" / "contigs.fasta"
    if staged.exists():
        shutil.copy2(staged, dest)
        return dest
    dest.write_text(
        ">contig_1 synthetic_toy\n"
        + "ATGCGATCGATTACGGCTAGCTAGGCTAACGGATCGGCTAGCTAGCATCGATCGTAGCTAG\n" * 4
        + ">contig_2 synthetic_toy\n"
        + "TTGACAGGCTAACGTTAGCGGCTAGCTAGGCATCGGCTAGCATCGGCTAACGATCGGATCG\n" * 4
    )
    return dest


_STD_LIB_SRC = Path(__file__).resolve().parents[6] / "research" / "metasmith" / "transforms" / "std"


def provision_metasmith(layout: SandboxLayout, ctx) -> Path:
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
        return dest

    if not _msm_bin(layout).exists():
        return dest

    r1, r2 = stage_reads(layout)
    type_lib = dest / "std.yml"
    if not type_lib.exists():
        shutil.copy(dest / "dtypes.yml", type_lib)
    specs = [
        DataItemSpec(name="ecoli_R1", dtype=INPUT_TYPES[0], host_path=r1),
        DataItemSpec(name="ecoli_R2", dtype=INPUT_TYPES[1], host_path=r2),
    ]
    if DB_BAKTA.exists():
        specs.append(DataItemSpec(name="bakta_db", dtype="std::bakta_database", host_path=DB_BAKTA))
    if DB_EGGNOG.exists():
        specs.append(DataItemSpec(name="eggnog_db", dtype="std::eggnog_database", host_path=DB_EGGNOG))
    build_data_lib(layout, layout.workspace / "inputs.xgdb", type_lib, specs)
    return dest


def install_conda_pipeline_env(layout: SandboxLayout, ctx) -> Path:
    raise NotImplementedError(
        "install_conda_pipeline_env is a team-fill hook: `mamba env create -f "
        f"{layout.workspace}/env/environment.yml`. The spec is staged by "
        "build_env_spec(layout, 'mamba'); implement host-side creation here if "
        "the arm should not create it during the run."
    )


def prefetch_images_remote(layout: SandboxLayout, ctx, host: str) -> None:
    raise NotImplementedError(
        f"prefetch_images_remote({host!r}) is a team-fill hook: mirror "
        f"{MATERIALS_ROOT}/images/*.sif and the reference DBs onto {host} before "
        f"the remote run (see materials/manifest.md § Mirroring)."
    )


def stage_nfcore_bacass(layout: SandboxLayout, ctx) -> Path:
    raise NotImplementedError(
        "stage_nfcore_bacass is a team-fill hook for the nf-core sub-study "
        f"(tests 6 & 7): copy the pinned checkout from {NFCORE_BACASS} and, for "
        "A10, stage the functionally-matched metasmith pipeline. Toy provenance "
        "is the default and is fully provisioned."
    )


ProvisionFn = Callable[[SandboxLayout, object], None]


def provision_for(arm: Arm) -> ProvisionFn:
    def _provision(layout: SandboxLayout, ctx) -> None:
        (layout.workspace / "results").mkdir(parents=True, exist_ok=True)

        if arm.is_metasmith:
            provision_metasmith(layout, ctx)
            return

        if _reads_present():
            stage_reads(layout)

        if arm.env == "ad-hoc":
            build_scripts_repo(layout)
        elif arm.env == "container":
            build_env_spec(layout, "container")
            build_scripts_repo(layout)
        elif arm.env == "mamba":
            build_env_spec(layout, "mamba")
        else:
            raise ValueError(f"provision_for: unknown env {arm.env!r}")

        if arm.orchestrator == "ad-hoc":
            pass
        elif arm.orchestrator == "snakemake":
            build_snakefile(layout)
        elif arm.orchestrator == "nextflow":
            build_nextflow(layout)
        else:
            raise ValueError(f"provision_for: unknown orchestrator {arm.orchestrator!r}")

    return _provision
