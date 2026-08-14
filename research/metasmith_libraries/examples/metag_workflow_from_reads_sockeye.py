#!/usr/bin/env python3
"""Run the full metagenomics workflow from short reads on an HPC cluster (W1).

Cluster port of the metagenomics-from-reads spec (see
metagenomics_from_paired_reads.py for the current local template). Same DAG
(seqkit_reads -> bbduk ->
megahit -> prodigal -> {diamond_uniref50, kofamscan}; metabuli; assembly_stats ->
3 binners -> checkm2 -> aggregator -> skani_dedup; gtdbtk; phyloFlash), retargeted
to run end-to-end over SSH on a SLURM cluster. Written against a Sockeye-style
cluster (module system + apptainer + allocation-coded /scratch); adjust the
SETUP_COMMANDS / scratch convention for a different site.

Run metag_setup_sockeye.py --run FIRST. That setup (W0) step:
  - prefetches every tool container into the shared apptainer store (compute
    nodes typically have no internet, so nothing can be pulled at run time),
  - uploads the R1/R2 reads to cluster scratch (metasmith does not auto-transfer
    non-resident inputs — it fails fast instead), and
  - verifies the reference DBs are present.
This script (W1) then references exactly what W0 staged.

Differences vs. the local driver:
  - Agent retargeted to the cluster over SSH with the APPTAINER runtime. Note the
    Sockeye module quirk (`module load gcc/9.4.0` BEFORE `module load apptainer`;
    a bare `module load apptainer` silently no-ops there).
  - R1/R2 and the DBs are referenced by their REMOTE (cluster) paths.
  - The five external DBs are supplied as pre-staged resources so the planner
    SKIPS the download* transforms — each is hard-labeled `local` (except
    downloadPhyloflashDB) → pinned to the login node where its 64GB+ ask + tens
    of GB of wget cannot run. Pattern mirrors launch_dl_embeddings.
  - Runs from allocation-coded /scratch (many clusters forbid SLURM jobs from
    $HOME, and Nextflow's work dir must live under scratch).

Configuration — set via environment (or edit the defaults):
  MSM_HPC_HOST       ssh host alias for the cluster        (default: sockeye)
  MSM_SLURM_ACCOUNT  SLURM allocation to submit under      (REQUIRED)
  MSM_REF_DB_DIR     cluster dir holding the reference DBs  (REQUIRED)
  MSM_READS_R1/R2    paired reads to run                    (REQUIRED)
  MSM_SRC            metasmith source checkout to import    (optional; else use
                                                             an installed metasmith)

Usage:
  python main/metag_workflow_from_reads_sockeye.py          # render DAG only
  python main/metag_workflow_from_reads_sockeye.py --run    # submit to SLURM

Expected reference-DB layout under $MSM_REF_DB_DIR:
  ref::uniref50_diamond_db  -> diamond/uniref50.dmnd
  ref::kofamscan_profiles   -> kofamscan/profiles.tgz   (the tar.gz itself)
  ref::kofamscan_ko_list    -> kofamscan/ko_list.tsv
  ref::metabuli_ref         -> metabuli/gtdb
  ref::gtdb                 -> gtdb/<release>            (GTDBTK_DATA_PATH root)*
  ref::phyloflash_db        -> phyloflash/138.2          **

  *  Point ref::gtdb at the GTDB release dir your gtdbtk container expects (the
     container binds GTDBTK_DATA_PATH directly to this dir); an older release
     works as long as the container version matches it.
  ** phyloFlash needs a bbmap-indexed dbhome (.udb + tree) built by
     phyloFlash_makedb.pl, not just raw SILVA. Build it once on the login node
     (has internet), e.g.:
         module load gcc/9.4.0 apptainer
         mkdir -p "$MSM_REF_DB_DIR/phyloflash/138.2"
         cd       "$MSM_REF_DB_DIR/phyloflash/138.2"
         apptainer exec <phyloflash.sif> phyloFlash_makedb.pl --remote_dbsource=138.2
     (if your cluster already mirrors SILVA 138.2, point makedb at it to skip the
     download). Until it exists the phyloFlash branch of the run fails at its
     step; the rest of the DAG is wired correctly and completes independently.
"""
import os
import sys
from pathlib import Path

# metasmith must be importable; set MSM_SRC to a source checkout if not installed.
if os.environ.get("MSM_SRC"):
    sys.path.insert(0, os.environ["MSM_SRC"])
from metasmith.python_api import (
    Agent, SshSource, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder,
)

# ── site config — set via env vars or edit the defaults (must match W0) ───────
HPC_HOST      = os.environ.get("MSM_HPC_HOST", "sockeye")            # ssh host alias
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "<slurm-allocation>")
SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer"]  # Sockeye module order

# ── pre-staged reference DBs on the cluster (skip the download* transforms) ───
REF = Path(os.environ.get("MSM_REF_DB_DIR", "<ref-db-dir-on-cluster>"))
REMOTE_UNIREF50_DMND   = REF / "diamond"    / "uniref50.dmnd"
REMOTE_KOFAM_PROFILES  = REF / "kofamscan"  / "profiles.tgz"   # ref::kofamscan_profiles IS the tarball
REMOTE_KOFAM_KO_LIST   = REF / "kofamscan"  / "ko_list.tsv"
REMOTE_METABULI_REF    = REF / "metabuli"   / "gtdb"
REMOTE_GTDB            = REF / "gtdb"        / "release226"    # GTDBTK_DATA_PATH root (set to your release)
REMOTE_PHYLOFLASH_DB   = REF / "phyloflash" / "138.2"          # see ** in the module docstring

# ── read inputs (uploaded to the cluster by metag_setup_sockeye.py) ──────────
# The local names are used only to derive the remote filenames; the reads
# themselves are referenced by their cluster paths under the shared inputs dir.
R1 = Path(os.environ.get("MSM_READS_R1", "<reads-R1.fq.gz>"))
R2 = Path(os.environ.get("MSM_READS_R2", "<reads-R2.fq.gz>"))
OUT_DIR = Path("results/metag_workflow_sockeye")

MLIB = Path(__file__).resolve().parent.parent.parent

SUBMIT = "--run" in sys.argv


def require_configured():
    unset = [k for k, v in {
        "MSM_SLURM_ACCOUNT": SLURM_ACCOUNT,
        "MSM_REF_DB_DIR":    str(REF),
        "MSM_READS_R1":      str(R1),
        "MSM_READS_R2":      str(R2),
    }.items() if "<" in str(v)]
    if unset:
        print("configure these first (env var, or edit the default in this file):",
              file=sys.stderr)
        for k in unset:
            print(f"  {k}", file=sys.stderr)
        raise SystemExit(2)


def ssh_capture(cmd: str) -> str:
    import subprocess
    return subprocess.run(
        ["ssh", HPC_HOST, cmd], capture_output=True, text=True, check=True
    ).stdout.strip()


def main():
    require_configured()
    user = ssh_capture("echo $USER")
    # Many clusters forbid running SLURM jobs from $HOME — the agent home (and thus
    # Nextflow's work dir) must live on allocation-coded scratch. Sockeye convention:
    scratch = f"/scratch/{SLURM_ACCOUNT}/{user}"
    agent_home = f"{scratch}/metasmith"
    remote_dir = f"{scratch}/metag_workflow/inputs"          # shared scheme with W0
    remote_r1 = f"{remote_dir}/{R1.name}"
    remote_r2 = f"{remote_dir}/{R2.name}"

    out = OUT_DIR.resolve()
    out.mkdir(parents=True, exist_ok=True)

    # inputs.xgdb references REMOTE (cluster) paths; not validated locally
    inputs = DataInstanceLibrary(out / "inputs.xgdb")
    for tl in ["sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
               "taxonomy.yml", "binning.yml", "binning_local.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    # meta -> reads lineage (same as the local driver), but R1/R2 are remote paths
    meta = inputs.AddValue("reads_metadata.json", {"parity": "paired", "length_class": "short"}, "sequences::read_metadata")
    pair = inputs.AddValue("read_pair.txt", "sample_1", "sequences::read_pair", parents={meta})
    inputs.AddItem(Path(remote_r1), "sequences::zipped_forward_short_reads", parents={pair})
    inputs.AddItem(Path(remote_r2), "sequences::zipped_reverse_short_reads", parents={pair})

    # pre-staged DBs → planner skips the (login-node-impossible) download steps
    inputs.AddItem(REMOTE_UNIREF50_DMND,  "ref::uniref50_diamond_db")
    inputs.AddItem(REMOTE_KOFAM_PROFILES, "ref::kofamscan_profiles")
    inputs.AddItem(REMOTE_KOFAM_KO_LIST,  "ref::kofamscan_ko_list")
    inputs.AddItem(REMOTE_METABULI_REF,   "ref::metabuli_ref")
    inputs.AddItem(REMOTE_GTDB,           "ref::gtdb")
    inputs.AddItem(REMOTE_PHYLOFLASH_DB,  "ref::phyloflash_db")
    inputs.Save()

    smith = Agent(
        home=SshSource(host=HPC_HOST, path=agent_home).AsSource(),
        runtime=Runtime.APPTAINER,
        setup_commands=SETUP_COMMANDS,
    )

    targets = TargetBuilder()
    targets.Add("sequences::read_qc_stats")
    targets.Add("sequences::orfs")
    targets.Add("sequences::assembly_stats")
    targets.Add("sequences::assembly_per_contig_coverage")
    targets.Add("sequences::assembly_per_bp_coverage")
    targets.Add("annotation::diamond_uniref50_results")
    targets.Add("annotation::kofamscan_results")
    targets.Add("taxonomy::metabuli")
    targets.Add("taxonomy::phyloflash_summary")
    targets.Add("binning_local::cluster_table")

    # Per-binner fan-out: distinct TargetSpec parents force a separate
    # checkm + gtdbtk instance for each binner's bins (without parent
    # constraints the planner would pick one binner to satisfy each).
    mb_bin = targets.Add("sequences::metabat2_bin_fasta")
    sb_bin = targets.Add("sequences::semibin2_bin_fasta")
    cb_bin = targets.Add("sequences::comebin_bin_fasta")
    for parent in (mb_bin, sb_bin, cb_bin):
        targets.Add("taxonomy::checkm_stats", parents={parent})
        targets.Add("taxonomy::gtdbtk",       parents={parent})
    targets.Add("binning::metabat2_contig_to_bin_table")
    targets.Add("binning::semibin2_contig_to_bin_table")
    targets.Add("binning::comebin_contig_to_bin_table")

    print("==> GenerateWorkflow()", flush=True)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / "env"), inputs],
        transforms=[
            TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
            TransformInstanceLibrary.Load(MLIB / "transforms" / "assembly"),
            TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
            TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
        ],
        targets=targets,
    )
    if not task.ok or len(task.plan.steps) == 0:
        print(f"!! plan failed: hints={list(task.plan.hints)}", file=sys.stderr)
        return 3
    task.plan.RenderDAG(str(out / "dag"), format="svg")
    print(f"==> task key: {task.GetKey()}  steps={len(task.plan.steps)}", flush=True)

    if not SUBMIT:
        print("render-only; run metag_setup_sockeye.py --run first, then pass --run here to submit")
        return 0

    print("==> StageWorkflow(on_exist=clear)", flush=True)
    smith.StageWorkflow(task, on_exist="clear")

    print(f"==> RunWorkflow (SLURM, account={SLURM_ACCOUNT})", flush=True)
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params=dict(slurmAccount=SLURM_ACCOUNT),
    )

    print("==> WaitForWorkflow", flush=True)
    result = smith.WaitForWorkflow(task, timeout_s=43200.0, poll_s=30.0)
    print(f"==> status: {result['status']} after {result['elapsed_s']:.1f}s", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    src = smith.GetResultSource(task)
    print(f"==> result source: {src.GetPath()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
