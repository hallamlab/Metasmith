#!/usr/bin/env python3
"""Setup (W0) for metag_workflow_from_reads_sockeye.py — prepare an HPC cluster
to run the full metagenomics workflow.

Compute nodes typically have NO outbound network, so everything the run needs
must be in place on the login node FIRST. This script does the three pre-run
staging jobs, then metag_workflow_from_reads_sockeye.py (W1) executes straight
from what is staged here, with no on-the-fly pulls or downloads:

  1. Prefetch the workflow's tool containers into the persistent apptainer store
     (<agent_home>/container_images) via the `env::pulled_container`
     transform, run on the LOCAL executor (login node = has internet).
  2. Upload the paired R1/R2 reads to cluster scratch (metasmith does not
     auto-transfer non-resident inputs).
  3. Verify the five pre-staged reference DBs are present, and flag phyloFlash
     (the one DB that still needs building — see W1's docstring).

Pairs with metag_workflow_from_reads_sockeye.py — the two share the same
HPC_HOST / account / agent_home / remote-inputs scheme so W1 finds what W0 stages.

Configuration — set via environment (or edit the defaults):
  MSM_HPC_HOST       ssh host alias for the cluster        (default: sockeye)
  MSM_SLURM_ACCOUNT  SLURM allocation (only used for the /scratch/<acct>/<user>
                     paths here; W0 itself runs on the login node)  (REQUIRED)
  MSM_REF_DB_DIR     cluster dir holding the reference DBs  (REQUIRED)
  MSM_READS_R1/R2    paired reads to upload                 (REQUIRED)
  MSM_SRC            metasmith source checkout to import    (optional; else use
                                                             an installed metasmith)

Usage:
  python main/metag_setup_sockeye.py          # render pull DAG + verify DBs only
  python main/metag_setup_sockeye.py --run    # deploy agent, upload reads, pull containers
"""
import os
import sys
import subprocess
from pathlib import Path

# metasmith must be importable; set MSM_SRC to a source checkout if not installed.
if os.environ.get("MSM_SRC"):
    sys.path.insert(0, os.environ["MSM_SRC"])
from metasmith.python_api import (
    Agent, SshSource, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
    Resources, Size,
)

# ── site config — set via env vars or edit the defaults (must match W1) ───────
HPC_HOST      = os.environ.get("MSM_HPC_HOST", "sockeye")            # ssh host alias
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "<slurm-allocation>")
SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer"]  # Sockeye module order

MLIB = Path(__file__).resolve().parent.parent.parent

# ── read inputs (uploaded to the cluster) ────────────────────────────────────
R1 = Path(os.environ.get("MSM_READS_R1", "<reads-R1.fq.gz>"))
R2 = Path(os.environ.get("MSM_READS_R2", "<reads-R2.fq.gz>"))

# ── reference DBs expected on the cluster (layout mirrored in W1) ─────────────
REF = Path(os.environ.get("MSM_REF_DB_DIR", "<ref-db-dir-on-cluster>"))
REQUIRED_DBS = {
    "ref::uniref50_diamond_db": REF / "diamond"   / "uniref50.dmnd",
    "ref::kofamscan_profiles":  REF / "kofamscan" / "profiles.tgz",
    "ref::kofamscan_ko_list":   REF / "kofamscan" / "ko_list.tsv",
    "ref::metabuli_ref":        REF / "metabuli"  / "gtdb",
    "ref::gtdb":                REF / "gtdb"      / "release226",   # set to your GTDB release
}
# Not pre-staged — build once on the login node before the phyloFlash branch of
# W1 can complete (see W1 docstring). Verified as a WARNING here, not an error.
PHYLOFLASH_DB = REF / "phyloflash" / "138.2"

# ── tool containers the full metag DAG uses (aggregator runs container-less) ──
PULL_ALL = False
W1_CONTAINERS = [
    "seqkit",      # seqkit_reads, assembly_stats
    "bbtools",     # bbduk, phyloFlash reformat
    "megahit",     # assembly
    "samtools",    # assembly_stats (bam/coverage)
    "minimap2",    # assembly_stats
    "bedtools",    # assembly_stats
    "pprodigal",   # prodigal (ORFs)
    "diamond",     # diamond_uniref50
    "kofamscan",   # kofamscan
    "metabuli",    # per-contig taxonomy
    "metabat2",    # binner
    "semibin",     # binner (semibin2)
    "comebin",     # binner
    "checkm",      # checkm2 bin QC
    "skani",       # skani_dedup
    "gtdbtk",      # per-bin taxonomy
    "phyloflash",  # SSU rRNA taxonomy
]

# ── script ───────────────────────────────────────────────────────────────────
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
    return subprocess.run(
        ["ssh", HPC_HOST, cmd], capture_output=True, text=True, check=True
    ).stdout.strip()


def verify_dbs():
    """Stat each expected DB on the cluster; hard-error on a missing REQUIRED DB, warn on phyloFlash."""
    checks = {**REQUIRED_DBS, "ref::phyloflash_db (optional)": PHYLOFLASH_DB}
    probe = "; ".join(
        f'test -e "{p}" && echo "OK  {t} -> {p}" || echo "MISS {t} -> {p}"'
        for t, p in checks.items()
    )
    print("==> verify reference DBs on the cluster", flush=True)
    out = ssh_capture(probe)
    print(out, flush=True)
    missing = [t for t in REQUIRED_DBS if f"MISS {t} " in out]
    if missing:
        raise RuntimeError(f"required DBs missing (check MSM_REF_DB_DIR / layout): {missing}")
    if "MISS ref::phyloflash_db" in out:
        print("!! phyloFlash DB not staged — the phyloFlash branch of W1 will fail until\n"
              f"   it is built at {PHYLOFLASH_DB} (see metag_workflow_from_reads_sockeye.py).",
              file=sys.stderr, flush=True)


def upload_reads(scratch: str):
    """Upload R1/R2 under the shared remote-inputs dir (idempotent). Mirrors W1's path scheme."""
    remote_dir = f"{scratch}/metag_workflow/inputs"
    ssh_capture(f"mkdir -p {remote_dir}")
    for local_path in (R1, R2):
        remote_path = f"{remote_dir}/{local_path.name}"
        remote_size = subprocess.run(
            ["ssh", HPC_HOST, f"stat -c %s {remote_path} 2>/dev/null || echo MISSING"],
            capture_output=True, text=True,
        ).stdout.strip()
        if remote_size == str(local_path.stat().st_size):
            print(f"==> read already on the cluster: {remote_path}", flush=True)
        else:
            print(f"==> uploading {local_path.name} -> {HPC_HOST}:{remote_path}", flush=True)
            subprocess.run(["scp", str(local_path), f"{HPC_HOST}:{remote_path}"], check=True)


def main():
    require_configured()
    user = ssh_capture("echo $USER")
    scratch = f"/scratch/{SLURM_ACCOUNT}/{user}"    # Sockeye scratch convention
    agent_home = f"{scratch}/metasmith"

    # ── 1. verify reference DBs (fail fast before any deploy) ─────────────────
    verify_dbs()

    # ── 2. build the container-pull plan (login-node/local executor) ──────────
    smith = Agent(
        home=SshSource(host=HPC_HOST, path=agent_home).AsSource(),
        runtime=Runtime.APPTAINER,
        setup_commands=SETUP_COMMANDS,
    )

    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    logistics  = TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics")

    all_samples = list(containers.AsSamples("env::env"))
    if PULL_ALL:
        samples = all_samples
    else:
        wl = {Path(f"{n}.oci") for n in W1_CONTAINERS}
        samples = [s for s in all_samples if s._mask.intersection(wl)]
        missing = wl - {p for s in samples for p in s._mask}
        assert not missing, f"requested containers not in {MLIB}/resources/env: {sorted(missing)}"
    print(f"==> containers to pull: {len(samples)}"
          + ("" if PULL_ALL else f" (W1 set: {W1_CONTAINERS})"), flush=True)

    targets = TargetBuilder()
    targets.Add("env::pulled_container")

    task = smith.GenerateWorkflow(
        samples=samples,
        resources=[],
        transforms=[logistics],
        targets=targets,
    )
    if not task.ok or len(task.plan.steps) == 0:
        print(f"!! pull plan failed: hints={list(task.plan.hints)}", file=sys.stderr)
        return 3
    out = Path(__file__).resolve().parent.parent.parent / "results" / "metag_workflow_sockeye"
    out.mkdir(parents=True, exist_ok=True)
    try:
        task.plan.RenderDAG(str(out / "w0_pull_dag"), format="svg", blacklist_namespaces=set())
    except Exception as e:
        print(f"(pull DAG render skipped: {e})")
    print(f"==> pull plan OK — {len(task.plan.steps)} steps, key={task.GetKey()}", flush=True)

    if not SUBMIT:
        print("render/verify-only; pass --run to deploy the agent, upload reads, and pull containers")
        return 0

    # ── run: deploy agent + driver image, upload reads, pull containers ───────
    # All three need internet → run on the login node (LOCAL executor).
    print("==> Deploy(assertive=True)", flush=True)
    smith.Deploy(assertive=True)

    upload_reads(scratch)

    print("==> pull containers into store (LOCAL executor)", flush=True)
    smith.StageWorkflow(task, on_exist="update")
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["local"],
        params=dict(executor=dict(queueSize=4)),
        resource_overrides={"all": Resources(memory=Size.GB(2), cpus=2)},
    )
    print("==> setup done — now run metag_workflow_from_reads_sockeye.py --run", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
