#!/usr/bin/env python3
"""Run a DIAMOND UniRef50 annotation workflow for a nucleotide assembly FASTA on
an HPC cluster.

Cluster port of the DIAMOND UniRef50 annotation spec (see
annotation_palette_from_assembly.py for the current local template covering
the same annotator). Written against a Sockeye-style
cluster (module system + apptainer + allocation-coded /scratch). Minimal
differences vs. the local driver:
  - Agent retargeted to the cluster over SSH with the APPTAINER runtime. Note the
    Sockeye module quirk (`module load gcc/9.4.0` BEFORE `module load apptainer`;
    a bare `module load apptainer` silently no-ops there).
  - The input FASTA is uploaded to the cluster and referenced by its REMOTE path —
    metasmith does not auto-transfer non-resident inputs (it fails fast instead),
    so the .xgdb item must point at a path that already exists on the agent host.
  - Deploys to allocation-coded /scratch (many clusters forbid SLURM jobs from
    $HOME, and Nextflow's work dir must live under scratch).
  - Runs end-to-end (Deploy -> Generate -> Stage -> Run -> Wait -> Result) rather
    than stopping at DAG generation.

Configuration — set via environment (or edit the defaults):
  MSM_HPC_HOST       ssh host alias for the cluster        (default: sockeye)
  MSM_SLURM_ACCOUNT  SLURM allocation to submit under      (REQUIRED)
  MSM_REF_DB_DIR     cluster dir holding the reference DBs (REQUIRED)
  MSM_ASSEMBLY       local nucleotide assembly FASTA       (REQUIRED)
  MSM_SRC            metasmith source checkout to import    (optional; else use
                                                             an installed metasmith)

The pre-built UniRef50 DIAMOND DB is expected at
$MSM_REF_DB_DIR/diamond/uniref50.dmnd. Supplying it as a
ref::uniref50_diamond_db resource lets the planner SKIP downloadUniRef50DB
(hard-labeled `local` → pinned to the login node, where its 64GB ask + 60GB wget
+ `diamond makedb` cannot run). Pattern mirrors launch_dl_embeddings.
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
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder,
)

# ── site config — set via env vars or edit the defaults ──────────────────────
HPC_HOST      = os.environ.get("MSM_HPC_HOST", "sockeye")            # ssh host alias
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "<slurm-allocation>")
SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer"]  # Sockeye module order

# Pre-built UniRef50 DIAMOND DB already on the cluster → skip downloadUniRef50DB.
REF = Path(os.environ.get("MSM_REF_DB_DIR", "<ref-db-dir-on-cluster>"))
REMOTE_UNIREF50_DMND = REF / "diamond" / "uniref50.dmnd"

LOCAL_ASSEMBLY = Path(os.environ.get("MSM_ASSEMBLY", "<assembly.fna>"))  # nucleotide assembly FASTA
OUT_DIR = Path("results/diamond_uniref50_sockeye")

MLIB = Path(__file__).resolve().parent.parent.parent


def require_configured():
    unset = [k for k, v in {
        "MSM_SLURM_ACCOUNT": SLURM_ACCOUNT,
        "MSM_REF_DB_DIR":    str(REF),
        "MSM_ASSEMBLY":      str(LOCAL_ASSEMBLY),
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


def stage_input_to_cluster(base_dir: str) -> str:
    """Upload the assembly under base_dir on the cluster, return its remote path (idempotent)."""
    remote_dir = f"{base_dir}/diamond_uniref50/inputs"
    remote_path = f"{remote_dir}/{LOCAL_ASSEMBLY.name}"
    ssh_capture(f"mkdir -p {remote_dir}")
    remote_size = subprocess.run(
        ["ssh", HPC_HOST, f"stat -c %s {remote_path} 2>/dev/null || echo MISSING"],
        capture_output=True, text=True,
    ).stdout.strip()
    if remote_size == str(LOCAL_ASSEMBLY.stat().st_size):
        print(f"==> input already on the cluster: {remote_path}", flush=True)
    else:
        print(f"==> uploading {LOCAL_ASSEMBLY.name} -> {HPC_HOST}:{remote_path}", flush=True)
        subprocess.run(["scp", str(LOCAL_ASSEMBLY), f"{HPC_HOST}:{remote_path}"], check=True)
    return remote_path


def prefetch_tool_containers(agent_home: str, task_key: str):
    """Pre-pull the plan's tool containers into the agent cache on the LOGIN node.

    Compute nodes typically have NO outbound network, so a step's lazy
    `apptainer exec docker://...` dies with 'no route to host'. metasmith's
    Deploy pulls only its own image, not per-transform tools. We warm the cache
    here: parse the staged workflow.nf for the `env::<x>.oci` it actually
    uses, read each .oci's docker URL, and pull it as a .sif (use-sif).
    """
    run = f"{agent_home}/runs/{task_key}"
    setup = " && ".join(SETUP_COMMANDS)
    remote = f"""
        set -e
        {setup}
        export APPTAINER_CACHEDIR="{agent_home}/.apptainer_cache"; mkdir -p "$APPTAINER_CACHEDIR"
        R="{run}"; CACHE="{agent_home}/container_images"; mkdir -p "$CACHE"
        names=$(grep -oE 'env::[A-Za-z0-9._-]+\\.oci' "$R/workflow.nf" | sed 's/env:://' | sort -u)
        echo "plan containers: $names"
        for n in $names; do
            oci=$(find "$R/_metasmith/task/data" -name "$n" | head -1)
            [ -z "$oci" ] && {{ echo "FAIL: no .oci file for $n"; exit 2; }}
            url=$(tr -d '[:space:]' < "$oci")
            cname=$(printf '%s' "$url" | sed 's#://#..#g; s#:#..#g; s#/#_#g')
            sif="$CACHE/$cname.sif"
            if [ -e "$sif" ]; then echo "cached: $n"; else
                echo "pulling: $n ($url)"
                apptainer pull "$sif" "$url" || {{ echo "FAIL pull: $n"; exit 3; }}
                echo "ok: $n"
            fi
        done
        echo "prefetch done"
    """
    print("==> prefetch tool containers on login node", flush=True)
    res = subprocess.run(["ssh", HPC_HOST, remote], capture_output=True, text=True)
    print(res.stdout, flush=True)
    if res.returncode != 0 or "prefetch done" not in res.stdout:
        print(res.stderr, file=sys.stderr)
        raise RuntimeError("tool container prefetch failed")


def main():
    require_configured()
    user = ssh_capture("echo $USER")
    # Many clusters forbid running SLURM jobs from $HOME — the agent home (and thus
    # Nextflow's work dir) must live on allocation-coded scratch. Sockeye convention:
    scratch = f"/scratch/{SLURM_ACCOUNT}/{user}"
    agent_home = f"{scratch}/metasmith"
    remote_assembly = stage_input_to_cluster(scratch)

    out = OUT_DIR.resolve()
    out.mkdir(parents=True, exist_ok=True)

    # inputs.xgdb references REMOTE (cluster) paths; not validated locally
    inputs = DataInstanceLibrary(out / "inputs.xgdb")
    inputs.AddTypeLibrary(MLIB / "data_types" / "sequences.yml")
    inputs.AddTypeLibrary(MLIB / "data_types" / "ref.yml")
    inputs.AddItem(Path(remote_assembly), "sequences::assembly")
    # pre-staged DB → planner skips the (login-node-impossible) download step
    inputs.AddItem(REMOTE_UNIREF50_DMND, "ref::uniref50_diamond_db")
    inputs.Save()

    smith = Agent(
        home=SshSource(host=HPC_HOST, path=agent_home).AsSource(),
        runtime=Runtime.APPTAINER,
        setup_commands=SETUP_COMMANDS,
    )

    print("==> Deploy()", flush=True)
    smith.Deploy()

    targets = TargetBuilder()
    targets.Add("annotation::diamond_uniref50_results")

    print("==> GenerateWorkflow()", flush=True)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::assembly")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / "env"), inputs],
        transforms=[
            TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
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

    print("==> StageWorkflow(on_exist=clear)", flush=True)
    smith.StageWorkflow(task, on_exist="clear")

    # compute nodes have no internet → warm the tool-container cache now
    prefetch_tool_containers(agent_home, task.GetKey())

    print(f"==> RunWorkflow (SLURM, account={SLURM_ACCOUNT})", flush=True)
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params=dict(slurmAccount=SLURM_ACCOUNT),
    )

    print("==> WaitForWorkflow", flush=True)
    result = smith.WaitForWorkflow(task, timeout_s=10800.0, poll_s=15.0)
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
