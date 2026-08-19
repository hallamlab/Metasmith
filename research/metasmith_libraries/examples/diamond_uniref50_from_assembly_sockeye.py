#!/usr/bin/env python3
import os
import sys
import subprocess
from pathlib import Path

if os.environ.get("MSM_SRC"):
    sys.path.insert(0, os.environ["MSM_SRC"])
from metasmith.python_api import (
    Agent, SshSource, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder,
)

HPC_HOST      = os.environ.get("MSM_HPC_HOST", "sockeye")
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "<slurm-allocation>")
SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer"]

REF = Path(os.environ.get("MSM_REF_DB_DIR", "<ref-db-dir-on-cluster>"))
REMOTE_UNIREF50_DMND = REF / "diamond" / "uniref50.dmnd"

LOCAL_ASSEMBLY = Path(os.environ.get("MSM_ASSEMBLY", "<assembly.fna>"))
OUT_DIR = Path("results/diamond_uniref50_sockeye")

MLIB = Path(__file__).resolve().parents[3] / "src" / "metasmith_libraries"


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
    scratch = f"/scratch/{SLURM_ACCOUNT}/{user}"
    agent_home = f"{scratch}/metasmith"
    remote_assembly = stage_input_to_cluster(scratch)

    out = OUT_DIR.resolve()
    out.mkdir(parents=True, exist_ok=True)

    inputs = DataInstanceLibrary(out / "inputs.xgdb")
    inputs.AddTypeLibrary(MLIB / "data_types" / "sequences.yml")
    inputs.AddTypeLibrary(MLIB / "data_types" / "ref.yml")
    inputs.AddItem(Path(remote_assembly), "sequences::assembly")
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
