#!/usr/bin/env python3
import os
import sys
import subprocess
from pathlib import Path

if os.environ.get("MSM_SRC"):
    sys.path.insert(0, os.environ["MSM_SRC"])
from metasmith.python_api import (
    Agent, SshSource, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
    Resources, Size,
)

HPC_HOST      = os.environ.get("MSM_HPC_HOST", "sockeye")
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "<slurm-allocation>")
SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer"]

MLIB = Path(__file__).resolve().parents[3] / "src" / "metasmith_libraries"

R1 = Path(os.environ.get("MSM_READS_R1", "<reads-R1.fq.gz>"))
R2 = Path(os.environ.get("MSM_READS_R2", "<reads-R2.fq.gz>"))

REF = Path(os.environ.get("MSM_REF_DB_DIR", "<ref-db-dir-on-cluster>"))
REQUIRED_DBS = {
    "ref::uniref50_diamond_db": REF / "diamond"   / "uniref50.dmnd",
    "ref::kofamscan_profiles":  REF / "kofamscan" / "profiles.tgz",
    "ref::kofamscan_ko_list":   REF / "kofamscan" / "ko_list.tsv",
    "ref::metabuli_ref":        REF / "metabuli"  / "gtdb",
    "ref::gtdb":                REF / "gtdb"      / "release226",
}
PHYLOFLASH_DB = REF / "phyloflash" / "138.2"

PULL_ALL = False
W1_CONTAINERS = [
    "seqkit",
    "bbtools",
    "megahit",
    "samtools",
    "minimap2",
    "bedtools",
    "pprodigal",
    "diamond",
    "kofamscan",
    "metabuli",
    "metabat2",
    "semibin",
    "comebin",
    "checkm",
    "skani",
    "gtdbtk",
    "phyloflash",
]

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
    scratch = f"/scratch/{SLURM_ACCOUNT}/{user}"
    agent_home = f"{scratch}/metasmith"

    verify_dbs()

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
