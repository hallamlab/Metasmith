from __future__ import annotations

import re
import csv
import io
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

_ENGINE = REPO / "src"
if (_ENGINE / "metasmith").is_dir() and str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

from metasmith.python_api import (                                      # noqa: E402
    Agent, Gpu, Runtime, Size, Source, SshSource,
)


FIR_HOST = "fir"
FIR_AGENT_HOME = "/scratch/phyberos/fabfos_refs/agent_home"

# 0.19.0-fabfos was a locally built SIF that was never pushed; `docker pull` cannot
# reach it and only fir's apptainer cache still holds one. This tracks the released
# image the source tree matches, so the dev overlay patches source rather than papering
# over a two-release gap in the base env.
FIR_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.21.0"
FIR_SETUP_COMMANDS = ["module load apptainer"]

FIR_ACCOUNT = "rrg-shallam-ab"
FIR_GPU_ACCOUNT = "def-shallam"

# `DevicesFor` divides the step's declared gpu_memory by this, so the declared size is
# what one device HOLDS, not what the job wants. CLEAN asks for 16 GB and is the only
# GPU step in the four-lane graph; a 2g.20gb MIG slice covers it and fir has far more of
# those free than whole H100s.
FIR_GPU = Gpu(memory=Size.GB(20), type="nvidia_h100_80gb_hbm3_2g.20gb",
              flag="--gpus-per-node=")


SOCKEYE_HOST = "sockeye"
SOCKEYE_AGENT_HOME = "/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home"

SOCKEYE_IMAGE_STORE = "/arc/project/st-shallam-1/metasmith/container_images"
SOCKEYE_SETUP_COMMANDS = [
    "module load gcc/9.4.0",
    "module load apptainer/1.3.1",
    f"export APPTAINER_CACHEDIR={SOCKEYE_IMAGE_STORE}",
]

SOCKEYE_ACCOUNT = "st-shallam-1"
SOCKEYE_GPU_ACCOUNT = "st-shallam-1-gpu"

SOCKEYE_GPU = Gpu(memory=Size.GB(32), extra=["--partition=gpu"])


SOCKEYE_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.20.4"


def ssh_once(host: str, command: str) -> str:
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", host, command],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(
            f"ssh to {host} failed ({r.returncode}):\n{r.stderr.strip()[-2000:]}\n"
            f"Connect once by hand (`ssh {host}`), leave it open, and re-run. "
            f"Do NOT retry in a loop -- that is what causes an account lockout.")
    return r.stdout


def fir_agent(*, host: str = FIR_HOST, agent_home: str = FIR_AGENT_HOME,
              container: str = FIR_CONTAINER) -> Agent:
    return Agent(home=SshSource(host=host, path=agent_home).AsSource(),
                 runtime=Runtime.APPTAINER, container=container,
                 setup_commands=FIR_SETUP_COMMANDS)


def sockeye_agent(*, host: str = SOCKEYE_HOST, agent_home: str = SOCKEYE_AGENT_HOME,
                  container: str = SOCKEYE_CONTAINER,
                  image_store: str = SOCKEYE_IMAGE_STORE) -> Agent:
    return Agent(home=SshSource(host=host, path=agent_home).AsSource(),
                 runtime=Runtime.APPTAINER, container=container,
                 setup_commands=[c for c in SOCKEYE_SETUP_COMMANDS
                                 if not c.startswith("export APPTAINER_CACHEDIR=")]
                                + [f"export APPTAINER_CACHEDIR={image_store}"])


LOCAL_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.20.4"


def local_agent(work: Path) -> Agent:
    return Agent(home=Source.FromLocal(work / "agent_home"), runtime=Runtime.APPTAINER,
                 container=LOCAL_CONTAINER)


def provision_dev_overlay_local(agent_home: Path, *, repo: Path = REPO) -> None:
    src = repo / "src" / "metasmith"
    if not (src / "__init__.py").exists():
        raise SystemExit(f"the pinned engine is not at {src}; "
                         f"`git submodule update --init src/metasmith`")
    dest = Path(agent_home) / "dev" / "metasmith"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_symlink() or dest.exists():
        if dest.is_symlink() and dest.resolve() == src.resolve():
            print(f"dev overlay: {dest} -> {src} (already)")
            return
        if dest.is_symlink() or dest.is_file():
            dest.unlink()
        else:
            shutil.rmtree(dest)
    dest.symlink_to(src)
    print(f"dev overlay: {dest} -> {src}")


def provision_dev_overlay_remote(host: str, agent_home: str, *, repo: Path = REPO) -> None:
    src = repo / "src" / "metasmith"
    if not (src / "__init__.py").exists():
        raise SystemExit(f"the pinned engine is not at {src}; "
                         f"`git submodule update --init src/metasmith`")
    dest = f"{agent_home}/dev/metasmith"
    ssh_once(host, f"mkdir -p {agent_home}/dev")
    subprocess.run(
        ["rsync", "-a", "--delete", "--exclude=__pycache__", "--exclude=*.pyc",
         f"{src}/", f"{host}:{dest}/"], check=True)
    ssh_once(host, f"cd {agent_home}/dev && tar -cf metasmith.tar "
                   f"--exclude=__pycache__ metasmith")
    stamp = ssh_once(host, f"stat -c '%Y-%s' {agent_home}/dev/metasmith.tar").strip()
    print(f"dev overlay: {src.relative_to(repo)} -> {host}:{dest}/ (+ metasmith.tar, "
          f"stage key {stamp})")


def envs_from_plan(task) -> list[str]:
    names = set()
    for step in task.plan.steps:
        for inst in step.uses:
            dtype = getattr(inst, "dtype_name", "") or ""
            if dtype.startswith("env::"):
                names.add(dtype.split("::", 1)[1])
    return sorted(names)


def preflight(host: str, agent_home: str, container: str, tool_envs, *,
              mlib: Path, image_store: str | None = None) -> int:
    def sif_name(uri: str) -> str:
        return uri.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"

    wanted = {"agent": container}
    for name in tool_envs:
        text = (mlib / "resources" / "env" / name).read_text()
        for line in text.splitlines():
            if line.startswith("container:"):
                wanted[name] = line.split(":", 1)[1].strip()
                break
        else:
            print(f"  {name}: declares no container: key", file=sys.stderr)

    checks = [
        f'[ -d {agent_home}/dev/metasmith ] && echo "OK   dev/metasmith (login-node bind)" '
        f'|| echo "MISSING dev/metasmith -- Deploy() binds it but does NOT create it"',
        f'[ -f {agent_home}/dev/metasmith.tar ] && echo "OK   dev/metasmith.tar (slurm-task stage)" '
        f'|| echo "MISSING dev/metasmith.tar -- every slurm task falls back to the container engine"',
    ]
    roots = "${APPTAINER_CACHEDIR:-$HOME/.apptainer} " + f"{agent_home}/container_images"
    if image_store:
        roots = f"{image_store} " + roots
    for label, uri in wanted.items():
        checks.append(
            f'find {roots} '
            f'-maxdepth 1 -name "{sif_name(uri)}" -print -quit 2>/dev/null | grep -q . '
            f'&& echo "OK   {label}: {uri}" '
            f'|| echo "MISSING {label}: {uri} -- a compute node cannot pull it"')
    out = ssh_once(host, "; ".join(checks))
    print(out.rstrip())
    bad = [ln for ln in out.splitlines() if ln.startswith("MISSING")]
    if bad:
        print(f"\n{len(bad)} prerequisite(s) absent on {host}. Pull the images on the "
              f"LOGIN node (`apptainer pull`) before running.", file=sys.stderr)
        return 1
    return 0


def check_staged_executor(host: str, agent_home: str, task_key: str) -> int:
    nf = f"{agent_home}/runs/{task_key}/workflow.nf"
    out = ssh_once(host, f"grep -n \"label 'xlocalx'\" -B 3 {nf} 2>/dev/null || true")
    procs = [ln.split("process ")[1].split()[0]
             for ln in out.splitlines() if "process " in ln]
    if procs:
        print(f"\nSTAGED WORKFLOW PINS {len(procs)} STEP(S) TO THE LOGIN NODE: "
              f"{procs}\n  Those carry labels=[\"local\"], which the slurm preset maps "
              f"to an 8-core / 8 GB local executor that refuses larger asks silently. "
              f"Drop the label unless the step genuinely needs outbound network.",
              file=sys.stderr)
        return 1
    print("    staged workflow: every step goes to slurm")
    return 0


def check_walltimes(host: str, overrides: dict) -> int:
    out = ssh_once(host, r'''now=$(date +%s); echo "NOW $now"
scontrol show reservation -o 2>/dev/null | grep ALL_NODES | while read -r line; do
  for tok in $line; do case "$tok" in StartTime=*)
    s=$(date -d "${tok#StartTime=}" +%s 2>/dev/null) || continue
    [ "$s" -gt "$now" ] && echo "START $s $(date -d @$s '+%Y-%m-%d %H:%M')" ;;
  esac; done
done''')
    now = next((int(ln.split()[1]) for ln in out.splitlines()
                if ln.startswith("NOW ")), None)
    windows = sorted((int(ln.split()[1]), ln.split(maxsplit=2)[2])
                     for ln in out.splitlines() if ln.startswith("START "))
    if now is None or not windows:
        print("    no whole-cluster maintenance window ahead")
        return 0
    start, when = windows[0]
    hours = (start - now) / 3600.0
    print(f"    next whole-cluster maintenance: {when} ({hours:.1f} h away)")

    def _hours(d) -> float:
        return d._delta.total_seconds() / 3600.0

    over = {n: r for n, r in overrides.items()
            if r.duration is not None and _hours(r.duration) > hours}
    if over:
        for n, r in sorted(over.items()):
            print(f"      {n}: asks {_hours(r.duration):.0f} h -- cannot be "
                  f"scheduled before the window", file=sys.stderr)
        print(f"\n{len(over)} step(s) ask for longer than the {hours:.1f} h until "
              f"maintenance. SLURM will hold them PENDING with "
              f"'ReqNodeNotAvail, Reserved for maintenance' and they will never start. "
              f"Lower their duration to fit, or wait out the window.", file=sys.stderr)
        return 1
    return 0


def check_schedulable(host: str, account: str, overrides: dict, *,
                      workdir: str | None = None) -> int:
    def _hours(d) -> float:
        return d._delta.total_seconds() / 3600.0

    durations = [(_hours(r.duration), n) for n, r in overrides.items()
                 if r.duration is not None]
    if not durations:
        return 0
    hours, name = max(durations)
    chdir = f"--chdir={workdir} " if workdir else ""
    out = ssh_once(host, f'sbatch --test-only --account={account} '
                         f'--time={int(hours * 60)} --nodes=1 --ntasks=1 {chdir}'
                         f'--wrap="true" 2>&1 || true')
    text = re.sub(r"\x1b\[[0-9;]*m", "", out).strip()
    ok = "Job" in text and "to start" in text
    if ok:
        print(f"    scheduler accepts a {hours:.0f} h job ({name}): {text.splitlines()[0]}")
        return 0
    why = text.splitlines()[-1] if text else "(no output)"
    print(f"\nTHE SCHEDULER WILL NOT ACCEPT A {hours:.0f} h JOB on {host} "
          f"(longest ask: {name}):\n    {why}\n"
          f"  This is what an in-progress maintenance window looks like: login nodes and "
          f"storage stay up, ssh answers, and only the scheduler refuses. Check "
          f"`sinfo -o '%P %a %D %t'` -- nodes ending in `$` are held for a reservation.",
          file=sys.stderr)
    return 1


def check_tasks(host: str, agent_home: str, task_key: str) -> int:
    csv_glob = f"{agent_home}/runs/{task_key}/_metasmith/logs.*/nxf_tasks.csv"
    out = ssh_once(host, f"cat {csv_glob} 2>/dev/null | sort -u")
    rows = [ln for ln in out.splitlines() if ln and not ln.startswith("task_id,")]
    failed = [ln for ln in rows if "FAILED" in ln]
    print(f"    nextflow tasks: {len(rows)} recorded, {len(failed)} FAILED")
    for ln in failed:
        print(f"      {ln}")
    return len(failed)


def publish_remote(host: str, remote_results: str, mapping: dict[str, str],
                   dest_root: str) -> int:
    lines = ["set -e", f"mkdir -p {dest_root}", "n=0"]
    for dtype, target in mapping.items():
        d = f"{remote_results}/{dtype.replace('::', '-')}"
        dest = f"{dest_root}/{target}"
        lines.append(
            f'if [ -d "{d}" ] && [ -n "$(ls -A {d} 2>/dev/null)" ]; then '
            f'mkdir -p "$(dirname {dest})"; rm -rf "{dest}"; '
            f'cp -rL "$(ls -d {d}/* | head -1)" "{dest}"; '
            f'echo "  {dtype} -> {target}"; n=$((n+1)); '
            f'else echo "  {dtype}: ABSENT"; fi')
    lines.append('echo "PUBLISHED $n"')
    out = ssh_once(host, "\n".join(lines))
    print(out.rstrip())
    n = next((int(ln.split()[1]) for ln in out.splitlines()
              if ln.startswith("PUBLISHED")), 0)
    if n != len(mapping):
        print(f"\n{len(mapping) - n} of {len(mapping)} product(s) absent on {host}. "
              f"A step that exhausted its retries leaves the workflow green with its "
              f"output simply missing.", file=sys.stderr)
        return 1
    return 0


def retrieve(host: str, remote_results: str, out: Path, *, includes=None) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    cmd = ["rsync", "-aL", "--info=stats1"]
    if includes:
        cmd += ["--include=*/"]
        cmd += [f"--include={pat}" for pat in includes]
        cmd += ["--exclude=*"]
    cmd += [f"{host}:{remote_results}/", f"{out}/"]
    print(f"=== retrieving {host}:{remote_results} -> {out} ===", flush=True)
    subprocess.run(cmd, check=True)
    return out


def provision_dev_overlay(work: Path, *, repo: Path = REPO) -> Path:
    src = repo / "src" / "metasmith"
    if not (src / "__init__.py").exists():
        raise SystemExit(
            f"the pinned engine is not at {src}; "
            f"`git submodule update --init src/metasmith`")
    dest = work / "agent_home" / "dev" / "metasmith"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    print(f"dev overlay: {src.relative_to(repo)} -> {dest.relative_to(repo)}")
    return dest


def wait_for_run(work: Path, task_key: str, timeout_s: int, *, poll_s: float = 30.0) -> Path:
    internals = work / "agent_home" / "runs" / task_key / "_metasmith"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if internals.exists():
            log_dirs = sorted(p for p in internals.glob("logs.*") if "latest" not in p.name)
            if log_dirs:
                last_log = log_dirs[-1] / "main.log"
                if last_log.exists() and "run completed at" in last_log.read_text(errors="ignore"):
                    return last_log
        time.sleep(poll_s)
    raise TimeoutError(f"workflow {task_key} did not finish within {timeout_s}s")


def landed_products(results: Path, dtypes) -> set[str]:
    landed = set()
    man = results / "_manifests"
    if not man.is_dir():
        return landed
    for dtype in dtypes:
        stem = dtype.replace("::", "-") + "."
        hits = [p for p in man.glob("*.json") if p.name.startswith(stem)]
        if hits and any(json.loads(p.read_text()) for p in hits):
            landed.add(dtype)
    return landed


def failed_tasks(run_dir: Path, site: dict | None = None) -> list[str]:
    rel = "_metasmith/logs.latest/nxf_tasks.csv"
    host = (site or {}).get("host") if (site or {}).get("remote") else None
    if host:
        text = ssh_once(host, f"cat {run_dir}/{rel} 2>/dev/null || true")
    else:
        table = run_dir / rel
        text = table.read_text(errors="ignore") if table.exists() else ""
    failed = []
    for row in csv.DictReader(io.StringIO(text)):
        if (row.get("status") or "").strip().upper() == "FAILED":
            failed.append((row.get("name") or "?").strip())
    return failed


def publish_by_type(results: Path, mapping: dict[str, str], dest_root: Path,
                    *, dry_run: bool, repo: Path = REPO) -> int:
    if not results.exists():
        raise SystemExit(f"no results at {results}; run with --run first")
    by_dir = {dtype.replace("::", "-"): (dtype, target) for dtype, target in mapping.items()}
    moved = 0
    for d in sorted(results.iterdir()):
        if not d.is_dir() or d.name.startswith("_"):
            continue
        key = re.sub(r"^\d+_", "", d.name)
        if key not in by_dir:
            print(f"  (skipping {d.name}: not in this driver's publish map)")
            continue
        dtype, target = by_dir[key]
        entries = [p for p in sorted(d.iterdir()) if not p.name.startswith(".")]
        if not entries:
            print(f"  {d.name}: EMPTY -- the step that produces {dtype} did not run")
            continue
        if len(entries) > 1:
            print(f"  {d.name}: {len(entries)} entries, expected 1 -- publishing all "
                  f"under {target}/")
        for src in entries:
            real = src.resolve()
            dest = dest_root / target if len(entries) == 1 else dest_root / target / src.name
            print(f"  {d.name}/{src.name}  ->  {dest.relative_to(repo)}")
            if not dry_run:
                dest.parent.mkdir(parents=True, exist_ok=True)
                if real.is_dir():
                    if dest.exists():
                        shutil.rmtree(dest)
                    shutil.copytree(real, dest)
                else:
                    if dest.exists() or dest.is_symlink():
                        dest.unlink()
                    shutil.copyfile(real, dest)
            moved += 1
    if not moved:
        print("  NOTHING PUBLISHED -- no result directory matched this driver's map. "
              "Check the run actually produced anything (landed_products).")
        return 1
    print(f"\n{moved} result(s) -> {dest_root.relative_to(repo)}/")
    return 0


def latest_results(work: Path) -> Path:
    runs = sorted((work / "agent_home" / "runs").glob("*"))
    if not runs:
        raise SystemExit(f"no runs under {work}/agent_home/runs")
    return runs[-1] / "results"
