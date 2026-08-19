from __future__ import annotations

import re
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
    Agent, DataInstanceLibrary, Gpu, Runtime, Size, SshSource,
)


FIR_HOST = "fir"
FIR_AGENT_HOME = "/scratch/phyberos/fabfos_refs/agent_home"
FIR_PROCESSED = "/scratch/phyberos/fabfos_refs/processed"

FIR_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.20.4"
FIR_SETUP_COMMANDS = ["module load apptainer"]

FIR_ACCOUNT = "rrg-shallam-ab_cpu"
FIR_GPU_ACCOUNT = "def-shallam_gpu"

FIR_GPU = Gpu(memory=Size.GB(80), type="h100", count=4, flag="--gpus-per-node=")


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


def pin_external_leaf_ids(inputs) -> None:
    from metasmith.models.libraries.identity import multihash_key

    pinned = 0
    for path in list(inputs.manifest):
        p = Path(path)
        local = p if p.is_absolute() else inputs.location / p
        if local.exists():
            continue
        inputs.instance_meta[p] = {
            "instance_id": multihash_key(b"external\x00" + str(p).encode("utf-8")).hex(),
            "origin": "leaf",
            "lineage_payload": None,
            "fork_id": inputs.fork_id,
        }
        pinned += 1
    if pinned:
        inputs.Save()
        print(f"    pinned {pinned} external leaf id(s) -- the task key is now stable "
              f"across invocations")


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
              f"to an 8-core / 8 GB local executor that refuses larger asks silently.",
              file=sys.stderr)
        return 1
    print("    staged workflow: every step goes to slurm")
    return 0


def check_walltimes(host: str, overrides: dict, *, retry_factor: float = 2.0) -> int:
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
    print(f"    next whole-cluster maintenance: {when} ({hours:.1f} h away); "
          f"checking the retry-doubled ask (x{retry_factor:g})")

    def _hours(d) -> float:
        return d._delta.total_seconds() / 3600.0

    over = {n: r for n, r in overrides.items()
            if r.duration is not None and _hours(r.duration) * retry_factor > hours}
    if over:
        for n, r in sorted(over.items()):
            print(f"      {n}: asks {_hours(r.duration):.0f} h, retry "
                  f"{_hours(r.duration) * retry_factor:.0f} h -- cannot be scheduled "
                  f"before the window", file=sys.stderr)
        print(f"\n{len(over)} step(s) ask for longer than the {hours:.1f} h until "
              f"maintenance once the retry doubling is counted. SLURM will hold them "
              f"PENDING with 'ReqNodeNotAvail, Reserved for maintenance' and they will "
              f"never start. Lower their duration, or wait out the window.",
              file=sys.stderr)
        return 1
    return 0


def check_schedulable(host: str, account: str, overrides: dict, *,
                      workdir: str | None = None, retry_factor: float = 2.0) -> int:
    def _hours(d) -> float:
        return d._delta.total_seconds() / 3600.0

    durations = [(_hours(r.duration) * retry_factor, n) for n, r in overrides.items()
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
        print(f"    scheduler accepts a {hours:.0f} h job ({name} retried): "
              f"{text.splitlines()[0]}")
        return 0
    why = text.splitlines()[-1] if text else "(no output)"
    print(f"\nTHE SCHEDULER WILL NOT ACCEPT A {hours:.0f} h JOB on {host} "
          f"(longest ask: {name}):\n    {why}\n"
          f"  This is what an in-progress maintenance window looks like: login nodes "
          f"and storage stay up, ssh answers, and only the scheduler refuses. Check "
          f"`sinfo -o '%P %a %D %t'` -- nodes ending in `$` are held for a reservation.",
          file=sys.stderr)
    return 1


def check_tasks(host: str, agent_home: str, task_key: str) -> int:
    log_dir = f"{agent_home}/runs/{task_key}/_metasmith/logs.latest"
    out = ssh_once(host, f"cat {log_dir}/nxf_tasks.csv 2>/dev/null | sort -u")
    rows = [ln for ln in out.splitlines() if ln and not ln.startswith("task_id,")]
    if not rows:
        print(f"\nNO TASK TABLE at {log_dir}/nxf_tasks.csv.\n"
              f"  It is written only in the post-run summary, so this means the run "
              f"is still in flight or nextflow exited without writing one -- NOT that "
              f"nothing failed. `nxf_trace.tsv` beside it carries the per-task rows "
              f"nextflow itself wrote and is the thing to read.", file=sys.stderr)
        return 1
    failed = [ln for ln in rows if "FAILED" in ln]
    print(f"    nextflow tasks: {len(rows)} recorded, {len(failed)} FAILED")
    for ln in failed:
        print(f"      {ln}")
    return len(failed)


def await_collection(host: str, remote_results: str, timeout_s: float = 1800,
                     poll_s: float = 15) -> bool:
    probe = f"test -f {remote_results}/_metadata/index.yml"
    deadline = time.monotonic() + timeout_s
    announced = False
    while time.monotonic() < deadline:
        if subprocess.run(["ssh", "-o", "BatchMode=yes", host, probe],
                          capture_output=True).returncode == 0:
            return True
        if not announced:
            print("    waiting for the host to finish collection "
                  "(_metadata/index.yml not written yet)", flush=True)
            announced = True
        time.sleep(poll_s)
    print(f"    collection did not produce an index within {timeout_s / 60:.0f} "
          f"min -- retrieving anyway so the products are local to inspect",
          file=sys.stderr)
    return False


def retrieve(host: str, remote_results: str, out: Path, *, includes=None) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    if includes is None:
        await_collection(host, remote_results)
    cmd = ["rsync", "-aL", "--info=stats1"]
    if includes:
        cmd += ["--include=*/"]
        cmd += [f"--include={pat}" for pat in includes]
        cmd += ["--exclude=*"]
    cmd += [f"{host}:{remote_results}/", f"{out}/"]
    print(f"=== retrieving {host}:{remote_results} -> {out} ===", flush=True)
    subprocess.run(cmd, check=True)
    if includes is None and not (out / "_metadata" / "index.yml").exists():
        print("    index absent after transfer -- re-syncing _metadata",
              flush=True)
        subprocess.run(["rsync", "-aL", f"{host}:{remote_results}/_metadata/",
                        f"{out}/_metadata/"], check=True)
    return out


def results_index(results: Path) -> DataInstanceLibrary:
    if not (results / "_metadata" / "index.yml").exists():
        raise SystemExit(
            f"no results index at {results}/_metadata/index.yml -- either nothing "
            f"was retrieved, or collection did not finish. Check "
            f"`_metasmith/logs.*/nxf_tasks.csv` on the host before anything else.")
    return DataInstanceLibrary.Load(results)


def landed_from_index(results: Path, dtypes) -> dict[str, list[Path]]:
    lib = results_index(results)
    wanted = set(dtypes)
    landed: dict[str, list[Path]] = {}
    for path, dtype_name in lib.manifest.items():
        if dtype_name not in wanted:
            continue
        real = lib.Get(path).ResolvePath()
        if real.exists():
            landed.setdefault(dtype_name, []).append(real)
    for dtype_name in wanted - set(landed):
        landed.setdefault(dtype_name, []).extend(
            _products_on_disk(results, dtype_name))
    return {k: sorted(v) for k, v in landed.items() if v}


def _products_on_disk(results: Path, dtype: str) -> list[Path]:
    suffix = dtype.replace("::", "-")
    dirs = [d for d in results.iterdir()
            if d.is_dir() and (d.name == suffix or
                               re.fullmatch(rf"\d+_{re.escape(suffix)}", d.name))]
    found = sorted(p for d in dirs for p in d.iterdir()
                   if p.is_file() and not p.name.startswith("."))
    if found:
        print(f"    index records no {dtype}; recovered {len(found)} product(s) "
              f"from {', '.join(d.name for d in dirs)} (a fully cached resume "
              f"publishes the files and writes an empty manifest)")
    return found


def publish_by_type(results: Path, mapping: dict[str, str], dest_root: Path,
                    *, dry_run: bool, repo: Path = REPO) -> int:
    if not results.exists():
        raise SystemExit(f"no results at {results}; run with --run first")
    by_dir = {dtype.replace("::", "-"): (dtype, target)
              for dtype, target in mapping.items()}
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
            dest = (dest_root / target if len(entries) == 1
                    else dest_root / target / src.name)
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
              "Check the run actually produced anything (landed_from_index).")
        return 1
    print(f"\n{moved} result(s) -> {dest_root.relative_to(repo)}/")
    return 0


def publish_gpr_by_source(results: Path, dest_root: Path, *, expect: set[str],
                          dtype: str = "annotation::gpr_table",
                          filename: str = "gpr_4lane.parquet",
                          dry_run: bool = False, repo: Path = REPO) -> int:
    import pandas as pd

    found = landed_from_index(results, [dtype]).get(dtype, [])
    if len(found) != len(expect):
        print(f"\n{len(found)} {dtype} product(s), expected {len(expect)}. A step that "
              f"exhausted its retries leaves the workflow green with its output "
              f"absent; read the task table on the host.", file=sys.stderr)
        return 1

    by_source: dict[str, Path] = {}
    for p in found:
        sources = sorted(set(pd.read_parquet(p, columns=["source"])["source"]))
        if len(sources) != 1:
            print(f"\n{p.name} carries {len(sources)} sources {sources}; validate_gpr "
                  f"should have made this impossible.", file=sys.stderr)
            return 1
        src = sources[0]
        if src in by_source:
            print(f"\ntwo tables both claim source [{src}] -- the lanes folded one "
                  f"organism's annotations onto another.", file=sys.stderr)
            return 1
        by_source[src] = p

    unexpected = set(by_source) - expect
    if unexpected:
        print(f"\nunexpected source(s) {sorted(unexpected)}; expected {sorted(expect)}",
              file=sys.stderr)
        return 1

    for src, p in sorted(by_source.items()):
        dest = dest_root / src / filename
        print(f"  {src}: {p.name}  ->  {dest.relative_to(repo)}")
        if dry_run:
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() or dest.is_symlink():
            dest.unlink()
        shutil.copyfile(p, dest)
    print(f"\n{len(by_source)} table(s) -> {dest_root.relative_to(repo)}/")
    return 0
