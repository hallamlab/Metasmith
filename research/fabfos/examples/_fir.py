"""What an EXECUTING driver needs to run on fir, and nothing about what it runs.

Lifted from `dev2/examples/_driver.py`, which is where every comment below was
paid for. Two things are deliberately NOT lifted, and both would have been
silently wrong at this repo's engine pin:

  * `landed_products` read `results/_manifests/*.json`. That sidecar route is
    **fully removed** at `src/metasmith` @ 2204002 -- `agents/collect.py` says so
    in four places and instead calls `output.Save()`, writing a typed index to
    `results/_metadata/index.yml`. Lifting it verbatim gives a driver that
    announces failure on a perfectly good run. `landed_from_index` below reads
    the index, which is strictly better anyway: it carries each product's
    parentage, so a product can be attributed to the input it came from.
  * `publish_by_type` maps one type to one destination. A three-organism run
    produces three `annotation::gpr_table` products in ONE directory under
    content-addressed names, so that publisher hits its `len(entries) > 1`
    branch and copies all three under hash names -- a publish that succeeds and
    is useless. `publish_gpr_by_source` attributes by the table's own `source`
    column instead, which `validate_gpr` has already proved single-valued before
    the table was written.

One thing is also tightened: `check_walltimes` now doubles the ask by default.
`slurm.nf` gives attempt >= 2 twice the time, so it is the DOUBLED walltime that
has to clear the next maintenance window -- the operations log proves it and the
code never enforced it.
"""
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

# ---------------------------------------------------------------------------
# fir
# ---------------------------------------------------------------------------

FIR_HOST = "fir"
FIR_AGENT_HOME = "/scratch/phyberos/fabfos_refs/agent_home"
FIR_PROCESSED = "/scratch/phyberos/fabfos_refs/processed"

# NOT the published `metasmith:{CONTAINER_TAG}` default. That tag is derived from
# the engine's own version string, so it resolves to a build nobody necessarily
# pushed; a tag that resolves to nothing fails as an apptainer `manifest unknown`
# against quay, then a FATAL about a missing .sif, then an AssertionError about a
# missing msm_relay binary -- none of which says "nobody built this image". So
# the tag names something that is actually in fir's store.
#
# THE OVERLAY DOES NOT MAKE THE BASE IRRELEVANT. It replaces the container's
# `metasmith` package, not the conda environment underneath it, so the base must
# already carry every third-party dependency the PINNED engine imports. The
# `0.19.0-fabfos` image the previous drivers used has no `cbor2`, which
# `caching/keys.py` imports at 0.20.1 -- staging died inside the container with
# `ModuleNotFoundError: No module named 'cbor2'` and surfaced here only as
# "launcher missing". A base from the same minor line as the pin is the rule.
#
# THE TAG THIS NAMED BEFORE IS GONE. `0.20.1-bf54d6f` was deleted upstream and
# quay answers `Tag ... was deleted or has expired`, so it cannot be pulled on
# any host that does not already hold the .sif -- which fir no longer does. Its
# successor on the same minor line is what fir has, and a base image swap does
# not move any lane's numbers: every lane runs in its own `*.env` container and
# this one only carries the engine.
FIR_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.20.4"
FIR_SETUP_COMMANDS = ["module load apptainer"]

# Charged on every sbatch. slurm.nf ships the literal placeholder
# '<slurm_account>', which sbatch rejects outright.
#
# THE `_cpu` / `_gpu` SUFFIX IS PART OF THE NAME, not a partition hint. fir's
# associations are `def-shallam_cpu`, `def-shallam_gpu`, `rpp-shallam_cpu`,
# `rrg-shallam-ab_cpu` -- there is no bare `rrg-shallam-ab`, and sbatch rejects
# one at submission with an "Invalid account" that reads like an expired
# allocation rather than a typo.
FIR_ACCOUNT = "rrg-shallam-ab_cpu"
# There is no `rrg-shallam-ab_gpu` association at all, so a GPU step charged to
# the CPU allocation is rejected outright. `def-shallam_gpu` is the only GPU
# association here, and it is also the healthier share: `def-shallam_cpu` has
# exhausted its fairshare (EffectvUsage 0.999 / FairShare 0.028) and would queue
# behind everything, while `def-shallam_gpu` sits at 0.409.
FIR_GPU_ACCOUNT = "def-shallam_gpu"

# What a device IS on fir: `sinfo` reports gpu:h100:4 on 48-core / 1152 GB nodes.
# Naming the type matters -- the interactive partition also advertises MIG slices
# (`nvidia_h100_80gb_hbm3_3g.4`), and a 20 GB slice is not what an embedding pass
# asked for.
FIR_GPU = Gpu(memory=Size.GB(80), type="h100", count=4, flag="--gpus-per-node=")


def ssh_once(host: str, command: str) -> str:
    """Run one non-interactive command on the host. NEVER call this in a loop.

    A retry loop against a failed connection is what causes an account lockout;
    a run in this project was halted by exactly that. When this raises, connect
    once by hand and re-run the driver.
    """
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
    """Give every input with no local bytes a DETERMINISTIC instance_id.

    `_mint_leaf_id` content-addresses a leaf when it can read the file and falls
    back to `uuid4 + time_ns` when it cannot -- which is every reference living
    on the agent's filesystem. The plan key is a hash over the given instances'
    ids, so a random id makes the task key change on every invocation: `--run`
    and a later `--retrieve` name different run directories, and a resubmission
    stages a fresh key that shares no cache with the hours of lanes that already
    succeeded.

    Deriving the id from the path string keeps the identity content-free, which
    is exactly what the library's own model already is for anything it cannot
    read -- it only lacked a stable way to say so. Local inputs are left alone:
    theirs are content-addressed, which is strictly better.
    """
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
    """The dev overlay on a remote agent is TWO artifacts, and only one is obvious.

    `<agent_home>/dev/metasmith/` is what the LOGIN node binds over the
    container's site-packages. `<agent_home>/dev/metasmith.tar` is what every
    SLURM task actually uses: `RenderBootstrap` stages the tarball to node-local
    `/tmp/msm_devstage_$USER/`, keyed on the tarball's own `stat -c %Y-%s`. Ship
    the directory and forget the tarball and the login node runs the pinned
    engine while every compute node runs the container's -- a split that shows up
    as an inexplicable engine-version error in a task log and nowhere else.

    The tarball is rebuilt unconditionally: its mtime+size IS the stage key, so a
    stale one is indistinguishable from a current one until a task fails.
    """
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
    """The `env::*` requirements of the RESOLVED plan, not a list kept by hand.

    A hand-kept list drifts in the direction that matters: naming an env that
    does not exist crashes the driver, and *omitting* one silently skips the
    check that stops a task dying on a compute node with no network.
    """
    names = set()
    for step in task.plan.steps:
        for inst in step.uses:
            dtype = getattr(inst, "dtype_name", "") or ""
            if dtype.startswith("env::"):
                names.add(dtype.split("::", 1)[1])
    return sorted(names)


def preflight(host: str, agent_home: str, container: str, tool_envs, *,
              mlib: Path, image_store: str | None = None) -> int:
    """Everything that must already exist on the host, in ONE ssh round trip.

    A compute node has no outbound network. An image absent from the store when a
    task starts cannot be pulled, so the task dies after queueing -- possibly
    hours in, with everything upstream of it already computed.
    """
    def sif_name(uri: str) -> str:
        # Exactly `Environment._cached_name()`, in that order: the scheme
        # separator collapses first, so `docker://quay.io/x:1` becomes
        # `docker..quay.io_x..1`. Re-deriving it differently here would report
        # every image missing on a host that holds all of them.
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
    """After staging, before running: refuse a workflow pinned to the login node.

    A transform carrying `labels=["local"]` renders `label 'xlocalx'`, and the
    slurm preset's `withLabel: 'xlocalx'` block sets `executor = 'local'` against
    a pool it declares as 8 cores / 8 GB. Nextflow's local executor REFUSES a
    process asking for more rather than queueing it, and the same block sets
    `errorStrategy='ignore'` with no retry -- so a 48 GB step is dropped silently
    and the workflow finishes green with its output absent.

    Reads the STAGED workflow, not the transform sources it was rendered from.
    """
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
    """Refuse a walltime that reaches past the next whole-cluster maintenance window.

    SLURM will not start a job that cannot finish before a reservation covering
    the nodes it needs, and when that reservation is flagged ALL_NODES there is
    no node it could run on instead. The job sits PENDING with
    `ReqNodeNotAvail, Reserved for maintenance` -- indistinguishable at a glance
    from ordinary queueing, and it never starts. Observed with hundreds of nodes
    idle: fir drains into a window at 08:00 and a 12 h ask had nowhere to go.

    RETRY DOUBLING IS THE DEFAULT, not an option. `slurm.nf` gives attempt >= 2
    twice the time and twice the memory, so a 4 h declaration is an 8 h second
    attempt; checking only the first attempt passes a job whose retry can never
    be scheduled, which is the shape that has actually gone wrong here.

    A *report*, not a cap. Silently shrinking the ask would trade a job that
    never starts for one that dies at the wall hours in, which is worse.
    """
    # One round trip: the host resolves every StartTime to epoch seconds itself,
    # so there is no per-reservation ssh and no date-format guessing on this end.
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
    """Ask the scheduler whether it would accept the longest job. Refuse if not.

    THE CHECK `check_walltimes` CANNOT MAKE, and the gap cost a wrong verdict on
    2026-07-27. That one reads reservations starting in the FUTURE, so it is
    blind to a window already open: fir went into a 31-hour cooling maintenance
    at 09:00, `scontrol show reservation` listed nothing ahead, and the check
    reported "no whole-cluster maintenance window ahead" on a cluster refusing
    every job. Login nodes and storage stay up, so ssh answers normally.

    Maintenance need not appear as a reservation at all -- fir's showed up as
    every node draining to `down$` -- so no reservation query covers both shapes.
    `sbatch --test-only` sidesteps it: the real submission path, validated
    against the real partitions, creating nothing.
    """
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
    """Count FAILED rows in THIS ATTEMPT's task table. Returns that count.

    `slurm.nf` sets `errorStrategy='ignore'` once a process exhausts its retries,
    so the workflow goes green with the output simply absent. The log tail says
    "run completed" either way; this table is where the truth is. Asked BEFORE
    the retrieve, which is the expensive half.

    READ ONE ATTEMPT, NOT THE UNION OF ALL OF THEM. Each launch writes its own
    `logs.<timestamp>/` under the run key, and `-resume` means a run key
    accumulates them. Globbing `logs.*` therefore reports failures the campaign
    has already repaired: batch `2:52` kept refusing on 12 FAILED CLEAN tasks
    that were, every one of them, the pre-exclusion fc10512 losses from the first
    attempt -- un-reproducible since, and irrelevant to a run that had just
    completed all 50 shards. A repaired failure is what `-resume` is FOR, so the
    question this gate asks is "did anything die THIS time", and the answer lives
    in `logs.latest`. Matching by task name cannot substitute: nextflow's
    `name (N)` index is assigned in task-creation order, so `clean (2)` in one
    attempt is a different shard than `clean (2)` in the next.

    AN ABSENT TABLE IS A REFUSAL, NOT A PASS. `nxf_tasks.csv` is written by
    `agents/runner.py` in the post-run summary, so it does not exist while the
    run is in flight or if nextflow died mid-run -- and `cat` of a missing file
    is empty, which counts zero FAILED rows and returns 0. That is the exact
    silent green this gate exists to prevent, arriving through the gate itself.
    """
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
    """Block until the host has finished writing `_metadata/index.yml`.

    THE WAIT RETURNS BEFORE COLLECTION FINISHES. `WaitForWorkflow` decides a run
    is over from a sentinel in `agent.log`; on batch `102:152` the last task
    completed at 21:16:32, the wait returned at 21:16:41, and the index was
    written at 21:17:19 -- after collection had copied 249 outputs. rsync builds
    its file list once, at the start, so a transfer launched inside that window
    brings down every product and no index, and the run then reads exactly like
    a collection that never ran. The cost of waiting is seconds; the cost of not
    waiting is repeating a 6 GB transfer to fetch one 400 KB file.

    Returns False on timeout rather than raising: the caller still retrieves,
    and `results_index` remains the gate that refuses.
    """
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
    """rsync a run's results down. `includes` selects; None takes everything."""
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
    # Belt as well as braces: the wait above closes the window this rsync could
    # start in, but the file list is still a snapshot, so re-sync the (tiny)
    # metadata tree when the index is the one thing that did not land.
    if includes is None and not (out / "_metadata" / "index.yml").exists():
        print("    index absent after transfer -- re-syncing _metadata",
              flush=True)
        subprocess.run(["rsync", "-aL", f"{host}:{remote_results}/_metadata/",
                        f"{out}/_metadata/"], check=True)
    return out


# ---------------------------------------------------------------------------
# results, read the way THIS engine writes them
# ---------------------------------------------------------------------------

def results_index(results: Path) -> DataInstanceLibrary:
    """A finished run's results, as the typed library `CollectResults` saved.

    `results/_metadata/index.yml`, written by `output.Save()` at the end of
    collection. The legacy `_manifests/*.json` sidecar this replaced is gone at
    this engine pin, so a driver that still globs it reports every product
    absent on a run that produced all of them.
    """
    if not (results / "_metadata" / "index.yml").exists():
        raise SystemExit(
            f"no results index at {results}/_metadata/index.yml -- either nothing "
            f"was retrieved, or collection did not finish. Check "
            f"`_metasmith/logs.*/nxf_tasks.csv` on the host before anything else.")
    return DataInstanceLibrary.Load(results)


def landed_from_index(results: Path, dtypes) -> dict[str, list[Path]]:
    """Which of `dtypes` a finished run produced, and where each product is.

    "run completed" IS NOT "every step succeeded": the slurm preset sets
    `errorStrategy='ignore'` once a process exhausts its retries, so a step that
    died on every attempt leaves the workflow green with its output absent -- and
    a zero-output run prints exactly like a successful one.

    Returns {dtype: [resolved paths]}; a dtype with no products is simply absent
    from the mapping, which is the tell.
    """
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
    """A type's products read from the results tree, when the manifest lost them.

    A FULLY CACHED RESUME PUBLISHES EVERY FILE AND RECORDS NONE OF THEM. The
    collection step's manifest is built from what came down the output channel,
    and on a resume where every task is CACHED that channel is empty -- so
    `index.yml` is written as literally `manifest: {}` while all 250 products sit
    in the results directories beside it, freshly copied. Batch `2:52` reached
    exactly that state: 50 tables per type on disk, an empty index, and a driver
    that reported the run had produced nothing.

    The directory name carries the type (`annotation::gpr_table` ->
    `annotation-gpr_table`, optionally behind the step-order prefix collection
    adds), so the mapping the manifest would have supplied is recoverable. This
    loses no rigour: the manifest never established a product's identity anyway,
    and the callers go on to read each table's `source` column and refuse on a
    missing shard, a duplicate, or an unexpected organism.
    """
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
    """Copy a run's results to the paths `data/` declares, keyed by produced type.

    THE SINGLE-SOURCE PUBLISHER. Use it when the run has one sample and the
    interesting thing is the *breadth* of what landed -- the tables plus the ORFs
    plus every lane output, which are only jointly meaningful. Use
    `publish_gpr_by_source` instead when one type has N products because the run
    fanned out over N organisms: this one hits its `len(entries) > 1` branch there
    and lands all N under content-addressed names, a publish that succeeds and is
    useless.

    Metasmith lays results out as one DIRECTORY per produced type, named
    `<namespace>-<type>`, holding the product under a content-addressed file name.
    So the type name is in the directory and never in the file: matching on the
    file name finds nothing, which looks exactly like a run that produced nothing.
    An INTERMEDIATE product's directory also carries its step order as a prefix
    (`1_sequences-orfs`) while a terminal one does not (`annotation-gpr_table`),
    so the prefix is stripped before matching -- otherwise only the last product
    in the graph matches and every lane output reads as a deliberate skip.

    Under `mode='rellink'` the entries are relative symlinks into the work tree,
    so the copy follows them; the work tree is transient and a published symlink
    into it dangles the moment the run directory is cleaned.
    """
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
            real = src.resolve()          # rellink into the transient work tree
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
                    # UNLINK, do not overwrite -- see publish_gpr_by_source.
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
    """Land one GPR table per organism, attributed by the table's `source` column.

    A fan-out run writes N products of ONE type into ONE results directory under
    content-addressed names, so neither the file name nor the directory says
    which organism a table describes. The `source` column does, and
    `lib::fabfos_evidence.validate_gpr` has already refused to write a table
    whose source is not single-valued -- so reading it here is a lookup, not an
    inference.

    Refuses on the wrong count, a duplicate source, or a source outside `expect`.
    A publish that lands three tables under hash names would succeed and be
    useless, which is the failure this exists to prevent.
    """
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
        # UNLINK, do not overwrite. Once `dvc add` has run, a published path
        # under `data/` is a read-only HARDLINK into a cache several worktrees
        # share (`cache.type = hardlink,symlink`), so copying *through* it would
        # rewrite that cache object for every one of them. Unlinking leaves the
        # object intact and drops only this name. Do not rely on the read-only
        # bit to catch it: a chunk published but not yet pinned is writable.
        if dest.exists() or dest.is_symlink():
            dest.unlink()
        shutil.copyfile(p, dest)
    print(f"\n{len(by_source)} table(s) -> {dest_root.relative_to(repo)}/")
    return 0
