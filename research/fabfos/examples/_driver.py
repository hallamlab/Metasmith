"""What every EXECUTING driver in this directory needs, and nothing runtime-specific.

The three drivers that used to live under `tests/` shared a helper that hard-failed at
import unless the engine carried the MAMBA executor — correct for the metabolism half,
which is conda-only, and wrong for the annotation half, which is container-only. This
one is runtime-agnostic: the runtime is the caller's choice, because `Agent.runtime` is
one global setting and the two halves genuinely differ.

Nothing here plans or targets anything. Planning is each driver's own business; what is
shared is the mechanical part that is easy to get subtly wrong:

  * the dev overlay, without which the executor runs a DIFFERENT engine from the planner
  * waiting on a run's own log rather than on a process
  * reading the MANIFESTS to decide what landed, rather than the file names
  * publishing to the paths `data/` declares, refusing a silent no-op

THE AGENT IS REMOTE BY DEFAULT
------------------------------
These drivers run their work on **fir**, not here. That is not only a policy about this
workstation: the two heavy steps in the annotation half — the DIAMOND database build and
the ProteinBERT pass over the Swiss-Prot label pool — are what a 68 GB desktop cannot
give a hard memory guarantee to, and the pool step was killed at 29 minutes on exactly
that. A batch scheduler either grants the ask or queues, and never silently starves.

`fir_agent()` is the whole of the remote setup. What it encodes, and what
`tests/assembly_stats_on_fir.py` paid for first:

  * `SshSource(host, path).AsSource()` as the agent home, `module load apptainer` as the
    setup command, and an agent image tagged for this project rather than the published
    default;
  * the dev overlay as **two** artifacts — the directory the LOGIN node binds, and the
    tarball every SLURM task stages to node-local `/tmp` (keyed on the tarball's own
    mtime+size, so rebuilding it is what invalidates the cache);
  * the slurm nextflow preset with `slurmAccount`, and a **separate** `slurmGpuAccount`,
    because `rrg-shallam-ab` has no GPU allocation on fir — only `def-shallam` does, and
    `clusterOptions` is a scalar directive so a GPU step replaces the account rather than
    appending to it;
  * `Gpu(...)` describing what a device IS on fir (H100 80 GB, four per node). A
    transform declares `Resources(gpus=...)` and never names a device, a partition or a
    flag; this is the one place that vocabulary appears.

A compute node on fir has **no outbound network**, so an image that is not already in
the apptainer store when a task starts cannot be pulled: the task dies hours into a
queue rather than seconds into a check. `preflight()` is why.
"""
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

REPO = Path(__file__).resolve().parent.parent

_ENGINE = REPO / "src" / "metasmith" / "src"
if (_ENGINE / "metasmith").is_dir() and str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

from metasmith.python_api import (                                      # noqa: E402
    Agent, Gpu, Runtime, Size, Source, SshSource,
)

# ---------------------------------------------------------------------------
# fir
# ---------------------------------------------------------------------------

FIR_HOST = "fir"
FIR_AGENT_HOME = "/scratch/phyberos/fabfos_refs/agent_home"

# NOT the published `metasmith:{CONTAINER_TAG}` default. That tag is derived from the
# engine's own version string, so it resolves to a build predating whatever
# `src/metasmith` is pinned at, and staging dies inside the container with a TypeError
# about an Agent keyword several steps after the mismatch. This image is already in
# fir's store; the dev overlay below is what actually makes the two ends agree.
FIR_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.19.0-fabfos"
FIR_SETUP_COMMANDS = ["module load apptainer"]

# Charged on every sbatch. slurm.nf ships the literal placeholder '<slurm_account>',
# which sbatch rejects outright.
FIR_ACCOUNT = "rrg-shallam-ab"
# `sacctmgr show assoc` on fir lists rrg-shallam-ab_cpu but NO rrg-shallam-ab_gpu, so a
# GPU step charged to it is rejected at submission. def-shallam has both.
FIR_GPU_ACCOUNT = "def-shallam"

# What a device is on fir: `sinfo` reports gpu:h100:4 on 48-core / 1152 GB nodes. Naming
# the type matters -- the interactive partition also advertises MIG slices
# (`nvidia_h100_80gb_hbm3_3g.4`), and a 20 GB slice is not what a 650M-parameter
# embedding pass asked for.
FIR_GPU = Gpu(memory=Size.GB(80), type="h100", count=4, flag="--gpus-per-node=")


# ---------------------------------------------------------------------------
# sockeye
# ---------------------------------------------------------------------------
# The second site, and it exists because fir goes away: a cooling-maintenance window
# takes the whole cluster out for a day and a half at a time, and the work does not
# stop for it. Everything below differs from fir for a reason the scheduler enforces,
# so none of it is a preference.

SOCKEYE_HOST = "sockeye"
# Scratch is ALLOCATION-scoped here -- there is no /scratch/<user>, so the allocation
# is the parent and the user is a directory inside it.
SOCKEYE_AGENT_HOME = "/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home"

# Sockeye's Lmod hides apptainer behind a gcc dependency, and the two loads must be
# SEPARATE commands: `module load gcc/9.4.0 apptainer/1.3.1` in one call resolves the
# second name against the module tree as it stood BEFORE gcc loaded, silently finds
# nothing, and the shell later reports `apptainer: command not found` with no hint that
# a module was skipped.
#
# APPTAINER_CACHEDIR is exported for BOTH sides: the login node writes the store here
# and the compute node reads it here. One side missing the variable resolves to a
# different directory, finds nothing, and attempts a pull on a node with no route out.
SOCKEYE_IMAGE_STORE = "/arc/project/st-shallam-1/metasmith/container_images"
SOCKEYE_SETUP_COMMANDS = [
    "module load gcc/9.4.0",
    "module load apptainer/1.3.1",
    f"export APPTAINER_CACHEDIR={SOCKEYE_IMAGE_STORE}",
]

SOCKEYE_ACCOUNT = "st-shallam-1"
# Sockeye charges GPU work to a separate allocation, exactly as fir does -- the names
# differ but the reason does not, and `clusterOptions` being a scalar directive means
# the account is REPLACED rather than appended.
SOCKEYE_GPU_ACCOUNT = "st-shallam-1-gpu"

# What a device IS on sockeye: `sinfo` reports gpu:v100:4 on the `gpu` partition.
#
# THE TYPE IS DELIBERATELY NOT NAMED, unlike fir's. Sockeye's job_submit plugin accepts
# `--gpus-per-node=N` and rejects BOTH `--gres=gpu:v100:N` and the typed
# `--gpus-per-node=v100:N`, the latter by quietly resolving to `requested_gpus 0` --
# a job that runs with no card rather than one that fails to submit. The partition is
# carried on the device rather than in the every-step clusterOptionsExtra because the
# default partition has no cards and only GPU steps belong on `gpu`.
SOCKEYE_GPU = Gpu(memory=Size.GB(32), extra=["--partition=gpu"])


# The agent BASE image, chosen the same way fir's is: whatever is actually in this site's
# store. It is NOT derived from the engine hash, and must not be -- `Agent.container`'s
# `metasmith:{VERSION}-{BUILD_HASH}` default names an image nobody necessarily built, and
# a tag that resolves to nothing fails as an apptainer `manifest unknown` against quay
# (new repos default to private, and compute nodes have no route out anyway), then a FATAL
# about a missing .sif, then an AssertionError about a missing msm_relay binary. None of
# those says "nobody built this image".
#
# THE BASE IMAGE'S ENGINE IS NOT THE ENGINE THAT RUNS. `provision_dev_overlay_remote`
# rsyncs the PINNED source into `<agent_home>/dev/metasmith` (plus the tarball every SLURM
# task stages) and Deploy binds it over the container's site-packages -- that is what makes
# the planner and the executor the same engine, and it is why fir runs a hand-tagged
# `0.19.0-fabfos` quite happily. So the two sites carry different base tags on purpose.
#
# This one is 6639608, built from the dev3 worktree's pin. dev1's own engine is one commit
# further on (`faf42dd`, hash 5ac3800 -- the fix that stops the host CUDA_VISIBLE_DEVICES
# being forwarded into the container, which matters here because CLEAN draws a GPU). The
# overlay is what delivers that commit to both ends; the base image only has to exist.
SOCKEYE_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.19.0-6639608"


def ssh_once(host: str, command: str) -> str:
    """Run one non-interactive command on the host. NEVER call this in a loop.

    A retry loop against a failed connection is what causes an account lockout. When
    this raises, connect once by hand and re-run the driver.
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


def sockeye_agent(*, host: str = SOCKEYE_HOST, agent_home: str = SOCKEYE_AGENT_HOME,
                  container: str = SOCKEYE_CONTAINER,
                  image_store: str = SOCKEYE_IMAGE_STORE) -> Agent:
    """The same agent as fir's, with the four things sockeye does differently.

    The base image is one of them -- see SOCKEYE_CONTAINER. The two stores hold different
    tags, so this default is site-specific and must stay that way.
    """
    return Agent(home=SshSource(host=host, path=agent_home).AsSource(),
                 runtime=Runtime.APPTAINER, container=container,
                 setup_commands=[c for c in SOCKEYE_SETUP_COMMANDS
                                 if not c.startswith("export APPTAINER_CACHEDIR=")]
                                + [f"export APPTAINER_CACHEDIR={image_store}"])


def local_agent(work: Path) -> Agent:
    """A local APPTAINER agent -- plan-shape checks, and the small steps that belong here.

    It was documented as plan-only, and that was a statement about this workstation's
    68 GB rather than about the agent: the two steps the annotation half cannot be given
    a hard memory guarantee for are the DIAMOND database build and the ProteinBERT pass
    over the label pool. A step that fits runs here perfectly well, and `--run` against
    this agent now means it.

    `Agent.container` defaults to `metasmith:{CONTAINER_TAG}`, which on an engine with
    no BUILD_HASH is the plain version tag. The image only has to exist; what makes the
    executor the same engine as the planner is `provision_dev_overlay_local` below.
    """
    return Agent(home=Source.FromLocal(work / "agent_home"), runtime=Runtime.APPTAINER)


def provision_dev_overlay_local(agent_home: Path, *, repo: Path = REPO) -> None:
    """Bind the PINNED engine over the local agent container's site-packages.

    The remote form is two artifacts because every SLURM task stages a tarball; locally
    there is one executor and one filesystem, so a symlink is the whole of it. The
    generated `msm` launcher checks for exactly `<agent_home>/dev/metasmith` and binds
    it if present -- Deploy never creates it, so an agent without this runs whatever
    engine the base image shipped, several commits behind the planner, and disagrees
    with it about the `Agent` constructor.
    """
    src = repo / "src" / "metasmith" / "src" / "metasmith"
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
    """The dev overlay on a remote agent is TWO artifacts, and only one is obvious.

    `<agent_home>/dev/metasmith/` is what the LOGIN node binds over the container's
    site-packages. `<agent_home>/dev/metasmith.tar` is what every SLURM task actually
    uses: `RenderBootstrap` stages the tarball to node-local `/tmp/msm_devstage_$USER/`,
    keyed on the tarball's own `stat -c %Y-%s`. Ship the directory and forget the
    tarball and the login node runs the pinned engine while every compute node runs the
    container's — a split that shows up as an inexplicable engine-version error in a
    task log and nowhere else.

    The tarball is rebuilt unconditionally: its mtime+size IS the stage key, so a stale
    one is indistinguishable from a current one until a task fails.
    """
    src = repo / "src" / "metasmith" / "src" / "metasmith"
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

    A hand-kept list drifts the moment a transform changes which image it reaches for,
    and it drifts in the direction that matters: naming an env that does not exist
    crashes the driver, and *omitting* one silently skips the check that exists to stop
    a task dying on a compute node with no network. The plan already knows.
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
    """Everything that must already exist on the host, checked in ONE ssh round trip.

    A compute node has no outbound network. An image absent from the store when a task
    starts cannot be pulled, so the task dies after queueing -- possibly hours in, and
    with everything upstream of it already computed. The images are named by the
    `env::*.env` declarations rather than guessed, so this cannot drift from what the
    transforms actually reach for.
    """
    def sif_name(uri: str) -> str:
        # Exactly `Environment._cached_name()` in the engine, in that order: the
        # scheme separator collapses first, so `docker://quay.io/x:1` becomes
        # `docker..quay.io_x..1`. Re-deriving it differently here would report every
        # image missing on a host that holds all of them.
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
    # WHERE TO LOOK IS SITE-SPECIFIC AND MUST BE PASSED, not inferred from the
    # environment. `$APPTAINER_CACHEDIR` is exported from fir's .bashrc, which an ssh
    # command shell happens to source -- but sockeye's store lives on /arc and is set by
    # the agent's own setup_commands, so a non-interactive shell there reports it EMPTY.
    # Falling back to $HOME/.apptainer then reports every image missing on a host holding
    # all of them, which reads as "nothing is staged" rather than "I looked in the wrong
    # directory".
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

    A transform carrying `labels=["local"]` renders `label 'xlocalx'`, and the slurm
    preset's `withLabel: 'xlocalx'` block sets `executor = 'local'` against a pool it
    declares as 8 cores / 8 GB. Nextflow's local executor REFUSES a process asking for
    more rather than queueing it, and the same block sets `errorStrategy='ignore'` with
    no retry -- so a 128 GB step is dropped silently and the workflow finishes green
    with its output absent.

    The label is right for a download, which needs the login node's network, and is
    copied from there onto compute steps without anything noticing. Nothing else in the
    pipeline distinguishes the two cases, so this reads the STAGED workflow -- what will
    actually run -- rather than trusting the transform sources it was rendered from.
    """
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
    """Refuse a walltime that reaches past the next whole-cluster maintenance window.

    SLURM will not start a job that cannot finish before a reservation covering the
    nodes it needs, and when that reservation is flagged ALL_NODES there is no node it
    could run on instead. The job sits PENDING with reason
    `ReqNodeNotAvail, Reserved for maintenance` -- indistinguishable at a glance from
    ordinary queueing, and it never starts. Observed with hundreds of nodes sitting
    idle: fir drains into a window at 08:00 and a 12 h ask simply had nowhere to go.

    So this is a *report*, not a cap. Silently shrinking the ask would trade a job that
    never starts for one that dies at the wall hours in, which is worse.
    """
    # One round trip: the host resolves every StartTime to epoch seconds itself, so
    # there is no per-reservation ssh and no date-format guessing on this end.
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
    """Ask the scheduler whether it would actually accept the longest job. Refuse if not.

    THIS IS THE CHECK `check_walltimes` COULD NOT MAKE, and the gap cost a wrong verdict
    on 2026-07-27. That function reads reservations that start in the FUTURE, so it is
    blind to the case where the window is already open: fir went into a 31-hour cooling
    maintenance at 09:00, `scontrol show reservation` listed nothing ahead, and the check
    reported "no whole-cluster maintenance window ahead" on a cluster that was refusing
    every job. Login nodes and storage stay up through such a window, so ssh answers
    normally and nothing else looks wrong either.

    Maintenance does not have to appear as a reservation at all -- fir's showed up as
    every node draining to `down$` with scheduling disabled -- so there is no reservation
    query that covers both shapes. `sbatch --test-only` sidesteps the question: it runs
    the real submission path, validates the request against the real partitions, reports
    when the job WOULD start, and creates nothing. On a cluster in maintenance it answers
    `allocation failure: Requested node configuration is not available`.

    Checked against the LONGEST declared duration, because that is the ask most likely to
    be unplaceable, and a shorter probe passing proves nothing about it.
    """
    def _hours(d) -> float:
        return d._delta.total_seconds() / 3600.0

    durations = [(_hours(r.duration), n) for n, r in overrides.items()
                 if r.duration is not None]
    if not durations:
        return 0
    hours, name = max(durations)
    # `--nodes=1 --ntasks=1` mirrors what the slurm preset actually emits. It is not
    # padding: sockeye's job_submit plugin REJECTS a submission that does not state a
    # node count ("No number of nodes specified"), so a probe without it fails on every
    # healthy sockeye and would report the cluster down whenever it is fine.
    #
    # sockeye also refuses a submission with no working directory ("Job cannot be
    # submitted without the current working directory specified"), and ssh lands in
    # $HOME, which is not where work runs. Pass the run's own directory so the probe
    # asks the question the real submission will ask.
    chdir = f"--chdir={workdir} " if workdir else ""
    # `--test-only` never queues anything, so this is safe to run against a live cluster.
    out = ssh_once(host, f'sbatch --test-only --account={account} '
                         f'--time={int(hours * 60)} --nodes=1 --ntasks=1 {chdir}'
                         f'--wrap="true" 2>&1 || true')
    # sockeye's plugin colours its refusals; the escapes make the one-line report
    # unreadable and would hide the reason behind a wall of control characters.
    text = re.sub(r"\x1b\[[0-9;]*m", "", out).strip()
    ok = "Job" in text and "to start" in text
    if ok:
        print(f"    scheduler accepts a {hours:.0f} h job ({name}): {text.splitlines()[0]}")
        return 0
    # The LAST line carries the verdict; sockeye prefixes it with a banner of
    # `sbatch: error:` decoration that says nothing.
    why = text.splitlines()[-1] if text else "(no output)"
    print(f"\nTHE SCHEDULER WILL NOT ACCEPT A {hours:.0f} h JOB on {host} "
          f"(longest ask: {name}):\n    {why}\n"
          f"  This is what an in-progress maintenance window looks like: login nodes and "
          f"storage stay up, ssh answers, and only the scheduler refuses. Check "
          f"`sinfo -o '%P %a %D %t'` -- nodes ending in `$` are held for a reservation.",
          file=sys.stderr)
    return 1


def check_tasks(host: str, agent_home: str, task_key: str) -> int:
    """Count FAILED rows in the run's own task table. Returns that count.

    `slurm.nf` sets `errorStrategy='ignore'` once a process exhausts its retries, so
    the workflow goes green with the output simply absent. The log tail says
    "run completed" either way; this table is where the truth is.
    """
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
    """Lay a run's results out ON THE HOST at the paths `data/` declares.

    The local mirror of this (`publish_by_type`) is what feeds `dvc add`. This one
    exists so the NEXT run can consume the references without a round trip: the
    compiled set is ~17.5 GB, almost all of it the DIAMOND database, and the only
    consumer that needs it is a job on this same host. Pulling it down to a
    workstation and pushing it back would move 35 GB to change nothing.

    Same layout rule as the local publisher: metasmith names one DIRECTORY per
    produced type, `<namespace>-<type>`, holding the product under a content-addressed
    file name. The type is in the directory and never in the file.
    """
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
    """rsync a run's results down. `includes` selects; None takes everything."""
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
    """Put the PINNED engine where the agent's `msm` wrapper will bind it over the
    container's installed one.

    The agent container is tagged from the engine's own version string, so it resolves
    to a PUBLISHED build -- which predates the commits this repo pins `src/metasmith`
    at. The planner would be the pinned engine and the executor the published one, and
    they disagree about the Agent constructor: staging dies with `Agent.__init__() got
    an unexpected keyword argument 'gpu_args'` inside the container, several steps after
    the mismatch and with nothing pointing at it.

    Deploy binds `$AGENT_HOME/dev/metasmith` over the container's site-packages IF IT
    EXISTS, and never creates it. So it is created here, from the submodule, and the two
    ends of the run are the same engine by construction.

    A no-op under a non-container runtime, which has no site-packages to bind over --
    harmless, so it is not conditional.
    """
    src = repo / "src" / "metasmith" / "src" / "metasmith"
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
    """Block until the run's own log says it finished.

    On the log, not on a process: `RunWorkflow` launches Nextflow detached, so there is
    no child to wait on, and a `pgrep` pattern for one matches the polling wrapper's own
    command line as often as it matches the run.
    """
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
    """Which of `dtypes` a finished run actually produced, read from the manifests.

    "run completed" IS NOT "every step succeeded". The local and slurm presets both set
    `errorStrategy='ignore'` once a process exhausts its retries, so a step that died on
    every attempt leaves the workflow green with its output simply absent -- and a
    zero-output run prints exactly like a successful one.

    Read the MANIFESTS, not the file names: `_manifests/<ns>-<name>.*.json` is written
    for every DECLARED product whether or not the step that produces it ran, so matching
    on the path reports a product "present" when only its placeholder exists. An empty
    list is the tell.
    """
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
    """The names of tasks Nextflow recorded as FAILED, read from its own task table.

    The counterpart to `landed_products`, and the earlier of the two: that one asks
    "did anything come out", this one asks "did anything die", and the answer is
    available the moment the run ends, BEFORE a result tree is copied anywhere. On a
    remote site the retrieve is the expensive step, so a lane whose every task failed
    should never reach it.

    Reads `_metasmith/logs.latest/nxf_tasks.csv`, which metasmith extracts from the
    Nextflow trace and which carries the per-task exit status that "run completed at"
    hides. A missing table is reported as no failures rather than an error: it means
    the extraction did not happen, which `landed_products` will catch downstream.
    """
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
    """Copy a run's results to the paths `data/` declares, keyed by produced type.

    Metasmith lays results out as one DIRECTORY per produced type, named
    `<namespace>-<type>`, holding the product under a content-addressed file name --
    `ref-mnxr_lookup/1-1-1.wYcBNxLxYq5f3PMy-Kl319l86.parquet`. So the type name is in
    the directory and never in the file: matching on the file name finds nothing, which
    looks exactly like a run that produced nothing.

    Under `mode='rellink'` those entries are relative symlinks into the work tree, so
    the copy has to follow them -- the work tree is transient and a published symlink
    into it dangles the moment the run directory is cleaned.

    An empty match set is a non-zero exit rather than a quiet success. Never automatic:
    publishing rewrites DVC directory hashes.
    """
    if not results.exists():
        raise SystemExit(f"no results at {results}; run with --run first")
    by_dir = {dtype.replace("::", "-"): (dtype, target) for dtype, target in mapping.items()}
    moved = 0
    for d in sorted(results.iterdir()):
        if not d.is_dir() or d.name.startswith("_"):
            continue
        # An INTERMEDIATE product's directory carries its step order as a prefix --
        # `1_sequences-orfs` -- while a terminal one does not: `annotation-gpr_table`.
        # Matching the raw name therefore finds only the last product in the graph and
        # calls every lane output "not in this driver's publish map", which reads as a
        # deliberate skip rather than as the near-miss it is.
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
            dest = dest_root / target if len(entries) == 1 else dest_root / target / src.name
            print(f"  {d.name}/{src.name}  ->  {dest.relative_to(repo)}")
            if not dry_run:
                dest.parent.mkdir(parents=True, exist_ok=True)
                # REPUBLISHING OVER A PINNED CHUNK MUST BREAK THE LINK FIRST. Once
                # `dvc add` has run, everything under the chunk is a read-only HARDLINK
                # into a cache that several worktrees share (`cache.type =
                # hardlink,symlink`), so copying *through* it would rewrite the cache
                # object in place and corrupt that artifact for every one of them.
                # Unlinking leaves the cache object intact and drops only this name.
                # The read-only bit is what caught this the first time; do not rely on
                # it, because a chunk published but not yet pinned is writable.
                if real.is_dir():
                    if dest.exists():
                        shutil.rmtree(dest)
                    shutil.copytree(real, dest)
                else:
                    # UNLINK, do not overwrite. A published destination under `data/` is
                    # usually a DVC hardlink into a cache several worktrees share, and
                    # `copyfile` opens the destination for writing -- which writes
                    # through the link into every worktree that shares that inode.
                    # Read-only cache files turn this into a PermissionError instead,
                    # which is the lucky case, not the designed one.
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
