#!/usr/bin/env python3
"""Run the frozen ECSPr X/Y benchmark v3 on an HPC host, then retrieve the result.

    PYTHONPATH=src python examples/benchmark_v3_on_hpc.py --user txyliu
    PYTHONPATH=src python examples/benchmark_v3_on_hpc.py --user txyliu --plan-only

The defaults target SOCKEYE. This ran on fir first and moved, for a reason that
belongs in the record rather than in a commit message: fir's /scratch was
silently dropping files. A 300-file write probe lost 14 under
/scratch/phyberos and 0 under /project/rpp-shallam/phyberos, `lfs quota`
reported "Some devices may be not working or deactivated", and the symptom
before the probe was a staged transform library arriving with 1-2 of its 18
files missing -- a DIFFERENT one or two on every run, surfacing as
`AssertionError: namespace [transforms] not found`. The same probe on
/scratch/st-shallam-1/txyliu loses 0 of 300.

The lesson generalizes past the host: a run that appears to succeed on a
filesystem that drops files at random is not a result. Retrying until it
passes would have produced a number, and the number would have been unsound.

THE ENGINE ON PYTHONPATH MUST BE THE PINNED ONE -- READ THIS BEFORE CHANGING IT
-------------------------------------------------------------------------------
Note the `src` FIRST. The `fabfos` conda env resolves `metasmith`
to a local editable checkout (~/lib/locals/metasmith) which is ahead of the
submodule pin. That matters here in a way it does not for a local plan, because
`Deploy()` pulls the agent container by a tag derived from the engine's OWN
version: an unpublished dev version asks quay for a manifest that does not
exist, and the run dies ~40 s in with "manifest unknown" followed by a confusing
cascade about a missing relay binary. Only released versions have images.

`assert_pinned_engine()` below turns that into an immediate, named refusal.

Deploy -> Generate -> Stage -> Run -> Wait -> Retrieve. The scoring step is
NOT here: it is a sub-minute local operation on the merged table, and running
it beside the solve would put a multi-hour cluster task behind every change to
how a score is computed.

THE USERNAME IS AN ARGUMENT, AND THAT IS THE POINT
---------------------------------------------------
`main/local_mock/smoke_hpc_deploy.py` resolves it by shelling `ssh <host> echo
$USER`. On this workstation that cannot work and must not be retried: the ssh
config sets `ControlMaster no` behind a ProxyCommand guard, so a bare `ssh fir`
cannot open a fresh authenticated session -- it can only ride an existing
multiplexed one. A loop around that call is a Duo push per iteration, and a
prior ECSPr run in this project was halted by an account lockout caused exactly
that way.

So: `--user` is required, exactly one session is opened, and on failure this
exits telling the human to connect once by hand. Never delete the
ControlMaster socket and never retry the connect in a loop.

WHY THERE IS NO ARRAY, AND SO NOTHING TO DISABLE
-------------------------------------------------
The plan called for disabling the engine's job-array batching, because array
contention is the exact condition under which fir's overlay filesystem throws
bus errors (recorded in the engine's own source). That turns out to be moot by
construction rather than by configuration: this workflow is TWO tasks, a solve
and a merge. `solve_benchmark.py` loops over the 16 (facet, element) shards
in-process -- see its docstring for why the fan-out is not real yet -- so there
is no array to contend. Concurrency comes from 32 worker PROCESSES inside the
one task, which is the shape that actually scales here: the solver factorizes
with SuperLU, which is serial, so the engine pins OMP/OPENBLAS/MKL/NUMEXPR to 1
before numpy loads and forks instead. Oversubscribing measured >10x slower.

If the shards are ever split into 16 real instances, revisit this: at that
point there IS an array, and the batching should be disabled in favour of
queue-size concurrency.

WHAT IS STAGED, AND WHAT IS DELIBERATELY NOT
---------------------------------------------
Five items, all from the frozen benchmark tree, all addressed through canon so
no absolute path appears here. Note what is absent: no annotation lane, no
evidence chain, no recovery experiment. That whole chain is upstream of the
freeze. A benchmark score has to measure the SOLVER, and anything that could
re-derive X would make it measure the annotation instead.

`benchmark_universe` is 57.6 MB and is staged even though it looks like a
build-time artifact, because `base_plus_reactions` re-reads it at SOLVE time to
find the atom-transit weights of an inserted reaction. X withholds the atom
mapping on purpose, so without the universe the run does not fail fast -- it
fails on the first gain-of-function condition, deep into the job.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))

from fabfos import canon  # noqa: E402
from fabfos.library import domains_for, resolve_library_root  # noqa: E402

from metasmith.python_api import (  # noqa: E402
    Agent, Runtime, DataInstanceLibrary, DataTypeLibrary, Source,
    SshSource, TargetBuilder, TransformInstanceLibrary,
)

LIB = resolve_library_root()

# The benchmark lane. `domains_for` drops ecsprNetA/ecsprNetB, which is not
# tidiness: on the benchmark, the annotation chain that feeds ecspr::base_graphs
# is upstream of the freeze and must not appear in the plan at all.
DOMAINS = domains_for(network="benchmark")

# type name -> canon symbol. All five live in the v3 tree.
STAGED: dict[str, str] = {
    "ecspr::benchmark_answer_key":  "BENCH_V3_Y",
    "ecspr::benchmark_base_graphs": "BENCH_V3_BASE_GRAPHS",
    "ecspr::benchmark_universe":    "BENCH_V3_UNIVERSE",
    "ecspr::benchmark_observations": "BENCH_V3_OBSERVATIONS",
    "ecspr::benchmark_inputs":      "BENCH_V3_X",
}

# Sockeye's Lmod hides apptainer/1.3.1 behind a gcc dependency, and the two
# loads must be SEPARATE commands: `module load gcc/9.4.0 apptainer/1.3.1` in
# one call resolves the second name against the module tree as it stood BEFORE
# gcc was loaded, so it silently finds nothing and the shell later reports
# `apptainer: command not found` with no hint that a module was skipped.
#
# 1.3.1 also sits just under the >=1.4 threshold at which the engine builds a
# sandbox instead of running the .sif, so that branch stays dormant here -- good,
# because the plan explicitly forbids sandbox mode as an overlay workaround.
SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer/1.3.1"]

# Every image the workflow's transforms ask for, as they appear in the
# containers resource library. Both benchmark transforms use only this one.
# The PUBLIC reference, deliberately. `quay.io/hallamlab/ecspr:2026.07.14` is the
# name the 2026-07-20 rename moved to, but quay defaults new repos to PRIVATE and
# that one was never opened up -- an anonymous manifest GET returns 401, so the
# pre-pull cannot resolve it and the run refuses before it starts.
#
# `external_ecspr:2026.07.14` is public (is_public: true) and resolves to
# sha256:97f7afad99f55fe66a213851f48f6293f6b328ce7db4fe035a93de6f24f582db --
# byte-identical to the digest provenance/containers/ecspr.yml pins, verified
# against the registry rather than assumed. So this is a reachability fix, not a
# change of image: the bits the solve runs on are the same bits.
#
# Pinned by TAG rather than by digest on purpose: the persistent .sif store keys
# its filenames off the reference string, and the tag form already has a 4.4 GB
# cached sif on sockeye from the previous run. A digest reference would miss that
# cache and force a fresh pull for no gain, since the digest is verified anyway.
REQUIRED_IMAGES = ["docker://quay.io/hallamlab/external_ecspr:2026.07.14"]


def cached_image_name(image: str) -> str:
    """The filename metasmith looks for in the image store.

    Mirrors `Container._cached_name` in the pinned engine
    (coms/containers.py). Kept as a copy rather than an import because the
    driver must be able to place the file BEFORE any engine code runs on the
    remote -- but it is a mirror, so if that method changes, this must too.
    """
    return image.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


def prepull_images(host: str, cache_dir: str, images: list[str],
                   local_sifs: dict[str, Path] | None = None) -> None:
    """Pull task images on the LOGIN node, because compute nodes have no network.

    This is the failure this function exists to prevent, in full, because it
    does not look like a network problem from the outside: the run reported
    `status: completed` in 3.6 minutes and produced a results directory. What
    actually happened is that `solve_benchmark` died pulling its image with
    "no route to host", and slurm.nf sets `errorStrategy = 'ignore'` after the
    retry budget -- so Nextflow logged "Error is ignored", the merge step ran
    on nothing, and the workflow exited zero with `[0] outputs`.

    A green run with an empty result is worse than a red one. The driver now
    checks the output is non-empty rather than trusting the status.

    Apptainer resolves its store as ${APPTAINER_CACHEDIR:-<agent home>}, and
    agent home carries a per-run timestamp -- so pointing APPTAINER_CACHEDIR at
    a stable path is what makes this pull survive to the next run instead of
    being re-fetched into a directory that is about to be abandoned.
    """
    subprocess.run(["ssh", "-o", "BatchMode=yes", host, f"mkdir -p {cache_dir}"],
                   check=True)
    # APPTAINER_CACHEDIR governs the .sif destination; APPTAINER_TMPDIR and the
    # blob cache are separate and default to $HOME/.apptainer. On a cluster with
    # a quota'd home that silently accreted ~23 GB of layer blobs during a run
    # whose .sif was correctly written to scratch -- the destination being an
    # absolute path masked it. Pin the staging dirs to scratch alongside it.
    setup = "; ".join(SETUP_COMMANDS + [
        f"export APPTAINER_CACHEDIR={cache_dir}",
        f"export APPTAINER_TMPDIR={cache_dir}/tmp",
        f"mkdir -p {cache_dir}/tmp",
    ])
    local_sifs = local_sifs or {}
    for image in images:
        dest = f"{cache_dir}/{cached_image_name(image)}"
        print(f"  {image}", flush=True)

        # We upload a locally-built .sif instead of pulling on the cluster.
        #
        # The reason is the compute nodes have NO outbound internet, so a pull
        # scheduled as part of the task dies with "no route to host" -- and
        # slurm.nf's errorStrategy='ignore' turns that into a silent green run
        # with empty outputs. Pre-placing the .sif in the persistent store is
        # what makes the failure impossible rather than invisible.
        #
        # Registry privacy was never the reason for this branch, and the two
        # readings of it that were written here are both wrong. Measured
        # anonymously (`/v2/<repo>/tags/list`): `hallamlab/ecspr` answers 200,
        # `hallamlab/external_ecspr` and `hallamlab/ecspr_bake` answer 401. So
        # the canonical image IS anonymously pullable and the branch is still
        # needed -- the compute nodes have no route to the registry at all.
        # Either way we do not put registry credentials on a shared cluster:
        # the login is a personal Docker Desktop credential, and a secret
        # copied onto a multi-user filesystem cannot be un-copied.
        local = local_sifs.get(image)
        if local is not None:
            if not local.exists():
                raise SystemExit(f"--image-sif given but absent: {local}")
            probe = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", host, f"[ -e {dest} ] && echo CACHED"],
                capture_output=True, text=True)
            if "CACHED" in probe.stdout:
                print(f"    cached -> {dest}", flush=True)
                continue
            print(f"    uploading {local.stat().st_size / 1e9:.1f} GB -> {dest}",
                  flush=True)
            subprocess.run(["rsync", "-a", "--partial", "--info=progress2",
                            str(local), f"{host}:{dest}"], check=True)
            print(f"    uploaded -> {dest}", flush=True)
            continue

        # `[ -e ] || pull` rather than an unconditional pull: the tag is
        # pinned, so a present file is the right file, and re-pulling costs
        # several minutes of login-node network per run.
        r = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", host,
             f"{setup}; [ -e {dest} ] && echo CACHED || apptainer pull {dest} {image}"],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            raise SystemExit(
                f"pre-pull failed for {image}:\n{r.stderr.strip()[-2000:]}\n"
                f"Without a cached image the solve dies on the compute node "
                f"with 'no route to host', and Nextflow's ignore strategy "
                f"turns that into a SILENT empty result."
            )
        print(f"    {'cached' if 'CACHED' in r.stdout else 'pulled'} -> {dest}",
              flush=True)


def assert_pinned_engine() -> str:
    """Refuse to deploy an engine version that has no published container.

    `Deploy()` derives the agent image tag from the engine's own version, so an
    unreleased local checkout asks quay for a manifest that does not exist. The
    failure surfaces as a registry error and then an assertion about a missing
    relay binary -- neither of which names the actual cause. Checked here, on
    the version the interpreter actually imported, so the message arrives before
    a remote directory is created rather than after.

    Only enforced for a real deploy; --plan-only touches no registry.
    """
    import metasmith
    got = (Path(metasmith.__file__).parent / "version.txt").read_text().strip()
    # noqa: E501 -- see agent_container() for why the version alone is not the tag
    want = (REPO / "src/metasmith/version.txt").read_text().strip()
    if got != want:
        raise SystemExit(
            f"engine version [{got}] is not the pin [{want}].\n"
            f"  imported from: {Path(metasmith.__file__).parent}\n"
            f"The agent container tag is derived from this version, and only "
            f"RELEASED versions have images on quay -- deploying [{got}] would "
            f"fail with 'manifest unknown' after creating a remote directory.\n"
            f"Re-run with the pin first on the path:\n"
            f"  PYTHONPATH=src python {Path(__file__).name} ..."
        )
    return got


def agent_container() -> str:
    """The published agent image for the engine we actually imported.

    `Agent.container` defaults to `metasmith:{CONTAINER_TAG}`, and CONTAINER_TAG
    is `{VERSION}-{BUILD_HASH}` where BUILD_HASH is a content hash of the engine
    source tree written by `_build_hash.py` AT BUILD TIME. A source checkout --
    which is what the submodule pin is -- has no `build_hash.txt`, so the tag
    silently degrades to the bare version, and bare `0.18.8` was never pushed:
    quay carries `0.18.8-60556ca`. The run then dies on "manifest unknown".

    So the hash is COMPUTED here from the same function the build uses, rather
    than the tag being hardcoded. That is the difference between "this tag
    happens to work today" and "this image is provably built from the source on
    our PYTHONPATH" -- if the pin moves, this follows it, and if the resulting
    image was never published the pull fails loudly instead of running an engine
    that does not match the planner that produced the workflow.
    """
    from metasmith._build_hash import compute_build_hash
    from metasmith.constants import VERSION
    return f"docker://quay.io/hallamlab/metasmith:{VERSION}-{compute_build_hash()}"


def push_data(host: str, local_root: Path, remote_root: str) -> None:
    """Copy the frozen benchmark tree to the execution host, once.

    REQUIRED, and not an optimisation. metasmith binds an item's OWN path into
    the task container -- the same string on both sides -- so an input declared
    at a workstation path is bind-mounted at that path on the cluster node,
    where it does not exist. Apptainer then refuses with "mount source ... does
    not exist" and the run dies as a missing launcher, which names neither the
    item nor the path. The tree has to BE on the far side, at the path the
    declaration uses.

    Safe to repeat: the tree is frozen and hash-pinned, so rsync converges and
    a second run re-transfers nothing. 81 MB over 84 files.
    """
    print(f"=== pushing {local_root.name} -> {host}:{remote_root} ===", flush=True)
    subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                    f"mkdir -p {remote_root}"], check=True)
    # --checksum, not the default size+mtime: hardlink placement and rsync can
    # give a re-staged file a fresh mtime with identical bytes, and re-sending
    # the 55 MB universe every run for that is waste. Frozen data justifies
    # paying the read to be sure.
    subprocess.run(["rsync", "-a", "--checksum", "--delete", "--info=stats1",
                    f"{local_root}/", f"{host}:{remote_root}/"], check=True)


def build_inputs(staging: Path, data_root: Path, remote_root: str | None) -> DataInstanceLibrary:
    xgdb = staging / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    inputs.AddTypeLibrary(namespace="ecspr",
                          lib=DataTypeLibrary.Load(LIB / "data_types/ecspr.yml"))

    missing = []
    for type_name, symbol in STAGED.items():
        p = Path(getattr(canon, symbol))
        # Existence is checked against the LOCAL tree even when the declared
        # path is remote: the planner resolves on types and lineage, never on
        # existence, so a missing tree plans perfectly and fails hours later
        # inside a container. The local copy is what was just pushed, so
        # checking it is checking the far side.
        if not p.exists():
            missing.append(f"{type_name} -> canon.{symbol} -> {p}")
            continue
        if remote_root is not None:
            # Re-root onto the execution host. The item keeps its identity and
            # type; only where it lives changes.
            p = Path(remote_root) / p.relative_to(data_root)
        inputs.AddItem(p, type_name)
    if missing:
        raise SystemExit("benchmark tree incomplete:\n  " + "\n  ".join(missing))

    inputs.Save()
    return inputs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="sockeye")
    ap.add_argument("--user", required=True,
                    help="remote username. REQUIRED and never auto-resolved -- "
                         "see this module's docstring on the lockout.")
    # Sockeye has no /scratch/<user>: scratch is allocation-scoped, so the
    # working root is /scratch/st-shallam-1/<user>. The default below is that
    # allocation, NOT a bare /scratch -- with /scratch the driver would build a
    # path that mkdir cannot create and the failure would arrive mid-deploy.
    ap.add_argument("--scratch-root", default="/scratch/st-shallam-1")
    ap.add_argument("--image-sif", default=None,
                    help="locally-built .sif for the ecspr image, uploaded to "
                         "the store instead of pulled. Needed because the "
                         "compute nodes have no outbound internet -- build it "
                         "with: apptainer build ecspr.sif "
                         "docker-daemon://quay.io/hallamlab/ecspr:2026.07.14")
    ap.add_argument("--apptainer-cache", default=None,
                    help="persistent image store on the host. Defaults to "
                         "{scratch}/{user}/apptainer_cache. Must outlive a "
                         "single run -- see prepull_images().")
    ap.add_argument("--slurm-account", default="st-shallam-1",
                    help="charged on every sbatch. slurm.nf's default is the "
                         "placeholder '<slurm_account>', which sbatch rejects.")
    ap.add_argument("--timeout-s", type=float, default=6 * 3600)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--remote-data", default=None,
                    help="where the benchmark tree lives ON THE HOST. Defaults to "
                         "{scratch}/{user}/ecspr_bench_v3_data.")
    ap.add_argument("--no-push", action="store_true",
                    help="skip the rsync and trust --remote-data is already "
                         "populated. The tree is frozen, so a repeat push is a "
                         "no-op; use this only to save the checksum pass.")
    ap.add_argument("--plan-only", action="store_true",
                    help="stage and plan, render the DAG, touch no remote host")
    ap.add_argument("--out", default="transforms/build/benchmark/v3_build/run",
                    help="local directory to retrieve the merged result into")
    a = ap.parse_args()

    ts = int(time.time())
    staging = REPO / ".awm" / "data" / "runs" / f"bench_v3_{ts}"
    staging.mkdir(parents=True, exist_ok=True)

    data_root = Path(canon.BENCH_V3_ROOT)
    remote_root = None
    if not a.plan_only:
        remote_root = a.remote_data or f"{a.scratch_root}/{a.user}/ecspr_bench_v3_data"
        if not a.no_push:
            push_data(a.host, data_root, remote_root)

    print(f"=== staging {len(STAGED)} benchmark items ===", flush=True)
    inputs = build_inputs(staging, data_root, remote_root)
    for t in sorted(STAGED):
        print(f"  lib   {t}")
    if remote_root:
        print(f"  (declared under {remote_root})")

    resources = [DataInstanceLibrary.Load(LIB / f"resources/{n}")
                 for n in ("containers", "lib")]
    transforms = [TransformInstanceLibrary.Load(LIB / f"transforms/{d}") for d in DOMAINS]

    if a.plan_only:
        agent = Agent(home=Source.FromLocal(staging / "agent_home"),
                      runtime=Runtime.APPTAINER)
    else:
        print(f"=== engine pin: {assert_pinned_engine()} ===", flush=True)
        agent_path = f"{a.scratch_root}/{a.user}/ecspr_bench_v3_{ts}"
        print(f"=== remote: {a.host}:{agent_path} ===", flush=True)
        container = agent_container()
        print(f"=== agent image: {container} ===", flush=True)
        # The image store, and it must be exported for BOTH sides: the login
        # node writes it here (prepull_images) and the compute node reads it
        # here. If only one side saw the variable they would silently resolve
        # to different directories -- the read side would find nothing, and the
        # symptom would be an attempted pull on a node with no route out.
        cache_dir = a.apptainer_cache or f"{a.scratch_root}/{a.user}/apptainer_cache"
        print(f"=== image store: {cache_dir} ===", flush=True)
        print("=== pre-pulling task images on the login node ===", flush=True)
        local_sifs = {REQUIRED_IMAGES[0]: Path(a.image_sif)} if a.image_sif else None
        prepull_images(a.host, cache_dir, REQUIRED_IMAGES, local_sifs)

        agent = Agent(home=SshSource(host=a.host, path=agent_path).AsSource(),
                      runtime=Runtime.APPTAINER,
                      container=container,
                      setup_commands=SETUP_COMMANDS +
                                     [f"export APPTAINER_CACHEDIR={cache_dir}"])

    print("=== planning ===", flush=True)
    targets = TargetBuilder()
    # The MERGED table only. Targeting the shards as well would let the planner
    # satisfy the merge from a separately-planned solve; one target, one chain.
    targets.Add("ecspr::benchmark_result")
    task = agent.GenerateWorkflow(
        samples=[inputs],
        resources=resources + [inputs],
        transforms=transforms,
        targets=targets,
    )
    if not task.ok:
        print("\nPLAN DID NOT RESOLVE. Planner hints:", file=sys.stderr)
        print(getattr(task.plan, "hints", task), file=sys.stderr)
        return 3

    print(f"resolved workflow: {len(task.plan.steps)} steps", flush=True)
    for i, step in enumerate(task.plan.steps):
        name = getattr(getattr(step, "transform", None), "name", None) or f"step{i}"
        print(f"  [{i}] {name}")

    dag = (REPO / "reports/dag/benchmark_v3").resolve()
    dag.parent.mkdir(parents=True, exist_ok=True)
    task.plan.RenderDAG(dag)
    print(f"DAG -> {dag.with_suffix('.svg')}", flush=True)

    if a.plan_only:
        print("\n--plan-only: nothing deployed, nothing run.")
        return 0

    print("=== Deploy() ===", flush=True)
    try:
        agent.Deploy()
    except subprocess.CalledProcessError as e:
        # One session, one failure, one message. NOT a retry loop -- each
        # attempt is a Duo push and a prior run here was halted by a lockout.
        print(f"\ndeploy failed ({e}). The connection is multiplexed: open ONE "
              f"session by hand (`ssh {a.host}`), leave it open, and re-run. "
              f"Do NOT delete the ControlMaster socket and do NOT retry in a "
              f"loop -- that is what causes an account lockout.", file=sys.stderr)
        return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    # on_exist="clear" is safe HERE and only here: agent_path carries a
    # timestamp, so it is a fresh directory every run and there is no prior
    # intermediate to destroy. Never carry this flag onto a resubmission.
    agent.StageWorkflow(task, on_exist="clear")

    # RunWorkflow's config_file DEFAULTS TO THE `local` PRESET, which runs every
    # process on whatever node the agent is sitting on -- here, the login node.
    # That is not a slow path, it is the wrong one twice over: it breaks the
    # "no local compute beyond sub-minute tests" constraint, and it would put a
    # 32-worker multi-hour solve on a shared interactive host. Selected
    # explicitly, so a future reader sees the choice rather than a default.
    nxf_config = agent.GetNxfConfigPresets()["slurm"]

    # slurm.nf ships slurmAccount as the literal placeholder '<slurm_account>',
    # which sbatch rejects; every submission would fail identically and the
    # cause would be one line deep in a per-task .command.err. Sockeye needs
    # --account on every job (st-shallam-1 for CPU; the -gpu sibling is a
    # different account and is not what this runs on).
    #
    # process_array=0 disables Nextflow's job-array batching, per the plan.
    # Array contention is the condition under which overlay filesystems throw
    # bus errors. With a 2-step workflow this is belt-and-braces rather than
    # load-bearing -- but the intent should not quietly depend on the step
    # count staying at 2.
    params = {
        "slurmAccount": a.slurm_account,
        "process_array": 0,
        "process_cpus": 32,
    }
    print(f"=== executor: slurm, account {a.slurm_account}, arrays disabled ===",
          flush=True)
    agent.RunWorkflow(task, config_file=nxf_config, params=params)

    print(f"=== waiting (timeout {a.timeout_s / 3600:.1f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_s, poll_s=a.poll_s)
    print(f"=== status: {result['status']} after {result['elapsed_s'] / 60:.1f} min ===",
          flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    # "completed" IS NOT "succeeded". slurm.nf sets errorStrategy to 'ignore'
    # once a process exhausts its retries, so a task that died every attempt
    # leaves the workflow green, the merge step running on nothing, and a
    # results directory that exists and is empty. The first sockeye run failed
    # exactly this way -- a missing container read as a 3.6-minute success.
    #
    # So the status is not trusted on its own: the log is checked for the
    # swallow, and the retrieved table is checked for rows.
    swallowed = [ln for ln in result["tail"]
                 if "Error is ignored" in ln or "terminated with an error" in ln]
    if swallowed:
        print("\nA TASK FAILED AND NEXTFLOW IGNORED IT -- this is not a result:",
              file=sys.stderr)
        for ln in swallowed:
            print(f"    {ln}", file=sys.stderr)
        print("\nThe real error is in the failing task's .command.err under "
              "<run>/nxf_work/<hash>/. Do not score this output.", file=sys.stderr)
        return 3

    src = agent.GetResultSource(task)
    out = (REPO / a.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    print(f"=== retrieving: {src.GetPath()} -> {out} ===", flush=True)
    subprocess.run(["rsync", "-a", "--info=stats1",
                    f"{a.host}:{src.GetPath()}/", f"{out}/"], check=True)

    # The merge transform's output is keyed by content hash, not by a stable
    # name -- `results/ecspr-benchmark_result/<key>.tsv`. Globbing the result
    # directory and normalising to observations.tsv is what makes the scorer
    # invocation below reproducible across runs.
    result_dir = out / "ecspr-benchmark_result"
    hits = sorted(result_dir.glob("*.tsv")) if result_dir.is_dir() else []
    if len(hits) != 1:
        raise SystemExit(
            f"workflow reported success but {result_dir} holds {len(hits)} "
            f"tsv(s); expected exactly the merged result.")
    obs = out / "observations.tsv"
    shutil.copyfile(hits[0], obs)

    n_rows = sum(1 for _ in obs.open()) - 1
    if n_rows <= 0:
        raise SystemExit(f"{obs} has no data rows -- an empty table is not a result.")
    print(f"=== observations.tsv: {n_rows} rows (from {hits[0].name}) ===", flush=True)
    print(f"\nnext: PYTHONPATH=src python transforms/build/benchmark/30_score_v3.py "
          f"--obs {obs} --mode signed --out {out}/score_signed.tsv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
