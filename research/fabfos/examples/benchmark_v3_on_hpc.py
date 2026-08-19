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

DOMAINS = domains_for(network="benchmark")

STAGED: dict[str, str] = {
    "ecspr::benchmark_answer_key":  "BENCH_V3_Y",
    "ecspr::benchmark_base_graphs": "BENCH_V3_BASE_GRAPHS",
    "ecspr::benchmark_universe":    "BENCH_V3_UNIVERSE",
    "ecspr::benchmark_observations": "BENCH_V3_OBSERVATIONS",
    "ecspr::benchmark_inputs":      "BENCH_V3_X",
}

SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer/1.3.1"]

REQUIRED_IMAGES = ["docker://quay.io/hallamlab/external_ecspr:2026.07.14"]


def cached_image_name(image: str) -> str:
    return image.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


def prepull_images(host: str, cache_dir: str, images: list[str],
                   local_sifs: dict[str, Path] | None = None) -> None:
    subprocess.run(["ssh", "-o", "BatchMode=yes", host, f"mkdir -p {cache_dir}"],
                   check=True)
    setup = "; ".join(SETUP_COMMANDS + [
        f"export APPTAINER_CACHEDIR={cache_dir}",
        f"export APPTAINER_TMPDIR={cache_dir}/tmp",
        f"mkdir -p {cache_dir}/tmp",
    ])
    local_sifs = local_sifs or {}
    for image in images:
        dest = f"{cache_dir}/{cached_image_name(image)}"
        print(f"  {image}", flush=True)

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
    from metasmith._build_hash import compute_build_hash
    from metasmith.constants import VERSION
    return f"docker://quay.io/hallamlab/metasmith:{VERSION}-{compute_build_hash()}"


def push_data(host: str, local_root: Path, remote_root: str) -> None:
    print(f"=== pushing {local_root.name} -> {host}:{remote_root} ===", flush=True)
    subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                    f"mkdir -p {remote_root}"], check=True)
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
        if not p.exists():
            missing.append(f"{type_name} -> canon.{symbol} -> {p}")
            continue
        if remote_root is not None:
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
        print(f"\ndeploy failed ({e}). The connection is multiplexed: open ONE "
              f"session by hand (`ssh {a.host}`), leave it open, and re-run. "
              f"Do NOT delete the ControlMaster socket and do NOT retry in a "
              f"loop -- that is what causes an account lockout.", file=sys.stderr)
        return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    agent.StageWorkflow(task, on_exist="clear")

    nxf_config = agent.GetNxfConfigPresets()["slurm"]

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
