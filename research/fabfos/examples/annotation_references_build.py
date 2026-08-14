#!/usr/bin/env python3
"""Build the five references the four canonical annotation lanes need, and pin them.

    python examples/annotation_references_build.py                 # plan + DAG only
    python examples/annotation_references_build.py --preflight     # what fir must hold
    python examples/annotation_references_build.py --run           # execute on fir
    python examples/annotation_references_build.py --retrieve      # pull the results down
    python examples/annotation_references_build.py --publish       # land them in data/processed/

This is the EXECUTING sibling of `examples/annotation_references_dag.py`. That one
proves the graph resolves from nothing -- no givens, every leaf a download -- on a
machine holding none of the bytes. This one runs it here, where the six upstream
distributions are already acquired and DVC-pinned under `data/originals/`, so the
six acquire steps are staged away and only the four compiles run.

WHICH REFERENCES, AND WHY EXACTLY THESE FIVE
--------------------------------------------
    kofamscan_profiles + kofamscan_ko_list   the HMM lane          (one chunk)
    uniref50_diamond_db                      the homology lane
    mnxr_lookup                              EVERY lane's terminus
    reference_label_pool                     the ProteinBERT kNN lane

CLEAN and ProteinBERT get no artifact each, and the asymmetry is not an oversight:
their weights are baked into their images, so under a container runtime there is
nothing for a transform to acquire. What R7 builds is the labelled POOL, not the
model. The build's entire obligation to the CLEAN lane is the `ec` route of
mnxr_lookup.

The profiles and the ko_list share ONE chunk. They are one artifact in two files:
the list carries each family's score threshold, and a threshold applied to a
profile from a different build is silently the wrong cut on every hit.

THIS RUNS ON FIR, AND THE SOURCES ARE ALREADY THERE
-----------------------------------------------------
Two of the four compiles are what a workstation cannot promise: the DIAMOND build reads
an 8.2 GB fasta, and the label pool embeds 222k Swiss-Prot sequences. The pool step was
SIGTERM'd at 29 minutes on this desktop under kernel page-allocation failures with 1 GB
free of 68. A scheduler either grants the ask or queues; it does not starve.

The six source folders are uploaded once to `--remote-originals` and declared at their
ABSOLUTE fir paths, which is what makes metasmith bind them in place instead of staging
11.4 GB through the task. `--sync-sources` does the upload and is idempotent.

WHY THIS IS A CONTAINER RUN AND THE METABOLISM BAKE IS NOT
-----------------------------------------------------------
`Agent.runtime` is one global setting, so a graph whose envs cannot all satisfy it
does not run. `proteinbert.env` publishes only an image and has no `conda:` key at
all; the metabolism bake's `build-refs-*` envs are the mirror image, conda specs
with no published image. The two halves are separate runs by construction.

logistics/ IS NOT LOADED, and this asserts it. That library carries downloaders
producing the same `ref::` types compile/ does, and two producers for one reference
makes provenance a planner tiebreak.

WHERE THE OUTPUT LANDS
----------------------
`--publish` copies each reference to `data/processed/<name>/`. Two levels, not
three: `data/.gitignore` excludes by the shape `/*/*` and re-includes only
`!/*/*.dvc`, so a pin any deeper is invisible to git AND makes DVC scatter a second
ignore file -- breaking the one invariant the data layer has. Pin with

    dvc add data/processed/<name>
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# The PINNED engine, ahead of whatever is installed in the env.
_ENGINE = REPO / "src" / "metasmith" / "src"
if (_ENGINE / "metasmith").is_dir():
    sys.path.insert(0, str(_ENGINE))

from metasmith.python_api import (                                      # noqa: E402
    DataInstanceLibrary, Duration, Gpus, Resources, Size,
    TargetBuilder, TransformInstanceLibrary,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _driver import (                                                   # noqa: E402
    FIR_ACCOUNT, FIR_AGENT_HOME, FIR_CONTAINER, FIR_GPU, FIR_GPU_ACCOUNT, FIR_HOST,
    check_staged_executor, check_tasks, check_walltimes, envs_from_plan, fir_agent,
    landed_products, preflight,
    provision_dev_overlay_remote, publish_by_type, publish_remote, retrieve, ssh_once,
)

MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "build_references"
DATA = REPO / "data"
ORIGINALS = DATA / "originals"
PROCESSED = DATA / "processed"
SCRATCH = DATA / "scratch"
ARTIFACTS = REPO / "tests" / "artifacts"

# Every source folder the four compiles read. Staged as-is: a `fabfos_data::` type is
# a WHOLE upstream distribution, and each compile asserts for itself that the folder
# holds exactly one release directory.
SOURCES = {
    "fabfos_data::metanetx": "metanetx",
    "fabfos_data::kegg": "kegg",
    "fabfos_data::rhea": "rhea",
    "fabfos_data::kofam": "kofam",
    "fabfos_data::uniref": "uniref",
    "fabfos_data::swissprot": "swissprot",
    # T4: the ESM-C checkpoint, for the two lanes the chosen-four omits. `acquire/
    # esm_c.py` writes it; the repository is public, so nothing here is gated.
    "fabfos_data::esm_c": "esm_c",
}

TARGETS = [
    "ref::kofamscan_profiles",
    "ref::kofamscan_ko_list",
    "ref::uniref50_diamond_db",
    "ref::mnxr_lookup",
    "ref::reference_label_pool",
    # The three decided-against lanes need these two. `ref::ezpred_model` is NOT here:
    # its compile is still in transforms/_deferred/, blocked on how a *patched* source
    # tree enters the graph (see that directory's README), so the bundle is staged as a
    # given for now and the open decision is recorded rather than silently resolved.
    "ref::esm_c_600m_weights",
    "ref::reference_label_pool_esmc",
]

EXPECTED = {"kofam_ref", "uniref50_dmnd", "mnxr_lookup", "reference_label_pool",
            "esm_c_weights", "reference_label_pool_esmc"}

# The processed tier. The KEY is the full path under data/processed/, because two
# products share the kofam chunk: `<chunk>` is the DVC pin's granularity, and a
# ko_list paired with profiles from a different build applies the wrong threshold to
# every hit with nothing raised. `kofamscan_profiles` is a DIRECTORY of .hmm files
# despite its `.tgz` type extension -- kofam_ref untars it, which is the whole point.
PUBLISH_AT = {
    "ref::kofamscan_profiles": "kofam_ref/profiles",
    "ref::kofamscan_ko_list": "kofam_ref/ko_list.tsv",
    "ref::uniref50_diamond_db": "uniref50_dmnd/uniref50.dmnd",
    "ref::mnxr_lookup": "mnxr_lookup/mnxr_lookup.parquet",
    "ref::reference_label_pool": "reference_label_pool/pool",
    # `.tgz` is not decoration here: the ESM SDK resolves `data/weights/<file>.pth`
    # from the process cwd, so the archive IS the unit that layout belongs to and both
    # consumers untar it before use.
    "ref::esm_c_600m_weights": "esm_c_weights/esmc_600m.tgz",
    "ref::reference_label_pool_esmc": "reference_label_pool_esmc/pool_esmc",
}

# The DVC chunks those paths land in -- one pin each, two levels deep.
CHUNKS = ("kofam_ref", "uniref50_dmnd", "mnxr_lookup", "reference_label_pool",
          "esm_c_weights", "reference_label_pool_esmc")

TYPE_LIBRARIES = (
    [MLIB / "data_types" / f for f in
     ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml")]
    + [BREF / "data_types" / f for f in
       ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml")]
)

# fir's cpubase nodes are 192 cores / 768 GB, so these asks are modest there and were
# not on a desktop. The two poles: DIAMOND over an 8.2 GB fasta, and ProteinBERT over
# the ~222k reviewed Swiss-Prot accessions the bridge selects.
#
# The pool's memory is the number that changed. It died locally at 142 of ~217 batches
# with the host at 1 GB free; the fix is not a smaller batch but an allocation that is
# actually reserved. TensorFlow sizes its arena off the visible core count, so cpus and
# memory move together.
#
# The DURATIONS are deliberately near the measured cost rather than generously above
# it. A walltime is not free optimism: SLURM refuses to start a job that cannot finish
# before a reservation covering the nodes it needs, and fir's maintenance windows carry
# ALL_NODES -- so a 12 h ask made both poles unschedulable with hundreds of nodes idle,
# PENDING on `ReqNodeNotAvail` forever rather than failing. `check_walltimes` reports
# that case now; these numbers are what actually fits.
RESOURCE_OVERRIDES = {
    "uniref50_dmnd": Resources(cpus=32, memory=Size.GB(128), duration=Duration(hours=4)),
    "reference_label_pool": Resources(cpus=16, memory=Size.GB(128), duration=Duration(hours=4)),
    "mnxr_lookup": Resources(cpus=4, memory=Size.GB(64), duration=Duration(hours=4)),
    "kofam_ref": Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=2)),
    "esm_c_weights": Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
    # The ESM-C pass over the same ~222k accessions. The GPU fields are repeated from
    # the transform's own declaration on purpose: these directives are appended AFTER
    # the rendered GPU block and win per-directive, so an override that named only
    # cpus/memory here would be fine -- but one that ever grew an accelerator line
    # would not, and stating the requirement twice is cheaper than relying on that.
    # 2 h, not 3: `slurm.nf` doubles the walltime on retry, and a retry has to stay
    # schedulable too. 3 h became a 6 h second attempt, which SLURM would not start
    # before the maintenance window and so left PENDING forever.
    "reference_label_pool_esmc": Resources(
        cpus=8, memory=Size.GB(64), duration=Duration(hours=2),
        gpus=Gpus.REQUIRED, gpu_memory=Size.GB(24)),
}

# Images are read off the RESOLVED plan (`envs_from_plan`) and checked against fir's
# apptainer store before anything is submitted. A compute node there has no outbound
# network, so an image not already present when a task starts cannot be pulled and the
# task dies after queueing rather than at submission.


def sync_sources(host: str, remote_root: str) -> None:
    """Upload the six pinned source folders. Idempotent; ~11.4 GB the first time."""
    ssh_once(host, f"mkdir -p {remote_root}")
    for name in sorted(set(SOURCES.values())):
        src = ORIGINALS / name
        if not src.is_dir():
            raise SystemExit(f"{src} is not checked out; "
                             f"`dvc checkout data/originals/{name}.dvc`")
        print(f"    {name}", flush=True)
        subprocess.run(["rsync", "-aL", "--info=stats1",
                        f"{src}/", f"{host}:{remote_root}/{name}/"], check=True)


def check_sources(host: str, remote_root: str) -> None:
    """Each source folder must hold EXACTLY ONE release directory, checked on the host.

    One ssh round trip for all six -- never a call per folder, and never in a loop.
    """
    probe = "; ".join(
        f'echo "{name} $(ls -d {remote_root}/{name}/*/ 2>/dev/null | wc -l)"'
        for name in sorted(set(SOURCES.values())))
    counts = dict(ln.split() for ln in ssh_once(host, probe).split("\n") if ln.strip())
    bad = {n: c for n, c in counts.items() if c != "1"}
    if bad:
        lines = "\n".join(f"    {n:12s} {c} release directories, expected 1"
                           for n, c in sorted(bad.items()))
        raise SystemExit(
            f"the originals tier is not staged on {host}:{remote_root}:\n{lines}\n"
            f"  Run with --sync-sources. These are NOT re-acquired on purpose: a "
            f"reference built from a fresh pull is not the reference this repo's "
            f"pins describe.")


def build_inputs(work: Path, remote_root: str, given_refs=None) -> DataInstanceLibrary:
    """Declare the six source folders at their ABSOLUTE fir paths.

    Absolute means metasmith references them in place; relative would copy them into
    the library and stage 11.4 GB through the task for no gain. Nothing here downloads:
    degrading to an acquire step would produce a reference whose provenance is a fresh
    pull rather than the pin this repo records.
    """
    xgdb = work / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for tl in TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)
    for dtype, name in SOURCES.items():
        remote = f"{remote_root}/{name}"
        print(f"    {dtype:28s} {remote}")
        inputs.AddItem(remote, dtype)
    # References already built and pinned, declared as GIVENS so a subset build reuses
    # them instead of rebuilding them. That is not just a saving: the ESM-C pool and the
    # ProteinBERT pool must be labelled from the SAME bridge, and a rebuilt
    # mnxr_lookup is only guaranteed to be the pinned one if the build is bit-for-bit
    # deterministic. Reusing the pinned artifact makes that a fact rather than a hope.
    for dtype, remote in (given_refs or {}).items():
        print(f"    {dtype:28s} {remote}   (given)")
        inputs.AddItem(remote, dtype)
    inputs.Save()
    return inputs


def plan(work: Path, agent, remote_root: str, targets=None, given_refs=None):
    inputs = build_inputs(work, remote_root, given_refs)
    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(BREF / "resources" / "buildlib"),
        inputs,
    ]
    # acquire/ is loaded even though everything it produces is staged. That is what
    # makes the staging checkable: an acquire transform appearing in the plan means a
    # source folder did not resolve, and it is named below rather than discovered from
    # the network graph an hour later.
    transforms = [
        TransformInstanceLibrary.Load(BREF / "transforms" / "acquire"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "compile"),
    ]

    tb = TargetBuilder()
    for dtype in (targets if targets is not None else TARGETS):
        tb.Add(dtype)

    task = agent.GenerateWorkflow(
        samples=[inputs],
        resources=resources,
        transforms=transforms,
        targets=tb,
    )
    return task


def check_plan(task, expected=None) -> int:
    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"\nPlan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<24} -> {prods}")

    bad = 0
    acquires = {Path(p).stem for p in
                (BREF / "transforms" / "acquire").glob("*.py")} & used
    if acquires:
        print(f"\nSTAGING DID NOT HOLD: {sorted(acquires)} are in the plan, so those "
              f"source folders did not resolve and would be re-downloaded.", file=sys.stderr)
        bad = 1
    missing = (EXPECTED if expected is None else expected) - used
    if missing:
        print(f"\nMISSING expected transforms: {sorted(missing)}", file=sys.stderr)
        bad = 1
    # Two producers for one reference makes provenance a tiebreak. Asserted by name
    # rather than by "we did not load it", because a future edit could.
    for dup in ("downloadKofamscanDB", "downloadUniref50", "downloadEsmC"):
        if dup in used:
            print(f"\nlogistics/{dup} is in the plan -- it produces the same ref:: type "
                  f"a compile/ transform does, so which one built the reference is a "
                  f"planner tiebreak.", file=sys.stderr)
            bad = 1
    if not bad:
        print("\nno acquisition, no duplicate producer, every compile present")
    return bad


def publish(results: Path, *, dry_run: bool, publish_at=None) -> int:
    publish_at = PUBLISH_AT if publish_at is None else publish_at
    rc = publish_by_type(results, publish_at, PROCESSED, dry_run=dry_run, repo=REPO)
    if rc == 0:
        chunks = sorted({rel.split("/")[0] for rel in publish_at.values()})
        print("Pin each chunk:  " + "  ".join(
            f"dvc add data/processed/{c}" for c in chunks))
    return rc


def verify(results: Path, publish_at=None) -> int:
    """Which references a finished run actually produced -- from the MANIFESTS.

    "run completed" IS NOT "every step succeeded". `slurm.nf` sets
    `errorStrategy='ignore'` once a process exhausts its retries, so a step that died on
    every attempt leaves the workflow green with its output simply absent -- and a
    zero-output run prints exactly like a successful one. `_manifests/ref-<name>.*.json`
    is written for every DECLARED product whether or not its step ran, so matching on
    the path reports a reference "present" when only its placeholder exists. An empty
    list is the tell.
    """
    publish_at = PUBLISH_AT if publish_at is None else publish_at
    landed = landed_products(results, publish_at)
    short = set(publish_at) - landed
    if short:
        print(f"\n{len(short)} REFERENCE(S) ABSENT: {sorted(short)}\n"
              f"  Nextflow ignores a process that exhausted its retries. Read "
              f"runs/<key>/_metasmith/logs.*/main.log for the step that died.",
              file=sys.stderr)
        return 2
    print(f"all {len(landed)} references present. Now: --publish (then `dvc add`).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", action="store_true",
                    help="execute on the host; without it this plans, renders the "
                         "DAG and stops")
    ap.add_argument("--preflight", action="store_true",
                    help="report what the host must already hold, then exit")
    ap.add_argument("--targets", default=None,
                    help="comma-separated CHUNK names to build, out of "
                         + ", ".join(CHUNKS) + ". Default: all of them. A subset gets "
                         "its own task key and its own run directory, which is the "
                         "point -- adding a reference later must not re-run (or "
                         "republish over) the ones already pinned.")
    ap.add_argument("--sync-sources", action="store_true",
                    help="upload data/originals/ to --remote-originals first. "
                         "Idempotent; ~11.4 GB the first time.")
    ap.add_argument("--retrieve", action="store_true",
                    help="pull an already-finished run's results down and verify "
                         "them, without re-running")
    ap.add_argument("--publish", action="store_true",
                    help="copy the retrieved results into data/processed/, ready to "
                         "`dvc add`")
    ap.add_argument("--publish-dry-run", action="store_true")
    ap.add_argument("--publish-remote", action="store_true",
                    help="lay the results out ON THE HOST at --remote-processed, so "
                         "the GPR run consumes them there instead of waiting on a "
                         "17 GB round trip through this workstation")
    ap.add_argument("--remote-processed",
                    default="/scratch/phyberos/fabfos_refs/processed",
                    help="the host-side mirror of data/processed/")
    ap.add_argument("--host", default=FIR_HOST)
    ap.add_argument("--agent-home", default=FIR_AGENT_HOME)
    ap.add_argument("--container", default=FIR_CONTAINER)
    ap.add_argument("--remote-originals",
                    default="/scratch/phyberos/fabfos_refs/originals",
                    help="where the six pinned source folders live ON THE HOST. They "
                         "are declared at these absolute paths so metasmith binds them "
                         "in place instead of staging 11.4 GB through the task.")
    ap.add_argument("--slurm-account", default=FIR_ACCOUNT,
                    help="charged on every sbatch. slurm.nf ships the literal "
                         "placeholder '<slurm_account>', which sbatch rejects.")
    ap.add_argument("--slurm-gpu-account", default=FIR_GPU_ACCOUNT,
                    help="GPU steps are charged here instead. rrg-shallam-ab has no "
                         "GPU allocation on fir; def-shallam does.")
    ap.add_argument("--timeout-hours", type=float, default=48.0)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--work", default=str(SCRATCH / "annotation_references"))
    a = ap.parse_args()

    # A chunk subset narrows the targets, what gets published, and what `verify`
    # insists on. All three move together: a run built for two references that then
    # checked for six would report four absent, and one that published all six would
    # copy placeholder paths over four already-pinned chunks.
    if a.targets:
        chunks = [c.strip() for c in a.targets.split(",") if c.strip()]
        unknown = [c for c in chunks if c not in CHUNKS]
        if unknown:
            print(f"unknown chunk(s) {unknown}; known: {list(CHUNKS)}", file=sys.stderr)
            return 3
        publish_at = {d: rel for d, rel in PUBLISH_AT.items()
                      if rel.split("/")[0] in chunks}
        targets = [d for d in TARGETS if d in publish_at]
        expected = set(chunks)
        given_refs = {d: f"{a.remote_processed}/{rel}"
                      for d, rel in PUBLISH_AT.items() if d not in publish_at}
        print(f"=== targets: {', '.join(chunks)} "
              f"({len(targets)} of {len(TARGETS)} products) ===")
    else:
        publish_at, targets, expected = PUBLISH_AT, TARGETS, EXPECTED
        given_refs = {}

    work = Path(a.work).resolve()
    if a.targets:
        # Its own work directory too: `build_inputs` wipes inputs.xgdb, and the run's
        # results land beside it. Sharing one would make a subset run's retrieval
        # overwrite the full run's.
        work = work.parent / f"{work.name}_{'+'.join(sorted(chunks))}"
    work.mkdir(parents=True, exist_ok=True)
    local_results = work / "results"

    if a.publish or a.publish_dry_run:
        return publish(local_results, dry_run=a.publish_dry_run, publish_at=publish_at)
    agent = fir_agent(host=a.host, agent_home=a.agent_home, container=a.container)

    if a.sync_sources:
        print(f"=== uploading data/originals -> {a.host}:{a.remote_originals} ===")
        sync_sources(a.host, a.remote_originals)

    print(f"=== checking the originals tier on {a.host} ===")
    check_sources(a.host, a.remote_originals)

    print("=== planning ===")
    task = plan(work, agent, a.remote_originals, targets, given_refs)
    if not task.ok:
        print(f"\nPLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 3
    if check_plan(task, expected):
        return 3

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    svg = ARTIFACTS / "annotation_references_build.svg"
    task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
    print(f"\nDAG -> {svg}")
    print(f"\n=== task key: {task.GetKey()} ===", flush=True)

    if a.preflight:
        return preflight(a.host, a.agent_home, a.container,
                         envs_from_plan(task), mlib=MLIB)

    if a.publish_remote:
        print(f"=== publishing on {a.host} -> {a.remote_processed} ===")
        return publish_remote(a.host, agent.GetResultSource(task).GetPath(),
                              publish_at, a.remote_processed)

    if a.retrieve:
        retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
        check_tasks(a.host, a.agent_home, task.GetKey())
        return verify(local_results, publish_at)

    if not a.run:
        print("\nplan only: nothing staged, nothing run. Add --run.")
        return 0

    # Deploy and the overlay come FIRST, then preflight: two of the six things
    # preflight checks are what these two steps create, so checking before them
    # refuses every first run against a fresh agent home. Both are cheap and neither
    # submits a job, so nothing is wasted by the order -- what must not happen is
    # reaching `sbatch` with a tool image absent from the store.
    print("=== Deploy() ===", flush=True)
    agent.Deploy()
    provision_dev_overlay_remote(a.host, a.agent_home)

    if preflight(a.host, a.agent_home, a.container, envs_from_plan(task), mlib=MLIB):
        print("\nrefusing to run: a compute node has no outbound network, so an "
              "image absent from the store cannot be pulled once a task starts.",
              file=sys.stderr)
        return 4

    agent.StageWorkflow(task, on_exist="update")
    if check_staged_executor(a.host, a.agent_home, task.GetKey()):
        return 4
    if check_walltimes(a.host, RESOURCE_OVERRIDES):
        return 4

    print(f"=== executor: slurm, account {a.slurm_account} "
          f"(gpu: {a.slurm_gpu_account}) ===", flush=True)
    agent.RunWorkflow(
        task,
        config_file=agent.GetNxfConfigPresets()["slurm"],
        params={"slurmAccount": a.slurm_account,
                "slurmGpuAccount": a.slurm_gpu_account},
        resource_overrides=RESOURCE_OVERRIDES,
        gpus=FIR_GPU,
    )

    print(f"=== waiting (timeout {a.timeout_hours:.1f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_hours * 3600,
                                   poll_s=a.poll_s)
    print(f"=== status: {result['status']} after "
          f"{result['elapsed_s'] / 3600:.2f} h ===", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    check_tasks(a.host, a.agent_home, task.GetKey())
    retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
    return verify(local_results, publish_at)


if __name__ == "__main__":
    sys.exit(main())
