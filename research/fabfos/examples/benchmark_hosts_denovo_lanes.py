"""Run ONE machine's share of B2's annotation lanes -- the scatter half of a split run.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/benchmark_hosts_denovo_lanes.py --site local --lanes diamond
    PATH="..." python examples/benchmark_hosts_denovo_lanes.py --site micb0 \\
        --lanes kofam,proteinbert --run
    PATH="..." python examples/benchmark_hosts_denovo_lanes.py --site sockeye \\
        --lanes clean --run

`benchmark_hosts_denovo_on_hpc.py` runs all four lanes plus the mapper on ONE cluster.
This runs a chosen subset of the same lane transforms, unchanged, wherever that lane
belongs; `benchmark_hosts_denovo_assemble.py` collects the outputs and runs the mapper.
The transforms are the method and are shared; what this file adds is the placement.

THE PLACEMENT IS DECIDED BY WHAT A LANE NEEDS, NOT BY WHERE THERE IS ROOM.

  diamond  -> local     its 17 GB database is already materialised on this workstation
                        from the DVC cache, so running it anywhere else means moving
                        the database rather than the 4 MB of protein it reads
  kofam    -> micb0     16 cores against 27,757 HMM profiles, and the profiles move as
  pbert                 ONE 1.5 GB archive rather than as 27,757 files; ProteinBERT is
                        CPU-fine and micb0 has 176 GB
  clean    -> a cluster the ONLY lane that declares Gpus.REQUIRED, and the only reason
                        any of this needs a scheduler at all

ORFS ARE NOT PRODUCED HERE, THEY ARE STAGED. A host proteome IS the ORF set -- NCBI
already called the genes -- and `benchmark/host_proteomes.py` copies the file byte for
byte under the name the acquisition gave it. Running it once per site would produce
three identical copies of each file under three different task keys, and the mapper's
`source` column is that file's stem, which is how B2 joins a table back to its host. So
the proteome is staged directly, declared as a child of the host set, and every site
annotates the same bytes under the same name.

EVERY LANE OUTPUT IS ATTRIBUTED FROM THE RUN'S OWN LINEAGE, never from a file name.
Metasmith names products by content hash and stages inputs under channel keys, so a
results directory holding three `clean_predictions` says nothing about which host each
belongs to. `--collect` reads each product's manifest, takes the ORF-channel hash out of
its lineage, and resolves that back through the run's staged input listing. That is the
only attribution that cannot silently pair a table with the wrong host.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from hashlib import md5
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

_ENGINE = REPO / "src" / "metasmith" / "src"
if (_ENGINE / "metasmith").is_dir():
    sys.path.insert(0, str(_ENGINE))

from metasmith.python_api import (                                      # noqa: E402
    Agent, DataInstanceLibrary, Duration, Resources, Runtime, Size, Source, SshSource,
    TargetBuilder, TransformInstanceLibrary,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _driver import (                                                   # noqa: E402
    SOCKEYE_ACCOUNT, SOCKEYE_GPU, SOCKEYE_GPU_ACCOUNT, SOCKEYE_HOST, SOCKEYE_IMAGE_STORE,
    check_schedulable, check_staged_executor, check_walltimes, envs_from_plan,
    landed_products, local_agent, preflight, provision_dev_overlay_local,
    failed_tasks, provision_dev_overlay_remote, retrieve, sockeye_agent, ssh_once,
)

import benchmark_hosts_denovo_on_hpc as B2                              # noqa: E402

MLIB = REPO / "src" / "metasmith_libraries"
DATA = REPO / "data"
PROCESSED = DATA / "processed"
ORIGINALS = DATA / "originals"
SCRATCH = DATA / "scratch"

GENOMES = B2.GENOMES
# Where every site's collected lane outputs meet, and what the assemble driver reads.
LANES_OUT = SCRATCH / "hosts_denovo_split" / "lanes"

# One entry per lane: the transform's file stem, the products it must land, and the
# references it needs staged. A lane with no reference entry needs none -- CLEAN and
# ProteinBERT carry their weights baked into their images, which is why neither has a
# weights acquisition anywhere in this tree.
LANES = {
    "kofam": dict(
        transform="kofamscan",
        products={"annotation::kofamscan_results": "kofamscan.csv"},
        # The EXPANDED profiles, addressed in place -- not the packed source compiled by
        # a `kofam_ref` step inside the workflow. Moving 1.5 GB instead of 7.2 GB is
        # still the rule, and still what happened: the tarball crossed the wire once and
        # was expanded on micb0. What cannot happen is expanding it as a workflow STEP.
        #
        # A step's output lives in nxf_work, and on a site whose driver runs inside the
        # metasmith container, metasmith knows that path only as `/msm_home/...`. The
        # tool container is launched by an apptainer on the HOST, where `/msm_home` does
        # not exist, so the bind fails and every kofamscan task dies with "mount source
        # doesn't exist". Staged givens do not have this problem -- they are bound
        # through the data mount, which is translated. So the expansion is a one-time
        # setup on the machine, and the workflow only ever reads a given.
        refs={"ref::kofamscan_profiles": "kofam/profiles",
              "ref::kofamscan_ko_list": "kofam/ko_list"},
    ),
    "clean": dict(
        transform="clean",
        products={"annotation::clean_predictions": "clean.tsv"},
    ),
    "diamond": dict(
        transform="diamond_uniref50",
        products={"annotation::diamond_uniref50_results": "diamond_uniref50.tsv"},
        refs={"ref::uniref50_diamond_db": "uniref50_dmnd/uniref50.dmnd"},
    ),
    "proteinbert": dict(
        transform="proteinbert",
        products={"annotation::proteinbert_embeddings": "proteinbert_embeddings.parquet",
                  "annotation::proteinbert_index": "proteinbert_index.csv"},
    ),
}

# micb0: a lab workstation, no scheduler. 16 cores / 176 GB, outbound network (so it
# pulls its own images rather than needing a store staged), and an agent home that has
# run metasmith before.
MICB0_HOST = "micb0"
MICB0_AGENT_HOME = "/home/tliu/fabfos_b2/agent_home"
# The staged source folders live beside the agent home rather than inside it: they are
# reference data with a lifetime longer than any one run's key.
MICB0_DATA = "/home/tliu/fabfos_b2"


def micb0_agent(*, host: str = MICB0_HOST, agent_home: str = MICB0_AGENT_HOME) -> Agent:
    """No setup commands: apptainer is on the default PATH here, not behind Lmod."""
    return Agent(home=SshSource(host=host, path=agent_home).AsSource(),
                 runtime=Runtime.APPTAINER)


SITES = {
    "local": dict(executor="local", remote=False),
    "micb0": dict(executor="local", remote=True, host=MICB0_HOST,
                  agent_home=MICB0_AGENT_HOME, data=MICB0_DATA, agent=micb0_agent),
    "sockeye": dict(executor="slurm", remote=True, host=SOCKEYE_HOST,
                    agent_home=B2.SITES["sockeye"]["agent_home"],
                    data="/scratch/st-shallam-1/txyliu/fabfos_b2",
                    agent=sockeye_agent, image_store=SOCKEYE_IMAGE_STORE,
                    account=SOCKEYE_ACCOUNT, gpu_account=SOCKEYE_GPU_ACCOUNT,
                    gpu=SOCKEYE_GPU),
}

# The local executor's pool, raised from `local.nf`'s 8-core / 8 GB default. NOT
# optional: Nextflow's local executor REFUSES a process asking for more than the pool
# ("Process requirement exceeds available CPUs -- req: 12; avail: 8") rather than
# queueing it, so a 12-thread DIAMOND against the shipped default is a run that fails
# every task and still prints "run completed" with an empty results tree.
#
# queueSize is 1 on the workstation deliberately: 12 of its 16 threads to one lane at a
# time is the ask, and two concurrent lanes would each get half of it. 56 GB leaves the
# desktop 12 of its 68. micb0 is a lab box with nothing else on it, so it gets its
# whole 16 cores and can overlap the ProteinBERT lane with kofamscan.
POOLS = {
    "local": dict(cpus=14, memory="56 GB", queueSize=1),
    "micb0": dict(cpus=16, memory="160 GB", queueSize=2),
}


def pool_config(preset: Path, pool: dict, out: Path) -> Path:
    """`local.nf` plus a LITERAL executor block -- passing the pool as params does not work.

    The preset writes `executor { cpus = params.executor.cpus }` with a default of 8 in
    its own `params` block, and the obvious move is to hand `RunWorkflow` a
    `params={'executor': {'cpus': 14}}`. It lands in `workflow.params.yml` correctly and
    changes nothing: Nextflow resolves config scopes while parsing the config, before
    `-params-file` is merged, so `params.executor.cpus` is still the config's own 8. The
    symptom is the pool refusing the process it was raised for -- "Process requirement
    exceeds available CPUs -- req: 12; avail: 8" -- with a params file on disk that
    plainly says 14.

    A trailing literal block is evaluated last and wins outright, with nothing to
    resolve. Written per run rather than kept as a second preset, because the numbers
    are properties of the machine this is pointed at.
    """
    out.write_text(
        preset.read_text()
        + f"\n\n// pool for this site, set literally -- see pool_config()\n"
          f"executor {{\n"
          f"    cpus = {pool['cpus']}\n"
          f"    memory = '{pool['memory']}'\n"
          f"    queueSize = {pool['queueSize']}\n"
          f"}}\n")
    return out

RESOURCE_OVERRIDES = {
    # 12 threads, and the number is the user's: it leaves this workstation four cores
    # to stay usable on. DIAMOND declares 16 cpus / 64 GB, which is above BOTH the local
    # executor's declared pool and this machine's 68 GB -- and Nextflow's local executor
    # REFUSES an over-sized process rather than queueing it, in a block that also sets
    # errorStrategy='ignore', so the lane would be dropped and the run would finish
    # green with the output absent.
    "diamond_uniref50": Resources(cpus=12, memory=Size.GB(48), duration=Duration(hours=3)),
    "kofamscan": Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=4)),
    "proteinbert": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=2)),
    "clean": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=2)),
}


def _host_proteomes() -> list[Path]:
    if not GENOMES.exists():
        raise SystemExit(
            f"the host set is not at {GENOMES.relative_to(REPO)}.\n"
            f"  Materialise the pin: `dvc checkout data/originals/genomes.dvc`")
    out = []
    for host in sorted(p for p in GENOMES.glob("*") if (p / "genome").is_dir()):
        faa = sorted((host / "genome").glob("*.faa"))
        if len(faa) != 1:
            raise SystemExit(f"{host.name}: expected one proteome under genome/, "
                             f"found {[p.name for p in faa]}")
        out.append(faa[0])
    return out


def build_inputs(work: Path, lanes: list[str], site: dict) -> DataInstanceLibrary:
    xgdb = work / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for tl in B2.TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)

    proteomes = _host_proteomes()
    shutil.copytree(GENOMES, xgdb / GENOMES.name, dirs_exist_ok=True)
    genomes_key = inputs.AddItem(GENOMES.name, "fabfos_data::genomes")
    print(f"    fabfos_data::genomes             {GENOMES.name}/ "
          f"({len(proteomes)} proteomes)")

    (xgdb / "orfs").mkdir(exist_ok=True)
    for faa in proteomes:
        rel = Path("orfs") / faa.name
        (xgdb / rel).write_bytes(faa.read_bytes())
        inputs.AddItem(rel, "sequences::orfs", parents=[genomes_key])
        print(f"    sequences::orfs                  {rel}")

    for lane in lanes:
        spec = LANES[lane]
        # Compiled references, addressed where they already are. Absolute means bound in
        # place rather than copied into the library and staged through every task -- the
        # DIAMOND database is 17 GB and it never moves.
        for dtype, rel in spec.get("refs", {}).items():
            at = (PROCESSED / rel) if not site["remote"] else \
                f"{site['data']}/processed/{rel}"
            if not site["remote"] and not Path(at).exists():
                raise SystemExit(
                    f"{dtype} is not at {at}.\n  `dvc checkout "
                    f"data/processed/{rel.split('/')[0]}.dvc`")
            print(f"    {dtype:32s} {at}")
            inputs.AddItem(at, dtype)
        # Source folders, compiled on the machine that reads them.
        for dtype, rel in spec.get("source_refs", {}).items():
            at = (ORIGINALS / rel) if not site["remote"] else \
                f"{site['data']}/originals/{rel}"
            if not site["remote"] and not Path(at).exists():
                raise SystemExit(f"{dtype} is not at {at}")
            print(f"    {dtype:32s} {at}")
            inputs.AddItem(at, dtype)

    inputs.Save()
    return inputs


def plan(work: Path, agent, lanes: list[str], site: dict):
    inputs = build_inputs(work, lanes, site)
    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        inputs,
    ]
    transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
        # compile/ is deliberately NOT loaded. It would let the planner satisfy a
        # missing kofam reference by adding a `kofam_ref` step, which plans clean and
        # then fails every downstream task on the bind described in LANES["kofam"].
        # Withholding the transform turns that silent detour into a planning error.
    ]
    tb = TargetBuilder()
    for lane in lanes:
        for dtype in LANES[lane]["products"]:
            tb.Add(dtype)
    return inputs, agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("fabfos_data::genomes")),
        resources=resources,
        transforms=transforms,
        targets=tb,
    )


def check_plan(task, lanes: list[str], n_hosts: int) -> int:
    steps = sorted(task.plan.steps, key=lambda s: s.order)
    used = [Path(s.transform._path).stem for s in steps]
    print(f"\nPlan OK -- {len(steps)} steps\n")
    for step in steps:
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<24} "
              f"x{len(step.group_by_instances):<3} -> {prods}")

    bad = 0
    for lane in lanes:
        name = LANES[lane]["transform"]
        n = sum(len(s.group_by_instances) for s in steps
                if Path(s.transform._path).stem == name)
        if n != n_hosts:
            print(f"\n{name} groups {n} ORF set(s), not {n_hosts}. Every lane is "
                  f"`group_by=orfs`, so one job per host is the only shape that keeps "
                  f"each output attributable.", file=sys.stderr)
            bad = 1
    # A lane transform this site was NOT asked for means another machine's work is
    # about to be done twice, on the wrong machine, at whatever this one costs.
    unasked = ({LANES[k]["transform"] for k in LANES}
               - {LANES[k]["transform"] for k in lanes}) & set(used)
    if unasked:
        print(f"\n{sorted(unasked)} entered the plan and was not asked for. The split "
              f"assigns each lane to one machine; a second one running it is duplicated "
              f"work whose output will be attributed to the same host.", file=sys.stderr)
        bad = 1
    if "host_proteomes" in used or "prodigal" in used:
        print("\nan ORF-producing step is in the plan. The proteomes are staged, and a "
              "step that re-derives them gives the mapper a different `source` stem "
              "from the one the other sites annotated.", file=sys.stderr)
        bad = 1
    if not bad:
        print(f"\n{lanes} over {n_hosts} hosts, nothing else")
    return bad


# ---------------------------------------------------------------------------
# collection -- results back to per-host lane files
# ---------------------------------------------------------------------------

def _path_hash(p: str) -> int:
    """The engine's own path hash: md5, first 15 hex digits, as an int.

    Fifteen and not sixteen, because the Nextflow side reads it as a signed Java long
    and the sign bit would make the two ends disagree. Re-derived here rather than
    imported because it is the join between two files the engine writes, and a
    reimplementation that drifts would mis-attribute silently.
    """
    return int(md5(str(p).encode()).hexdigest()[:15], 16)


def orf_by_hash(staged: Path) -> dict[int, str]:
    """hash(staged path) -> accession, for every ORF file this run staged.

    `inputs/<channel>` is the listing the workflow reads; the ORF channel is the one
    whose entries are the proteome file names. Identified by suffix rather than by
    channel key, because the key is a content hash that changes with the run.
    """
    out = {}
    for listing in sorted((staged / "inputs").iterdir()):
        lines = [ln for ln in listing.read_text().splitlines() if ln.strip()]
        if not lines or not all(ln.endswith(".faa") for ln in lines):
            continue
        for ln in lines:
            out[_path_hash(ln)] = Path(ln).stem
    return out


def collect(results: Path, staged: Path, lanes: list[str], dest: Path,
            *, dry_run: bool = False) -> int:
    """Lane products -> `<dest>/<accession>/<name>`, attributed from run lineage.

    The manifest of each product carries the lineage of the task that made it: one hash
    per input channel. Intersecting that with the ORF channel's own hashes names the
    host, and nothing else in the results tree does -- product file names are content
    hashes and the directory names are types.
    """
    man_dir = results / "_manifests"
    if not man_dir.is_dir():
        raise SystemExit(f"no _manifests under {results}; the run produced nothing")
    by_hash = orf_by_hash(staged)
    if not by_hash:
        raise SystemExit(f"no ORF input listing under {staged}/inputs -- cannot "
                         f"attribute any lane output to a host")
    wanted = {d: n for lane in lanes for d, n in LANES[lane]["products"].items()}

    n_ok = 0
    for dtype, fname in wanted.items():
        stem = dtype.replace("::", "-") + "."
        entries = []
        for mf in sorted(man_dir.glob("*.json")):
            if not mf.name.startswith(stem):
                continue
            entries += json.loads(mf.read_text())
        if not entries:
            print(f"  {dtype}: NO manifest entries -- the step did not run, or died on "
                  f"every retry and Nextflow ignored it")
            continue
        for e in entries:
            hosts = {by_hash[h] for hs in e.get("lineage", {}).values() for h in hs
                     if h in by_hash}
            if len(hosts) != 1:
                print(f"  {dtype} {e['instance_id']}: lineage names {sorted(hosts)} -- "
                      f"cannot attribute to one host", file=sys.stderr)
                return 1
            acc = hosts.pop()
            src = (results / e["path"])
            if not src.exists():
                cands = list((results / e["path"]).parent.glob(
                    Path(e["path"]).name + "*"))
                if not cands:
                    print(f"  {dtype} {acc}: manifest names {e['path']}, which is not "
                          f"in the results tree", file=sys.stderr)
                    return 1
                src = cands[0]
            out = dest / acc / fname
            print(f"  {src.relative_to(results)}  ->  {out.relative_to(dest.parent)}")
            if not dry_run:
                out.parent.mkdir(parents=True, exist_ok=True)
                real = src.resolve()
                if real.is_dir():
                    if out.exists():
                        shutil.rmtree(out)
                    shutil.copytree(real, out)
                else:
                    shutil.copyfile(real, out)
            n_ok += 1
    if not n_ok:
        print("  NOTHING COLLECTED.", file=sys.stderr)
        return 1
    print(f"\n{n_ok} lane output(s) -> {dest}")
    return 0


def push_sources(site: dict, lanes: list[str]) -> None:
    """Move only what this site's lanes actually read, and only if it is not there.

    Sized to the packed form on purpose: kofam's profiles are 1.5 GB as `profiles.tar.gz`
    and 7.2 GB in 27,757 files unpacked, and the unpack is one `tar` on the far end. The
    same rule that makes a globus transfer one tarball rather than a directory.
    """
    host = site["host"]
    for lane in lanes:
        for _, rel in LANES[lane].get("source_refs", {}).items():
            src = ORIGINALS / rel
            dest = f"{site['data']}/originals/{rel}"
            if not src.exists():
                raise SystemExit(f"{src} is not materialised; `dvc checkout "
                                 f"data/originals/{rel}.dvc`")
            ssh_once(host, f"mkdir -p {Path(dest).parent}")
            print(f"  {src.relative_to(REPO)}  ->  {host}:{dest}")
            subprocess.run(["rsync", "-a", "--info=progress2",
                            f"{src}/", f"{host}:{dest}/"], check=True)
        for _, rel in LANES[lane].get("refs", {}).items():
            src = PROCESSED / rel
            dest = f"{site['data']}/processed/{rel}"
            # Already there wins over materialised here. kofam's profiles are expanded
            # ON micb0 and have no local counterpart, so demanding a local copy first
            # would fail a push that has nothing left to do.
            if ssh_once(host, f"test -e {dest} && echo yes || true").strip() == "yes":
                print(f"  {rel}: already at {host}:{dest}")
                continue
            if not src.exists():
                raise SystemExit(f"{src} is not materialised")
            ssh_once(host, f"mkdir -p {Path(dest).parent}")
            print(f"  {src.relative_to(REPO)}  ->  {host}:{dest}")
            subprocess.run(["rsync", "-a", "--info=progress2",
                            str(src), f"{host}:{Path(dest).parent}/"], check=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--site", choices=sorted(SITES), required=True)
    ap.add_argument("--lanes", required=True,
                    help=f"comma-separated: {','.join(LANES)}")
    ap.add_argument("--push-sources", action="store_true",
                    help="rsync this site's references over first, in their PACKED "
                         "form; skip if they are already there")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--retrieve", action="store_true",
                    help="pull a finished run's results and collect them, without "
                         "re-running")
    ap.add_argument("--collect", action="store_true",
                    help="attribute already-retrieved results to hosts and lay them "
                         "out for the assemble driver")
    ap.add_argument("--collect-dry-run", action="store_true")
    ap.add_argument("--timeout-hours", type=float, default=12.0)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--work", default=None)
    a = ap.parse_args()

    lanes = [x.strip() for x in a.lanes.split(",") if x.strip()]
    bad = [x for x in lanes if x not in LANES]
    if bad:
        raise SystemExit(f"unknown lane(s) {bad}; known: {sorted(LANES)}")
    site = SITES[a.site]
    work = (Path(a.work).resolve() if a.work
            else SCRATCH / f"hosts_denovo_lanes_{a.site}")
    work.mkdir(parents=True, exist_ok=True)
    local_results = work / "results"
    print(f"=== site: {a.site}, lanes: {lanes} ===")

    if a.push_sources:
        if not site["remote"]:
            print("  (local site: references are read where they already are)")
        else:
            push_sources(site, lanes)

    agent = site["agent"](host=site["host"], agent_home=site["agent_home"]) \
        if site["remote"] else local_agent(work)

    print("=== staging ===")
    inputs, task = plan(work, agent, lanes, site)
    if not task.ok:
        print(f"\nPLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 3
    if check_plan(task, lanes, len(_host_proteomes())):
        return 3
    print(f"\n=== task key: {task.GetKey()} ===", flush=True)
    (work / "RUN_KEY").write_text(task.GetKey())

    staged_dir = (work / "agent_home" / "runs" / task.GetKey()) if not site["remote"] \
        else work / "staged"

    if a.preflight:
        if not site["remote"]:
            print("local: apptainer pulls what it needs; nothing to preflight")
            return 0
        return preflight(site["host"], site["agent_home"], agent.container,
                         envs_from_plan(task), mlib=MLIB,
                         image_store=site.get("image_store"))

    if a.collect or a.collect_dry_run:
        return collect(local_results, staged_dir, lanes, LANES_OUT,
                       dry_run=a.collect_dry_run)

    if a.retrieve:
        if site["remote"]:
            retrieve(site["host"], agent.GetResultSource(task).GetPath(), local_results)
            retrieve(site["host"],
                     str(Path(agent.GetResultSource(task).GetPath()).parent / "inputs"),
                     staged_dir / "inputs")
        else:
            # A local retrieve still has to REFRESH the copy, dereferencing as the run
            # path does. Collecting from whatever is already there re-collects the last
            # attempt, which is exactly wrong after a run that failed partway.
            src = agent.GetResultSource(task).GetPath()
            if local_results.exists():
                shutil.rmtree(local_results)
            shutil.copytree(src, local_results, symlinks=False)
        return collect(local_results, staged_dir, lanes, LANES_OUT)

    if not a.run:
        print("\nplan only: nothing staged, nothing run. Add --run.")
        return 0

    print("=== Deploy() ===", flush=True)
    agent.Deploy()
    if site["remote"]:
        provision_dev_overlay_remote(site["host"], site["agent_home"])
        if preflight(site["host"], site["agent_home"], agent.container,
                     envs_from_plan(task), mlib=MLIB,
                     image_store=site.get("image_store")):
            # micb0 has an outbound route and can pull; sockeye's compute nodes do not.
            if site["executor"] == "slurm":
                print("\nrefusing to run: a compute node has no outbound network, so "
                      "an image absent from the store cannot be pulled once a task "
                      "starts.", file=sys.stderr)
                return 4
            print("\n(images missing but this host has a route out; apptainer will "
                  "pull them)")
    else:
        provision_dev_overlay_local(work / "agent_home")

    agent.StageWorkflow(task, on_exist="update")

    run_kwargs = dict(resource_overrides=RESOURCE_OVERRIDES)
    if site["executor"] == "slurm":
        if check_staged_executor(site["host"], site["agent_home"], task.GetKey()):
            return 4
        if check_walltimes(site["host"], RESOURCE_OVERRIDES):
            return 4
        if check_schedulable(site["host"], site["account"], RESOURCE_OVERRIDES,
                             workdir=site["agent_home"]):
            return 4
        run_kwargs.update(
            config_file=agent.GetNxfConfigPresets()["slurm"],
            params={"slurmAccount": site["account"],
                    "slurmGpuAccount": site["gpu_account"]},
            gpus=site["gpu"])
    else:
        run_kwargs.update(config_file=pool_config(
            agent.GetNxfConfigPresets()["local"], POOLS[a.site],
            work / "workflow.local_pool.nf"))

    print(f"=== executor: {site['executor']} ===", flush=True)
    agent.RunWorkflow(task, **run_kwargs)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_hours * 3600,
                                   poll_s=a.poll_s)
    print(f"=== status: {result['status']} after "
          f"{result['elapsed_s'] / 3600:.2f} h ===", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    src = agent.GetResultSource(task).GetPath()
    # BEFORE retrieving, not after. Both presets set errorStrategy='ignore', so a run
    # whose every task failed still reports "completed" -- and the retrieve that follows
    # is the expensive part. The first kofam attempt lost all three tasks and then began
    # dragging 27,756 profile files back over ssh to prove it.
    if failed := failed_tasks(Path(src).parent, site):
        print(f"\nTHE RUN IS GREEN BUT {len(failed)} TASK(S) FAILED: "
              f"{', '.join(failed)}.\n  Nothing retrieved -- fix the failure and re-run. "
              f"The task's .command.err on {site.get('host', 'this host')} says why.",
              file=sys.stderr)
        return 2
    if site["remote"]:
        retrieve(site["host"], src, local_results)
        retrieve(site["host"], str(Path(src).parent / "inputs"), staged_dir / "inputs")
    else:
        if local_results.exists():
            shutil.rmtree(local_results)
        # symlinks=False DEREFERENCES, and it has to. Metasmith lays results out under
        # `mode='rellink'`, so each product is a RELATIVE symlink into the work tree --
        # correct where it was written, dangling the moment the tree is copied to a
        # different depth. Preserving the links here produced a results directory that
        # passed every manifest check and then raised FileNotFoundError on the first
        # file collected, naming a path two levels short of the real one.
        shutil.copytree(src, local_results, symlinks=False)

    dtypes = [d for lane in lanes for d in LANES[lane]["products"]]
    landed = landed_products(local_results, dtypes)
    absent = [d for d in dtypes if d not in landed]
    if absent:
        print(f"\nTHE RUN IS GREEN BUT {absent} IS ABSENT -- errorStrategy='ignore' "
              f"means a step that died on every retry leaves the workflow green with "
              f"its output missing.", file=sys.stderr)
        return 2
    return collect(local_results, staged_dir, lanes, LANES_OUT)


if __name__ == "__main__":
    sys.exit(main())
