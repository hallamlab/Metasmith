"""Assemble B2 from lane outputs computed ELSEWHERE -- the collect half of a split run.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/benchmark_hosts_denovo_assemble.py                # plan only
    PATH="..." python examples/benchmark_hosts_denovo_assemble.py --run

`benchmark_hosts_denovo_on_hpc.py` runs the WHOLE de-novo half on one cluster. This runs
the last two steps of that same graph -- `gpr_4lane` per host, then `host_gpr_denovo` --
against lane outputs that were produced on whatever machine each lane belongs on. The
graph, the transforms and the targets are identical; what changes is where the lanes
came from.

THE ONE THING THAT MAKES THIS POSSIBLE IS THAT A GIVEN CAN CARRY LINEAGE.
`gpr_4lane` pins all five of its lane requirements with `parents={orfs}` and groups on
`orfs`; `host_gpr_denovo` pins the mapper table with `parents={genomes}`. Those pins are
what stop a kofam result for one host pairing with a DIAMOND result for another. Lane
outputs staged as ordinary givens have no lineage at all, so they satisfy nothing and
the per-host pairing has nothing to key on. `DataInstanceLibrary.AddItem(path, dtype,
parents=[...])` is the escape: this driver stages

    genomes                              (no parent)
      -> <accession>.faa                 parents=[genomes]
           -> that host's five lanes     parents=[<accession>.faa]

and the planner then reconstructs exactly the graph the single-site run would have built
in one place, minus the steps whose products are already on disk.

THE PLAN IS THE PROOF, so plan-only mode is a real check rather than a dry run -- but
what it counts is INSTANCES, not steps. A plan step is an archetype: `gpr_4lane` appears
once and carries three instances of every lane requirement, and the executor turns that
into three jobs because `group_by=orfs` and three ORF sets are bound to it. Counting
steps would report "1 mapper" for both the right answer and the collapsed one. So this
asserts three instances on the mapper's group-by, one on the collector's, three on every
lane requirement, and that no lane transform and no `host_proteomes` entered the plan.

Fewer than three mapper instances means the staged parents collapsed into one lineage,
which would produce a table whose `source` column names three ORF sets --
`host_gpr_denovo` refuses that, but only after every mapper has already run. A lane
transform in the plan means a staged lane was not recognised as satisfying that
requirement, so the planner scheduled the work again on the machine that was supposed to
do none of it.

`--placeholder` stages EMPTY lane files. Planning never opens an input, so an empty file
resolves a type exactly as a real one does -- the same trick `metabolism_references_dag`
uses to plan the MetaCyc drop-in on a machine that has not licensed it. That is the
cheapest possible falsification of the lineage assumption, and it is what runs before
any lane is computed anywhere.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

_ENGINE = REPO / "src"
if (_ENGINE / "metasmith").is_dir():
    sys.path.insert(0, str(_ENGINE))

from metasmith.python_api import (                                      # noqa: E402
    DataInstanceLibrary, Duration, Resources, Size,
    TargetBuilder, TransformInstanceLibrary,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _driver import (                                                   # noqa: E402
    landed_products, local_agent, provision_dev_overlay_local, publish_by_type,
)

import benchmark_hosts_denovo_on_hpc as B2                              # noqa: E402
import benchmark_hosts_denovo_lanes as LN                               # noqa: E402

BREF = REPO / "src" / "fabfos" / "build_references"
MLIB = REPO / "src" / "metasmith_libraries"
DATA = REPO / "data" / "fabfos"
PROCESSED = DATA / "processed"
SCRATCH = DATA / "scratch"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"

GENOMES = B2.GENOMES
TARGET_4 = B2.TARGET_4
TARGET_DENOVO = B2.TARGET_DENOVO

LANES = {d: n for lane in ("kofam", "clean", "diamond", "proteinbert")
         for d, n in LN.LANES[lane]["products"].items()}

REFS = {
    "ref::mnxr_lookup": "mnxr_lookup/mnxr_lookup.parquet",
    "ref::reference_label_pool": "reference_label_pool/pool",
}

RECOMPUTED = {"kofamscan", "clean", "diamond_uniref50", "proteinbert", "host_proteomes",
              "prodigal"}

RESOURCE_OVERRIDES = {
    "gpr_4lane": Resources(cpus=4, memory=Size.GB(48), duration=Duration(hours=1)),
    "host_gpr_denovo": Resources(cpus=1, memory=Size.GB(8),
                                 duration=Duration(minutes=30)),
}


def host_proteomes() -> list[Path]:
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
    if not out:
        raise SystemExit(f"no host/genome/*.faa under {GENOMES}")
    return out


def build_inputs(work: Path, lanes_dir: Path, *, placeholder: bool
                 ) -> DataInstanceLibrary:
    xgdb = work / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for tl in B2.TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)

    proteomes = host_proteomes()
    shutil.copytree(GENOMES, xgdb / GENOMES.name, dirs_exist_ok=True)
    genomes_key = inputs.AddItem(GENOMES.name, "fabfos_data::genomes")
    print(f"    fabfos_data::genomes             {GENOMES.name}/  "
          f"({len(proteomes)} proteomes)")

    (xgdb / "orfs").mkdir(exist_ok=True)
    for faa in proteomes:
        acc = faa.stem
        rel = Path("orfs") / faa.name
        (xgdb / rel).write_bytes(faa.read_bytes())
        orfs_key = inputs.AddItem(rel, "sequences::orfs", parents=[genomes_key])
        print(f"    sequences::orfs                  {rel}  (parent: "
              f"{GENOMES.name})")

        src = lanes_dir / acc
        (xgdb / "lanes" / acc).mkdir(parents=True, exist_ok=True)
        for dtype, name in LANES.items():
            lrel = Path("lanes") / acc / f"{acc}.{name}"
            if placeholder:
                (xgdb / lrel).write_bytes(b"")
            else:
                have = src / name
                if not have.exists():
                    raise SystemExit(
                        f"{dtype} absent for {acc}: expected {have}.\n"
                        f"  Lanes are computed elsewhere and collected here; see the "
                        f"module docstring. Use --placeholder to check the PLAN only.")
                shutil.copyfile(have, xgdb / lrel)
            inputs.AddItem(lrel, dtype, parents=[orfs_key])
        print(f"      + {len(LANES)} lane outputs"
              f"{' (EMPTY placeholders)' if placeholder else ''}  (parent: {rel})")

    for dtype, rel in REFS.items():
        p = PROCESSED / rel
        if not p.exists():
            raise SystemExit(
                f"{dtype} is not at {p.relative_to(REPO)}.\n"
                f"  Materialise the pin: `dvc checkout data/processed/"
                f"{rel.split('/')[0]}.dvc`")
        print(f"    {dtype:32s} {p.relative_to(REPO)}")
        inputs.AddItem(p, dtype)

    inputs.Save()
    return inputs


def plan(work: Path, agent, lanes_dir: Path, *, placeholder: bool):
    inputs = build_inputs(work, lanes_dir, placeholder=placeholder)
    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        inputs,
    ]
    transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "fabfos"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "benchmark"),
    ]
    tb = TargetBuilder()
    tb.Add(TARGET_4)
    tb.Add(TARGET_DENOVO)
    return agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("fabfos_data::genomes")),
        resources=resources,
        transforms=transforms,
        targets=tb,
    )


def check_plan(task, n_hosts: int) -> int:
    steps = sorted(task.plan.steps, key=lambda s: s.order)
    used = [Path(s.transform._path).stem for s in steps]
    print(f"\nPlan OK -- {len(steps)} steps, {len(set(used))} distinct transforms\n")
    by_name = {}
    for step in steps:
        name = Path(step.transform._path).stem
        by_name.setdefault(name, []).append(step)
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {name:<24} x{len(step.group_by_instances):<3} "
              f"-> {prods}")

    bad = 0

    def instances(name, dep_type):
        got = []
        for step in by_name.get(name, []):
            for dep, insts in step.dependency_map.items():
                if insts and insts[0].dtype_name == dep_type:
                    got += insts
        return got

    n_map = sum(len(s.group_by_instances) for s in by_name.get("gpr_4lane", []))
    if n_map != n_hosts:
        print(f"\ngpr_4lane groups {n_map} ORF set(s) for {n_hosts} hosts. The lane pins "
              f"are `parents={{orfs}}` and the grouping is `group_by=orfs`, so one job "
              f"per host is the ONLY shape that pairs each host's lanes with its own "
              f"ORFs. Fewer means the staged parents collapsed into one lineage.",
              file=sys.stderr)
        bad = 1
    for dtype in LANES:
        n = len(instances("gpr_4lane", dtype))
        if n != n_hosts:
            print(f"\n{dtype} is bound {n} time(s), not once per host. A lane that "
                  f"binds fewer times than the ORF sets it annotates is a lane the "
                  f"mapper will reuse across hosts.", file=sys.stderr)
            bad = 1
    n_collect = sum(len(s.group_by_instances) for s in by_name.get("host_gpr_denovo", []))
    if n_collect != 1:
        print(f"\nhost_gpr_denovo groups {n_collect} host set(s); B2 is ONE table over "
              f"the host set.", file=sys.stderr)
        bad = 1
    again = sorted(RECOMPUTED & set(used))
    if again:
        print(f"\n{again} entered the plan. Those lanes are staged as givens and were "
              f"supposed to be reused, not recomputed -- a staged lane that does not "
              f"satisfy its requirement is the lineage assumption failing.",
              file=sys.stderr)
        bad = 1
    if not bad:
        print(f"\n{n_hosts} mapper jobs over {n_hosts} ORF sets, five lanes bound "
              f"{n_hosts}x each, one collector, nothing recomputed -- the staged "
              f"lineage satisfies both pins.")
    return bad


def check_given_lineage(agent, task, n_hosts: int) -> int:
    staged = Path(agent.GetResultSource(task).GetPath()).parent
    lf = staged / "workflow.lineage_of_given.json"
    if not lf.exists():
        print(f"\nno {lf.name} in the staged workflow -- nothing declared a parent, so "
              f"the lane channels will be joined by position.", file=sys.stderr)
        return 4
    data = json.loads(lf.read_text())
    lineage = data.get("lineage", {})
    bad = 0
    print(f"\nstaged lineage ({lf.relative_to(staged.parent)}):")
    for channel, entries in sorted(lineage.items()):
        parents = {p for e in entries for p in e}
        print(f"  {Path(channel).name:<12} {len(entries)} file(s), parent channels "
              f"{sorted(parents)}")
    if not lineage:
        print("\nthe lineage map is EMPTY: no staged given declared a parent that is "
              "also used by the workflow, so every lane channel will be joined by "
              "position rather than by host.", file=sys.stderr)
        return 4

    for channel, entries in lineage.items():
        name = Path(channel).name
        if len(entries) != n_hosts:
            print(f"\n{name}: {len(entries)} staged files for {n_hosts} hosts.",
                  file=sys.stderr)
            bad = 1
            continue
        for pchannel in {p for e in entries for p in e}:
            got = [e.get(pchannel, []) for e in entries]
            if any(len(g) != 1 for g in got):
                print(f"\n{name}: an entry names {[len(g) for g in got]} parents in "
                      f"{pchannel}, not one each.", file=sys.stderr)
                bad = 1
            elif len({g[0] for g in got if g}) != n_hosts and pchannel != _root(lineage):
                print(f"\n{name}: its {n_hosts} files name only "
                      f"{len({g[0] for g in got if g})} distinct parent(s) in "
                      f"{pchannel} -- two hosts' lanes are pinned to one ORF set.",
                      file=sys.stderr)
                bad = 1
    if not bad:
        print(f"  -> {len(lineage)} channels, each {n_hosts} files deep, each file "
              f"naming its own parent")
    return bad


def _root(lineage: dict) -> str:
    parents = {p for entries in lineage.values() for e in entries for p in e}
    return next(iter(parents - {Path(c).name for c in lineage}), "")


def verify(results: Path) -> int:
    targets = [TARGET_4, TARGET_DENOVO]
    landed = landed_products(results, targets)
    absent = [t for t in targets if t not in landed]
    if absent:
        print(f"\nTHE RUN IS GREEN BUT {absent} IS ABSENT. Both nextflow presets set "
              f"errorStrategy='ignore' once a process exhausts its retries, so a step "
              f"that died every attempt leaves the workflow green with its output "
              f"simply missing; read _metasmith/logs.*/main.log.", file=sys.stderr)
        return 2
    print(f"{', '.join(targets)} present. Now: --publish")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lanes-from", default=None,
                    help="directory of <accession>/<lane file> produced elsewhere; "
                         "defaults to data/scratch/hosts_denovo_split/lanes")
    ap.add_argument("--placeholder", action="store_true",
                    help="stage EMPTY lane files. Planning never opens an input, so "
                         "this checks the plan shape before any lane exists.")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--stage-only", action="store_true",
                    help="deploy and stage, check the staged lineage index, and stop "
                         "without launching. Computes nothing, so it is safe with "
                         "--placeholder and is where the lineage claim is checked.")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--publish-dry-run", action="store_true")
    ap.add_argument("--timeout-hours", type=float, default=6.0)
    ap.add_argument("--poll-s", type=float, default=30.0)
    ap.add_argument("--work", default=None)
    a = ap.parse_args()

    work = Path(a.work).resolve() if a.work else SCRATCH / "hosts_denovo_assemble"
    work.mkdir(parents=True, exist_ok=True)
    local_results = work / "results"
    lanes_dir = Path(a.lanes_from).resolve() if a.lanes_from else LN.LANES_OUT

    if a.publish or a.publish_dry_run:
        return publish_by_type(local_results, B2.PUBLISH_AT,
                               REPO / "data" / "fabfos" / "benchmarks" / "denovo",
                               dry_run=a.publish_dry_run, repo=REPO)

    agent = local_agent(work)
    print(f"=== staging (lanes from {lanes_dir if not a.placeholder else 'PLACEHOLDERS'})"
          f" ===")
    task = plan(work, agent, lanes_dir, placeholder=a.placeholder)
    if not task.ok:
        print(f"\nPLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 3
    if check_plan(task, len(host_proteomes())):
        return 3

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    svg = ARTIFACTS / "hosts_denovo_assemble.svg"
    task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env"})
    print(f"\nDAG -> {svg}")
    print(f"\n=== task key: {task.GetKey()} ===", flush=True)
    (work / "RUN_KEY").write_text(task.GetKey())

    if not (a.run or a.stage_only):
        print("\nplan only: nothing staged, nothing run. Add --run.")
        return 0
    if a.placeholder and a.run:
        print("\nrefusing to run on placeholder lanes: every mapper would read an "
              "empty file and the validator would refuse the table one whole run "
              "later. --stage-only is the placeholder path.", file=sys.stderr)
        return 4

    print("=== Deploy() ===", flush=True)
    agent.Deploy()
    provision_dev_overlay_local(work / "agent_home")
    agent.StageWorkflow(task, on_exist="update")
    if check_given_lineage(agent, task, len(host_proteomes())):
        return 4
    if a.stage_only:
        print("\nstaged only: the lineage index is what was checked; nothing launched.")
        return 0
    agent.RunWorkflow(
        task,
        config_file=LN.pool_config(
            agent.GetNxfConfigPresets()["local"],
            dict(cpus=14, memory="56 GB", queueSize=1),
            work / "workflow.local_pool.nf"),
        resource_overrides=RESOURCE_OVERRIDES,
    )
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_hours * 3600,
                                   poll_s=a.poll_s)
    print(f"=== status: {result['status']} after "
          f"{result['elapsed_s'] / 3600:.2f} h ===", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    src = agent.GetResultSource(task).GetPath()
    if local_results.exists():
        shutil.rmtree(local_results)
    shutil.copytree(src, local_results, symlinks=False)
    return verify(local_results)


if __name__ == "__main__":
    sys.exit(main())
