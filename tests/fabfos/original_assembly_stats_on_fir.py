#!/usr/bin/env python3
"""Assembly statistics for the 35 SCADC pools' OWN assemblies, both assemblers, on fir.

    PYTHONPATH=src python tests/fabfos/original_assembly_stats_on_fir.py
    PYTHONPATH=src python tests/fabfos/original_assembly_stats_on_fir.py --offline
    PYTHONPATH=src python tests/fabfos/original_assembly_stats_on_fir.py --preflight
    PYTHONPATH=src python tests/fabfos/original_assembly_stats_on_fir.py --run
    PYTHONPATH=src python tests/fabfos/original_assembly_stats_on_fir.py --summarize <dir>/results

WHY THIS EXISTS, BESIDE ITS SIBLING
-----------------------------------
`assembly_stats_on_fir.py` measures how much of each pool's sequencing lands on
the RECOVERED INSERT SET -- one reference, 35 jobs. Nothing has ever computed N50
and its neighbours for the assemblies the inserts were recovered FROM, so the
insert-set numbers have no denominator: "0.79-0.85 of reads map to the inserts" is
uninterpretable without knowing what fraction maps to the whole assembly.

This driver measures the other side: 35 megahit assemblies and 35 spades
assemblies, each against its own pool's reads. Everything that is not the fan-out
is imported from the sibling rather than copied -- the single-shot ssh helper, the
preflight, the task-table check, the orphan resolver, the resource trim and the
retrieval discipline are all identical and there is exactly one copy of each.

THE FAN-OUT INVERTS, AND THAT IS THE WHOLE DIFFERENCE
-----------------------------------------------------
In the sibling, ONE insert FASTA is parented to ALL 35 `read_metadata` items, and
the orchestrator's intersection filter distributes it to every pool's job.

Here each job has its OWN reference, so the lineage runs the other way: one
`read_metadata` per (pool, assembler) -- 70 of them -- with each assembly parented
to exactly one. The reads are added ONCE per pool, parented to BOTH of that pool's
metadata items. That is the aggregate-then-distribute shape again, now on the
reads, and it is what lets `seqkit_reads` (which requires only `sequences::reads`
and groups on it) stay at 35 jobs while `assembly_stats` (which groups on
`read_metadata`) runs 70 -- without declaring a 32 GB read set twice under two
names, which `AddItem` refuses anyway.

The same shape carries the read-QC product across. `assembly_stats` pins
`rstats` to `meta`, but `seqkit_reads` never sees a `read_metadata` at all; the
QC product inherits its pool's reads' ancestry, which is BOTH of that pool's
metadata items, so it intersects with either one. One QC run therefore serves both
of a pool's stats jobs. `check_plan` is what proves it: 35 tasks for the QC step
and 70 for the stats step, read off the resolved plan before anything is deployed.

THE ASSEMBLIES MUST BE DECLARED AS THE GENERIC TYPE, NOT THE ASSEMBLER'S OWN
----------------------------------------------------------------------------
`sequences::megahit_assembly` and `sequences::spades_assembly` both extend
`sequences::assembly` by adding a `method` property, so by property superset
either satisfies what `assembly_stats` requires. Declaring them that way -- so
`check_plan` could count the halves separately -- silently loses half the run: the
planner binds the `asm` requirement to ONE concrete given type, and the resolved
plan then carried 35 `spades_assembly` givens and no megahit at all while STILL
reporting 70 tasks. The 35 megahit jobs would have run against nothing.

Both are therefore declared as plain `sequences::assembly`, one slot, 70 givens,
and the guarantee that the halves are balanced comes from `pools_from_assemblies`
refusing a pool that has only one of them. `check_plan`'s given count is 70 of one
type, and a collapse to 35 is what it catches.

ATTRIBUTION JOINS ON THE ASSEMBLY
----------------------------------
The sibling joins outputs back to pools through the READS given, because its
single insert FASTA is parented to all 35 metadata items and so inherits all 35
indices through it. The same union bites here from the other side: the reads
carry BOTH of a pool's metadata items as parents, so every stats output inherits
both and `read_metadata` -- the obvious key, and the one this driver's fan-out is
built on -- resolves to two labels, not one.

The ASSEMBLY is the key that is genuinely one-to-one with a job, and its filename
already names pool and assembler. Metadata and reads stay as second and third
tiers: the QC products carry no metadata lineage at all and land on the reads,
where one per pool is exactly right for them.

Everything else -- the two-artifact dev overlay, one ssh session and never a retry
loop, the `sequences::reads` / `qc: none` understatement, "completed is not
succeeded" -- is documented in `assembly_stats_on_fir.py` and applies unchanged.
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(REPO / "src"))

import assembly_stats_on_fir as ins  # noqa: E402  the shared machinery

from metasmith.python_api import (  # noqa: E402
    Agent, DataInstanceLibrary, DataTypeLibrary, Runtime, SshSource,
    TargetBuilder, TransformInstanceLibrary,
)

LIB = ins.LIB
ARTIFACTS = ins.ARTIFACTS
ASSEMBLIES = REPO / "data" / "fabfos" / "runs" / "scadc_fosmids" / "assembly"

ASSEMBLERS = ("megahit", "spades")
ASSEMBLY_TYPE = "sequences::assembly"

ORPHAN_MAP = ins.ORPHAN_MAP
READS_TYPE = ins.READS_TYPE
READS_SUFFIX = ins.READS_SUFFIX
EXPECTED_TRANSFORMS = ins.EXPECTED_TRANSFORMS
SETUP_COMMANDS = ins.SETUP_COMMANDS
AGENT_CONTAINER = ins.AGENT_CONTAINER

TRIMMED_RESOURCES = {
    "assembly_stats": ins.Resources(
        cpus=4, memory=ins.Size.GB(24), duration=ins.Duration(hours=4)),
    "seqkit_reads": ins.Resources(
        cpus=4, memory=ins.Size.GB(8), duration=ins.Duration(hours=1)),
}


def pools_from_assemblies() -> dict[str, dict[str, Path]]:
    found: dict[str, dict[str, Path]] = {}
    for asm in sorted(ASSEMBLERS):
        for p in sorted(ASSEMBLIES.glob(f"*.{asm}.fna")):
            pool = p.name[: -len(f".{asm}.fna")]
            found.setdefault(pool, {})[asm] = p
    incomplete = {p: sorted(v) for p, v in found.items() if len(v) != len(ASSEMBLERS)}
    if incomplete:
        raise SystemExit(
            f"{len(incomplete)} pool(s) under {ASSEMBLIES} have only some "
            f"assemblers: {incomplete}")
    if not found:
        raise SystemExit(
            f"no *.{{{','.join(sorted(ASSEMBLERS))}}}.fna under {ASSEMBLIES}. "
            f"`mamba run -n dvc dvc checkout data/fabfos/runs/scadc_fosmids/assembly.dvc`")
    return found


def label(pool: str, assembler: str) -> str:
    return f"{pool}__{assembler}"


def build_inputs(staging: Path, pools: dict[str, str],
                 assemblies: dict[str, dict[str, Path]]) -> DataInstanceLibrary:
    xgdb = staging / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for ns, yml in (("sequences", "sequences.yml"), ("fabfos", "fabfos.yml")):
        inputs.AddTypeLibrary(namespace=ns,
                              lib=DataTypeLibrary.Load(LIB / "data_types" / yml))

    for pool in sorted(pools):
        pool_metas = []
        for asm in sorted(ASSEMBLERS):
            meta = inputs.AddValue(
                name=f"read_metadata_{label(pool, asm)}.json",
                value={"parity": "paired", "length_class": "short"},
                dtype="sequences::read_metadata",
            )
            pool_metas.append(meta)
            src = assemblies[pool][asm]
            shutil.copy(src, xgdb / src.name)
            inputs.AddItem(src.name, ASSEMBLY_TYPE, parents={meta})
        inputs.AddItem(pools[pool], READS_TYPE, parents=set(pool_metas))
    inputs.Save()
    return inputs


def check_plan(task, n_pools: int) -> list[str]:
    problems: list[str] = []
    n_jobs = n_pools * len(ASSEMBLERS)

    names = {Path(s.transform._path).stem for s in task.plan.steps}
    if names != EXPECTED_TRANSFORMS:
        problems.append(
            f"plan uses {sorted(names)}, expected exactly "
            f"{sorted(EXPECTED_TRANSFORMS)}. An assembler in this list means the "
            f"planner would BUILD an assembly and report coverage of that.")

    given = Counter(g.dtype_name for g in task.plan.given)
    for dtype, want in ((READS_TYPE, n_pools),
                        ("sequences::read_metadata", n_jobs),
                        (ASSEMBLY_TYPE, n_jobs)):
        if given.get(dtype, 0) != want:
            problems.append(
                f"plan carries {given.get(dtype, 0)} x [{dtype}], expected {want}")

    per_step = {"seqkit_reads": n_pools, "assembly_stats": n_jobs}
    for step in task.plan.steps:
        stem = Path(step.transform._path).stem
        n = len(step.group_by_instances)
        want = per_step.get(stem)
        if want is not None and n != want:
            problems.append(
                f"step [{stem}] would run {n} task(s), expected {want} "
                f"({n_pools} pools x {len(ASSEMBLERS)} assemblers)")
    return problems


ATTRIBUTION_KEYS = ("asm", "meta", "reads")


def attribute(results: Path) -> dict[Path, str]:
    manifests = results / "_manifests"
    keys: dict[str, str] = {}
    index_to_label: dict[str, dict[int, str]] = {}
    with open(manifests / "given.csv") as f:
        for row in csv.DictReader(f):
            t, k = row["type_name"], row["instance_key"]
            name = Path(row["path"]).name
            if t == ASSEMBLY_TYPE:
                keys["asm"] = k
                pool, _, asm = name[: -len(".fna")].rpartition(".")
                lab = label(pool, asm)
            elif t == "sequences::read_metadata":
                keys["meta"] = k
                lab = name[len("read_metadata_"): -len(".json")]
            elif t == READS_TYPE:
                keys["reads"] = k
                lab = name[: -len(READS_SUFFIX)]
            else:
                continue
            index_to_label.setdefault(k, {})[int(row["instance_index"])] = lab
    for want in ATTRIBUTION_KEYS:
        if want not in keys:
            raise SystemExit(f"no {want} rows in {manifests/'given.csv'}")

    orphans: dict[str, str] = {}
    omap = results / ORPHAN_MAP
    if omap.exists():
        for line in omap.read_text().splitlines()[1:]:
            token, lab = line.split("\t")
            orphans[token] = lab

    out: dict[Path, str] = {}
    unresolved: list[str] = []
    for mf in sorted(manifests.glob("*.json")):
        for entry in json.loads(mf.read_text()):
            path = entry["path"]
            lineage = entry.get("lineage", {})
            for which in ATTRIBUTION_KEYS:
                k = keys[which]
                labs = {index_to_label[k][i]
                        for i in lineage.get(k, []) if i in index_to_label[k]}
                if len(labs) == 1:
                    out[Path(path)] = labs.pop()
                    break
            else:
                lab = orphans.get(ins._output_token(path))
                if lab is None:
                    unresolved.append(path)
                else:
                    out[Path(path)] = lab
    if unresolved:
        raise SystemExit(
            f"{len(unresolved)} output(s) have no usable lineage and no entry in "
            f"{omap}, e.g. {unresolved[0]}.\nRun --resolve-orphans "
            f"<results_dir> --run-dir <host run dir> while the run still exists.")
    return out


def summarize(results: Path, out_dir: Path) -> int:
    label_of = attribute(results)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "job_map.tsv", "w") as f:
        f.write("product_path\tlabel\n")
        for p in sorted(label_of):
            f.write(f"{p}\t{label_of[p]}\n")

    def _load(pred):
        got = {}
        for p, lab in label_of.items():
            if not pred(p):
                continue
            local = results / p
            if not local.exists():
                continue
            got[lab] = local
        return got

    stats = _load(lambda p: "assembly_stats" in str(p))
    qc = _load(lambda p: "read_qc_stats" in str(p))

    rows = []
    for lab in sorted(stats):
        pool, _, assembler = lab.rpartition("__")
        s = json.loads(stats[lab].read_text())
        q = json.loads(qc[pool].read_text()) if pool in qc else {}
        sk = s.get("_raw_seqkit", {})
        raw = s.get("_raw_mapping", {})
        rows.append(dict(
            pool=pool,
            assembler=assembler,
            number_of_contigs=s["number_of_contigs"],
            length=s["length"],
            N50=s["N50"],
            L50=sk.get("N50_num"),
            max_length=sk.get("max_len"),
            GC=s["GC"],
            reads=q.get("reads"),
            bases=q.get("bases"),
            total_alignment_records=raw.get(
                "total (QC-passed reads + QC-failed reads)"),
            mapped=raw.get("mapped"),
            fraction_reads_mapped=s["fraction_reads_mapped"],
        ))

    cols = ["pool", "assembler", "number_of_contigs", "length", "N50", "L50",
            "max_length", "GC", "reads", "bases", "total_alignment_records",
            "mapped", "fraction_reads_mapped"]
    with open(out_dir / "assembly_summary.tsv", "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join("" if r[c] is None else str(r[c]) for c in cols) + "\n")

    by_asm = Counter(r["assembler"] for r in rows)
    print(f"=== wrote to {out_dir} ===")
    print(f"  job_map.tsv           {len(label_of)} products named")
    print(f"  assembly_summary.tsv  {len(rows)} rows ({dict(by_asm)})")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="fir")
    ap.add_argument("--reads-dir",
                    default="/scratch/phyberos/metasmith_fabfos/"
                            "host_filtered_reads_pairaware_20260724")
    ap.add_argument("--agent-home",
                    default="/scratch/phyberos/metasmith_fabfos/"
                            "scadc_multiassembly_iso",
                    help="reused deliberately: it carries the dev/metasmith tree "
                         "and tarball the msm wrapper binds over the engine.")
    ap.add_argument("--container", default=AGENT_CONTAINER)
    ap.add_argument("--slurm-account", default="rrg-shallam-ab")
    ap.add_argument("--pools", nargs="*", default=None,
                    help="pool barcodes to measure; default is all of them.")
    ap.add_argument("--run", action="store_true",
                    help="deploy, stage and execute. Default is plan-only.")
    ap.add_argument("--offline", action="store_true",
                    help="fabricate the read paths instead of listing them on "
                         "the host. Implies plan-only.")
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--summarize", metavar="RESULTS_DIR")
    ap.add_argument("--resolve-orphans", metavar="RESULTS_DIR")
    ap.add_argument("--run-dir", help="host-side run directory, for "
                                      "--resolve-orphans")
    ap.add_argument("--no-deploy", action="store_true")
    ap.add_argument("--stock-resources", action="store_true")
    ap.add_argument("--timeout-s", type=float, default=12 * 3600)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    if a.preflight:
        return ins.preflight(a.host, a.agent_home, a.container)
    if a.resolve_orphans:
        if not a.run_dir:
            raise SystemExit("--resolve-orphans needs --run-dir")
        return ins.resolve_orphans(Path(a.resolve_orphans).resolve(),
                                   a.host, a.run_dir)
    if a.summarize:
        res = Path(a.summarize).resolve()
        return summarize(res, res.parent)

    plan_only = not a.run or a.offline

    assemblies = pools_from_assemblies()
    expected = set(assemblies)
    print(f"=== {len(expected)} pools x {len(ASSEMBLERS)} assemblers under "
          f"{ASSEMBLIES.name} ===", flush=True)

    if a.offline:
        available = {p: f"{a.reads_dir}/{p}{READS_SUFFIX}" for p in sorted(expected)}
        print("    --offline: read paths fabricated, host not contacted")
    else:
        print(f"=== listing {a.host}:{a.reads_dir} ===", flush=True)
        available = ins.discover_pools(a.host, a.reads_dir)
        only_asm = expected - set(available)
        only_host = set(available) - expected
        if only_asm or only_host:
            print(f"\nPOOL SET MISMATCH.\n"
                  f"  assembled but no reads on {a.host}: {sorted(only_asm)}\n"
                  f"  reads on {a.host} but not assembled: {sorted(only_host)}",
                  file=sys.stderr)
            return 3

    if a.pools:
        missing = [p for p in a.pools if p not in available]
        if missing:
            raise SystemExit(f"requested pool(s) not present: {missing}\n"
                             f"available: {sorted(available)}")
        pools = {p: available[p] for p in a.pools}
    else:
        pools = available
    print(f"    {len(pools)} of {len(available)} pools -> "
          f"{len(pools) * len(ASSEMBLERS)} stats jobs", flush=True)

    ts = int(time.time())
    staging = REPO / "data" / "fabfos" / "scratch" / "resolve" / f"asm_stats_{ts}"
    staging.mkdir(parents=True, exist_ok=True)

    inputs = build_inputs(staging, pools, assemblies)

    resources = [DataInstanceLibrary.Load(LIB / f"resources/{n}")
                 for n in ("env", "lib")]
    transforms = [TransformInstanceLibrary.Load(LIB / "transforms/assembly")]

    home = SshSource(host=a.host, path=a.agent_home).AsSource()
    agent = Agent(home=home, runtime=Runtime.APPTAINER,
                  container=a.container, setup_commands=SETUP_COMMANDS)

    print("=== planning ===", flush=True)
    targets = TargetBuilder()
    targets.Add("sequences::assembly_stats")

    task = agent.GenerateWorkflow(
        samples=[inputs],
        resources=resources,
        transforms=transforms,
        targets=targets,
    )
    if not task.ok:
        print("\nPLAN DID NOT RESOLVE. Planner hints:", file=sys.stderr)
        print(getattr(task.plan, "hints", task), file=sys.stderr)
        return 3

    problems = check_plan(task, len(pools))
    print(f"resolved workflow: {len(task.plan.steps)} steps", flush=True)
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        stem = Path(step.transform._path).stem
        print(f"  [{step.order}] {stem}: {len(step.group_by_instances)} task(s)")

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    ins.render_dag(ARTIFACTS / "original_assembly_stats_dag", task)

    if problems:
        print("\nPLAN REJECTED:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 3
    print("plan checks: OK", flush=True)

    if plan_only:
        print("\nplan-only: nothing deployed, nothing run. Pass --run to execute.")
        return 0

    if not a.no_deploy:
        print("=== Deploy() ===", flush=True)
        try:
            agent.Deploy()
        except subprocess.CalledProcessError as e:
            print(f"\ndeploy failed ({e}). Connect once by hand and re-run; "
                  f"never retry in a loop.", file=sys.stderr)
            return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    agent.StageWorkflow(task, on_exist="update")

    nxf_config = agent.GetNxfConfigPresets()["slurm"]
    params = {"slurmAccount": a.slurm_account}
    overrides = None if a.stock_resources else TRIMMED_RESOURCES
    print(f"=== executor: slurm, account {a.slurm_account}, "
          f"resources: {'stock' if a.stock_resources else 'trimmed'} ===", flush=True)
    agent.RunWorkflow(task, config_file=nxf_config, params=params,
                      resource_overrides=overrides)

    print(f"=== waiting (timeout {a.timeout_s / 3600:.1f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_s, poll_s=a.poll_s)
    print(f"=== status: {result['status']} after {result['elapsed_s'] / 3600:.2f} h ===",
          flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    n_failed = ins.check_tasks(a.host, a.agent_home, task.GetKey())
    if n_failed:
        print(f"\n{n_failed} TASK(S) FAILED AND NEXTFLOW IGNORED IT. The results "
              f"below are incomplete.", file=sys.stderr)

    src = agent.GetResultSource(task)
    out = Path(a.out).resolve() if a.out else (staging / "results")
    out.mkdir(parents=True, exist_ok=True)
    print(f"=== retrieving (all products except BAM): {src.GetPath()} -> {out} ===",
          flush=True)
    subprocess.run([
        "rsync", "-a", "--info=stats1",
        "--include=*/",
        "--include=_manifests/***",
        "--include=*assembly_stats*/***",
        "--include=*per_contig_coverage*/***",
        "--include=*per_bp_coverage*/***",
        "--include=*read_qc_stats*/***",
        "--exclude=*",
        f"{a.host}:{src.GetPath()}/", f"{out}/",
    ], check=True)

    print(f"\nretrieved {len(list(out.rglob('*.json')))} json + "
          f"{len(list(out.rglob('*.tsv')))} tsv + "
          f"{len(list(out.rglob('*.gz')))} gz under {out}")
    print(f"BAMs remain at {a.host}:{src.GetPath()}")

    ins.resolve_orphans(out, a.host, str(Path(src.GetPath()).parent))
    summarize(out, out.parent)
    return 2 if n_failed else 0


if __name__ == "__main__":
    sys.exit(main())
