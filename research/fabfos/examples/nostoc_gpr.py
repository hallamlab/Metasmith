#!/usr/bin/env python3
"""One canonical four-lane GPR table per member of the three-organism community.

    python research/fabfos/examples/nostoc_gpr.py                 # plan + DAG, nothing staged
    python research/fabfos/examples/nostoc_gpr.py --preflight     # what fir must already hold
    python research/fabfos/examples/nostoc_gpr.py --stub          # run the whole graph as no-ops
    python research/fabfos/examples/nostoc_gpr.py --run           # execute on fir
    python research/fabfos/examples/nostoc_gpr.py --retrieve      # pull a finished run down
    python research/fabfos/examples/nostoc_gpr.py --publish       # land them under data/fabfos/nostoc/

    {NOS,ERY,RHI}.faa -> {kofamscan, CLEAN, diamond_uniref50, proteinbert}
                      -> gpr_4lane -> annotation::gpr_table  (x3)

THREE ORGANISMS, ONE RUN, FIVE STEPS
------------------------------------
Not fifteen. Every lane and the mapper are `group_by=orfs` with `parents={orfs}`
pins, and the planner deduplicates sample groups by their endpoint set -- three
ORF roots beside the same five references are ONE case, so the fan-out happens
inside each step (15 nextflow tasks, 5 plan steps). A gate written as "expect 15
steps" fails on a correct plan.

One run rather than three is not a convenience. The `pbert` lane picks its
neighbours with `np.argpartition`, whose order among ties is unspecified, so its
vote is not bit-reproducible between runs; three separate runs would put that
noise inside every cross-organism comparison, looking like biology.

THE ORFs ARE STAGED, NEVER RE-DERIVED
-------------------------------------
`data/fabfos/nostoc/orfs/` holds prodigal proteomes produced outside this repo. No
ORF-producing transform belongs in this plan, and `check_plan` refuses one --
a `prodigal` step here would mean the lanes annotated something other than the
pinned bytes the tables claim to describe.

REFERENCES ARE READ ON FIR, AT ABSOLUTE PATHS
---------------------------------------------
~24.5 GB, almost all of it the DIAMOND database, and the only consumer is a job
on the host that holds them. `check_refs` proves each one is there and non-empty
in one ssh round trip and REFUSES if not; a stand-in would hand a lane an empty
database, and most lanes then produce an empty output and *succeed* -- the exact
failure `validate_gpr` catches one whole run too late.

Do not point this at another reference copy that happens to exist on fir from an
unrelated job. Different releases, different bytes, and the recorded DVC pin
would then be a false description of what produced the table.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

_ENGINE = REPO / "src"
if (_ENGINE / "metasmith").is_dir() and str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from metasmith.python_api import Duration, Resources, Runtime, Size   # noqa: E402

from fabfos.pipelines import annotation                              # noqa: E402
from _fir import (                                                   # noqa: E402
    FIR_ACCOUNT, FIR_AGENT_HOME, FIR_CONTAINER, FIR_GPU, FIR_GPU_ACCOUNT,
    FIR_HOST, FIR_PROCESSED,
    check_schedulable, check_staged_executor, check_tasks, check_walltimes,
    envs_from_plan, fir_agent, landed_from_index, pin_external_leaf_ids, preflight,
    provision_dev_overlay_remote, publish_gpr_by_source, retrieve, ssh_once,
)

MLIB = REPO / "src" / "metasmith_libraries"
DATA = REPO / "data"
PROCESSED = DATA / "fabfos" / "processed"
NOSTOC = DATA / "fabfos" / "nostoc"
ORFS_DIR = NOSTOC / "orfs"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"

ORGANISMS = ["NOS", "ERY", "RHI"]
TARGET = "annotation::gpr_table"

def expected_transforms() -> set[str]:
    sys.path.insert(0, str(REPO / "tests" / "fabfos"))
    from test_annotation_driver import EXPECTED_TRANSFORMS  # noqa: E402
    return set(EXPECTED_TRANSFORMS)


RESOURCE_OVERRIDES = {
    "kofamscan": Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=3)),
    "diamond_uniref50": Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=3)),
    "proteinbert": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=2)),
    "clean": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=2)),
    "gpr_4lane": Resources(cpus=4, memory=Size.GB(48), duration=Duration(hours=1)),
}


def orf_paths() -> list[Path]:
    paths = [ORFS_DIR / f"{k}.faa" for k in ORGANISMS]
    missing = [p for p in paths if not p.exists()]
    if missing:
        raise SystemExit(
            f"the pinned proteomes are not present: "
            f"{[str(p.relative_to(REPO)) for p in missing]}\n"
            f"  Materialise the pin: `dvc checkout data/fabfos/nostoc/orfs.dvc`")
    return paths


def check_refs(host: str, remote_processed: str) -> None:
    probes = [f'[ -e "{remote_processed}/{rel}" ] || echo "MISSING {d} {rel}"'
              for d, rel in annotation.REF_LAYOUT.items()]
    prof = f'{remote_processed}/{annotation.REF_LAYOUT["ref::kofamscan_profiles"]}'
    pool = f'{remote_processed}/{annotation.REF_LAYOUT["ref::reference_label_pool"]}'
    probes += [
        f'[ "$(ls -1 "{prof}" 2>/dev/null | head -1)" ] || '
        f'echo "EMPTY ref::kofamscan_profiles -- the directory holds no profiles"',
        f'for f in orf_index.parquet emb_pbert.npy; do '
        f'[ -s "{pool}/$f" ] || echo "MISSING ref::reference_label_pool {pool}/$f"; done',
    ]
    out = ssh_once(host, "; ".join(probes)).strip()
    if out:
        raise SystemExit(
            f"references unusable at {host}:{remote_processed}:\n"
            + "\n".join(f"    {ln}" for ln in out.splitlines())
            + "\n  Every chunk is pinned and materialised here, so the recovery is an "
              "rsync of the missing ones, not a rebuild. Ship the KOfam profiles as "
              "their packed archive rather than as 27,756 files.")
    print(f"    all {len(annotation.REF_LAYOUT)} references present and non-empty")


def plan(work: Path, agent, remote_processed: str):
    paths = orf_paths()
    for p in paths:
        n = sum(1 for line in p.open() if line.startswith(">"))
        print(f"    sequences::orfs              {p.relative_to(REPO)}  ({n} records)")
    for dtype, rel in annotation.REF_LAYOUT.items():
        print(f"    {dtype:32s} {remote_processed}/{rel}")
    _agent, task, stubs = annotation.generate_workflow(
        work, orfs=paths,
        kofam_profiles=None, kofam_ko_list=None, uniref50_db=None,
        mnxr_lookup=None, label_pool=None,
        runtime=Runtime.APPTAINER,
        refs_root=remote_processed, verify_refs=False, agent=agent,
        on_inputs=pin_external_leaf_ids,
    )
    assert not stubs, f"unreachable: verify_refs=False never stubs; got {stubs}"
    return task


def check_plan(task, n_orfs: int) -> int:
    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"\nPlan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<24} "
              f"x{len(step.group_by_instances)}  -> {prods}")

    bad = 0
    expected = expected_transforms()
    if used != expected:
        print(f"\ntransform set is {sorted(used)}, expected {sorted(expected)}",
              file=sys.stderr)
        bad = 1
    off = [s for s in task.plan.steps if len(s.group_by_instances) != n_orfs]
    if off:
        for s in off:
            print(f"\n{Path(s.transform._path).stem} groups "
                  f"{len(s.group_by_instances)} instance(s), expected {n_orfs} -- the "
                  f"organisms are not fanning out inside the step.", file=sys.stderr)
        bad = 1
    for producer in ("prodigal", "host_proteomes"):
        if producer in used:
            print(f"\n{producer} is in the plan -- the lanes would annotate ORFs this "
                  f"driver did not stage, and the tables would describe bytes that are "
                  f"not the pinned ones.", file=sys.stderr)
            bad = 1
    for dl in used:
        if dl.startswith("download"):
            print(f"\nlogistics/{dl} is in the plan -- the planner satisfied a "
                  f"'missing' reference by producing it. That plans cleanly and dies "
                  f"on every task.", file=sys.stderr)
            bad = 1
    if not bad:
        print(f"\n{len(task.plan.steps)} steps x {n_orfs} organisms, every expected "
              f"transform, no ORF producer, no downloader")
    return bad


def write_provenance(dest_root: Path, run_key: str) -> None:
    import pandas as pd

    per_org = []
    channel_rows = []
    for org in ORGANISMS:
        table = dest_root / org / "gpr_4lane.parquet"
        faa = dest_root / org / f"{org}.faa"
        if not table.exists():
            continue
        g = pd.read_parquet(table)
        n_orfs = sum(1 for line in faa.open() if line.startswith(">")) if faa.exists() else 0
        per_org.append(f"| `{org}` | {n_orfs:,} | {len(g):,} | "
                       f"{g['orf'].nunique():,} ({100.0 * g['orf'].nunique() / max(n_orfs, 1):.1f}%) | "
                       f"{g['mnxr'].nunique():,} |")
        for ch in sorted(g["channel"].unique()):
            d = g[g["channel"] == ch]
            v = d["raw_score"].astype(float)
            channel_rows.append(
                f"| `{org}` | `{ch}` | {len(d):,} | "
                f"{d['orf'].nunique():,} ({100.0 * d['orf'].nunique() / max(n_orfs, 1):.1f}%) | "
                f"{d['mnxr'].nunique():,} | `{d['score_kind'].iat[0]}` | "
                f"{v.min():.4g} / {v.median():.4g} / {v.max():.4g} |")

    pins = []
    for chunk in sorted({v.split("/")[0] for v in annotation.REF_LAYOUT.values()}):
        dvc = PROCESSED / f"{chunk}.dvc"
        pins.append(f"| `data/fabfos/processed/{chunk}` | " +
                    (dvc.read_text().split("md5:")[1].split()[0] if dvc.exists()
                     else "**unpinned**") + " |")
    orfs_pin = NOSTOC / "orfs.dvc"
    pins.append(f"| `data/fabfos/nostoc/orfs` | " +
                (orfs_pin.read_text().split("md5:")[1].split()[0] if orfs_pin.exists()
                 else "**unpinned**") + " |")

    out = dest_root / "PROVENANCE.md"
    out.unlink(missing_ok=True)
    out.write_text(f"""# `data/fabfos/nostoc/annotation/` — the canonical four-lane GPR tables

Produced by `research/fabfos/examples/nostoc_gpr.py --run` on fir, task `{run_key}`, from
`data/fabfos/nostoc/orfs/` through `kofamscan`, `CLEAN`, `diamond_uniref50` and
`proteinbert` into `gpr_4lane`.

One table per community member. Schema, channel vocabulary and the `raw_score`
direction/range contract are declared in `lib::fabfos_evidence.py` and enforced by
`validate_gpr` before each table is written; the `source` column is what attributes a
table to an organism, and it is single-valued by that same check.

| organism | ORFs | rows | ORFs with a call (coverage) | distinct MNXR |
|---|---|---|---|---|
{chr(10).join(per_org)}

| organism | channel | rows | ORFs (coverage) | MNXR | score_kind | min / med / max |
|---|---|---|---|---|---|---|
{chr(10).join(channel_rows)}

## All three come from ONE run, and that is load-bearing

The `pbert` lane picks its neighbours with `np.argpartition`, whose order among ties is
unspecified, over embeddings whose last bits move with how the embedder's reductions
were scheduled. Measured on identical inputs, references and code: `clean`, `kofam` and
`uniref50` come back bit-identical and `pbert` does not — 1,118 of 68,824 shared keys
carried a different `raw_score` between two runs, and the donor recorded in
`intermediate_id` can change outright.

Three separate runs would therefore put that noise inside every cross-organism
comparison, where it would read as biology. These three tables share one task key, one
set of references and one engine.

## Which bytes produced it

| chunk | DVC md5 |
|---|---|
{chr(10).join(pins)}

## Checked against the 2026-07-09 ad-hoc annotation of these same proteins

`research/fabfos/examples/nostoc_gpr_crosscheck.py`. A different method — two deep-learning annotators
wired together by hand, KO through a DIAMOND-to-KEGG lane, no `clean` or `kofam` — so
agreement is evidence, not a gate.

| | canonical MNXR | ad-hoc MNXR | shared | ORFs with a call |
|---|---|---|---|---|
| `NOS` | 13,127 | 14,114 | 9,224 (70% / 65%) | 5,921 vs 5,047 |
| `ERY` | 12,221 | 12,096 | 8,184 (67% / 68%) | 3,184 vs 2,751 |
| `RHI` | 13,857 | 13,166 | 9,383 (68% / 71%) | 4,396 vs 3,806 |

Same order of magnitude, ~2/3 of reactions shared, and the canonical run reaches
several hundred more ORFs per organism.

The ORF id sets are NOT identical, and the difference is the ad-hoc method's own
workaround showing through: 15-117 of its ids per organism are named
`..._SEPARATED_SEQUENCE_(101_1101)`, overlapping windows it cut long proteins into to
fit an embedding model's positional limit. These three proteomes hold 233 proteins over
that limit (142 / 39 / 52). This run did not need to split them — every one of its 15
tasks completed and 99.7-99.9% of each proteome carries a call — so the canonical table
keys on whole proteins throughout.

## THIS IS NOT THE DEPLOYED METHOD; numbers here are not comparable to archived ones

Two substitutions, both deliberate and both stated where they are made:

- **The label pool is Swiss-Prot against the Rhea-derived bridge**, not the
  KEGG-gene-keyed pool the deployed method used (54,005 sequences keyed on KEGG gene
  ids, labelled by KO). One label source instead of two, and no KEGG-licensed sequences
  in the tree — but a *different set*, so the `pbert` lane's numbers move.
- **The MNXR bridge routes KO through KEGG reactions, not through EC.** Measured on the
  three reference hosts, routing through the ko_list's `[EC:...]` tag takes the kofam
  lane from 646 to 8,548 reactions and makes 91% of them reactions the EC lane already
  reaches — the two lanes stop being independent evidence, which is the entire reason
  for having four of them.

## The CLEAN score

`clean_score` is CLEAN's **maxsep distance** — lower is better, unbounded above. It is
stored through `1/(1+d)`: monotone decreasing, positive, bounded, order-preserving
within the lane, and lossless (`d = 1/s - 1`). The `clean` rows above are that
transformed value.

## Why `kofam` and `uniref50` coverage looks low, and why that is not a truncated reference

Both lanes cover ~12-19% of each proteome here, which is where the fosmid-era table
landed too (kofam 15.9%, uniref50 12.5% over 5,892 insert ORFs). Complete genomes
covering no more than fosmid inserts reads like a truncated reference, and it was
checked rather than assumed. It is not:

| lane | ORFs with a raw hit | of those, reaching a reaction |
|---|---|---|
| `kofam` | 2,754 / 2,632 / 1,877 (NOS/RHI/ERY) — 46-60% of each proteome | 29-33% |
| `uniref50` | 5,779 / 4,345 / 3,148 — 73-98% of each proteome | 12-16% |

The annotators reach most of each proteome. What does not carry through is the
**reaction bridge**: most KOs and most UniRef50 hits are transporters, regulators,
ribosomal and structural proteins with no MetaNetX reaction to map to. So the coverage
in the tables above is a statement about how much of a bacterial proteome is enzymatic
*and* mapped — which is roughly constant between a fosmid library and a whole genome,
and is why the two numbers agree.

Read the coverage column as "fraction of the proteome this lane contributes reaction
evidence for", never as "fraction the lane annotated".
""")
    print(f"  PROVENANCE.md  ->  {out.relative_to(REPO)}")


def publish(results: Path, *, dry_run: bool, run_key: str) -> int:
    import shutil
    dest = NOSTOC / "annotation"
    rc = publish_gpr_by_source(results, dest, expect=set(ORGANISMS),
                               dry_run=dry_run, repo=REPO)
    if rc or dry_run:
        return rc
    for p in orf_paths():
        d = dest / p.stem / p.name
        d.parent.mkdir(parents=True, exist_ok=True)
        if d.exists() or d.is_symlink():
            d.unlink()
        shutil.copyfile(p, d)
        print(f"  {p.stem}: {p.name}  ->  {d.relative_to(REPO)}")
    write_provenance(dest, run_key)
    print("\nPin the chunk:  dvc add data/fabfos/nostoc/annotation")
    return 0


def verify(results: Path) -> int:
    landed = landed_from_index(results, [TARGET]).get(TARGET, [])
    if len(landed) != len(ORGANISMS):
        print(f"\nTHE RUN IS GREEN BUT {len(landed)} of {len(ORGANISMS)} "
              f"{TARGET} products are present. Nextflow ignores a process that "
              f"exhausted its retries; read _metasmith/logs.*/nxf_tasks.csv for the "
              f"step that died.", file=sys.stderr)
        return 2
    print(f"{len(landed)} x {TARGET} present. Now: --publish "
          f"(then `dvc add data/fabfos/nostoc/annotation`).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", action="store_true",
                    help="execute on the host; without it this plans and stops")
    ap.add_argument("--stub", action="store_true",
                    help="stage and launch the graph as no-ops -- proves the container "
                         "/ overlay pairing, the relay, the slurm preset and the "
                         "rendered workflow before any queue time is spent. It does "
                         "NOT prove a task can run: at this engine pin the stub bodies "
                         "themselves die in groovy (`--testSpread` arrives from the "
                         "nextflow CLI as a String and `nextFloat() * String` has no "
                         "signature), so nothing is ever submitted")
    ap.add_argument("--stub-delay", type=float, default=1.0)
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--wait", action="store_true",
                    help="re-attach to a run already executing on the host, then "
                         "check, retrieve and verify it. The run is detached -- "
                         "`--run` only WATCHES it -- so losing the local process "
                         "loses nothing, and this is how you pick it back up")
    ap.add_argument("--retrieve", action="store_true",
                    help="pull an already-finished run down and verify it")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--publish-dry-run", action="store_true")
    ap.add_argument("--host", default=FIR_HOST)
    ap.add_argument("--agent-home", default=FIR_AGENT_HOME)
    ap.add_argument("--container", default=FIR_CONTAINER)
    ap.add_argument("--remote-processed", default=FIR_PROCESSED,
                    help="the host-side mirror of data/fabfos/processed/ the references are "
                         "read from")
    ap.add_argument("--slurm-account", default=FIR_ACCOUNT)
    ap.add_argument("--slurm-gpu-account", default=FIR_GPU_ACCOUNT,
                    help="CLEAN declares a GPU. rrg-shallam-ab has no GPU allocation "
                         "on fir; def-shallam does.")
    ap.add_argument("--timeout-hours", type=float, default=24.0)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--work", default=None)
    a = ap.parse_args()

    work = Path(a.work).resolve() if a.work else (DATA / "scratch" / "nostoc_gpr")
    work.mkdir(parents=True, exist_ok=True)
    local_results = work / "results"

    if a.publish or a.publish_dry_run:
        return publish(local_results, dry_run=a.publish_dry_run,
                       run_key=(work / "RUN_KEY").read_text().strip()
                       if (work / "RUN_KEY").exists() else "?")

    agent = fir_agent(host=a.host, agent_home=a.agent_home, container=a.container)

    print(f"=== checking the references on {a.host} ===")
    check_refs(a.host, a.remote_processed)

    print("=== staging inputs + planning ===")
    task = plan(work, agent, a.remote_processed)
    if not task.ok:
        print(f"\nPLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 3
    if check_plan(task, len(ORGANISMS)):
        return 3

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    svg = ARTIFACTS / "nostoc_gpr_4lane.svg"
    task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env"})
    print(f"\nDAG -> {svg}")
    print(f"\n=== task key: {task.GetKey()} ===", flush=True)

    if a.preflight:
        return preflight(a.host, a.agent_home, a.container, envs_from_plan(task),
                         mlib=MLIB)

    (work / "RUN_KEY").write_text(task.GetKey())

    if a.retrieve:
        retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
        check_tasks(a.host, a.agent_home, task.GetKey())
        return verify(local_results)

    if a.wait:
        return _watch(agent, task, a, local_results)

    if not (a.run or a.stub):
        print("\nplan only: nothing staged, nothing run. Add --run (or --stub).")
        return 0

    print("=== Deploy() ===", flush=True)
    agent.Deploy()
    provision_dev_overlay_remote(a.host, a.agent_home)

    if preflight(a.host, a.agent_home, a.container, envs_from_plan(task), mlib=MLIB):
        print("\nrefusing to run: a compute node has no outbound network, so an image "
              "absent from the store cannot be pulled once a task starts.",
              file=sys.stderr)
        return 4

    agent.StageWorkflow(task, on_exist="update")
    if check_staged_executor(a.host, a.agent_home, task.GetKey()):
        return 4
    if check_walltimes(a.host, RESOURCE_OVERRIDES):
        return 4
    if check_schedulable(a.host, a.slurm_account, RESOURCE_OVERRIDES,
                         workdir=f"{a.agent_home}/runs/{task.GetKey()}"):
        return 4

    print(f"=== executor: slurm, account {a.slurm_account} "
          f"(gpu: {a.slurm_gpu_account}){' [STUB]' if a.stub else ''} ===", flush=True)
    agent.RunWorkflow(
        task,
        config_file=agent.GetNxfConfigPresets()["slurm"],
        params={"slurmAccount": a.slurm_account,
                "slurmGpuAccount": a.slurm_gpu_account},
        resource_overrides=RESOURCE_OVERRIDES,
        gpus=FIR_GPU,
        stub_delay=a.stub_delay if a.stub else 0,
    )

    return _watch(agent, task, a, local_results)


def _watch(agent, task, a, local_results: Path) -> int:
    print(f"=== waiting (timeout {a.timeout_hours:.1f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_hours * 3600,
                                   poll_s=a.poll_s)
    print(f"=== status: {result['status']} after "
          f"{result['elapsed_s'] / 3600:.2f} h ===", flush=True)
    for line in result["tail"]:
        print(f"    {line}")

    n_failed = check_tasks(a.host, a.agent_home, task.GetKey())
    if n_failed:
        print(f"\nrefusing to retrieve: nextflow recorded {n_failed} FAILED task(s). "
              f"The retrieve is the expensive half and a dead lane should not reach it.",
              file=sys.stderr)
        return 2
    if result["status"] != "completed":
        print(f"\nthe wait reported [{result['status']}] but the task table records no "
              f"failure -- trusting the table. Verify below is the real gate.")
    if a.stub:
        print("\nstub run finished: staging, the container / overlay pairing, the "
              "relay and the rendered workflow all hold -- nextflow parsed and started "
              "it. Read the tail above before believing more than that: the stub BODIES "
              "die in groovy at this engine pin, so no task was submitted.")
        return 0
    retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
    return verify(local_results)


if __name__ == "__main__":
    sys.exit(main())
