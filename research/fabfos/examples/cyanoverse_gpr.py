#!/usr/bin/env python3
"""The Cyanoverse 4-lane GPR campaign: one shard batch per invocation, on fir.

    python examples/cyanoverse_gpr.py --shards 0:2                  # plan only
    python examples/cyanoverse_gpr.py --shards 0:2 --preflight
    python examples/cyanoverse_gpr.py --shards 0:50 --run
    python examples/cyanoverse_gpr.py --shards 0:50 --wait          # re-attach
    python examples/cyanoverse_gpr.py --shards 0:50 --retrieve

    shard_XXXX.faa ─┬─ (kofamscan)     GIVEN, from the legacy pass
                    ├─ clean ──────────┐
                    ├─ diamond_uniref50┤
                    └─ proteinbert ────┴─→ gpr_4lane ─→ annotation::gpr_table

ONE OF THE FOUR LANES IS ALREADY COMPUTED, AND THAT IS THE POINT
----------------------------------------------------------------
An earlier pass ran kofamscan over all 2,844 assemblies. Those products are
repacked into the shards (`examples/fir/repack_lanes.py`) and handed to the
planner as GIVENS, each parented to its own shard's ORFs. The solver prefers a
given over producing one (`solver.py:534-554`), so the producer drops out of the
plan and the campaign runs four steps instead of five.

That pass also produced 87 GB of ProteinBERT embeddings, and they are NOT
reused: see REUSED below. Reuse is only ever as good as the evidence for it.

The mechanism has one silent failure and this driver exists mostly to refuse it.
If a given fails its lineage check, `solver.py:603-605` re-solves with
`include_produced=True` and quietly puts the producer BACK, with no error and no
warning -- you discover it as a surprise re-run of a lane you already have.
`check_plan` therefore treats the presence of `kofamscan` as fatal.

The second silent failure is in the codegen: parent lineage rides the channel
only when EVERY row of an input has a parent (`nextflow_codegen.py:820-838`).
One missing lane file strips lineage from ALL of them, and the runtime falls
back to matching by POSITION (`workflow/grouping.py:82-92`) -- so every shard
gets a plausible, non-empty, wrong lane product. `check_lineage` reads the
staged join table and proves each product is bound to its own shard before any
queue time is spent.

NOTHING IS COPIED TO THIS MACHINE
---------------------------------
The corpus, the shards, the repacked lanes and the references all live on fir;
30 GB of shards alone. They are added to the library as verbatim remote paths
and bound into the container by the runtime. That makes their leaf identities
random by default (`identity.py:79-113` falls back to uuid4 for a file it
cannot read), which would change the task key on every invocation --
`--run` and a later `--retrieve` would name different runs. `pin_external_leaf_ids`
derives the id from the path instead, which is what makes a batch re-attachable
and a resubmission cache-hitting.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[3]

_ENGINE = REPO / "src"
if (_ENGINE / "metasmith").is_dir() and str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from metasmith.python_api import Duration, Resources, Size            # noqa: E402
from metasmith.python_api import Runtime                              # noqa: E402

from fabfos.pipelines import annotation                               # noqa: E402
from _fir import (                                                    # noqa: E402
    FIR_ACCOUNT, FIR_CONTAINER, FIR_GPU, FIR_GPU_ACCOUNT, FIR_HOST,
    check_schedulable, check_staged_executor, check_tasks, check_walltimes,
    envs_from_plan, fir_agent, landed_from_index, pin_external_leaf_ids,
    preflight, provision_dev_overlay_remote, retrieve, ssh_once,
)

MLIB = REPO / "src" / "metasmith_libraries"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"

GPR = "/project/rpp-shallam/phyberos/cyanoverse/gpr"
SHARDS_DIR = f"{GPR}/shards"
LANES_DIR = f"{GPR}/lanes"
REFS_ROOT = f"{GPR}/refs"
AGENT_HOME = f"{GPR}/agent_home"

TARGET = "annotation::gpr_table"

REUSED = {
    "annotation::kofamscan_results": "{shard}.kofam.csv",
}

EXPECTED_TRANSFORMS = {
    "chunkOrfsForAnnotation",
    "clean", "diamond_uniref50", "proteinbert",
    "merge_diamond_uniref50", "merge_proteinbert",
    "gpr_4lane",
}
FORBIDDEN_TRANSFORMS = {"kofamscan", "merge_kofamscan"}

RESOURCE_OVERRIDES = {
    "diamond_uniref50": Resources(cpus=16, memory=Size.GB(96), duration=Duration(hours=3)),
    "proteinbert": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=3)),
    "clean": Resources(cpus=4, memory=Size.GB(48), duration=Duration(hours=3)),
    "gpr_4lane": Resources(cpus=8, memory=Size.GB(24), duration=Duration(hours=2)),
    "kofamscan": Resources(cpus=32, memory=Size.GB(64), duration=Duration(hours=3)),
}


def run_params(a) -> dict:
    return {"slurmAccount": a.slurm_account,
            "slurmGpuAccount": a.slurm_gpu_account}


def exclude_flags(a) -> list[str]:
    nodes = (a.exclude_nodes or "").strip()
    if not nodes:
        return []
    print(f"    keeping GPU steps off {nodes}")
    return [f"--exclude={nodes}"]


def shard_names(spec: str) -> list[str]:
    lo, hi = (int(x) for x in spec.split(":"))
    if not 0 <= lo < hi:
        raise SystemExit(f"--shards wants 'A:B' with 0 <= A < B; got {spec!r}")
    return [f"shard_{i:04d}" for i in range(lo, hi)]


def check_inputs_present(host: str, shards: list[str],
                         require_reused: bool = True) -> None:
    probes = []
    for s in shards:
        probes.append(f'[ -s "{SHARDS_DIR}/{s}.faa" ] || echo "MISSING {s}.faa"')
        for pat in (REUSED.values() if require_reused else ()):
            f = pat.format(shard=s)
            probes.append(f'[ -s "{LANES_DIR}/{f}" ] || echo "MISSING {f}"')
    for dtype, rel in annotation.REF_LAYOUT.items():
        probes.append(f'[ -e "{REFS_ROOT}/{rel}" ] || echo "MISSING {dtype} {rel}"')
    prof = f'{REFS_ROOT}/{annotation.REF_LAYOUT["ref::kofamscan_profiles"]}'
    lm = f'{REFS_ROOT}/{annotation.REF_LAYOUT["ref::label_transfer_landmarks"]}'
    probes += [
        f'[ "$(ls -1 "{prof}" 2>/dev/null | head -1)" ] || '
        f'echo "EMPTY ref::kofamscan_profiles"',
        f'[ -s "{lm}/landmarks.parquet" ] || '
        f'echo "MISSING ref::label_transfer_landmarks landmarks.parquet"',
    ]
    out = ssh_once(host, "; ".join(probes)).strip()
    if out:
        lines = out.splitlines()
        raise SystemExit(
            f"{len(lines)} input(s) unusable on {host}:\n"
            + "\n".join(f"    {ln}" for ln in lines[:20])
            + ("\n    ..." if len(lines) > 20 else "")
            + "\n  An absent lane product does not merely skip that shard: it costs "
              "EVERY shard in the batch its lineage, and the run then pairs each "
              "one with a plausible, non-empty, wrong file.")
    print(f"    {len(shards)} shard(s) x {1 + len(REUSED)} file(s) + "
          f"{len(annotation.REF_LAYOUT)} references: present and non-empty")


def make_on_inputs(shards: list[str]):
    def _on_inputs(inputs):
        orfs_by_shard = {
            Path(p).stem: p for p, d in inputs.manifest.items()
            if d == "sequences::orfs"
        }
        if set(orfs_by_shard) != set(shards):
            raise SystemExit(
                f"the library holds ORF stems {sorted(orfs_by_shard)[:5]}... "
                f"but the batch is {shards[:5]}...")

        seen: dict[str, str] = {p.name: "sequences::orfs" for p in
                                (Path(x) for x in orfs_by_shard.values())}
        for shard in shards:
            for dtype, pat in REUSED.items():
                name = pat.format(shard=shard)
                if name in seen:
                    raise SystemExit(
                        f"basename collision: {name} is supplied as both "
                        f"{seen[name]} and {dtype}. Nextflow stages inputs by "
                        f"basename, so one would overwrite the other and the "
                        f"lane would read the wrong file -- which has already "
                        f"cost this project a run after every lane succeeded.")
                seen[name] = dtype
                inputs.AddItem(f"{LANES_DIR}/{name}", dtype,
                               parents=[orfs_by_shard[shard]])
        print(f"    supplied {len(shards) * len(REUSED)} already-computed lane "
              f"product(s) across {len(REUSED)} types")
        pin_external_leaf_ids(inputs)
    return _on_inputs


def plan(work: Path, agent, shards: list[str], control: bool = False):
    orfs = [f"{SHARDS_DIR}/{s}.faa" for s in shards]
    _agent, task, stubs = annotation.generate_workflow(
        work, orfs=orfs,
        kofam_profiles=None, kofam_ko_list=None, uniref50_db=None,
        mnxr_lookup=None, landmarks=None,
        runtime=Runtime.APPTAINER,
        refs_root=REFS_ROOT, verify_refs=False, stage_orfs="remote",
        agent=agent,
        on_inputs=(pin_external_leaf_ids if control else make_on_inputs(shards)),
    )
    assert not stubs, f"unreachable: verify_refs=False never stubs; got {stubs}"
    return task


def check_control_plan(task, n: int) -> int:
    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"\nControl plan: {len(task.plan.steps)} steps, {sorted(used)}\n")
    if not FORBIDDEN_TRANSFORMS <= used:
        print(f"\nTHE CONTROL IS NOT A CONTROL: {sorted(FORBIDDEN_TRANSFORMS)} "
              f"is absent from a plan that supplies nothing, so this run would "
              f"compare the reused lane against itself and report agreement it "
              f"never tested.", file=sys.stderr)
        return 3
    if used != EXPECTED_TRANSFORMS | FORBIDDEN_TRANSFORMS:
        print(f"\ncontrol transform set is {sorted(used)}, expected "
              f"{sorted(EXPECTED_TRANSFORMS | FORBIDDEN_TRANSFORMS)}",
              file=sys.stderr)
        return 3
    print(f"{len(task.plan.steps)} steps x {n} shard(s); {sorted(FORBIDDEN_TRANSFORMS)} "
          f"PRESENT, as a control requires")
    return 0


def check_plan(task, n: int) -> int:
    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"\nPlan: {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<20} "
              f"x{len(step.group_by_instances)}  -> {prods}")

    bad = 0
    back = used & FORBIDDEN_TRANSFORMS
    if back:
        print(f"\nTHE REUSE DID NOT TAKE: {sorted(back)} is in the plan.\n"
              f"  A given whose lineage does not satisfy the mapper's parents pin "
              f"makes the solver re-resolve with include_produced=True and put the "
              f"producer back, with nothing raised (solver.py:603-605). Running "
              f"this would recompute, over {n} shard(s), lanes that already exist.",
              file=sys.stderr)
        bad = 1
    if used != EXPECTED_TRANSFORMS:
        print(f"\ntransform set is {sorted(used)}, expected "
              f"{sorted(EXPECTED_TRANSFORMS)}", file=sys.stderr)
        bad = 1
    off = [s for s in task.plan.steps if len(s.group_by_instances) != n]
    if off:
        for s in off:
            print(f"\n{Path(s.transform._path).stem} groups "
                  f"{len(s.group_by_instances)} instance(s), expected {n} -- the "
                  f"shards are not fanning out inside the step.", file=sys.stderr)
        bad = 1
    for dl in used:
        if dl.startswith("download"):
            print(f"\nlogistics/{dl} is in the plan -- the planner satisfied a "
                  f"'missing' reference by producing it. That plans cleanly and "
                  f"dies on every task.", file=sys.stderr)
            bad = 1
    if not bad:
        print(f"\n{len(task.plan.steps)} steps x {n} shards; "
              f"{sorted(FORBIDDEN_TRANSFORMS)} absent from the plan, as "
              f"intended -- the kofam lane is supplied, not produced "
              f"(ProteinBERT is NOT reused; its legacy row order was measured "
              f"not to be the fasta's)")
    return bad


def check_lineage(host: str, task_key: str, shards: list[str], task) -> int:
    from metasmith.models.libraries.identity import multihash_key

    def ext_id(path: str) -> str:
        return multihash_key(b"external\x00" + path.encode("utf-8")).hex()

    raw = ssh_once(host, f"cat {AGENT_HOME}/runs/{task_key}/workflow.lineage_of_given.json")
    data = json.loads(raw)
    lineage = data.get("lineage", {})

    owner = {ext_id(f"{SHARDS_DIR}/{s}.faa"): s for s in shards}
    for s in shards:
        for pat in REUSED.values():
            owner[ext_id(f"{LANES_DIR}/{pat.format(shard=s)}")] = s

    checked = unparented = 0
    for key, rows in lineage.items():
        for row in rows:
            self_ids = row.get("__self__") or []
            if not self_ids or self_ids[0] not in owner:
                continue
            mine = owner[self_ids[0]]
            parents = [p for k, v in row.items() if k != "__self__" for p in v]
            if not parents:
                unparented += 1
                continue
            for p in parents:
                if owner.get(p) != mine:
                    print(f"\nLINEAGE IS WRONG: an input under {key!r} belonging to "
                          f"{mine} names a parent belonging to "
                          f"{owner.get(p, '(unknown)')}.", file=sys.stderr)
                    return 1
            checked += 1

    n_expect = len(shards) * len(REUSED)
    if checked != n_expect:
        print(f"\nONLY {checked} of {n_expect} reused product(s) carry parent "
              f"lineage ({unparented} carry none).\n"
              f"  Lineage is all-or-nothing per input type "
              f"(nextflow_codegen.py:820-838): one row without a parent strips it "
              f"from every row of that type, and the runtime then matches lane "
              f"products to shards BY POSITION (workflow/grouping.py:82-92). Each "
              f"shard would get a plausible, non-empty, wrong file.",
              file=sys.stderr)
        return 1
    print(f"    lineage: all {checked} reused product(s) bound to their own shard")
    return 0


def verify(results: Path, shards: list[str]) -> int:
    landed = landed_from_index(results, [TARGET]).get(TARGET, [])
    if len(landed) != len(shards):
        print(f"\nTHE RUN IS GREEN BUT {len(landed)} of {len(shards)} {TARGET} "
              f"products are present. Nextflow ignores a process that exhausted "
              f"its retries; read _metasmith/logs.*/nxf_tasks.csv for the step "
              f"that died.", file=sys.stderr)
        return 2

    sources = []
    for p in landed:
        try:
            b = next(pq.ParquetFile(p).iter_batches(batch_size=1,
                                                    columns=["source"]))
            sources.append(b.column("source")[0].as_py())
        except Exception as e:
            print(f"\n{p} is not a readable {TARGET}: {e}", file=sys.stderr)
            return 2
    missing = sorted(set(shards) - set(sources))
    dupes = sorted({s for s in sources if sources.count(s) > 1})
    if missing or dupes:
        print(f"\nTHE RUN IS GREEN BUT the {len(landed)} products do not cover "
              f"the {len(shards)} shards asked for: missing={missing[:5]} "
              f"duplicated={dupes[:5]}. The count matched, so only the shard "
              f"identities catch this.", file=sys.stderr)
        return 2
    print(f"{len(landed)} x {TARGET} present, one per shard, for "
          f"{len(shards)} shard(s).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--shards", required=True, metavar="A:B",
                    help="half-open shard index range; this IS the batching knob")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--stub", action="store_true")
    ap.add_argument("--stub-delay", type=float, default=1.0)
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--control", action="store_true",
                    help="plan WITHOUT the reused kofam lane, so kofamscan is "
                         "computed from the shard's ORFs. Its output is what "
                         "the repacked legacy lane gets compared against")
    ap.add_argument("--wait", action="store_true",
                    help="re-attach to a batch already executing. The run is "
                         "detached, so losing the local process loses nothing")
    ap.add_argument("--retrieve", action="store_true")
    ap.add_argument("--host", default=FIR_HOST)
    ap.add_argument("--agent-home", default=AGENT_HOME)
    ap.add_argument("--container", default=FIR_CONTAINER)
    ap.add_argument("--slurm-account", default=FIR_ACCOUNT)
    ap.add_argument("--slurm-gpu-account", default=FIR_GPU_ACCOUNT)
    ap.add_argument("--exclude-nodes", default="fc10512",
                    help="comma-separated SLURM nodes to keep off; '' disables")
    ap.add_argument("--timeout-hours", type=float, default=48.0)
    ap.add_argument("--poll-s", type=float, default=120.0)
    ap.add_argument("--work", default=None)
    a = ap.parse_args()

    shards = shard_names(a.shards)
    suffix = "_control" if a.control else ""
    work = Path(a.work).resolve() if a.work else (
        REPO / "data" / "fabfos" / "scratch"
        / f"cyanoverse_gpr_{a.shards.replace(':', '_')}{suffix}")
    work.mkdir(parents=True, exist_ok=True)
    local_results = work / "results"

    agent = fir_agent(host=a.host, agent_home=a.agent_home, container=a.container)

    print(f"=== batch {a.shards}: {len(shards)} shard(s), "
          f"{shards[0]}..{shards[-1]} ===")
    print(f"=== checking inputs on {a.host} ===")
    check_inputs_present(a.host, shards, require_reused=not a.control)

    print("=== staging inputs + planning ===")
    task = plan(work, agent, shards, control=a.control)
    if not task.ok:
        print(f"\nPLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 3
    if (check_control_plan(task, len(shards)) if a.control
            else check_plan(task, len(shards))):
        return 3

    print(f"\n=== task key: {task.GetKey()} ===", flush=True)
    (work / "RUN_KEY").write_text(task.GetKey())

    if a.preflight:
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        svg = ARTIFACTS / "cyanoverse_gpr.svg"
        task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env"})
        print(f"DAG -> {svg}")
        return preflight(a.host, a.agent_home, a.container, envs_from_plan(task),
                         mlib=MLIB)

    if a.retrieve:
        retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
        check_tasks(a.host, a.agent_home, task.GetKey())
        return verify(local_results, shards)

    if a.wait:
        return _watch(agent, task, a, local_results, shards)

    if not (a.run or a.stub):
        print("\nplan only: nothing staged, nothing run. Add --run (or --stub).")
        return 0

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
    if not a.control and check_lineage(a.host, task.GetKey(), shards, task):
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
        params=run_params(a),
        resource_overrides=RESOURCE_OVERRIDES,
        gpus=replace(FIR_GPU, extra=[*FIR_GPU.extra, *exclude_flags(a)]),
        stub_delay=a.stub_delay if a.stub else 0,
    )
    return _watch(agent, task, a, local_results, shards)


def _watch(agent, task, a, local_results: Path, shards) -> int:
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
        print(f"\nrefusing to retrieve: nextflow recorded {n_failed} FAILED "
              f"task(s). The retrieve is the expensive half and a dead lane "
              f"should not reach it.", file=sys.stderr)
        return 2
    if result["status"] != "completed":
        print(f"\nthe wait reported [{result['status']}] but the task table records "
              f"no failure -- trusting the table. Verify below is the real gate.")
    if a.stub:
        print("\nstub run finished: staging, the container / overlay pairing, the "
              "relay and the rendered workflow all hold.")
        return 0
    retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
    return verify(local_results, shards)


if __name__ == "__main__":
    sys.exit(main())
