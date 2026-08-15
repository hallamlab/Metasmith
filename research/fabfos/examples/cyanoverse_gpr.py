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

# ---------------------------------------------------------------------------
# The layout on fir. All of it on /project, none of it on /scratch: a write
# probe measured scratch dropping 14 of 300 files silently, and scratch is
# purged -- neither is survivable for a campaign measured in weeks. The
# reference copy under gpr/refs is this campaign's own, so the paths (and hence
# every leaf id, and hence the task key) cannot move under a running batch.
# ---------------------------------------------------------------------------
GPR = "/project/rpp-shallam/phyberos/cyanoverse/gpr"
SHARDS_DIR = f"{GPR}/shards"
LANES_DIR = f"{GPR}/lanes"
REFS_ROOT = f"{GPR}/refs"
AGENT_HOME = f"{GPR}/agent_home"

TARGET = "annotation::gpr_table"

# The already-computed lane, and the basename it is repacked under. Distinct
# names are not cosmetic: nextflow stages a process's inputs by basename with no
# `stageAs` (`nextflow_codegen.py:638-646`), so two inputs sharing a name
# collide -- which has already killed a run here AFTER all seven of its lanes
# succeeded, reporting "run completed" with 0 FAILED.
#
# ONE lane is reused, not two. The legacy ProteinBERT embeddings were measured
# on 2026-08-05 to be in an order that is NOT the fasta's -- 2,032 of 49,522 rows
# aligned on a 49-chunk assembly, mean cosine 0.48 against 0.998 for the best
# match elsewhere in the same stack. The index that recorded their order was not
# retained, and the obvious lexicographic-chunk explanation was tested and
# refuted. Re-running the lane costs ~10 min/shard on CPU and makes the order
# irrelevant, because a fresh run keeps index and stack together.
#
# kofam is reused because its rows are KEYED by gene id rather than positional,
# so no ordering assumption is involved at all.
REUSED = {
    "annotation::kofamscan_results": "{shard}.kofam.csv",
}

# Three of the four lanes are sharded now: kofamscan, diamond_uniref50 and
# proteinbert consume `sequences::orf_chunk` and produce `*_chunk`, so the lane
# is a chunk step, the annotators, and a merge back to the whole-proteome product
# `gpr_4lane` requires. `clean` is not sharded and needs no merge.
EXPECTED_TRANSFORMS = {
    "chunkOrfsForAnnotation",
    "clean", "diamond_uniref50", "proteinbert",
    "merge_diamond_uniref50", "merge_proteinbert",
    "gpr_4lane",
}
# Either one present means the reuse silently did not take -- see the module
# docstring on `solver.py:603-605`. The merge belongs here as much as the
# annotator: reusing the finished whole-proteome product must remove the whole
# kofam lane, and a plan that kept the merge would be recombining chunks it
# never computed.
FORBIDDEN_TRANSFORMS = {"kofamscan", "merge_kofamscan"}

# Nothing above 3 h: `slurm.nf` doubles the ask on retry, and a 6 h retry cannot
# be scheduled ahead of fir's 08:00 ALL_NODES maintenance window -- it sits
# PENDING forever, indistinguishable from ordinary queueing. `check_walltimes`
# enforces the doubled figure.
#
# DIAMOND is the one that was actually measured (2026-08-05): 10,000 queries in
# 489 s at 8 threads against the 17 GB UniRef50 db with the transform's own
# `--sensitive --block-size 10`. Linear extrapolation puts 100,000 at 1.4 h, and
# larger query files amortise the database load, so 3 h at 16 threads is margin
# rather than a guess. Nothing in this tree had ever timed that shape.
# Every number below is MEASURED on the 2-shard pilot (2026-08-05, 100,000 ORFs
# per shard), not estimated. Peak RSS from SLURM's cgroup accounting, because
# nextflow's own trace sees only the wrapper process outside the container and
# reports a flat ~167 MB for every step regardless of what it did.
#
# Changing anything here does NOT change the task key -- the key is fixed when
# the plan resolves, and these are handed to RunWorkflow afterwards. So a memory
# correction between batches is safe; a transform edit is not.
RESOURCE_OVERRIDES = {
    # 10:28 and 14:55 measured. THE MEMORY IS THE REASON THIS IS 96 AND NOT 64:
    # the two pilot shards peaked at 57.2 GiB and 64.0 GiB against a 64 GB ask
    # -- the second one landed exactly on its own ceiling and completed only
    # because nothing pushed it further. `--block-size 10` sizes DIAMOND's
    # working set from the database, and ORF lengths vary between shards, so a
    # heavier shard OOMs. An OOM here is not loud: slurm.nf sets
    # errorStrategy='ignore' after retries, so the run stays green and the
    # uniref50 lane is simply absent from that shard's table.
    "diamond_uniref50": Resources(cpus=16, memory=Size.GB(96), duration=Duration(hours=3)),
    # 8:50 measured, 18.0 GiB peak.
    "proteinbert": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=3)),
    # 33:22 measured, 22.8 GiB peak, 5 internal batches of 20,000. The GPU step,
    # and the campaign's critical path.
    "clean": Resources(cpus=4, memory=Size.GB(48), duration=Duration(hours=3)),
    # 18:44 measured, 4.7 GiB peak at 100,000 ORFs -- against ~17 GB at 5,884
    # before the dense reference-by-reaction matrix was replaced by a sparse
    # gather. The 24 GB ask stays: headroom is free here and the largest shards
    # carry more evidence rows than these two did.
    "gpr_4lane": Resources(cpus=8, memory=Size.GB(24), duration=Duration(hours=2)),
    # ONLY reachable on a --control run. The campaign never plans this step --
    # that is the whole point of the reuse -- but the control does, and without
    # a declaration here it would inherit the transform's own, which is not
    # bounded by fir's 3 h rule.
    "kofamscan": Resources(cpus=32, memory=Size.GB(64), duration=Duration(hours=3)),
}


def run_params(a) -> dict:
    """What RunWorkflow needs. Passed AFTER the plan resolves, so nothing here
    changes the task key and any of it is safe to adjust between batches.

    NOTE: `process_clusterOptionsExtra` is NOT used for the node exclusion, even
    though `slurm.nf` documents it as the generic sbatch-flag injection point.
    MEASURED: it does not reach sbatch. Batch 2 shipped
    `workflow.params.yml: process.clusterOptionsExtra: --exclude=fc10512` and the
    submitted `.command.run` still read
    `#SBATCH --nodes=1 --ntasks=1 --account=rrg-shallam-ab`, byte-identical to
    batch 1 which shipped no such param -- because `slurm.nf` itself declares
    `params.process.clusterOptionsExtra = ''` and the config default wins over
    the `-params-file` value. `slurmAccount` looks like a counter-example but is
    not: it is a template placeholder substituted into the config, never read
    from the params file. See `exclude_flags` for where the exclusion actually
    goes.
    """
    return {"slurmAccount": a.slurm_account,
            "slurmGpuAccount": a.slurm_gpu_account}


def exclude_flags(a) -> list[str]:
    """`--exclude=` for the GPU step, via `Gpu.extra`.

    This is the injection point that WORKS. `_render_gpu_config` writes a
    literal `withName: '<step>' { clusterOptions = ... }` block into a config
    file -- process scope, not params scope -- so nothing can silently override
    it, and the same string carries the `--gpus-per-node` flag that is
    demonstrably reaching sbatch today.

    GPU-only is not a limitation here, it is the correct scope: CLEAN is the one
    step that declares a GPU and the one step that fails on the bad node. 12 of
    12 failures were CLEAN on fc10512; no CPU step has ever failed there.

    Still free of the task key -- `gpus=` is handed to `RunWorkflow` after the
    plan resolves -- so this remains safe to change between batches.
    """
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
    """Every shard fasta and every reused lane product, present and non-empty.

    ONE ssh round trip, and a hard refusal. A missing lane file does not fail
    loudly later -- it strips lineage from every OTHER row of that input
    (`nextflow_codegen.py:820-838`) and the run silently pairs each shard with
    somebody else's annotations.
    """
    probes = []
    for s in shards:
        probes.append(f'[ -s "{SHARDS_DIR}/{s}.faa" ] || echo "MISSING {s}.faa"')
        # A control run supplies nothing, so the repacked lane's absence is not
        # a defect there -- it is the point.
        for pat in (REUSED.values() if require_reused else ()):
            f = pat.format(shard=s)
            probes.append(f'[ -s "{LANES_DIR}/{f}" ] || echo "MISSING {f}"')
    for dtype, rel in annotation.REF_LAYOUT.items():
        probes.append(f'[ -e "{REFS_ROOT}/{rel}" ] || echo "MISSING {dtype} {rel}"')
    prof = f'{REFS_ROOT}/{annotation.REF_LAYOUT["ref::kofamscan_profiles"]}'
    pool = f'{REFS_ROOT}/{annotation.REF_LAYOUT["ref::reference_label_pool"]}'
    probes += [
        f'[ "$(ls -1 "{prof}" 2>/dev/null | head -1)" ] || '
        f'echo "EMPTY ref::kofamscan_profiles"',
        f'for f in orf_index.parquet emb_pbert.npy; do [ -s "{pool}/$f" ] || '
        f'echo "MISSING ref::reference_label_pool $f"; done',
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
    """Supply the two already-computed lanes, parented to their own shard.

    This is the whole reuse. `parents=[orf_path]` is what makes each product
    satisfy `gpr_4lane`'s `parents={orfs}` pin (`solver.py:527-532`,
    `:516-525`), and what the runtime later joins on.
    """
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
        mnxr_lookup=None, label_pool=None,
        runtime=Runtime.APPTAINER,
        refs_root=REFS_ROOT, verify_refs=False, stage_orfs="remote",
        # A control supplies no lane products, but it must STILL pin external
        # leaf ids. `_mint_leaf_id` falls back to uuid4+time_ns for any input it
        # cannot read locally -- which is every input here -- so without the pin
        # the task key changes on every invocation, and `--wait` then polls a
        # run directory that was never written. That happened: the control ran
        # as EGDuhnAC and the re-attach went looking for 95kkDDh6.
        agent=agent,
        on_inputs=(pin_external_leaf_ids if control else make_on_inputs(shards)),
    )
    assert not stubs, f"unreachable: verify_refs=False never stubs; got {stubs}"
    return task


def check_control_plan(task, n: int) -> int:
    """The control run is the campaign's plan with the reuse REMOVED.

    Schema matching is what makes the legacy KOfam hits look reusable; identical
    evidence rows are what would make them actually reusable. Only running one
    shard's KOfam from its ORFs catches the case where the legacy pass used
    different profiles or a different score threshold -- a difference that is
    invisible in every check the campaign otherwise makes, because a wrong-but-
    well-formed hit table joins, validates and delivers exactly like a right one.

    So here the assertion is INVERTED: `kofamscan` must be PRESENT. A control
    that silently reused the thing it exists to check would be worse than no
    control, because it would report agreement it never tested.
    """
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
    """Prove each reused product is bound to ITS OWN shard, before any compute.

    The staged `workflow.lineage_of_given.json` is the join the runtime actually
    performs. Every row carries `__self__` (the given's own instance id) and, when
    lineage survived the all-or-nothing gate, one entry per parent input naming
    the parent's instance id. Ids here are the ones `pin_external_leaf_ids` minted
    from the paths, so they can be re-derived on this side and compared -- no
    trusting the file to describe itself.

    A batch that reaches compute with this wrong produces a full, non-empty,
    schema-valid table per shard, every one of them describing another shard's
    annotations.
    """
    from metasmith.models.libraries.identity import multihash_key

    def ext_id(path: str) -> str:
        return multihash_key(b"external\x00" + path.encode("utf-8")).hex()

    raw = ssh_once(host, f"cat {AGENT_HOME}/runs/{task_key}/workflow.lineage_of_given.json")
    data = json.loads(raw)
    lineage = data.get("lineage", {})

    # id -> which shard it belongs to, for everything we supplied
    owner = {ext_id(f"{SHARDS_DIR}/{s}.faa"): s for s in shards}
    for s in shards:
        for pat in REUSED.values():
            owner[ext_id(f"{LANES_DIR}/{pat.format(shard=s)}")] = s

    checked = unparented = 0
    for key, rows in lineage.items():
        for row in rows:
            self_ids = row.get("__self__") or []
            if not self_ids or self_ids[0] not in owner:
                continue                    # a reference or a produced item
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

    # Counting products is not the same as covering the shards. A batch that
    # produced two tables for one shard and none for another has the right
    # count and the wrong corpus, and nothing downstream would notice until the
    # per-assembly split refused -- after the whole campaign had run.
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
    # fc10512 killed 5 of 5 CLEAN tasks it was handed on 2026-08-05 with
    # `CUDA error: out of memory` at model load, while every other GPU node ran
    # the same step fine -- including nodes hosting three of our tasks at once.
    # A probe confirmed the cgroup hands each task its own device renumbered to
    # index 0, so this is not co-tenant tasks colliding; that node's devices are
    # simply not usable. Excluding it is a scheduling hint, fully reversible,
    # and passed at RunWorkflow time so it does NOT change the task key.
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

    # `update`, not `clear`: clearing destroys every cached lane, which on a
    # resubmission is hours of recomputing work that already succeeded.
    agent.StageWorkflow(task, on_exist="update")
    if check_staged_executor(a.host, a.agent_home, task.GetKey()):
        return 4
    # A control run supplies no products, so there is no lineage to bind and the
    # gate would refuse the very thing it is there to permit.
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

    # THE WAIT'S VERDICT IS NOT AUTHORITATIVE; the task table is. `WaitForWorkflow`
    # decides on a sentinel string in `agent.log`, and it has called a run
    # `errored` when every task COMPLETED with exit 0 and the results index was
    # written. The table is consulted either way and the disagreement is said out
    # loud rather than acted on.
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
