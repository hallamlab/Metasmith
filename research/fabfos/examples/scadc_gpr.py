#!/usr/bin/env python3
"""Annotate the recovered SCADC fosmid inserts and fold the lanes into a GPR table.

    python examples/scadc_gpr.py                    # plan + DAG only
    python examples/scadc_gpr.py --preflight        # what fir must hold
    python examples/scadc_gpr.py --run --lanes 7    # execute on fir
    python examples/scadc_gpr.py --retrieve --lanes 7
    python examples/scadc_gpr.py --publish --lanes 7

    # one insert into a scratch tree, leaving the real chunks alone
    python examples/scadc_gpr.py --inserts data/scratch/new_insert/inserts.fna \
        --into data/scratch/new_insert/published --work data/scratch/new_insert/work \
        --max-walltime-minutes 30 --run --lanes 7

    inserts -> prodigal -> {kofamscan, CLEAN, diamond_uniref50, proteinbert} -> gpr_4lane
                        (+ {deepec, ezpred, esm_c}                           -> gpr_7lane)

WHERE IT PUBLISHES
------------------
Three chunks beside the inserts, under the run folder `data/fabfos/runs/scadc_fosmids/`.
`annotations/` takes the ORF set and the four lanes the 4-lane mapper is built from;
`annotation_alts/` takes the three alternative-tool lanes; `gpr/` takes the two mapper
tables. The per-lane layout is `<lane>/fosmids.<lane>.<ext>`, which is what every other
run folder in `data/fabfos/` already uses. See PUBLISH_AT below.

The 4/alt split is not cosmetic: `annotations/` is the set a host's `.faa` is checked
for total coverage against (`build_references/derive_annotation_faa.py`), so a tool that
is not part of that contract cannot live in it.

THE INSERTS HAD NEVER BEEN ANNOTATED
------------------------------------
`annotations/` used to ship tables keyed on an individually-picked clone scheme
(`C001`..`C132`); the current insert set is keyed on
`pool01_ATTGAGCC:megahit:k141_112:1-18882`. The two id spaces do not intersect, no file
in the tree maps between them, and the ORF fasta the legacy tables were built from is
not in this checkout. So this was not a re-run of those numbers -- it is the first
annotation of this insert set, and the legacy tables were retired when it landed.

ONE SHARD, DELIBERATELY
-----------------------
`prodigal`'s `orf_shard_size` defaults to 0, which is a single shard. 183 inserts /
6,064,622 bp is ~6k ORFs at prokaryotic coding density -- far below anything worth
fanning out, and multi-shard is not free: `proteinbert` stacks every shard's embeddings
while the mappers key the query index per task. Re-check the count `--plan-only`
reports before assuming this still holds for a different insert set.

REFERENCES ARE STAGED FROM THE PROCESSED TIER, NEVER REBUILT
-------------------------------------------------------------
Each is checked for existence by name and the run refuses if any is missing. Falling
back to a stand-in would hand a lane an empty database, and most of them produce an
empty output and *succeed* -- exactly the failure the GPR validator exists to catch,
one whole run too late to be cheap.

That tier has TWO copies and they are not interchangeable. `data/processed/` is the
DVC-pinned one and is what makes the references reproducible; `--remote-processed` on
fir is what a job actually reads. The references total ~17.5 GB, almost all of it the
DIAMOND database, and the only consumer is a job on the same host that built them.

THE WHOLE RUN IS ON FIR
-----------------------
CLEAN and esm_c want a GPU and DECLARE one (`Resources(gpus=...)`), which is what gets
one allocated; `--slurm-gpu-account` exists because there is no `rrg-shallam-ab_gpu`
association on fir and `def-shallam_gpu` is the only GPU one. Everything else is CPU.
See `examples/_fir.py`.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import replace
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

_ENGINE = REPO / "src"
if (_ENGINE / "metasmith").is_dir():
    sys.path.insert(0, str(_ENGINE))

from metasmith.python_api import (                                      # noqa: E402
    DataInstanceLibrary, Duration, Gpus, Resources, Size,
    TargetBuilder, TransformInstanceLibrary,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _fir import (                                                     # noqa: E402
    FIR_ACCOUNT, FIR_AGENT_HOME, FIR_CONTAINER, FIR_GPU, FIR_GPU_ACCOUNT, FIR_HOST,
    FIR_PROCESSED, check_schedulable, check_staged_executor, check_tasks,
    check_walltimes, envs_from_plan, fir_agent, landed_from_index,
    pin_external_leaf_ids, preflight, provision_dev_overlay_remote, publish_by_type,
    retrieve, ssh_once,
)

MLIB = REPO / "src" / "metasmith_libraries"
DATA = REPO / "data" / "fabfos"
PROCESSED = DATA / "processed"
RUNS = DATA / "runs"
SCRATCH = DATA / "scratch"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"

FOSMIDS = RUNS / "scadc_fosmids"
INSERTS = FOSMIDS / "sequences" / "inserts" / "inserts.fna"
# THREE destinations, because they are three DVC chunks. `annotations/` is the per-lane
# tree keyed on the ORF set; `annotation_alts/` is the same shape for the alternative
# tools; `gpr/` holds the mapper tables built from both. Splitting them here is what
# keeps the publish from writing `annotations/../gpr/...`.
DEST = FOSMIDS / "annotations"
ALTS_DEST = FOSMIDS / "annotation_alts"
GPR_DEST = FOSMIDS / "gpr"

# The compiled references, by type -> the file or directory under data/processed/.
# `ref::reference_label_pool` is a DIRECTORY (index + embedding stack), which is the
# whole reason it is one product: the consumer addresses the stack by row, so an index
# from one build against a stack from another misindexes every row silently.
REFS_4 = {
    "ref::kofamscan_profiles": "kofam_ref/profiles",
    "ref::kofamscan_ko_list": "kofam_ref/ko_list.tsv",
    "ref::uniref50_diamond_db": "uniref50_dmnd/uniref50.dmnd",
    "ref::mnxr_lookup": "mnxr_lookup/mnxr_lookup.parquet",
    "ref::reference_label_pool": "reference_label_pool/pool",
}
# The three extra lanes need three more. Two are built by the reference driver
# alongside the canonical five; `ref::ezpred_model` is staged as a given, because its
# compile is still in build_references/transforms/_deferred/ -- blocked on how a
# *patched* upstream tree enters the graph, which is a tier decision rather than a
# missing implementation. See that directory's README.
#
# THE ESM-C POOL IS NAMED DIFFERENTLY IN THE TWO COPIES. `data/processed/` calls the
# directory `pool`, matching every other pool chunk; the fir mirror calls it
# `pool_esmc`. Same three files, same bytes -- the publish that placed it disambiguated
# a name that did not need it. Both are listed and `resolve_refs` takes whichever the
# host actually has, because hardcoding either one turns a naming difference into
# "reference absent" and sends the reader off to rebuild 1 GB that is already there.
REFS_7 = dict(REFS_4, **{
    "ref::esm_c_600m_weights": "esm_c_weights/esmc_600m.tgz",
    "ref::reference_label_pool_esmc": ("reference_label_pool_esmc/pool_esmc",
                                       "reference_label_pool_esmc/pool"),
    "ref::ezpred_model": "ezpred_model/EZpred",
})

TARGET_4 = "annotation::gpr_table"
TARGET_7 = "annotation::gpr_table_7lane"

# What the plan must contain, per lane count. Kept here rather than imported from a
# compile-check: `tests/test_gpr_workflow.py` no longer exists, and the surviving
# gates (`test_annotation_driver.py`) start at ORFs, so they cannot speak to the
# prodigal step that makes this driver's entry point different.
LANES_4 = {"kofamscan", "clean", "diamond_uniref50", "proteinbert"}
LANES_7 = LANES_4 | {"deepec", "ezpred", "esm_c"}


def expected_transforms(lanes: int) -> set[str]:
    if lanes == 7:
        return LANES_7 | {"prodigal", "gpr_4lane", "gpr_7lane"}
    return LANES_4 | {"prodigal", "gpr_4lane"}


# Published beside the inserts: the ORFs the lanes ran on and every lane output. They
# are only jointly meaningful -- a GPR table read against another run's ORFs joins on
# ids that happen to look alike.
#
# THE LAYOUT IS THE RUN-FOLDER LAYOUT, not one this driver invents. A lane lands at
# `<lane>/fosmids.<lane>.<ext>`, which is what every `data/fabfos/runs/<run>/` already does
# -- so a reader who has walked one annotated sequence set in this tree has walked all
# of them. `ezpred` gets its own directory rather than joining the hosts' `ecpred/`:
# the near-identical spelling is a coincidence between two different tools, and
# `ecpred` has no producer here at all.
PUBLISH_AT = {
    "sequences::orfs": "fosmids.faa",
    "sequences::gff": "fosmids.gff",
    "annotation::kofamscan_results": "kofam/fosmids.kofam.csv",
    "annotation::clean_predictions": "clean/fosmids.clean.tsv",
    "annotation::diamond_uniref50_results": "uniref50/fosmids.uniref50.blast6.tsv",
    "annotation::proteinbert_embeddings": "proteinbert/fosmids.pbert.parquet",
    "annotation::proteinbert_index": "proteinbert/fosmids.pbert.index.csv",
}

# The three lanes that only the 7-lane mapper reads, into the sibling
# `annotation_alts/` chunk. Same `<lane>/fosmids.<lane>.<ext>` shape -- the split is by
# which contract the lane belongs to, not by how it is named.
PUBLISH_ALTS_AT = {
    "annotation::deepec_predictions": "deepec/fosmids.deepec.tsv",
    "annotation::ezpred_predictions": "ezpred/fosmids.ezpred.csv",
    "annotation::esm_c_embeddings": "esmc/fosmids.esmc.parquet",
    "annotation::esm_c_index": "esmc/fosmids.esmc.index.csv",
    # esm_c's third product. No mapper reads it, but it is a per-layer mean stack
    # only this run can produce, and leaving it on fir's scratch is how it gets
    # purged. Listing it here also puts it in verify()'s lane set, which is right:
    # if esm_c ran, all three of its products landed. No host has a third ESM-C
    # product, so the distinguishing word stays in the suffix.
    "annotation::esm_c_layer_means": "esmc/fosmids.esmc.layer_means.npy",
}

# The mapper tables, into the sibling `gpr/` chunk. Named for the lane set that built
# them rather than for the hosts' `gpr_denovo`: `gpr_4lane` is indeed the transform
# behind a host's de-novo table, but there is no host analog for a 7-lane one, and
# the lane count is the thing a reader needs to know here.
PUBLISH_GPR_AT = {
    "annotation::gpr_table": "gpr_4lane.parquet",
    "annotation::gpr_table_7lane": "gpr_7lane.parquet",
}

TYPE_LIBRARIES = [MLIB / "data_types" / f for f in
                  ("sequences.yml", "annotation.yml", "ref.yml", "lib.yml",
                   "fabfos.yml", "ncbi.yml")]

# KOfam over ~6k ORFs against ~26k profiles is the CPU pole; the GPU lanes want a
# device for minutes. Everything else is minutes.
#
# Durations sit near the measured cost, not generously above it. SLURM will not start a
# job that cannot finish before a reservation covering the nodes it needs, fir's
# maintenance windows are ALL_NODES, and `slurm.nf` DOUBLES the walltime on retry -- so
# a 4 h declaration is an 8 h second attempt, which may never be schedulable at all. An
# over-generous walltime is not caution: it is a job that never runs.
RESOURCE_OVERRIDES = {
    "kofamscan": Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=4)),
    "diamond_uniref50": Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=3)),
    "proteinbert": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=2)),
    "clean": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=2)),
    # The three extra lanes. Their declared durations (3-4 h) are sized for a
    # metagenome, not for ~6k ORFs; these are what the work costs here, so the retry
    # stays schedulable too.
    "deepec": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=1)),
    "esm_c": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=1),
                       gpus=Gpus.REQUIRED, gpu_memory=Size.GB(24)),
    "ezpred": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=1)),
    # The mappers declare 8 GB, which is what a mapper reading five tables looks like
    # it needs -- and is not. `lane_embed` materialises a DENSE (reference x MNXR)
    # one-hot label matrix to do the kNN vote as a matmul, and this pool is 222,019
    # references over 13,112 distinct MNXR: 10.84 GiB of float32 that is 99.97% zeros,
    # before the embeddings or the bridge. Measured off the pinned pool, not guessed.
    # `da48c76` hardened the transfer itself for a 100,000-ORF shard; the label matrix
    # is upstream of that and still wants the headroom.
    "gpr_4lane": Resources(cpus=4, memory=Size.GB(48), duration=Duration(hours=1)),
    "gpr_7lane": Resources(cpus=4, memory=Size.GB(64), duration=Duration(hours=1)),
}


def resolve_refs(host: str, remote_processed: str, lanes: int) -> dict[str, str]:
    """Every reference must already be on the host, checked in ONE ssh round trip.

    A missing reference is a refusal, never a stand-in: an empty database makes most
    of these lanes produce an empty output and *succeed*, which is the exact failure
    `validate_gpr` exists to catch -- one whole run too late to be cheap.

    Returns the resolved `{dtype: relpath}` -- a value may be several candidate paths
    (see REFS_7), and which one the host has is not knowable from here.
    """
    refs = {d: ((v,) if isinstance(v, str) else tuple(v))
            for d, v in (REFS_7 if lanes == 7 else REFS_4).items()}
    probe = "; ".join(f'[ -e "{remote_processed}/{rel}" ] && echo "HAVE {d} {rel}"'
                      for d, cands in refs.items() for rel in cands)
    have: dict[str, str] = {}
    for line in ssh_once(host, probe).splitlines():
        _, dtype, rel = line.split(maxsplit=2)
        have.setdefault(dtype, rel)
    absent = {d: cands for d, cands in refs.items() if d not in have}
    if absent:
        raise SystemExit(
            f"references absent from {host}:{remote_processed}:\n"
            + "\n".join(f"    {d}  (tried {', '.join(c)})"
                        for d, c in sorted(absent.items()))
            + f"\n  Build and place them: "
              f"`python examples/annotation_references_build.py --run` then "
              f"`--publish-remote`.")
    return have


def build_inputs(work: Path, lanes: int, remote_processed: str,
                 refs: dict[str, str]) -> DataInstanceLibrary:
    xgdb = work / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for tl in TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)

    if not INSERTS.exists():
        raise SystemExit(
            f"the recovered inserts are not at {INSERTS.relative_to(REPO)}.\n"
            f"  Materialise the pin: `dvc checkout data/fabfos/runs/scadc_fosmids/sequences.dvc`")
    n = sum(1 for line in INSERTS.open() if line.startswith(">"))
    print(f"    fabfos::putative_inserts     {INSERTS.relative_to(REPO)}  ({n} records)")
    # Copied in, so it is a RELATIVE member of the library and travels with the task.
    # 6 MB; the 17.5 GB of references it is annotated against stay where they are.
    #
    # `putative_inserts` declares `origin: assembler`, which makes it a property
    # superset of `sequences::assembly` -- so prodigal matches it unchanged. That is
    # the whole mechanism; there is no insert-specific ORF caller.
    shutil.copy(INSERTS, xgdb / INSERTS.name)
    inputs.AddItem(INSERTS.name, "fabfos::putative_inserts")

    # ABSOLUTE host paths -> referenced in place on fir. Relative would copy 17.5 GB
    # into the library and stage it through the task for no gain.
    for dtype, rel in sorted(refs.items()):
        remote = f"{remote_processed}/{rel}"
        print(f"    {dtype:32s} {remote}")
        inputs.AddItem(remote, dtype)
    # NOT optional. Those eight references have no local bytes, so the engine mints
    # their leaf ids from `uuid4 + time_ns` -- and the plan key is a hash over the
    # given instances' ids, so the task key would change on every invocation: `--run`
    # and a later `--retrieve` would name different run directories.
    pin_external_leaf_ids(inputs)
    inputs.Save()
    return inputs


def plan(work: Path, agent, lanes: int, remote_processed: str,
         refs: dict[str, str]):
    inputs = build_inputs(work, lanes, remote_processed, refs)
    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        inputs,
    ]
    # NOT logistics/: it carries downloaders producing the same ref:: types the
    # compiles do, and two producers for one reference makes provenance a tiebreak.
    transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "fabfos"),
    ]
    tb = TargetBuilder()
    # The 7-lane run asks for BOTH tables. `gpr_7lane` does not read `gpr_4lane`'s
    # output -- both mappers consume the same lane results directly -- so the four
    # canonical lanes are computed once and read twice. Asking for both here costs one
    # extra mapper step over outputs already on disk; asking for `gpr_table` in a
    # separate run would recompute all four, because a different task key is a
    # different run directory and nextflow shares no cache across them.
    tb.Add(TARGET_7 if lanes == 7 else TARGET_4)
    if lanes == 7:
        tb.Add(TARGET_4)

    return agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("fabfos::putative_inserts")),
        resources=resources,
        transforms=transforms,
        targets=tb,
    )


def check_plan(task, lanes: int) -> int:
    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"\nPlan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<24} -> {prods}")

    bad = 0
    missing = expected_transforms(lanes) - used
    if missing:
        print(f"\nMISSING expected transforms: {sorted(missing)}", file=sys.stderr)
        bad = 1
    for dup in ("downloadKofamscanDB", "downloadUniref50", "downloadEsmC"):
        if dup in used:
            print(f"\nlogistics/{dup} is in the plan -- duplicate reference producer.",
                  file=sys.stderr)
            bad = 1
    # One prodigal, not one per lane. Without the mapper's `parents={orfs}` pins the
    # planner is free to satisfy each lane from whatever ORF set is cheapest to reach,
    # and every gene-id join across lanes then comes back empty.
    n_prodigal = sum(1 for s in task.plan.steps if Path(s.transform._path).stem == "prodigal")
    if n_prodigal != 1:
        print(f"\n{n_prodigal} prodigal steps. Every lane must annotate ONE ORF set or "
              f"the mapper folds annotations of one onto another.", file=sys.stderr)
        bad = 1
    if not bad:
        print("\none ORF set, every expected transform, no duplicate producer")
    return bad




def publish(results: Path, *, dry_run: bool, lanes: int) -> int:
    rc = publish_by_type(results, PUBLISH_AT, DEST, dry_run=dry_run, repo=REPO)
    # The alt lanes and the tables land in their own chunks. Separate passes rather than
    # one map with `../gpr/` in it: the publisher joins `dest_root / target` and prints
    # the result, so a traversal in the value works but reads as a mistake every time
    # anyone looks.
    if rc == 0 and lanes == 7:
        rc = publish_by_type(results, PUBLISH_ALTS_AT, ALTS_DEST,
                             dry_run=dry_run, repo=REPO)
    if rc == 0:
        rc = publish_by_type(results, PUBLISH_GPR_AT, GPR_DEST,
                             dry_run=dry_run, repo=REPO)
    if rc == 0 and not dry_run:
        # The pin IS the provenance: this commit carries both this script and the
        # .dvc files the next line writes, so re-running it here reproduces them.
        print(f"Pin the chunks:  dvc add {DEST.relative_to(REPO)} "
              + (f"{ALTS_DEST.relative_to(REPO)} " if lanes == 7 else "")
              + f"{GPR_DEST.relative_to(REPO)}")
    return rc


def verify(results: Path, lanes: int) -> int:
    targets = [TARGET_7, TARGET_4] if lanes == 7 else [TARGET_4]
    landed = landed_from_index(results, targets)
    absent = [t for t in targets if t not in landed]
    if absent:
        print(f"\nTHE RUN IS GREEN BUT {absent} IS ABSENT. Nextflow ignores a process "
              f"that exhausted its retries; read _metasmith/logs.*/main.log for the "
              f"step that died.", file=sys.stderr)
        return 2
    # Every lane output too, not just the tables: a mapper writes a table from
    # whatever lanes reached it, and a lane that died leaves a thinner table rather
    # than an absent one.
    # PUBLISH_AT and PUBLISH_ALTS_AT are lanes-only -- the tables live in
    # PUBLISH_GPR_AT -- so every `annotation::` key in either is a lane by construction,
    # and the alt map is exactly the three lanes a 4-lane run does not produce.
    lane_types = [d for d in PUBLISH_AT if d.startswith("annotation::")]
    if lanes == 7:
        lane_types += [d for d in PUBLISH_ALTS_AT if d.startswith("annotation::")]
    lane_landed = landed_from_index(results, lane_types)
    thin = sorted(set(lane_types) - set(lane_landed))
    if thin:
        print(f"\nthe tables landed but these lane outputs did not: {thin}\n"
              f"  A mapper writes a table from whatever reached it, so a missing lane "
              f"is a THINNER table, not an absent one.", file=sys.stderr)
        return 2
    print(f"{', '.join(targets)} present, and all {len(lane_types)} lane outputs. "
          f"Now: --publish (then `dvc add {DEST.relative_to(REPO)} "
          + (f"{ALTS_DEST.relative_to(REPO)} " if lanes == 7 else "")
          + f"{GPR_DEST.relative_to(REPO)}`).")
    return 0


def main() -> int:
    global INSERTS, DEST, ALTS_DEST, GPR_DEST
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lanes", type=int, choices=(4, 7), default=7)
    ap.add_argument("--run", action="store_true",
                    help="execute on the host; without it this plans, renders the "
                         "DAG and stops")
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--retrieve", action="store_true",
                    help="pull an already-finished run's results down and verify "
                         "them, without re-running")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--publish-dry-run", action="store_true")
    # The insert set and the publish root, so this driver can annotate ONE insert
    # into a scratch tree without going near the real chunks. Both default to the
    # values above, so an invocation that names neither is the run this file
    # documents. Used to retarget the tables when the insert set moved and a
    # single new insert had no ORFs -- see tests/retarget_annotations.py.
    ap.add_argument("--inserts", type=Path, default=INSERTS,
                    help="the fasta to annotate (default: the run's inserts.fna)")
    ap.add_argument("--into", type=Path, default=FOSMIDS,
                    help="root to publish annotations/, annotation_alts/ and gpr/ "
                         "under (default: the run folder, i.e. the real chunks)")
    ap.add_argument("--max-walltime-minutes", type=int, default=None,
                    help="cap every step's declared walltime. RESOURCE_OVERRIDES is "
                         "sized for ~6k ORFs; for a handful it is an over-generous "
                         "declaration, which is a job that may never be scheduled.")
    ap.add_argument("--host", default=FIR_HOST)
    ap.add_argument("--agent-home", default=FIR_AGENT_HOME)
    ap.add_argument("--container", default=FIR_CONTAINER)
    ap.add_argument("--remote-processed", default=FIR_PROCESSED,
                    help="the host-side mirror of data/processed/, where the "
                         "references are read from. Placed there by "
                         "annotation_references_build.py --publish-remote.")
    ap.add_argument("--slurm-account", default=FIR_ACCOUNT)
    ap.add_argument("--slurm-gpu-account", default=FIR_GPU_ACCOUNT,
                    help="CLEAN and esm_c declare a GPU. There is no "
                         "rrg-shallam-ab_gpu association on fir; def-shallam_gpu is "
                         "the only one.")
    ap.add_argument("--timeout-hours", type=float, default=24.0)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--work", default=None)
    a = ap.parse_args()

    # Rebound before anything reads them. They are module constants because they
    # are what this driver is FOR; the options exist so the same code can serve a
    # one-insert top-up without a second copy of it drifting out of step.
    # Resolved, not taken as given: every message below prints them
    # `relative_to(REPO)`, which raises on a relative path handed in on the CLI.
    INSERTS = a.inserts.resolve()
    into = a.into.resolve()
    DEST = into / "annotations"
    ALTS_DEST = into / "annotation_alts"
    GPR_DEST = into / "gpr"
    if a.max_walltime_minutes:
        cap = Duration(minutes=a.max_walltime_minutes)
        for name, res in RESOURCE_OVERRIDES.items():
            if res.duration is not None and res.duration._delta > cap._delta:
                RESOURCE_OVERRIDES[name] = replace(res, duration=cap)
        print(f"=== every step's walltime capped at {a.max_walltime_minutes} min ===")

    work = Path(a.work).resolve() if a.work else (SCRATCH / f"scadc_gpr_{a.lanes}lane")
    work.mkdir(parents=True, exist_ok=True)
    local_results = work / "results"

    if a.publish or a.publish_dry_run:
        return publish(local_results, dry_run=a.publish_dry_run, lanes=a.lanes)
    agent = fir_agent(host=a.host, agent_home=a.agent_home, container=a.container)

    print(f"=== checking the references on {a.host} ===")
    refs = resolve_refs(a.host, a.remote_processed, a.lanes)

    print(f"=== staging ({a.lanes} lanes) ===")
    task = plan(work, agent, a.lanes, a.remote_processed, refs)
    if not task.ok:
        print(f"\nPLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 3
    if check_plan(task, a.lanes):
        return 3

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    svg = ARTIFACTS / f"scadc_gpr_{a.lanes}lane.svg"
    try:
        task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env"})
        print(f"\nDAG -> {svg}")
    except Exception as e:                                   # graphviz is optional
        print(f"\n(DAG not rendered: {e})")
    print(f"\n=== task key: {task.GetKey()} ===", flush=True)

    # The images the RESOLVED plan reaches for -- read off the plan rather than kept
    # in a list here, because a hand-kept list drifts exactly where it hurts.
    if a.preflight:
        rc = preflight(a.host, a.agent_home, a.container, envs_from_plan(task), mlib=MLIB)
        rc |= check_walltimes(a.host, RESOURCE_OVERRIDES)
        rc |= check_schedulable(a.host, a.slurm_account, RESOURCE_OVERRIDES)
        rc |= check_schedulable(a.host, a.slurm_gpu_account, RESOURCE_OVERRIDES)
        return rc

    # run_campaign.py reads this to resume an attempt, and it is not recoverable
    # from a retrieved results tree.
    (work / "RUN_KEY").write_text(task.GetKey())

    if a.retrieve:
        retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
        check_tasks(a.host, a.agent_home, task.GetKey())
        return verify(local_results, a.lanes)

    if not a.run:
        print("\nplan only: nothing staged, nothing run. Add --run.")
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
    if check_walltimes(a.host, RESOURCE_OVERRIDES):
        return 4
    if check_schedulable(a.host, a.slurm_account, RESOURCE_OVERRIDES):
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
    # NOT a return on `!= completed`. `WaitForWorkflow` decides a run is over from a
    # sentinel in `agent.log` and reports `errored` on runs that finished perfectly;
    # the task table below is the authority either way.
    if result["status"] != "completed":
        print("\n(wait reported a non-completed status -- reading the task table, "
              "which is where the truth is)", file=sys.stderr)

    if check_tasks(a.host, a.agent_home, task.GetKey()):
        return 2
    retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
    return verify(local_results, a.lanes)


if __name__ == "__main__":
    sys.exit(main())
