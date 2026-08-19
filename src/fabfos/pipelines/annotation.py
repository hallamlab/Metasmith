#!/usr/bin/env python3
"""Driver 2/3: an ORF fasta -> the canonical GPR table.

    orfs -> {kofamscan, clean, diamond_uniref50, proteinbert} -> gpr_4lane
        -> annotation::gpr_table

``gpr_4lane`` is canonical: it produces ``annotation::gpr_table`` directly
rather than a subtype, so this stage has exactly one producer and no tiebreak
is needed (unlike ``gpr_7lane`` -> ``gpr_table_7lane``, which stays available
but is not this driver's target). See ``transforms/fabfos/gpr_4lane.py``.

REFERENCE DEFAULTS. All five staged references this stage needs have real
pinned copies in this repo's DVC-tracked ``data/processed/`` and are used as
defaults when not overridden: KOfam profiles + KO list, the UniRef50 DIAMOND db,
the MNXR lookup bridge, and ``ref::reference_label_pool`` -- the ProteinBERT
label stack the fourth lane votes against, built by ``compile/reference_label_pool.py``
over Swiss-Prot. Every default can be overridden with the matching flag; omit
both and a stub is staged so planning still succeeds.

SEVERAL ORF SETS, ONE RUN. ``--orfs`` repeats. Every lane and the mapper are
``group_by=orfs`` with ``parents={orfs}`` pins, so N proteomes fan out INSIDE
each step rather than adding steps: the planner deduplicates sample groups by
their endpoint set, and N ORF roots beside the same five references collapse to
one case. A three-organism run is five steps of three instances, not fifteen
steps -- and one run rather than three is what makes the tables comparable,
because the ``pbert`` lane's kNN vote is not bit-reproducible across runs.

Usage:

    python -m fabfos.pipelines.annotation --orfs orfs.faa

    python -m fabfos.pipelines.annotation --orfs orfs.faa --output ./out --run
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from metasmith.python_api import (
    Agent,
    Gpu,
    Runtime,
    Size,
    DataInstanceLibrary,
    Source,
    TargetBuilder,
    TransformInstanceLibrary,
)

from . import common

DOMAINS = ["functionalAnnotation", "fabfos", "logistics"]

ORFS_DIR_GLOB = "*.faa"

REF_LAYOUT = {
    "ref::kofamscan_profiles": "kofam_ref/profiles",
    "ref::kofamscan_ko_list": "kofam_ref/ko_list.tsv",
    "ref::uniref50_diamond_db": "uniref50_dmnd/uniref50.dmnd",
    "ref::mnxr_lookup": "mnxr_lookup/mnxr_lookup.parquet",
    "ref::reference_label_pool": "reference_label_pool/pool",
}

DEFAULT_KOFAM_PROFILES = common.DATA_PROCESSED / REF_LAYOUT["ref::kofamscan_profiles"]
DEFAULT_KOFAM_KO_LIST = common.DATA_PROCESSED / REF_LAYOUT["ref::kofamscan_ko_list"]
DEFAULT_UNIREF50_DB = common.DATA_PROCESSED / REF_LAYOUT["ref::uniref50_diamond_db"]
DEFAULT_MNXR_LOOKUP = common.DATA_PROCESSED / REF_LAYOUT["ref::mnxr_lookup"]
DEFAULT_LABEL_POOL = common.DATA_PROCESSED / REF_LAYOUT["ref::reference_label_pool"]


def _as_orf_list(orfs) -> list[Path]:
    if isinstance(orfs, (str, Path)):
        orfs = [orfs]
    return [Path(o).expanduser().resolve() for o in orfs]


def build_inputs(work: Path, *, orfs, kofam_profiles: Path | None, kofam_ko_list: Path | None,
                  uniref50_db: Path | None, mnxr_lookup: Path | None, label_pool: Path | None,
                  refs_root: "str | Path | None" = None, verify_refs: bool = True,
                  stage_orfs: str = "copy",
                  ) -> tuple[DataInstanceLibrary, dict[str, Path]]:
    lib = common.resolve_library_root()

    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    inputs.Purge()
    for ns in ("sequences", "annotation", "ref"):
        inputs.AddTypeLibrary(lib / "data_types" / f"{ns}.yml")

    paths = _as_orf_list(orfs) if stage_orfs != "remote" else [
        Path(o) for o in ([orfs] if isinstance(orfs, (str, Path)) else orfs)
    ]
    stems = [p.name for p in paths]
    if len(set(stems)) != len(stems):
        raise ValueError(f"ORF file names must be distinct; got {stems}")
    for p in paths:
        if stage_orfs == "copy":
            shutil.copy(p, inputs.location / p.name)
            inputs.AddItem(p.name, "sequences::orfs")
        elif stage_orfs == "remote":
            inputs.AddItem(str(p), "sequences::orfs")
        else:
            inputs.AddItem(p, "sequences::orfs")

    given = {
        "ref::kofamscan_profiles": kofam_profiles,
        "ref::kofamscan_ko_list": kofam_ko_list,
        "ref::uniref50_diamond_db": uniref50_db,
        "ref::mnxr_lookup": mnxr_lookup,
        "ref::reference_label_pool": label_pool,
    }
    stubs: dict[str, Path] = {}
    for dtype, rel in REF_LAYOUT.items():
        if refs_root is None:
            default = common.DATA_PROCESSED / rel
        else:
            default = f"{str(refs_root).rstrip('/')}/{rel}"
        path, real = common.stage_ref(inputs, work, dtype, given=given[dtype],
                                      default=default, verify=verify_refs)
        if not real:
            stubs[dtype] = path

    inputs.Save()
    return inputs, stubs


def generate_workflow(work: Path, *, orfs, kofam_profiles: Path | None, kofam_ko_list: Path | None,
                       uniref50_db: Path | None, mnxr_lookup: Path | None, label_pool: Path | None,
                       runtime: Runtime, agent_env: str | None = None,
                       refs_root: "str | Path | None" = None,
                       verify_refs: bool = True, stage_orfs: str = "copy",
                       agent: "Agent | None" = None, on_inputs=None):
    lib = common.resolve_library_root()
    inputs, stubs = build_inputs(
        work, orfs=orfs, kofam_profiles=kofam_profiles, kofam_ko_list=kofam_ko_list,
        uniref50_db=uniref50_db, mnxr_lookup=mnxr_lookup, label_pool=label_pool,
        refs_root=refs_root, verify_refs=verify_refs, stage_orfs=stage_orfs,
    )
    if on_inputs is not None:
        on_inputs(inputs)

    resources = [
        DataInstanceLibrary.Load(lib / "resources" / "env"),
        DataInstanceLibrary.Load(lib / "resources" / "lib"),
        inputs,
    ]
    transforms = [TransformInstanceLibrary.Load(lib / f"transforms/{d}") for d in DOMAINS]

    targets = TargetBuilder()
    targets.Add("annotation::gpr_table")

    if agent is None:
        agent = common.make_agent(work, runtime, container=agent_env)
    task = agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::orfs")),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )
    return agent, task, stubs


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--orfs", action="append", default=None, metavar="FASTA",
                    help="ORF/protein fasta to annotate; repeat for several. They fan "
                         "out inside each step, not into more steps, and one run is "
                         "what makes the tables comparable")
    p.add_argument("--orfs-dir", default=None, metavar="DIR",
                    help=f"directory of ORF fastas, one sample each (globbed as "
                         f"{ORFS_DIR_GLOB}, sorted); combines with --orfs")
    p.add_argument("--refs-root", default=None, metavar="DIR",
                    help="root the five references are addressed under (default: this "
                         "repo's data/processed/). Point it at a remote agent's mirror "
                         "together with --no-verify-refs")
    p.add_argument("--no-verify-refs", dest="verify_refs", action="store_false",
                    default=True,
                    help="stage reference paths verbatim, without a local existence "
                         "check. Required when they live on the agent's filesystem; "
                         "the caller then owns proving they are there")
    p.add_argument("--kofam-profiles", default=None, metavar="DIR")
    p.add_argument("--kofam-ko-list", default=None, metavar="FILE")
    p.add_argument("--uniref50-db", default=None, metavar="FILE")
    p.add_argument("--mnxr-lookup", default=None, metavar="FILE")
    p.add_argument("--label-pool", default=None, metavar="DIR")
    p.add_argument("--staging", default=None, help="working dir (default: <output>/_fabfos)")
    p.add_argument("--output", default="./fabfos_annotation_out", help="output directory")
    p.add_argument("--dag", default="research/fabfos/reports/dag/annotation", help="path base for the rendered SVG")
    p.add_argument("--runtime", choices=[r.value for r in Runtime],
                    default=Runtime.APPTAINER.value)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--run", action="store_true", default=False, help="also execute the plan")
    g = p.add_argument_group("GPU (CLEAN's inference lane)")
    g.add_argument("--gpu", action="store_true", default=False,
                   help="allocate a GPU for the run; without it CLEAN falls back to CPU")
    g.add_argument("--gpu-memory", type=int, default=16, metavar="GB",
                   help="per-device VRAM to ask the scheduler for (default: 16)")
    common.add_execution_args(p)
    return p


def _gpus(a) -> "Gpu | None":
    return Gpu(memory=Size.GB(a.gpu_memory)) if a.gpu else None


def _collect_orfs(a) -> list[Path]:
    orfs = [Path(o) for o in (a.orfs or [])]
    if a.orfs_dir:
        d = Path(a.orfs_dir).expanduser().resolve()
        if not d.is_dir():
            raise SystemExit(f"--orfs-dir is not a directory: {d}")
        found = sorted(d.glob(ORFS_DIR_GLOB))
        if not found:
            raise SystemExit(f"--orfs-dir matched no {ORFS_DIR_GLOB} under {d}")
        orfs.extend(found)
    if not orfs:
        raise SystemExit("give at least one ORF fasta: --orfs FASTA (repeatable) "
                         "and/or --orfs-dir DIR")
    return orfs


def main(argv=None) -> int:
    a = _build_parser().parse_args(argv)
    orfs = _collect_orfs(a)

    output = Path(a.output).resolve()
    staging = Path(a.staging).resolve() if a.staging else output / "_fabfos"
    staging.mkdir(parents=True, exist_ok=True)

    runtime = Runtime(a.runtime)

    common.require_method(a.require_method)

    print("=== annotation: staging inputs + planning ===")
    def _given(v):
        if not v:
            return None
        return Path(v) if a.verify_refs else v

    agent, task, stubs = generate_workflow(
        staging, orfs=orfs,
        kofam_profiles=_given(a.kofam_profiles),
        kofam_ko_list=_given(a.kofam_ko_list),
        uniref50_db=_given(a.uniref50_db),
        mnxr_lookup=_given(a.mnxr_lookup),
        label_pool=_given(a.label_pool),
        runtime=runtime, agent_env=a.agent_env, refs_root=a.refs_root, verify_refs=a.verify_refs,
    )

    if not task.ok:
        print("\nPLAN DID NOT RESOLVE:", file=sys.stderr)
        print(task.plan, file=sys.stderr)
        return 1

    common.print_plan(task)
    common.report_stubs("annotation", stubs)

    if a.dag:
        base = (common.REPO_ROOT / a.dag).resolve() if not Path(a.dag).is_absolute() else Path(a.dag)
        svg = common.render_dag(task, base)
        print(f"\nDAG -> {svg}")

    if a.run:
        print("\n=== annotation: running ===")
        results = common.run_workflow(
            agent, task, staging, threads=a.threads,
            config_file=common.resolve_nxf_config(a.config, runtime),
            gpus=_gpus(a),
        )
        print(f"results -> {results}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
