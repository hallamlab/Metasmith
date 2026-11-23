#!/usr/bin/env python3
"""Dry-run solves of short-read and hybrid samples in one plan: does a hybrid target steer the downstream?

One pool holds two short-PE samples and two hybrid samples (short PE + Nanopore, E5's fences). E5's short lane
(bbduk -> MEGAHIT) and hybrid lane (MEGAHIT draft -> OPERA-MS) load together, with assembly_stats, Prodigal and
MetaBAT2 downstream. Each variant plans once and reports, per step, the cases it serves and the assembly type it
reads. A per-case variant plans the short samples and the hybrid samples as two cases, each with its own targets. Build the library first: python -m metasmith build all -t src/metasmith_libraries/data_types
-t research/hybrid_solve/library/data_types -u research/hybrid_solve/library/resources/hyb -r <transforms/hyb or hyb_fenced>.
Usage: [HS_ONLY=short|hybrid] solve.py [variant ...]
"""

import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "research" / "metasmith_benchmark" / "drivers"))

import _common as c  # noqa: E402
from metasmith.agents import CaseViews, plan_views  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, DataInstanceLibrary, Runtime, Source, TargetBuilder, TransformInstanceLibrary,
)

LIB = HERE / "library"
MLIB = REPO / "src" / "metasmith_libraries"
CACHE = HERE / ".cache"
TYPE_LIBS = [MLIB / "data_types" / "sequences.yml", LIB / "data_types" / "hyb.yml"]
SAMPLES = [("S1", False), ("S2", False), ("H1", True), ("H2", True)]
# HS_ONLY=short|hybrid declares one shape alone, to diagnose a failing case.
if os.environ.get("HS_ONLY"):
    SAMPLES = [x for x in SAMPLES if x[1] == (os.environ["HS_ONLY"] == "hybrid")]
ASSEMBLY_SLOTS = {"bbduk_hybrid", "megahit", "assembly_stats", "prodigal", "metabat2", "opera_ms"}


def dummy(name):
    p = CACHE / "inputs" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        p.write_text(f"@{name}\nACGT\n+\nIIII\n")
    return p


def declare_givens(smith, nano_type="hyb::nanopore_reads", hybrid_short_type="sequences::short_reads_pe", tag="base",
                   one_study=False, samples=None):
    # A study per shape, or one study for every sample with one_study.
    givens = smith.PoolGivens()
    studies = {}
    for sid, hybrid in samples or SAMPLES:
        key = "mixed" if one_study else ("hybrid" if hybrid else "short")
        if key not in studies:
            studies[key] = c.add_value(givens, f"hs/study/{key}", {"study": key}, "sequences::study")
        study = studies[key]
        # The study is in every name: the pool matches a given by name alone, and keeps the first import's parents.
        ns = f"hs/{key}/{sid}"
        meta = c.add_value(givens, f"{ns}/read_metadata",
                           {"sample": sid, "parity": "paired", "length_class": "short"},
                           "sequences::read_metadata", parents=[study])
        short_type = hybrid_short_type if hybrid else "sequences::short_reads_pe"
        c.add_file(givens, f"{ns}/reads/{short_type}", dummy(f"{sid}_R.fq.gz"), short_type, parents=[meta])
        if hybrid:
            c.add_file(givens, f"{ns}/long/{nano_type}", dummy(f"{sid}_ont.fq.gz"), nano_type, parents=[meta])
    return c.cite(givens, CACHE / f"inputs_{tag}{os.environ.get('HS_ONLY', '')}.xgdb", TYPE_LIBS, ensure=True)


def transforms(hyb_view=None, megahit=True, hyb_dir="hyb"):
    asm = {"seqkit_reads.py", "bbduk.py", "assembly_stats.py"} | ({"megahit.py"} if megahit else set())
    hyb = TransformInstanceLibrary.Load(LIB / "transforms" / hyb_dir)
    return [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "assembly").AsView({Path(p) for p in asm}),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics").AsView(
            {Path("prodigal.py"), Path("binning/metabat2.py")}),
        hyb if hyb_view is None else hyb.AsView({Path(p) for p in hyb_view}),
    ]


def downstream(t, asm):
    t.Add("sequences::assembly_stats", parents=[asm])
    t.Add("sequences::orfs", parents=[asm])
    t.Add("sequences::metabat2_bin_fasta", parents=[asm])


def targets_generic():
    t = TargetBuilder()
    downstream(t, t.Add("sequences::assembly"))
    return t


def targets_hybrid():
    t = TargetBuilder()
    downstream(t, t.Add("hyb::opera_ms_assembly"))
    return t


def targets_both():
    t = TargetBuilder()
    asm = t.Add("sequences::megahit_assembly")
    t.Add("hyb::opera_ms_assembly")
    downstream(t, asm)
    return t


def targets_generic_and_hybrid():
    t = TargetBuilder()
    downstream(t, t.Add("sequences::assembly"))
    downstream(t, t.Add("hyb::opera_ms_assembly"))
    return t


def short_case(label):
    return "hybrid" if "nanopore" in label else ("short" if "short_reads" in label else label[:20])


def report(name, task, dt):
    plan = task.plan
    print(f"\n### {name}: ok={task.ok} solve={dt:.1f}s steps={len(plan.steps)} cases={[short_case(x) for x in plan.cases]}")
    if not task.ok:
        print(f"  dropped targets: {plan.dropped_targets}  dropped samples: {plan.dropped_samples}")
        for h in plan.hints or []:
            print(f"  [hint {getattr(h, 'kind', '?')}] {getattr(h, 'target', '')}: {getattr(h, 'message', h)}")
            for line in (getattr(h, "chain", None) or []) + (getattr(h, "near_misses", None) or []):
                print(f"      {line}")
    for s in plan.steps:
        stem = Path(s.transform._path).stem
        uses = sorted({i.dtype_name for i in s.uses if i.dtype_name.split("::")[0] in ("sequences", "hyb")
                       and ("assembly" in i.dtype_name or "draft" in i.dtype_name or "reads" in i.dtype_name)})
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        mark = "*" if stem in ASSEMBLY_SLOTS else " "
        print(f" {mark}{s.order:>2}. {stem:16s} cases={[short_case(x) for x in s.cases]} uses={uses} -> {prods}")
    for tg in plan.targets:
        print(f"  target {tg.name:34s} <- step {tg.producing_step.order} {Path(tg.producing_step.transform._path).stem}")
    if plan.given:
        from collections import Counter
        print(f"  givens used: {dict(Counter(i.dtype_name for i in plan.given))}")


def resources():
    return [DataInstanceLibrary.Load(MLIB / "resources" / "env"),
            DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
            DataInstanceLibrary.Load(LIB / "resources" / "hyb")]


def planned(name, plan):
    t0 = time.time()
    try:
        task = plan()
    except Exception as e:  # a refusal is a result here
        print(f"\n### {name}: RAISED after {time.time() - t0:.1f}s: {type(e).__name__}: {e}")
        return None
    report(name, task, time.time() - t0)
    return task


def solve(smith, name, inputs, trs, targets):
    return planned(name, lambda: smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=resources(), transforms=trs, targets=targets))


def solve_per_case(smith, name, inputs, trs, short_targets, hybrid_targets):
    by_shape = {"short": [], "hybrid": []}
    for sample in inputs.AsSamples("sequences::read_metadata"):
        hybrid = any(dtype == "hyb::nanopore_reads" for _, dtype, _ in sample.Iterate())
        by_shape["hybrid" if hybrid else "short"].append(sample)
    cases = [CaseViews(shape, samples, [], targets())
             for (shape, samples), targets in zip(by_shape.items(), (short_targets, hybrid_targets))
             if samples]
    return planned(name, lambda: plan_views(cases, resources=resources(), transforms=trs))


def targets_megahit():
    t = TargetBuilder()
    downstream(t, t.Add("sequences::megahit_assembly"))
    return t


VARIANTS = {
    "V0": dict(targets=targets_generic, givens=dict(one_study=True, tag="mixed")),
    "V0h": dict(targets=targets_hybrid, givens=dict(one_study=True, tag="mixed")),
    "V1": dict(targets=targets_generic),
    "V2": dict(targets=targets_hybrid),
    "V3": dict(targets=targets_both),
    # Each case its own target, still one solve.
    "V4a": dict(per_case=(targets_megahit, targets_hybrid)),
    "V4b": dict(per_case=(targets_generic, targets_hybrid)),
    # One short sample and one hybrid sample as two cases: the generic lane for both, OPERA-MS for the hybrid.
    "V5": dict(per_case=(targets_generic, targets_generic_and_hybrid),
               givens=dict(samples=[("S1", False), ("H1", True)], tag="v5")),
    # One shared generic target; the hybrid sample's short reads sit behind a fence MEGAHIT cannot cross.
    "V4c": dict(targets=targets_generic, transforms=dict(hyb_dir="hyb_fenced"),
                givens=dict(hybrid_short_type="hyb::hybrid_short_reads", tag="fenced")),
}


def main():
    want = sys.argv[1:] or list(VARIANTS)
    smith = Agent(home=Source.FromLocal(CACHE / "dryrun_home"), runtime=Runtime.APPTAINER)
    for name in want:
        v = VARIANTS[name]
        inputs = declare_givens(smith, **v.get("givens", {}))
        trs = transforms(**v.get("transforms", {}))
        if "per_case" in v:
            solve_per_case(smith, name, inputs, trs, *v["per_case"])
        else:
            solve(smith, name, inputs, trs, v["targets"]())


if __name__ == "__main__":
    main()
