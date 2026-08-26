#!/usr/bin/env python3
"""The Fuhrer deletion screen as the three tables `ecspr fba` reads.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/fuhrer/fba/build_tables.py

THE POINT IS THAT THIS IS THE SAME COHORT, NOT A SIMILAR ONE. `sweeps/sweep_fuhrer_ratio.py`
reads the same host GPR table and the same deletion table and asks a conductance question of
them; this writes down the same 3,806 constructions as an `ecspr::conditions` table, so a
mutant measured both ways is one mutant.

WHY FLUX BALANCE CAN BITE HERE AND COULD NOT ON THE EYDALLIN CLONES. Every Eydallin clone is
a chromosomal ORF the host already carries, so a curated carve is a bit-exact no-op and the
arm is vacuous by construction. A deletion is the opposite: the gene's reactions leave the
model, biomass can go to zero, and the answer is a property of the network rather than of the
convention. 1,348 of the 3,806 screened deletions remove at least one reaction of iML1515.

THREE TABLES OUT.

  conditions.parquet  the host's rows, minus the deleted ORF's. One condition per SCREENED
                      mutant, including the 2,458 whose gene carries no reaction of this
                      frame -- those come out identical to the baseline, which is the honest
                      answer (the model cannot represent that deletion) and keeps the
                      population the 3,806 the screen measured.
  gem_tests.parquet   one test per (mutant, axis). `read_against` names that axis's baseline
                      test, so `ecspr fba` builds the host ONCE and differences every mutant
                      against it.
  media.parquet       M9 glucose on this frame, which is the medium the screen used.

THE SUBTRACT GOES ON `orf`. Keying it on `mnxr` would renormalise the surviving nominators --
by as much as 41% on a fan-out of three -- and would also delete an isozyme's contribution
along with the deleted gene's. `orf` is a nominator: subtracting one leaves another gene's
row for a reaction two genes nominate, which is what a single-gene knockout actually does.

THE FRAME IS iML1515 AND NOTHING IS BORROWED. Fuhrer screened the Keio collection, whose
parent is BW25113, and iML1515 is a BW25113-curated model. The 2010 Eydallin arm had to
borrow DH1's model for AG1; the numbers here are not comparable to that arm's for a reason
that has nothing to do with the method.

**CAUTION** THE HOST GPR IS CURRENT AND THE COHORT GPR IS NOT.
`data/fabfos/runs/fuhrer_clones/gpr/` is one of the tables `tests/fabfos/test_gpr_schema.py`
still reports on a pre-schema layout, so its `llr` is a migration constant rather than a
calibrated weight. That does not reach this arm: the deletion is read off the cohort table as
a SET of ORFs and the carve runs on `min_lanes`, which is a lane count and reads no weight at
all. A `--carve-threshold` run would read one, and must wait for the rebuild.

**CAUTION** ALL SEVENTEEN DECLARED SINKS RESOLVE ONTO THIS FRAME, by direct BiGG crosswalk,
which is why every axis is emitted. That is checked here rather than assumed: an axis whose
sink the frame does not carry abstains with `_missing_target`, and a column of abstentions
reads like a null unless something says it was never askable.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


HERE = Path(__file__).resolve().parent
ROOT = _repo_root(HERE)
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model import conditions as cond_mod                        # noqa: E402
from ecspr.model import fba as fba_mod                                # noqa: E402
from ecspr.model import mnx                                           # noqa: E402
from ecspr.model import tests as test_mod                             # noqa: E402

FRAME = ROOT / "data/fabfos/originals/genomes/e_coli_k12/GEM/iML1515.json"
MNX_DIR = ROOT / "data/fabfos/originals/metanetx/4.5"
HOST_GPR = ROOT / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet"
COHORT_GPR = ROOT / "data/fabfos/runs/fuhrer_clones/gpr/gpr_gem.parquet"
CENSUS = ROOT / "data/fabfos/runs/fuhrer_clones/gpr/mutant_census.tsv"
AXES = ROOT / "data/fabfos/benchmarks/fuhrer/axes.tsv"
CACHE = HERE / "cache"

HOST_TABLE = HOST_GPR.name
BASELINE_ID = "fuhrer:BASELINE"
MEDIA = "m9"
GLC_UPTAKE = -10.0
GROWTH_PIN = 0.1

#: The denominator of the conductance arm's ratio. It is not a readout of the screen, and it
#: is carried here for the same reason it is carried there: every axis shares it, so a flux
#: to CO2 measured on the same models is the one column that says whether an axis moved or
#: the whole carbon budget did.
DENOMINATOR_AXIS = "denominator"


def conditions(cohort: pd.DataFrame, screened: pd.DataFrame) -> list:
    """The host, and the host minus one ORF, once per screened mutant."""
    background = (cond_mod.Step(table=HOST_TABLE, type="", set=(), operation="union"),)
    dropped = (cohort.groupby("condition_id").orf
               .apply(lambda s: tuple(sorted(set(s.astype(str))))).to_dict())
    out = [cond_mod.Condition(BASELINE_ID, background)]
    for cid in screened.condition_id:
        orfs = dropped.get(cid, ())
        steps = background + ((cond_mod.Step(table=HOST_TABLE, type="orf", set=orfs,
                                             operation="subtract"),) if orfs else ())
        out.append(cond_mod.Condition(cid, steps))
    return out


def targets(axes: pd.DataFrame) -> list:
    """`[(axis, role, frame metabolite id)]` -- every declared sink, on this frame.

    RESOLVED THROUGH THE REACTION-LEVEL BRIDGE rather than by matching names. A name grep
    has pinned the wrong metabolite in this project before, and the two arms have to be
    aimed at one molecule for their answers to be about one thing.
    """
    model = fba_mod.load_frame(FRAME)
    bridge = mnx.Release(MNX_DIR).bridge(model)
    out, absent = [], []
    for r in axes.to_dict("records"):
        met, tier = bridge.get(r["sink_mnxm"], create=False)
        if met is None:
            absent.append(f"{r['axis']} ({r['sink_name']}, {r['sink_mnxm']})")
            continue
        out.append((r["axis"], r["role"], met.id, tier))
    if absent:
        print(f"[axes] {len(absent)} declared sink(s) are NOT metabolites of this frame, "
              f"so flux balance cannot be asked about them at all: {absent}")
    return out


def gem_tests(conds: list, axis_targets: list) -> list:
    """One test per (condition, axis), each reading against that axis's own baseline."""
    settings = dict(frame=FRAME.stem, media=MEDIA, objective="", growth_pin=GROWTH_PIN,
                    min_lanes=1)
    out = []
    for axis, role, met, _tier in axis_targets:
        base_id = f"{axis}.{BASELINE_ID}"
        out.append(test_mod.Test(
            test_id=base_id, network=BASELINE_ID, arm="gem",
            settings=dict(settings, target_hub=met),
            meta=dict(cohort="fuhrer", is_control=1, stratum=0, n_units=0,
                      draw_id=None)))
        for c in conds:
            if c.condition_id == BASELINE_ID:
                continue
            out.append(test_mod.Test(
                test_id=f"{axis}.{c.condition_id}", network=c.condition_id, arm="gem",
                read_against=base_id, settings=dict(settings, target_hub=met),
                # A DELETION IS ONE UNIT WHETHER OR NOT IT REACHES THE FRAME. `n_units`
                # is what the null is size-matched on, and the matched null for this
                # cohort is "delete one random gene" -- so it is 1 for every mutant, and
                # a mutant whose gene carries no reaction of this frame is a real
                # perturbation that cannot move the measurement, which is a control.
                meta=dict(cohort="fuhrer", is_control=int(len(c.steps) == 1),
                          stratum=1, n_units=1, draw_id=None)))
    return out


def media_rows() -> list:
    """M9 glucose on this frame: the model's own minimal medium at a fixed uptake."""
    model = fba_mod.load_frame(FRAME)
    med = dict(model.medium)
    med["EX_glc__D_e"] = -GLC_UPTAKE if GLC_UPTAKE > 0 else GLC_UPTAKE
    return [dict(media=MEDIA, exchange=rid, lower_bound=(-abs(v)), upper_bound=1000.0)
            for rid, v in sorted(med.items())]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", type=Path, default=CACHE)
    ap.add_argument("--roles", nargs="+", default=["target", "control"],
                    help="which declared axes to emit. The precursor controls are the "
                         "sharpest control this panel has: a probe ranking a target and "
                         "its own immediate precursor alike has ranked the module rather "
                         "than the metabolite")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)

    axes = pd.read_csv(AXES, sep="\t", dtype=str).fillna("")
    axes = axes[axes.role.isin(a.roles) | (axes.axis == DENOMINATOR_AXIS)]
    cohort = pd.read_parquet(COHORT_GPR)
    census = pd.read_csv(CENSUS, sep="\t").drop_duplicates("gene")
    screened = census[census.screened.astype(bool)]

    conds = conditions(cohort, screened)
    perturbing = sum(1 for c in conds if len(c.steps) > 1)
    print(f"[conditions] {len(conds):,} ({perturbing:,} remove at least one reaction of "
          f"this frame, {len(conds) - 1 - perturbing:,} are the baseline over again "
          f"because the gene carries none)")

    tgt = targets(axes)
    print(f"[axes] {len(tgt)} of {len(axes)} declared sinks are on this frame")
    ts = gem_tests(conds, tgt)
    rows = media_rows()

    cond_mod.write(conds, a.out / "conditions.parquet")
    test_mod.write(ts, a.out / "gem_tests.parquet", "gem")
    fba_mod.write_media(rows, a.out / "media.parquet")
    print(f"[tests] {len(ts):,} -> {a.out / 'gem_tests.parquet'}")
    print(f"[media] {len(rows)} bounds -> {a.out / 'media.parquet'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
