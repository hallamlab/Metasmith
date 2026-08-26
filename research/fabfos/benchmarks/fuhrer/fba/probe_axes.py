#!/usr/bin/env python3
"""Can flux balance carry any flux to each declared axis, before any sweep is run?

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/fuhrer/fba/probe_axes.py

THE FLUX BALANCE COUNTERPART OF `sinks/probe_axes.py`, and it exists for the same reason.
A sweep over a sink the model cannot make returns a column of exact zeros and a flat delta
for all 3,806 mutants, which is indistinguishable in the output from a real null. So the
three cases are separated by column here rather than inferred downstream from a float:

    absent      the metabolite is not in the frame at all
    unreachable it is, and the model cannot carry flux to it under this medium
    reachable   it can

ONE DEMAND REACTION AT A TIME, MAXIMISED AT THE GROWTH PIN, which is exactly what
`ecspr.model.fba.measure` does per test. If the pin is what makes an axis unreachable that
is worth knowing, so the unpinned maximum is reported beside it.

**CAUTION** THIS RUNS WITHOUT GAPFILL AND THE ARM SHOULD TOO. The carve removes seven
reactions -- ARAI, LACZ, LYXI, RBK_L1, RMI, RMK, RMPA -- and those seven ARE the host's
genotype: BW25113 is daraBAD, dlacZ, drhaBAD against the MG1655 model iML1515. A gapfill
draws on exactly what the carve removed, so repairing this baseline would put the strain's
own deletions back. The carve is right and the repair would be wrong.
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

import importlib.util as _ilu                                         # noqa: E402
_spec = _ilu.spec_from_file_location("_bt", HERE / "build_tables.py")
_bt = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_bt)

FIELDS = ("axis", "role", "sink_mnxm", "sink_name", "metabolite", "bridge_tier",
          "growth", "target_at_pin", "target_unpinned", "state")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", type=Path, default=_bt.CACHE / "probe_axes.tsv")
    ap.add_argument("--gapfill", action="store_true",
                    help="repair the carve before probing. Read this file's header first: "
                         "the carve here removes the host's own genotype")
    a = ap.parse_args()

    axes = pd.read_csv(_bt.AXES, sep="\t", dtype=str).fillna("")
    corpus = fba_mod.load_corpus([_bt.HOST_GPR])
    conds = {c.condition_id: c
             for c in cond_mod.read(_bt.CACHE / "conditions.parquet")}
    medium = fba_mod.load_media(_bt.CACHE / "media.parquet")[_bt.MEDIA]

    assertion = fba_mod.assertions(conds[_bt.BASELINE_ID], corpus)
    base = fba_mod.build_baseline(_bt.FRAME, assertion, None, medium, corpus=corpus,
                                  do_gapfill=a.gapfill, log=print,
                                  label=_bt.BASELINE_ID)
    model = base.model
    bridge = mnx.Release(_bt.MNX_DIR).bridge(model)

    with model:
        fba_mod.apply_medium(model, medium)
        gmax = fba_mod._growth(model)
    print(f"[probe] unperturbed growth {gmax:.6f} /h on {_bt.MEDIA}")

    rows = []
    for r in axes.to_dict("records"):
        met, tier = bridge.get(r["sink_mnxm"], create=False)
        row = dict(axis=r["axis"], role=r["role"], sink_mnxm=r["sink_mnxm"],
                   sink_name=r["sink_name"], metabolite="", bridge_tier=tier,
                   growth=round(gmax, 6), target_at_pin=None, target_unpinned=None,
                   state="absent")
        if met is not None:
            row["metabolite"] = met.id
            for pinned in (True, False):
                with model:
                    fba_mod.apply_medium(model, medium)
                    dm = fba_mod.demand_for(model, met.id)
                    bio = fba_mod._biomass(model)
                    if pinned and bio is not None:
                        bio.lower_bound = max(bio.lower_bound,
                                              _bt.GROWTH_PIN * gmax)
                    model.objective = dm.id
                    sol = model.optimize()
                    v = (float(sol.objective_value)
                         if sol.status == "optimal" else 0.0)
                key = "target_at_pin" if pinned else "target_unpinned"
                row[key] = round(v, 9)
            row["state"] = ("reachable" if (row["target_unpinned"] or 0) > 1e-9
                            else "unreachable")
        rows.append(row)

    df = pd.DataFrame(rows, columns=list(FIELDS))
    a.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.out, sep="\t", index=False)
    print(df.to_string(index=False))
    n = df.state.value_counts().to_dict()
    print(f"\n[probe] {n} -> {a.out}")
    bad = df[(df.role == "target") & (df.state != "reachable")]
    if len(bad):
        print(f"[probe] {len(bad)} TARGET axis/axes cannot be answered by flux balance "
              f"on this frame and medium: {sorted(bad.axis)}. A sweep over them returns "
              f"a column of zeros, which is not a null result.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
