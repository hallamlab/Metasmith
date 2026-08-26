#!/usr/bin/env python3
"""Rank the cohort from ONE baseline LP, using reduced costs instead of 3,096 re-solves.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin_fba/reduced_costs.py

`force` answers a clone by re-solving the whole model with that clone's reactions pinned
at twice their reference flux.  A reduced cost answers the same question analytically: it
is the rate at which the objective moves per unit of forced flux, read off the baseline
solve's dual.  One LP ranks every clone, where `force` needs one LP per clone per medium
per pin, and the dual carries MAGNITUDE where the sign accuracy carries none.

**CAUTION** A reduced cost is a LOCAL derivative at the baseline vertex, and `force`
demands a step of `FOLD x max(|v_ref|, VMIN)`, which is not local.  The two answer
different questions and the reduced cost is the cheaper, weaker one.  It is reported
because it is the only statistic in this arm that has ever come back significant.

**CAUTION** The baseline pFBA state this reads is DEGENERATE -- two alternative optima of
identical total flux differing on 63 of 2,196 reactions -- so the dual is one of several.
`fba_cohort.py`'s docstring carries the measurement.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))
import common as C  # noqa: E402

from ecspr.model import conditions as cond_mod  # noqa: E402
from ecspr.model import fba as F  # noqa: E402
from ecspr.model import mnx as MX  # noqa: E402
from ecspr.model import tests as test_mod  # noqa: E402
from ecspr.model.probes import load_corpus  # noqa: E402

TABLES = C.OUT / "tables"
FRAME = Path(C.GEM_DH1).stem


def baseline_tests(channel: str, medium: str, pin: float) -> list:
    return [test_mod.Test(
        test_id=f"{channel}.{medium}.p{pin}.baseline", network=f"{channel}:baseline",
        arm="gem", settings=dict(frame=FRAME, media=medium, target_hub=C.GLYCOGEN_MET,
                                 growth_pin=pin, min_lanes=1))]


def clone_reactions(channel: str, model, corpus, conds, xwalk) -> dict:
    """lowercased condition_id -> the frame reactions that clone asserts."""
    background = {s.table for s in conds[f"{channel}:baseline"].steps}
    by_mnxr = {}
    for rid, mnxr in xwalk.items():
        if rid in model.reactions:
            by_mnxr.setdefault(mnxr, rid)
    for r in model.reactions:
        if r.id.startswith(MX.HET_PREFIX):
            by_mnxr.setdefault(r.id[len(MX.HET_PREFIX):], r.id)

    out = {}
    for cid, cond in conds.items():
        if not cid.startswith(f"{channel}:") or cid.endswith(":baseline"):
            continue
        alone = cond_mod.Condition(
            cid, tuple(s for s in cond.steps if s.table not in background))
        a = F.assertions(alone, corpus, min_lanes=1)
        hit = [x for x in a.ids if x in model.reactions]
        covered = {xwalk.get(x) for x in hit}
        hit += [by_mnxr[x] for x in a.mnxrs if x in by_mnxr and x not in covered]
        out[cid.split(":", 1)[1].lower()] = sorted(set(hit))
    return out


def main() -> int:
    from scipy.stats import spearmanr

    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", choices=("gem", "denovo"), default="gem")
    ap.add_argument("--medium", default="kornberg")
    ap.add_argument("--pin", type=float, default=0.1)
    ap.add_argument("--out", type=Path, default=C.OUT / "reduced_costs.tsv")
    a = ap.parse_args()

    tables = (sorted(TABLES.glob("*_gem.parquet"))
              + sorted(TABLES.glob("*_denovo.parquet")))
    corpus = load_corpus(tables)
    conds = {c.condition_id: c for c in cond_mod.read(TABLES / "conditions.parquet")}
    media = F.load_media(TABLES / "media.parquet")

    df = F.run([C.GEM_DH1], tables, list(conds.values()),
               baseline_tests(a.channel, a.medium, a.pin), media,
               release=MX.Release(C.MNX, cache=C.CACHE), emit="reduced-costs",
               gapfill_media="m9", census_out=None, log=lambda m: None)
    rc = {r.readout[3:]: r.value for r in df.itertuples()
          if str(r.readout).startswith("rc:")}

    model = F.load_frame(C.GEM_DH1)
    xwalk, _cen = F.crosswalk(model, corpus)
    built = C.OUT / "models" / (
        "ag1_gem.json" if a.channel == "gem" else "ag1_denovo_l1.json")
    import cobra
    host = cobra.io.load_json_model(str(built))
    reactions = clone_reactions(a.channel, host, corpus, conds, xwalk)

    meas = C.measured()
    rows = []
    for r in meas.to_dict("records"):
        rids = reactions.get(r["_k"], [])
        costs = [rc.get(x, 0.0) for x in rids]
        if not rids:
            continue
        rows.append(dict(condition_id=r["condition_id"], gene=r["gene"],
                         measured_pct_wt=r["pct_wt"], n_reactions=len(rids),
                         rc_sum=sum(costs), rc_max=max(costs, key=abs) if costs else 0.0,
                         n_nonzero=sum(1 for c in costs if abs(c) > 1e-12)))
    out = pd.DataFrame(rows)
    out.to_csv(a.out, sep="\t", index=False)

    # SIGNED AND UNSIGNED ARE DIFFERENT CLAIMS. Signed asks whether the dual predicts the
    # direction a clone moves glycogen; unsigned asks the weaker and more interesting
    # question of whether it predicts how FAR the clone moves it, in either direction.
    out["abs_effect"] = (out.measured_pct_wt - 100.0).abs()
    for col in ("rc_sum", "rc_max"):
        out[f"{col}_abs"] = out[col].abs()
    print(f"{a.channel} / {a.medium} / pin {a.pin}: {len(out)} clones with reactions, "
          f"{int((out.n_nonzero > 0).sum())} with a non-zero reduced cost")
    live = out[out.n_nonzero > 0]
    for col, against in (("rc_sum", "measured_pct_wt"), ("rc_max", "measured_pct_wt"),
                         ("rc_sum_abs", "abs_effect"), ("rc_max_abs", "abs_effect")):
        for name, d in (("all clones", out), ("non-zero only", live)):
            if len(d) < 3:
                continue
            rho, p = spearmanr(d[col], d[against])
            print(f"  {col:11s} vs {against:16s} {name:15s} n={len(d):3d} "
                  f"rho={rho:+.3f} p={p:.4f}")
    print(f"-> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
