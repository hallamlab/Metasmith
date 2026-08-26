#!/usr/bin/env python3
"""Conductance against flux balance on the Eydallin 2007 deletions, one drawn null.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/eydallin/fba/head_to_head.py \
            --ecspr-obs <two-point results> --ecspr-null <two-point null results> \
            --gem-obs   <fba results>       --gem-null   <fba null results>

BOTH ARMS READ ONE CONDITIONS TABLE AND ONE DRAWN POOL. `ecspr draw --like-tests
<ecspr_tests> --like-gem-tests <gem_tests>` emitted a single conditions table -- checked
byte-identical to the one measured before the second arm was added -- so the 1,500 random
single-gene deletions here are the SAME 1,500 constructions on both sides. Two arms on two
pools have error bars that are adjacent rather than comparable, and that is the whole
reason the draw is arm-neutral.

THE STATISTIC IS THE RATIO ON BOTH SIDES, and it has to be, because neither arm's raw
readout is comparable to the other's. Conductance reports C(glucose->glycogen) over
C(glycogen->pyruvate); flux balance reports max flux to glycogen over max flux to pyruvate
at a common growth pin. Their quotients are both dimensionless percent changes against the
same baseline condition, which is the one column that crosses.

WHY THE RATIO IS FORMED HERE AND NOT BY THE SCORER. `two-point` names its readout `total`
for every test, so the two legs of one ratio share `scoring.KEYS["ecspr"]` and would pool
-- the same defect the gem key had before `target` entered it. Forming the quotient first
sidesteps it and is what the arm reports anyway; the two legs are never scored apart.

A LETHAL DRAW IS AN ABSTENTION, NOT A ZERO. Flux balance abstains where growth is zero
(107 of 1,500 draws); conductance always solves. The pools are therefore not the same size
and each arm's `null_n` is printed rather than assumed equal.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model import scoring                                        # noqa: E402
from ecspr.model import tests as test_mod                              # noqa: E402

BASELINE = "eydallin2007:BASELINE"
NULL_BASELINE = "null:baseline"


#: `(source_hub, sink_hub)` for each leg of the ratio. THE LEG IS READ OFF THE TESTS
#: TABLE, never off the test id: an observed test is named `<leg>.<condition>` and a drawn
#: one `null:t<i>:s<n>:<j>`, where `i` is the prototype's position in a sorted-by-settings
#: list. Which prototype is which leg is then a lexicographic accident of two MNXM ids.
LEGS = {"gg": ("MNXM1364061", "MNXM738130"),
        "gp": ("MNXM738130", "MNXM23")}


def _ratio_rows(df: pd.DataFrame, tests: pd.DataFrame, num: str, den: str,
                readout: str, probe: str) -> pd.DataFrame:
    """Two legs -> one `ratio` measurement per condition, in the ecspr result schema."""
    want = {v: k for k, v in LEGS.items()}
    leg_of, net_of = {}, {}
    for r in tests.to_dict("records"):
        k = want.get((str(r.get("source_hub") or ""), str(r.get("sink_hub") or "")))
        if k:
            leg_of[str(r["test_id"])] = k
            net_of[str(r["test_id"])] = str(r["network"])
    m = df[df.readout == readout].copy()
    m["leg"] = m.test_id.map(leg_of)
    m["network"] = m.test_id.map(net_of)
    m = m.dropna(subset=["leg", "network"])
    w = m.pivot_table(index="network", columns="leg", values="value")
    if num not in w.columns or den not in w.columns:
        raise SystemExit(f"legs {num!r}/{den!r} not in {sorted(w.columns)}; the tests "
                         f"table declares {sorted(set(leg_of.values()))}")
    r = w[num] / w[den]
    return pd.DataFrame(dict(test_id=r.index, probe=probe, orientation="as_written",
                             element="C", readout="ratio", value=r.to_numpy()))


def _gem_ratio(df: pd.DataFrame, tests: pd.DataFrame, num: str,
               den: str) -> pd.DataFrame:
    """THE NETWORK COMES FROM THE TESTS TABLE, not from the test id. An observed gem test
    is `<axis>.<condition>` and a drawn one is `null:t<i>:s<n>:<j>` with no dot in it at
    all, so splitting on `.` silently emptied the whole null side."""
    net_of = dict(zip(tests.test_id.astype(str), tests.network.astype(str)))
    m = df[df.readout == "target"].copy()
    m["network"] = m.test_id.map(net_of)
    m = m.dropna(subset=["network"])
    w = m.pivot_table(index="network", columns="target", values="value")
    missing = [c for c in (num, den) if c not in w.columns]
    if missing:
        raise SystemExit(f"target axes {missing} not in {sorted(w.columns)}")
    r = w[num] / w[den]
    return pd.DataFrame(dict(test_id=r.index, probe="gem", orientation="as_written",
                             element="C", readout="ratio", value=r.to_numpy()))


def _tests(ids, baseline: str, meta_of: dict) -> list:
    out = [test_mod.Test(test_id=baseline, network=baseline, arm="ecspr",
                         meta=dict(is_control=1, n_units=0))]
    for t in ids:
        if t == baseline:
            continue
        out.append(test_mod.Test(test_id=t, network=t, arm="ecspr",
                                 read_against=baseline, meta=meta_of.get(t, {})))
    return out


def score_arm(obs: pd.DataFrame, nul: pd.DataFrame, meta_of: dict) -> pd.DataFrame:
    ts = _tests(obs.test_id, BASELINE, meta_of)
    ts += _tests(nul.test_id, NULL_BASELINE, {t: dict(stratum=1, n_units=1)
                                              for t in nul.test_id})
    return scoring.score(obs, nul, tests=ts, arm="ecspr")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--ecspr-obs", type=Path, required=True)
    ap.add_argument("--ecspr-null", type=Path, required=True)
    ap.add_argument("--gem-obs", type=Path, required=True)
    ap.add_argument("--gem-null", type=Path, required=True)
    ap.add_argument("--ecspr-tests", type=Path, required=True,
                    help="the observed ecspr_tests table -- which test is which leg")
    ap.add_argument("--ecspr-null-tests", type=Path, required=True)
    ap.add_argument("--gem-tests", type=Path, required=True)
    ap.add_argument("--gem-null-tests", type=Path, required=True)
    ap.add_argument("--phenotypes", type=Path,
                    default=ROOT / "data/fabfos/benchmarks/eydallin_2007/Y/measured_glycogen.tsv")
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parent
                    / "cache" / "head_to_head.tsv")
    a = ap.parse_args()

    et = pd.read_parquet(a.ecspr_tests)
    ent = pd.read_parquet(a.ecspr_null_tests)
    gt = pd.read_parquet(a.gem_tests)
    gnt = pd.read_parquet(a.gem_null_tests)
    meta = {}
    arms = {}
    for name, obs_p, nul_p, mk_o, mk_n in (
            ("ecspr", a.ecspr_obs, a.ecspr_null,
             lambda d: _ratio_rows(d, et, "gg", "gp", "total", "two-point"),
             lambda d: _ratio_rows(d, ent, "gg", "gp", "total", "two-point")),
            ("gem", a.gem_obs, a.gem_null,
             lambda d: _gem_ratio(d, gt, "glycogen_c", "pyr_c"),
             lambda d: _gem_ratio(d, gnt, "glycogen_c", "pyr_c"))):
        obs = mk_o(pd.read_parquet(obs_p))
        nul = mk_n(pd.read_parquet(nul_p))
        nul = nul[nul.test_id.astype(str).str.startswith("null:")]
        arms[name] = score_arm(obs, nul, meta)

    ph = pd.read_csv(a.phenotypes, sep="\t")
    ph["k"] = ph.condition_id.str.lower()
    rows = []
    for name, s in arms.items():
        s = s.assign(arm=name)
        s["k"] = s.test_id.str.lower()
        rows.append(s.merge(ph[["k", "pct_wt"]], on="k", how="left"))
    d = pd.concat(rows, ignore_index=True)
    d["excess"] = d.pct_wt >= 100
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.unlink(missing_ok=True)
    d.to_csv(a.out, sep="\t", index=False)

    pd.set_option("display.width", 220)
    print(f"\n{'='*92}\nEYDALLIN 2007 DELETIONS -- both arms, one drawn pool of 1,500\n{'='*92}")
    for name, s in arms.items():
        sub = d[d.arm == name]
        moved = sub[sub.delta.abs() > 1e-9]
        print(f"\n  {name}: {len(sub)} mutants | null_n {int(sub.null_n.iloc[0])} "
              f"(sd {sub.null_sd.iloc[0]:.4g}, mean {sub.null_mean.iloc[0]:+.4g})")
        print(f"      move at all {len(moved)} | q_bh < 0.05 {int((sub.q_bh < 0.05).sum())} "
              f"| p_stratum < 0.05 {int((sub.p_stratum < 0.05).sum())}")
        top = sub.reindex(sub.delta.abs().sort_values(ascending=False).index).head(6)
        print(top[["test_id", "delta", "z", "p_emp", "q_bh", "pct_wt"]]
              .to_string(index=False, float_format=lambda v: f"{v:.4g}"))
    w = d.pivot_table(index="test_id", columns="arm", values="delta")
    both = w.dropna()
    live = both[(both.abs() > 1e-9).any(axis=1)]
    print(f"\n  CONCORDANCE over the {len(live)} mutants either arm moves:")
    print(f"      both move   {int(((live.ecspr.abs() > 1e-9) & (live.gem.abs() > 1e-9)).sum())}")
    print(f"      ecspr only  {int(((live.ecspr.abs() > 1e-9) & (live.gem.abs() <= 1e-9)).sum())}")
    print(f"      gem only    {int(((live.ecspr.abs() <= 1e-9) & (live.gem.abs() > 1e-9)).sum())}")
    agree = int((np.sign(live.ecspr) == np.sign(live.gem)).sum())
    print(f"      same sign   {agree}/{len(live)}")
    print(f"      Spearman between the two arms' deltas: "
          f"{live.ecspr.corr(live.gem, method='spearman'):+.4f}")
    print(f"\n-> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
