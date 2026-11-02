# THE QUESTION -- does calibrating the lane weights change what ECSPr predicts about the
# Eydallin glycogen screen, and is the change an improvement?
#
# SCOPE   The AG1 de-novo channel, which is the only eydallin path that reads the weight
#         column at all. See the CAUTION in `pooling_sanity.py` for why the others
#         cannot answer this.
# INPUT   two `pooling_sanity.py --cohort --measured` runs, one `--legacy`
# METHOD  Join on condition, compare the deltas, and correlate each against the
#         digitised Fig. 1 phenotype on magnitude.
# ENV     mamba run -n msm python \
#             research/fabfos/annotation_lanes/calibration/before_after_eydallin.py \
#             --before <before.json> --after <after.json>
# OUT     out/eydallin_before_after.tsv
#
# HOW TO PRODUCE THE TWO INPUTS
#
#   reparse_run.py --host e_coli_dh1 --out $T
#   derive_ag1_denovo.py --from $T/e_coli_dh1/gpr_denovo.parquet   # AG1 borrows DH1
#   reparse_run.py --host eydallin_clones --orfs <clone faa> \
#       --graft-onto data/fabfos/runs/eydallin_clones/gpr/gpr_denovo.parquet --out $T
#   pooling_sanity.py --cohort --measured --legacy --out before.json
#   pooling_sanity.py --cohort --measured --host-gpr ... --clone-gpr ... --out after.json
#
# **CAUTION** THE COMPARISON IS UNDERPOWERED AND THE TIE COUNT SAYS SO. Roughly half the
# cohort produces a delta of exactly zero -- a clone adds a second copy of reactions the
# host already asserts, and pooling an extra assertion onto a saturated reaction moves it
# by nothing. A Spearman over 54 values of which 24 to 30 are tied at zero cannot resolve
# a modest reweighting either way. Read the null as "this benchmark cannot see it", not
# as "there is nothing there".
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _cal as C  # noqa: E402

MEASURED = C.REPO / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
WT = 100.0
FLOOR = 1e-9   # below this a delta is float noise, not a prediction


def load(path: Path) -> tuple[dict, pd.DataFrame]:
    doc = json.loads(Path(path).read_text())
    return doc["base"], pd.DataFrame(doc["conditions"]).set_index("label")


def measured() -> pd.DataFrame:
    m = pd.read_csv(MEASURED, sep="\t")
    m["label"] = "eydallin:" + m["gene"].astype(str)
    # MAGNITUDE, not sign. Effective conductance is non-decreasing in every edge
    # conductance, so selecting a clone's rows can only raise this readout and a signed
    # correlation is unanswerable by construction.
    m["mag"] = (m["pct_wt"] / WT).apply(lambda v: abs(np.log2(v)))
    return m.set_index("label")[["pct_wt", "mag"]]


def score(d: pd.DataFrame, m: pd.DataFrame, tag: str) -> dict:
    j = d.join(m, how="inner").dropna(subset=["mag"])
    nz = j[j["d_rel"].abs() > FLOOR]
    r_all = spearmanr(j["d_rel"], j["mag"])
    r_nz = spearmanr(nz["d_rel"], nz["mag"]) if len(nz) >= 3 else None
    top4 = set(j["d_rel"].nlargest(4).index)
    meas4 = set(j["mag"].nlargest(4).index)
    return dict(weighting=tag, n=len(j), n_moved=len(nz), n_tied_at_zero=len(j) - len(nz),
                rho_all=round(float(r_all.statistic), 4),
                p_all=round(float(r_all.pvalue), 4),
                rho_moved=round(float(r_nz.statistic), 4) if r_nz else None,
                p_moved=round(float(r_nz.pvalue), 4) if r_nz else None,
                top4_predicted=";".join(sorted(x.split(":")[1] for x in top4)),
                top4_measured=";".join(sorted(x.split(":")[1] for x in meas4)),
                top4_overlap=len(top4 & meas4))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--before", type=Path, required=True)
    ap.add_argument("--after", type=Path, required=True)
    a = ap.parse_args()

    bb, bd = load(a.before)
    ab, ad = load(a.after)
    m = measured()

    # THE MEMBERSHIP INVARIANT. A weighting change moves the value attached to a
    # reaction and never which reactions are in the graph. If this fails, something
    # other than the weighting changed and nothing below is a comparison.
    if bb["n_reactions"] != ab["n_reactions"]:
        raise SystemExit(
            f"[before/after] {bb['n_reactions']:,} reactions before and "
            f"{ab['n_reactions']:,} after. A weighting change cannot do that -- the two "
            f"runs are not reading the same tables")
    print(f"reactions weighted   {bb['n_reactions']:,} both  (membership invariant holds)")
    print(f"base conductance     {bb['conductance']:.6g} -> {ab['conductance']:.6g}")
    print(f"sum(E)               {bb['E_sum']:.1f} -> {ab['E_sum']:.1f}")

    j = (bd[["d_rel"]].rename(columns={"d_rel": "before"})
         .join(ad[["d_rel"]].rename(columns={"d_rel": "after"}), how="outer"))
    j["rank_before"] = j["before"].rank(ascending=False)
    j["rank_after"] = j["after"].rank(ascending=False)
    j = j.join(m, how="left")
    rho = spearmanr(j["before"], j["after"]).statistic
    print(f"rank agreement       rho {rho:.4f} between the two weightings\n")

    panel = pd.DataFrame([score(bd, m, "legacy"), score(ad, m, "calibrated")])
    print(panel.to_string(index=False))

    j = j.sort_values("before", ascending=False)
    out = C.OUT / "eydallin_before_after.tsv"
    C.OUT.mkdir(parents=True, exist_ok=True)
    j.reset_index().to_csv(out, sep="\t", index=False)
    panel.to_csv(C.OUT / "eydallin_before_after_panel.tsv", sep="\t", index=False)
    print(f"\ntop of the ranking, both weightings:")
    print(j.head(8)[["before", "after", "rank_before", "rank_after", "pct_wt"]]
          .to_string(float_format=lambda v: f"{v:.6g}"))
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
