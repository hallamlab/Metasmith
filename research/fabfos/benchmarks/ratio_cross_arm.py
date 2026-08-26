#!/usr/bin/env python3
"""The three ratio arms on one cut: how much of each screen is metabolically active,
what carries the response, and whether the sign is right.

    mamba run -n msm python research/fabfos/benchmarks/ratio_cross_arm.py

Each arm already ships a classifier report and each prints the same reach 2x2, the same
mid-rank Mann-Whitney AUC and the same reaction-count size control. None of them prints
the REACTION-level cut, which is the question this file answers: of the positives a
conductance probe can see at all, how many actually move, how few reactions carry that
movement, and does the movement point the way the phenotype says it should.

THE CUT IS ONE DEFINITION APPLIED THREE TIMES, and the whole point is that it is the same
one, so read the columns across arms rather than down:

    metabolically active   a positive carrying at least one atom-mapped reaction (n_rxn > 0).
                           This is the only population a carbon-flow probe has a mechanism
                           for; the rest are regulators, transporters and hypotheticals a
                           stoichiometric model has no representation for.
    responder              |delta_ratio_pct| > 1e-12. Separates "the probe sees it and it
                           does not move" from "the probe cannot see it", which the AUC
                           conflates and which decide different things.
    mover                  |delta_ratio_pct| >= 1%. Every arm shows an order-of-magnitude
                           gap at roughly this point; below it the values are solver dust
                           (eydallin's 23 responders run down to 6e-7%).
    carrying reactions     the union of the movers' `rxns`. A gene is not a lever, its
                           reactions are, and two genes nominating one shared reaction is
                           one measurement rather than two.

THE SIGN NEEDS A DECLARED EXPECTATION AND ONLY TWO ARMS HAVE ONE. eydallin's labels are
signed (glycogen_excess vs glycogen_deficient) and fang's are (up vs down vs flat, flat
excluded because it expects nothing). woodruff's are not: every positive is "tolerant",
so there is no direction to be right or wrong about and this file reports the raw up/down
split instead of manufacturing an agreement statistic. Do not add one -- a one-sided label
set makes any such number a restatement of the probe's own bias.

REPORT SIGN AGREEMENT AGAINST THE MAJORITY-CLASS BASE RATE, NEVER BARE. Every one of these
label sets is lopsided (eydallin's positives are 74% deficient, fang's signed ones 70%
down), so a coin that always guesses the majority scores 70-76% and beats most of the real
numbers here. The bare percentage reads as a result and is not one.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import binomtest, fisher_exact, mannwhitneyu

ROOT = Path(__file__).resolve().parents[3]
RUNS = ROOT / "data/fabfos/runs"
OUT = RUNS / "ratio_cross_arm"

MOVER_PCT = 1.0

SWEEPS = {
    "eydallin": dict(
        ratio="C(glucose->glycogen) / C(glycogen->pyruvate)",
        host="e_coli_ag1",
        files={"gem": RUNS / "aska/ecspr/aska_ratio_sweep_gem_e_coli_ag1_fold2.0_C.tsv",
               "denovo": RUNS / "aska/ecspr/aska_ratio_sweep_denovo_e_coli_ag1_fold2.0_C.tsv"},
    ),
    "fang": dict(
        ratio="C(acetyl-CoA->axis sink) / C(acetyl-CoA->CO2)",
        host="e_coli_k12",
        files={"gem": RUNS / "fang/ecspr/fang_ratio_sweep_gem_e_coli_k12_fold2.0_C.tsv",
               "denovo": RUNS / "fang/ecspr/fang_ratio_sweep_denovo_e_coli_k12_fold2.0_C_lanes2.tsv"},
    ),
    "woodruff": dict(
        ratio="C(pyruvate->ethanol) / C(pyruvate->oxaloacetate)",
        host="e_coli_lw06",
        files={"gem": RUNS / "woodruff_clones/ecspr/woodruff_ratio_sweep_gem_e_coli_lw06_fold2.0_C.tsv",
               "denovo": RUNS / "woodruff_clones/ecspr/woodruff_ratio_sweep_denovo_e_coli_lw06_fold2.0_C_lanes2.tsv"},
    ),
    # The only DELETION arm in the cut, at fold 0 rather than fold 2. Its deltas are
    # therefore not on the other three's scale and the `movers` column is not comparable
    # across the boundary -- severing an edge and doubling one are different sizes of
    # perturbation. What IS comparable is every RANK statistic: AUC, the size control, and
    # sign agreement against a base rate.
    "fuhrer": dict(
        ratio="C(glucose->axis sink) / C(glucose->CO2)",
        host="e_coli_bw25113",
        files={"gem": RUNS / "fuhrer_clones/ecspr/fuhrer_ratio_sweep_gem_e_coli_bw25113_fold0.0_C_ctl.tsv",
               "denovo": RUNS / "fuhrer_clones/ecspr/fuhrer_ratio_sweep_denovo_e_coli_bw25113_fold0.0_C_lanes2_ctl.tsv"},
    ),
}


def auc(score, label):
    label = np.asarray(label, bool)
    if label.sum() == 0 or (~label).sum() == 0:
        return float("nan")
    u, _ = mannwhitneyu(np.asarray(score, float)[label],
                        np.asarray(score, float)[~label], alternative="greater")
    return u / (label.sum() * (~label).sum())


def cut(sub: pd.DataFrame) -> dict:
    live = sub[sub.delta_ratio_pct.abs() > 1e-12]
    mov = live[live.delta_ratio_pct.abs() >= MOVER_PCT]
    union = lambda f: sorted({r for s in f.rxns.dropna() for r in str(s).split(",") if r})
    return dict(active=len(sub), responders=len(live), movers=len(mov),
                rxn_all=len(union(live)), rxn_movers=len(union(mov)),
                mover_rxns=union(mov), live=live, mov=mov)


def sign(live: pd.DataFrame, expect_pos: pd.Series) -> dict:
    """Agreement against the majority-class base rate. expect_pos is per-row."""
    if not len(live):
        return dict(n=0)
    ep = expect_pos.loc[live.index].astype(bool)
    obs = live.delta_ratio_pct > 0
    base = float(max(ep.mean(), 1 - ep.mean()))
    tab = [[int((ep & obs).sum()), int((ep & ~obs).sum())],
           [int((~ep & obs).sum()), int((~ep & ~obs).sum())]]
    ok = int((ep == obs).sum())
    return dict(n=len(live), ok=ok, pct=100 * ok / len(live), base_pct=100 * base,
                fisher_p=float(fisher_exact(tab)[1]),
                vs_base_p=float(binomtest(ok, len(live), base, alternative="greater").pvalue))


def rows_eydallin(d, ch):
    met = d[(d.is_positive == True) & (d.n_rxn > 0)]  # noqa: E712
    c = cut(met)
    s = sign(c["live"], met.eydallin_phenotype == "glycogen_excess")
    sm = sign(c["mov"], met.eydallin_phenotype == "glycogen_excess")
    am = d[d.n_rxn > 0]
    yield dict(arm="eydallin", channel=ch, stratum="86 hits", n_labelled=int(d.is_positive.sum()),
               auc=auc(am.delta_ratio_pct.abs(), am.is_positive),
               size=auc(am.n_rxn, am.is_positive), **c, **{f"sign_{k}": v for k, v in s.items()},
               mover_sign=f"{sm.get('ok','-')}/{sm.get('n',0)}")


def rows_fang(d, ch):
    for ax in d.axis.unique():
        a = d[d.axis == ax]
        assayed = a[a.assayed == True]  # noqa: E712
        met = assayed[assayed.n_rxn > 0]
        signed = met[met.fang_direction.isin(["up", "down"])]
        c = cut(met)
        cs = cut(signed)
        s = sign(cs["live"], signed.fang_direction == "up")
        sm = sign(cs["mov"], signed.fang_direction == "up")
        am = a[a.n_rxn > 0]
        yield dict(arm="fang", channel=ch, stratum=f"axis {ax}", n_labelled=len(assayed),
                   auc=auc(am.delta_ratio_pct.abs(), am.is_positive),
                   size=auc(am.n_rxn, am.is_positive), **c,
                   **{f"sign_{k}": v for k, v in s.items()},
                   mover_sign=f"{sm.get('ok','-')}/{sm.get('n',0)}")


def rows_woodruff(d, ch):
    sets = {"tolerant_15": d.phenotype.isin(["tolerant_15", "tolerant_both"]),
            "tolerant_30": d.phenotype.isin(["tolerant_30", "tolerant_both"]),
            "tolerant_both": d.phenotype == "tolerant_both"}
    for name, sel in sets.items():
        met = d[sel & (d.n_rxn > 0)]
        c = cut(met)
        am = d[d.n_rxn > 0]
        lab = sel.loc[am.index]
        yield dict(arm="woodruff", channel=ch, stratum=name, n_labelled=int(sel.sum()),
                   auc=auc(am.delta_ratio_pct.abs(), lab), size=auc(am.n_rxn, lab), **c,
                   sign_n=len(c["live"]), sign_ok=None, sign_pct=None, sign_base_pct=None,
                   sign_fisher_p=None, sign_vs_base_p=None,
                   mover_sign=f"{int((c['live'].delta_ratio_pct>0).sum())}up/"
                              f"{int((c['live'].delta_ratio_pct<0).sum())}down")


def rows_fuhrer(d, ch):
    """One stratum per declared TARGET axis. The `_precursor` size-control axes are swept
    into the same file and are excluded here: they carry the target's population but no
    readout of their own, so a row for one would report the target's labels under a
    different axis name."""
    for ax in sorted(a for a in d.axis.unique() if not str(a).endswith("_precursor")):
        a = d[(d.axis == ax) & d.screened.astype(bool) & d.assayed.astype(bool)]
        met = a[a.is_positive.astype(bool) & (a.n_rxn > 0)]
        signed = met[met.direction.isin(["+", "-"])]
        c = cut(met)
        cs = cut(signed)
        # `direction == "+"` is the metabolite RISING. The expectation on a deletion arm is
        # the opposite of the gof arms', and `sign` compares against the label set's own
        # base rate either way, which is what makes the two comparable at all.
        s = sign(cs["live"], signed.direction == "+")
        sm = sign(cs["mov"], signed.direction == "+")
        am = a[a.n_rxn > 0]
        yield dict(arm="fuhrer", channel=ch, stratum=f"axis {ax}",
                   n_labelled=int(a.is_positive.sum()),
                   auc=auc(am.delta_ratio_pct.abs(), am.is_positive),
                   size=auc(am.n_rxn, am.is_positive), **c,
                   **{f"sign_{k}": v for k, v in s.items()},
                   mover_sign=f"{sm.get('ok','-')}/{sm.get('n',0)}")


BUILDERS = {"eydallin": rows_eydallin, "fang": rows_fang, "woodruff": rows_woodruff,
            "fuhrer": rows_fuhrer}


def main():
    recs, lines = [], []
    for arm, spec in SWEEPS.items():
        lines.append(f"\n{'='*100}\n{arm.upper()}   ratio = {spec['ratio']}   host {spec['host']}\n{'='*100}")
        for ch, path in spec["files"].items():
            if not path.exists():
                print(f"missing: {path}", file=sys.stderr)
                continue
            d = pd.read_csv(path, sep="\t")
            for r in BUILDERS[arm](d, ch):
                mv = r.pop("mover_rxns")
                r.pop("live"), r.pop("mov")
                recs.append(r)
                lines.append(
                    f"\n  {ch:7s} {r['stratum']:16s}  labelled {r['n_labelled']:4d}  "
                    f"metabolically active {r['active']:4d}  responders {r['responders']:4d}  "
                    f">={MOVER_PCT:g}% movers {r['movers']:2d}")
                lines.append(
                    f"          reactions: {r['rxn_all']:3d} across all responders, "
                    f"{r['rxn_movers']:3d} across the movers"
                    + (f"  [{','.join(mv)}]" if 0 < len(mv) <= 10 else ""))
                lines.append(
                    f"          AUC atom-mapped {r['auc']:.4f}  |  size control {r['size']:.4f}")
                if r.get("sign_ok") is None:
                    lines.append(f"          sign: {r['mover_sign']} "
                                 f"(no signed expectation -- every label is one-sided)")
                else:
                    lines.append(
                        f"          sign correct {r['sign_ok']}/{r['sign_n']} = {r['sign_pct']:.1f}%  "
                        f"vs majority base rate {r['sign_base_pct']:.1f}%  "
                        f"(Fisher p={r['sign_fisher_p']:.3g}, vs-base p={r['sign_vs_base_p']:.3g})"
                        f"   movers {r['mover_sign']}")

    OUT.mkdir(parents=True, exist_ok=True)
    text = "\n".join(lines) + "\n"
    # Both outputs are DVC-pinned, and a checkout is a read-only hardlink into the shared
    # cache. Writing through one raises rather than corrupting -- but only on the SECOND
    # run of this script, after the first has been pinned, which is why it survived three
    # arms before the fourth hit it.
    for path, payload in (("ratio_cross_arm.tsv", None), ("ratio_cross_arm.txt", text)):
        (OUT / path).unlink(missing_ok=True)
        if payload is not None:
            (OUT / path).write_text(payload)
    pd.DataFrame(recs).to_csv(OUT / "ratio_cross_arm.tsv", sep="\t", index=False)
    print(text)
    print(f"-> {(OUT / 'ratio_cross_arm.tsv').relative_to(ROOT)}", file=sys.stderr)


if __name__ == "__main__":
    main()
