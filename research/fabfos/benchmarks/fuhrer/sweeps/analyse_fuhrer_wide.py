#!/usr/bin/env python3
"""The genome-wide arm, scored as a distribution: one AUC per metabolite, each beside its
size control and beside two transpose baselines.

    mamba run -n msm-fabfos python \\
        research/fabfos/benchmarks/fuhrer/sweeps/analyse_fuhrer_wide.py

WHY THIS IS SCORED AS A DISTRIBUTION AND NOT AS A NUMBER. The declared panel asks "does the
probe rank this metabolite's movers", eight times, on metabolites chosen for the paper's own
AUC. This arm asks the same question of every metabolite the probe can reach and the screen
can score, chosen on reachability and statistical power alone. One axis beating its size
control proves nothing; a hundred axes beating it more often than chance is a result, and a
hundred axes NOT beating it is a stronger one than any single arm has produced.

THREE COMPARISONS PER AXIS, AND THEY ANSWER DIFFERENT QUESTIONS:

    size control     the same AUC on `n_rxn` alone. Beating it says the probe is doing
                     something other than counting the gene's reactions.
    recomputed AUC   the TRANSPOSE on the SAME adjacency this arm uses -- rank deletions by
                     |z|, label them by whether their curated reactions touch the sink.
                     This is the fair external comparison, because both sides then mean the
                     same thing by "adjacent".
    published AUC    Fuhrer's own Table EV4 number. `reproduce_published_auc.py` establishes
                     that it does NOT come back from this tree's adjacency (Spearman 0.14
                     over 1,283 ion-annotations, median difference 0.095), so it is a
                     baseline on a label set this arm cannot reproduce. It is printed
                     because it is the paper's published claim, and it is never used as
                     though the two label sets agreed.

THE SIGN TEST IS THE HEADLINE. Whether ECSPr beats its size control on more than half the
axes is a binomial question with a hundred trials, and it does not depend on any axis being
special. The Spearman against each baseline says whether the probe and the baseline agree
about WHICH metabolites are well predicted, which is a different and weaker claim.

EVERY FILTER STEP PRINTS ITS COUNT. A bounded run that says what it bounded is a result; a
silent truncation reads as complete coverage.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import binomtest, spearmanr, wilcoxon


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin/sweeps"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/fuhrer/sweeps"))

import analyse_aska_sweep as A                                          # noqa: E402
import bake_pairs                                                       # noqa: E402
from analyse_fuhrer_ratio_sweep import sink_module                      # noqa: E402

SWEEPS = ROOT / "data/fabfos/runs/fuhrer_clones/ecspr"
AXES = ROOT / "data/fabfos/benchmarks/fuhrer/axes_wide.tsv"
CHECK = SWEEPS / "published_auc_check.tsv"
Z_THRESHOLD = 2.765

COLS = ("axis", "channel", "sink_mnxm", "sink_name", "kegg_id", "mode", "ion_index",
        "n_measured", "n_atom_mapped", "n_positive", "n_positive_atom_mapped",
        "module_size", "reach_odds_ratio", "reach_p",
        "ecspr_auc", "ecspr_p", "size_auc", "size_p", "struck_auc",
        "recomputed_auc", "published_auc", "sign_rate", "sign_base_rate")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--fold", type=float, default=0.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--channels", nargs="+", default=["gem"])
    ap.add_argument("--host", default="e_coli_bw25113")
    ap.add_argument("--label", default="fuhrer")
    ap.add_argument("--stem", default="axes_wide")
    ap.add_argument("--sweep-dir", type=Path, default=SWEEPS)
    ap.add_argument("--out-dir", type=Path, default=SWEEPS)
    a = ap.parse_args()

    lines: list[str] = []

    def log(s=""):
        print(s)
        lines.append(s)

    decl = pd.read_csv(AXES, sep="\t", dtype=str).fillna("").set_index("axis")
    check = pd.read_csv(CHECK, sep="\t")
    recomputed = {(r.mode, int(r.ion_index)): r.recomputed_auc
                  for r in check.itertuples()}
    pairs = pd.read_parquet(bake_pairs.atom_pairs(),
                            columns=["mnxr", "element", "substrate", "product"])
    pairs = pairs[pairs.element == "C"]

    rows: list[dict] = []
    for ch in a.channels:
        f = (a.sweep_dir / f"{a.label}_ratio_sweep_{ch}_{a.host}_fold{a.fold}_"
                           f"{a.element}_{a.stem}.tsv")
        if not f.exists():
            log(f"### {ch}: no genome-wide sweep at {f.name} -- skipped")
            continue
        gpr = pd.read_parquet(ROOT / f"data/fabfos/runs/fuhrer_clones/gpr/gpr_{ch}.parquet",
                              columns=["condition_id", "mnxr"])
        all_df = pd.read_csv(f, sep="\t")
        all_df["is_positive"] = all_df.is_positive.astype(bool)
        swept = sorted(set(all_df.axis))
        n_census = len(all_df[all_df.axis == swept[0]])
        all_df = all_df[all_df.screened.astype(bool) & all_df.assayed.astype(bool)]
        n_kept = len(all_df[all_df.axis == swept[0]])

        log(f"\n{'=' * 78}\n### channel {ch}  --  {f.name}\n{'=' * 78}")
        log(f"  {len(decl) - 1:>5} metabolites declared on reachability and power")
        log(f"  {len(swept):>5} of those reach on this channel and were swept "
            f"({len(decl) - 1 - len(swept)} unreachable, named in the axis probe)")
        log(f"  {n_census:>5} census rows per axis -> {n_kept} kept "
            f"({n_census - n_kept} never measured: unlabelled, not negative)")

        for axis in swept:
            d = decl.loc[axis]
            df = all_df[all_df.axis == axis].reset_index(drop=True)
            df["score"] = df.delta_ratio_pct.abs().astype(float)
            mapped = df[df.n_rxn > 0]
            if mapped.is_positive.nunique() != 2:
                log(f"  {axis:22} skipped: its atom-mapped rows are all one class")
                continue
            pos = mapped.is_positive.to_numpy()
            e_auc, e_p = A.auc(mapped.score.to_numpy(), pos)
            s_auc, s_p = A.auc(mapped.n_rxn.to_numpy().astype(float), pos)
            tab = [[int((df.is_positive & (df.n_rxn > 0)).sum()),
                    int((df.is_positive & (df.n_rxn == 0)).sum())],
                   [int((~df.is_positive & (df.n_rxn > 0)).sum()),
                    int((~df.is_positive & (df.n_rxn == 0)).sum())]]
            from scipy.stats import fisher_exact
            orr, fp = fisher_exact(tab)

            mod = df.gene.isin(sink_module(d.sink_mnxm, gpr, pairs))
            rest = df[~mod & (df.n_rxn > 0)]
            st_auc = (A.auc(rest.score.to_numpy(), rest.is_positive.to_numpy())[0]
                      if rest.is_positive.nunique() == 2 else float("nan"))

            sp = mapped[mapped.is_positive & mapped.direction.isin(("+", "-"))]
            if len(sp):
                pred = np.where(sp.delta_ratio_pct.to_numpy() < 0, "-", "+")
                rate = float((pred == sp.direction.to_numpy()).mean())
                up = int((sp.direction == "+").sum())
                base = max(up, len(sp) - up) / len(sp)
            else:
                rate = base = float("nan")

            rows.append(dict(
                axis=axis, channel=ch, sink_mnxm=d.sink_mnxm, sink_name=d.sink_name,
                kegg_id=d.kegg_id, mode=d["mode"], ion_index=d.ion_index,
                n_measured=len(df), n_atom_mapped=len(mapped),
                n_positive=int(df.is_positive.sum()),
                n_positive_atom_mapped=int(pos.sum()),
                module_size=int(mod.sum()), reach_odds_ratio=float(orr), reach_p=float(fp),
                ecspr_auc=e_auc, ecspr_p=e_p, size_auc=s_auc, size_p=s_p,
                struck_auc=st_auc,
                recomputed_auc=recomputed.get((d["mode"], int(d.ion_index)), float("nan")),
                published_auc=float(d.published_auc) if d.published_auc else float("nan"),
                sign_rate=rate, sign_base_rate=base))

    out = pd.DataFrame(rows, columns=list(COLS))
    if out.empty:
        log("\nnothing scored")
        return 1

    log(f"\n{'=' * 78}\n### the distribution over {len(out)} metabolites\n{'=' * 78}")
    beats = int((out.ecspr_auc > out.size_auc).sum())
    bt = binomtest(beats, len(out), 0.5, alternative="greater")
    log(f"  ECSPr AUC   median {out.ecspr_auc.median():.4f}  "
        f"IQR [{out.ecspr_auc.quantile(.25):.4f}, {out.ecspr_auc.quantile(.75):.4f}]  "
        f"range [{out.ecspr_auc.min():.4f}, {out.ecspr_auc.max():.4f}]")
    log(f"  size control median {out.size_auc.median():.4f}  "
        f"IQR [{out.size_auc.quantile(.25):.4f}, {out.size_auc.quantile(.75):.4f}]")
    log(f"  ECSPr beats its size control on {beats}/{len(out)} metabolites "
        f"({beats / len(out):.1%}), binomial p={bt.pvalue:.3g}")
    w = wilcoxon(out.ecspr_auc, out.size_auc)
    log(f"  paired Wilcoxon on the difference: p={w.pvalue:.3g}, "
        f"median difference {(out.ecspr_auc - out.size_auc).median():+.4f}")
    over = int((out.ecspr_auc > 0.5).sum())
    log(f"  {over}/{len(out)} axes score above 0.5 at all "
        f"(binomial p={binomtest(over, len(out), 0.5, alternative='greater').pvalue:.3g})")

    struck = out.dropna(subset=["struck_auc"])
    log(f"\n  tautology control, struck from both classes on {len(struck)} axes: "
        f"median ECSPr {struck.ecspr_auc.median():.4f} -> "
        f"{struck.struck_auc.median():.4f} "
        f"(median change {(struck.struck_auc - struck.ecspr_auc).median():+.4f}); "
        f"module size median {int(out.module_size.median())}")

    for name, col in (("this tree's adjacency (the fair transpose)", "recomputed_auc"),
                      ("Fuhrer's Table EV4 (a different adjacency)", "published_auc")):
        g = out.dropna(subset=["ecspr_auc", col])
        if len(g) < 3:
            continue
        rho, p = spearmanr(g.ecspr_auc, g[col])
        log(f"\n  vs {name}: n={len(g)}, median baseline {g[col].median():.4f}, "
            f"Spearman rho {rho:+.4f} (p={p:.3g}), "
            f"ECSPr higher on {int((g.ecspr_auc > g[col]).sum())}/{len(g)}")

    sg = out.dropna(subset=["sign_rate", "sign_base_rate"])
    above = int((sg.sign_rate > sg.sign_base_rate).sum())
    log(f"\n  sign agreement exceeds its own majority-class base rate on "
        f"{above}/{len(sg)} axes (binomial p="
        f"{binomtest(above, len(sg), 0.5, alternative='greater').pvalue:.3g}); "
        f"median agreement {sg.sign_rate.median():.3f} against median base rate "
        f"{sg.sign_base_rate.median():.3f}")

    log(f"\n  the 15 metabolites ECSPr ranks best:")
    log(out.nlargest(15, "ecspr_auc")[["axis", "sink_name", "n_positive_atom_mapped",
                                       "module_size", "ecspr_auc", "size_auc",
                                       "recomputed_auc", "published_auc"]]
        .to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    a.out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{a.label}_wide_report"
    for path, text in ((a.out_dir / f"{stem}.txt", "\n".join(lines) + "\n"),
                       (a.out_dir / f"{stem}.json",
                        json.dumps({"n_axes": len(out), "beats_size_control": beats,
                                    "binomial_p": bt.pvalue}, indent=2, default=float))):
        path.unlink(missing_ok=True)
        path.write_text(text)
    tsv = a.out_dir / f"{stem}.tsv"
    tsv.unlink(missing_ok=True)
    out.to_csv(tsv, sep="\t", index=False)
    print(f"\n-> {a.out_dir}/{stem}.{{tsv,txt,json}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
