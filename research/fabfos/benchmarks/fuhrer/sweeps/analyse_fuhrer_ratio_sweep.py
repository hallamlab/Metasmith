#!/usr/bin/env python3
"""Score the declared Fuhrer panel: does |delta_ratio_pct| rank the deletions that actually
moved each metabolite?

    mamba run -n msm-fabfos python \\
        research/fabfos/benchmarks/fuhrer/sweeps/analyse_fuhrer_ratio_sweep.py

SAME INSTRUMENTS AS THE OTHER THREE ARMS, IMPORTED RATHER THAN FORKED. `analyse_aska_sweep`
supplies the mid-rank Mann-Whitney AUC, the reaction-count size control, the reach 2x2 and
its Fisher test, precision at k, and the seeded resample. Reproducing an arm means
reproducing its INSTRUMENTS, not just its perturbation, so nothing here reimplements one.

THREE THINGS THIS ARM CAN DO THAT NO PREVIOUS ARM COULD, and they are the reason it exists:

  MEASURED NEGATIVES.  Every gene in the population was profiled. A negative here is a
  deletion the mass spectrometer read and found not to move the metabolite -- not an
  unlabelled gene assumed innocent. The other three arms all had to caveat this.

  A SIGNED LABEL.  The z-score's sign says whether the metabolite ROSE or FELL. So the
  probe can be asked which, and not only whether. Sign agreement is reported HERE ONLY
  BESIDE THE MAJORITY-CLASS BASE RATE OF THAT AXIS'S OWN LABEL SET -- three of this
  campaign's sign figures previously sat below their own base rates and read as results
  until the base rate was printed next to them. One axis in this panel is 55 positives and
  55 of them up, so its base rate is 1.000 and no sign figure on it can mean anything.

  AN EXTERNAL BASELINE.  Table EV4 publishes, per ion-annotation, an ROC AUC for the same
  gene-metabolite association -- ranking deletions by |z| and asking whether the
  metabolite's own enzymes come out on top. It is printed beside every ECSPr AUC.

**CAUTION** THE PUBLISHED AUC IS THE TRANSPOSE OF THIS ONE, NOT A SECOND RUN OF IT. Fuhrer
ranks by the PHENOTYPE and labels by ADJACENCY; this arm ranks by a topology-derived
conductance and labels by the phenotype. Both measure the same association and both are
monotone in it, so they are comparable as rankings -- but a difference between the two
numbers is not a difference in performance at one task. The report must say which is which
every time it prints them together.

THE SIZE CONTROL IS RUN TWICE, TWO DIFFERENT WAYS, AND THEY ARE NOT INTERCHANGEABLE:

    reaction count      the same AUC recomputed on `n_rxn` alone -- a competing
                        DETERMINISTIC ranker that says "the probe is counting reactions"
    the precursor axis  the same sweep re-solved against the sink's own one-reaction carbon
                        neighbour -- if a target and its precursor rank the population
                        alike, the probe has ranked the module and not the metabolite

Neither is the resample, which is the error bar and is a third thing again. The three get
confused on sight, so each is printed under its own heading.

THE TAUTOLOGY CONTROL IS COMPUTED OFF THE ATOM-PAIR TABLE, not from a hand-written gene
list. The struck module is every gene whose credited reactions carry a mapped carbon atom
into or out of the declared sink -- the genes the two-point conductance cannot help but
rank first, because they are the sink's own edges. It is struck from BOTH classes.

NEVER-ASSAYED GENES ARE DROPPED AND THE COUNT IS PRINTED. The census carries 163 iML1515
genes the screen never measured, 120 of them Keio-essential. An unmeasured gene is not a
negative one.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin/sweeps"))

import analyse_aska_sweep as A                                          # noqa: E402
import bake_pairs                                                       # noqa: E402

SWEEPS = ROOT / "data/fabfos/runs/fuhrer_clones/ecspr"
AXES = ROOT / "data/fabfos/benchmarks/fuhrer/axes.tsv"
CONTROL_SUFFIX = "_precursor"


def sink_module(sink_mnxm: str, gpr: pd.DataFrame, pairs: pd.DataFrame) -> set[str]:
    """Genes whose credited reactions carry a mapped carbon atom into or out of the sink.

    This is the tautology control's struck set, and it is derived rather than written down:
    a two-point conductance grounded at the sink ranks the sink's own edges first by
    construction, so leaving them in measures the declaration and not the method."""
    touching = set(pairs.loc[(pairs.substrate == sink_mnxm)
                             | (pairs["product"] == sink_mnxm), "mnxr"].astype(str))
    hit = gpr[gpr.mnxr.astype(str).isin(touching)]
    return set(hit.condition_id.astype(str).str.split(":", n=1).str[1])


def sign_agreement(df: pd.DataFrame, log) -> dict:
    """Does the sign of the ratio's move predict the sign of the metabolite's?

    A deletion that LOWERS the glucose->sink ratio should leave less of the sink; one that
    raises it, more. Both happen under fold 0, because Rayleigh binds each leg of the ratio
    but not their quotient.

    REPORTED ONLY BESIDE THE BASE RATE. `base_rate` is the majority class of THIS axis's own
    positives -- the accuracy of answering "up" every time. An agreement below it is worse
    than a constant.
    """
    pos = df[df.is_positive & (df.n_rxn > 0) & df.direction.isin(("+", "-"))]
    if pos.empty:
        log("  sign: no atom-mapped positive carries a direction")
        return dict(n=0)
    predicted = np.where(pos.delta_ratio_pct.to_numpy() < 0, "-", "+")
    agree = int((predicted == pos.direction.to_numpy()).sum())
    up = int((pos.direction == "+").sum())
    base = max(up, len(pos) - up) / len(pos)
    log(f"  sign: {agree}/{len(pos)} atom-mapped positives agree ({agree / len(pos):.3f}) "
        f"against a majority-class base rate of {base:.3f} "
        f"({up} up / {len(pos) - up} down)"
        + ("   <- BELOW ITS OWN BASE RATE" if agree / len(pos) < base else ""))
    return dict(n=int(len(pos)), agree=agree, rate=agree / len(pos), base_rate=base,
                up=up, down=int(len(pos) - up))


def precursor_control(all_df: pd.DataFrame, axis: str, log) -> dict:
    """The same population re-scored against the sink's own one-reaction carbon neighbour.

    The declared-sink-adjacency confound, measured rather than argued: if the target's AUC
    and its precursor's are the same number, the probe has ranked the module containing both
    and the target's own AUC says nothing about the metabolite.
    """
    ctl = all_df[all_df.axis == axis + CONTROL_SUFFIX]
    tgt = all_df[all_df.axis == axis]
    if ctl.empty:
        log(f"  precursor axis: {axis + CONTROL_SUFFIX} was not swept "
            f"(run the sweep with --roles target control)")
        return {}
    # The control axis is not a readout, so it carries no labels of its own -- it is scored
    # against the TARGET's label set, which is the whole point of the comparison.
    merged = tgt[["gene", "is_positive", "n_rxn"]].merge(
        ctl[["gene", "delta_ratio_pct"]], on="gene", how="inner")
    mapped = merged[merged.n_rxn > 0]
    a_c, p_c = A.auc(mapped.delta_ratio_pct.abs().to_numpy().astype(float),
                     mapped.is_positive.to_numpy())
    a_t, p_t = A.auc(tgt.loc[tgt.n_rxn > 0, "delta_ratio_pct"].abs().to_numpy()
                     .astype(float),
                     tgt.loc[tgt.n_rxn > 0, "is_positive"].to_numpy())
    log(f"  precursor axis {axis + CONTROL_SUFFIX}: AUC {a_c:.4f} (p={p_c:.3g}) on the "
        f"TARGET's labels, against the target's own {a_t:.4f} (p={p_t:.3g}) "
        f"-- difference {a_t - a_c:+.4f}")
    return dict(axis=axis + CONTROL_SUFFIX, auc=a_c, p=p_c,
                target_auc=a_t, target_p=p_t, difference=a_t - a_c)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--fold", type=float, default=0.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--channels", nargs="+", default=["gem", "denovo"])
    ap.add_argument("--suffix", nargs="+", default=["_ctl", "_lanes2_ctl", "", "_lanes2"],
                    help="filename tags to try per channel, first match wins")
    ap.add_argument("--reps", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--host", default="e_coli_bw25113")
    ap.add_argument("--label", default="fuhrer")
    ap.add_argument("--sweep-dir", type=Path, default=SWEEPS)
    ap.add_argument("--out-dir", type=Path, default=SWEEPS)
    a = ap.parse_args()

    lines: list[str] = []

    def log(s=""):
        print(s)
        lines.append(s)

    axes_decl = pd.read_csv(AXES, sep="\t", dtype=str).fillna("")
    targets = axes_decl[axes_decl.role == "target"].set_index("axis")
    pairs = pd.read_parquet(bake_pairs.atom_pairs(),
                            columns=["mnxr", "element", "substrate", "product"])
    pairs = pairs[pairs.element == "C"]

    report: dict = {}
    for ch in a.channels:
        stem = (f"{a.label}_ratio_sweep_{ch}_{a.host}_fold{a.fold}_{a.element}")
        f = next((a.sweep_dir / f"{stem}{s}.tsv" for s in a.suffix
                  if (a.sweep_dir / f"{stem}{s}.tsv").exists()), None)
        if f is None:
            log(f"\n### {ch}: no sweep on disk -- skipped")
            continue
        gpr = pd.read_parquet(ROOT / f"data/fabfos/runs/fuhrer_clones/gpr/gpr_{ch}.parquet",
                              columns=["condition_id", "mnxr"])
        all_df = pd.read_csv(f, sep="\t")
        all_df["is_positive"] = all_df.is_positive.astype(bool)
        n_all = len(all_df[all_df.axis == targets.index[0]])
        all_df = all_df[all_df.screened.astype(bool) & all_df.assayed.astype(bool)]
        n_kept = len(all_df[all_df.axis == targets.index[0]])
        report[ch] = {"file": f.name, "dropped_unassayed": n_all - n_kept, "axes": {}}
        log(f"\n{'=' * 78}\n### channel {ch}  --  {f.name}\n"
            f"### {n_kept:,} of {n_all:,} census rows kept: a gene the screen never "
            f"measured is unlabelled, not negative\n{'=' * 78}")

        for axis in [x for x in targets.index if x in set(all_df.axis)]:
            decl = targets.loc[axis]
            df = all_df[all_df.axis == axis].reset_index(drop=True)
            df["score"] = df.delta_ratio_pct.abs().astype(float)
            # `analyse_aska_sweep.analyse` dumps each positive beside its phenotype under
            # its own cohort's column name. Aliasing keeps one implementation of the AUC.
            df["eydallin_phenotype"] = df.direction.astype(str)
            log(f"\n{'-' * 78}\n## axis {axis}  ({ch})  --  {decl.sink_name}\n"
                f"   {decl.kegg_id} on {decl['mode']} ion {decl.ion_index}; "
                f"PUBLISHED ANNOTATION AUC {decl.published_auc} "
                f"(Fuhrer ranks by |z| and labels by adjacency; this arm is the transpose)\n"
                f"   {len(df):,} measured deletions, {int((df.n_rxn > 0).sum()):,} "
                f"atom-mapped, {int(df.is_positive.sum())} past |z| > 2.765\n"
                f"   host ratio {df.host_ratio.iloc[0]:.9f} "
                f"(num {df.host_num.iloc[0]:.6f} / den {df.host_den.iloc[0]:.6f}); "
                f"states {df.ratio_state.value_counts().to_dict()}\n{'-' * 78}")
            r: dict = {"published_auc": decl.published_auc,
                       "n_positive_declared": decl.n_positive}

            log("\n  -- the whole measured population " + "-" * 44)
            r["measured"] = A.analyse(df, f"{ch}:{axis}", log)
            r["measured"]["resample"] = A.resample(df, n_neg=100, reps=a.reps,
                                                   seed=a.seed, log=log)
            r["sign"] = sign_agreement(df, log)
            r["precursor"] = precursor_control(all_df, axis, log)

            mod = df.gene.isin(sink_module(decl.sink_mnxm, gpr, pairs))
            struck = sorted(df.loc[mod & df.is_positive, "gene"])
            log(f"\n  -- tautology control: {int(mod.sum())} gene(s) carrying a reaction "
                f"incident to {decl.sink_mnxm} removed from BOTH classes, {len(struck)} of "
                f"them positive ({', '.join(struck) or 'none'}) " + "-" * 4)
            rest = df[~mod].reset_index(drop=True)
            if rest.is_positive.nunique() == 2:
                r["no_sink_module"] = A.analyse(rest, f"{ch}:{axis}:no_sink_module", log)
                r["no_sink_module"]["struck"] = struck
            else:
                log("     every positive is in the module; no AUC remains to compute")
                r["no_sink_module"] = dict(struck=struck, note="all positives struck")

            log("\n  the top 20 of the measured population by |delta_ratio_pct|:")
            log(df.nlargest(20, "score")[["gene", "n_rxn", "delta_ratio_pct", "z",
                                          "direction", "is_positive"]]
                .to_string(index=False, float_format=lambda v: f"{v:.6g}"))
            report[ch]["axes"][axis] = r

        log(f"\n{'=' * 78}\n### {ch} SUMMARY -- every AUC beside its two size controls\n"
            f"{'=' * 78}")
        log(f"{'axis':24} {'n+':>4} {'ECSPr':>7} {'n_rxn':>7} {'precursor':>9} "
            f"{'struck':>7} {'published':>9} {'sign':>6} {'base':>6}")
        for axis, r in report[ch]["axes"].items():
            m = r["measured"]["auc_atom-mapped"]
            nm = r.get("no_sink_module", {}).get("auc_atom-mapped", {})
            log(f"{axis:24} {m['n_positive']:>4} {m['ecspr']:>7.4f} {m['size']:>7.4f} "
                f"{r['precursor'].get('auc', float('nan')):>9.4f} "
                f"{nm.get('ecspr', float('nan')):>7.4f} "
                f"{float(r['published_auc']):>9.4f} "
                f"{r['sign'].get('rate', float('nan')):>6.3f} "
                f"{r['sign'].get('base_rate', float('nan')):>6.3f}")

    a.out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{a.label}_ratio_classifier_report"
    for path, text in ((a.out_dir / f"{stem}.json",
                        json.dumps(report, indent=2, default=float)),
                       (a.out_dir / f"{stem}.txt", "\n".join(lines) + "\n")):
        path.unlink(missing_ok=True)
        path.write_text(text)
    print(f"\n-> {a.out_dir}/{stem}.{{json,txt}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
