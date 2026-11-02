#!/usr/bin/env python3
"""Does the ethanol/oxaloacetate RATIO nominate the SCALEs screen's genes?

    mamba run -n ecspr python \
        research/fabfos/benchmarks/woodruff/sweeps/analyse_woodruff_ratio_sweep.py
    ... --channel denovo
    ... --channel denovo --suffix _lanes2

ONE CHANNEL PER RUN, AND THE CHANNEL BELONGS BESIDE EVERY NUMBER. The curated and de-novo
backgrounds are 1,409 and 8,560 reactions on the same host, so their conductances and their
`delta_ratio_pct` magnitudes are not comparable to each other -- only ranks within one
channel are. Nothing here reads two sweeps at once, and the report tables state the channel
on every row.

`sweeps/score_scales.py` scores the one-probe sweep and is imported here whole -- same
reach 2x2 before any ranking statistic, same mid-rank Mann-Whitney AUC, same reaction-count
size control, same precision-at-k, same three nested positive sets, same resampled draw.
Only the score changes: |delta_ratio_pct| from `sweep_woodruff_ratio.py` instead of a raw
one-probe delta. The ratio is two-sided by construction, so the magnitude is what a ranking
question can use and the sign is reported separately.

TWO POPULATIONS, BOTH REPORTED, AND THE SECOND IS THE ONE THE PROTOCOL ASKS FOR.

  * ALL 4,225 measured genes. This is the screen's own population and the number that says
    whether the method could be run as a screen.
  * The genes whose primary reaction CHANGES A CARBON SKELETON -- `carbon_bond_change` in
    {breaks, creates, both}, read off `gof.csv`, which reads it off the baked atom map by
    connected components. A conductance probe measures carbon flow; a gene whose reaction
    leaves the skeleton intact (a phosphorylation, a redox step, a transport event) can
    only ever move the readout through the graph's plumbing. Restricting to skeleton-changing
    genes is the population the method has a mechanism for, and if it cannot rank there it
    cannot rank anywhere.

THE TAUTOLOGY CONTROL IS MECHANICAL AND STRIKES ALL THREE TERMINALS. `score_scales.py`
defines an axis's module as the genes carrying a reaction incident to that axis's sink,
read off the atom-pair table so it cannot be tuned per axis. A ratio has three terminals,
not one, so the module here is every gene incident to pyruvate, to ethanol, or to
oxaloacetate. A probe grounded at pyruvate ranking a pyruvate-adjacent enzyme first is
arithmetic; the claim worth testing is about the other four thousand genes.

THE NULL TAIL IS WHAT DECIDES WHETHER THIS COULD BE RUN AS A SCREEN. For each positive
set, how much of the measured-negative population moves the ratio at least as far as the
positives do -- the false-positive rate a threshold at each hit would carry.

THE PHENOTYPE IS THE COMPANION PAPER'S AND THE HOST IS THE PRODUCTION PAPER'S, and that
mismatch is the arm's largest limit rather than an oversight. `parse/build_gof_table.py`
carries the whole argument: the production screen's per-gene identities are unrecoverable,
so the only nameable per-gene phenotype for this library is ethanol TOLERANCE in
BW25113 delta-recA, while the conductance is measured on LW06 because LW06 is the organism
the production selection ran in. Nothing here can turn one into the other, and no AUC below
should be read as a statement about production fitness.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin/sweeps"))

import bake_pairs                                                        # noqa: E402
from ecspr.model.build import load_pairs                                 # noqa: E402
from analyse_aska_sweep import resample                                  # noqa: E402
import score_scales as S                                                 # noqa: E402

SWEEPS = ROOT / "data/fabfos/runs/woodruff_clones/ecspr"
GPR = ROOT / "data/fabfos/runs/woodruff_clones/gpr"
GOF = ROOT / "data/fabfos/runs/woodruff_clones/parse/gof/gof.csv"

HOST = "e_coli_lw06"
SKELETON = ("breaks", "creates", "both")


def null_tail(df: pd.DataFrame, name: str, log) -> dict:
    null = df.loc[~df.is_positive, "score"].to_numpy()
    pos = df[df.is_positive].nlargest(10, "score")
    out = {"null_n": int(null.size), "top_positives": []}
    log(f"\n  -- the null tail ({null.size:,} measured non-hits), top 10 {name} --")
    for r in pos.itertuples(index=False):
        n_ge = int((null >= r.score).sum())
        rec = dict(gene=r.gene, score=float(r.score),
                   delta_ratio_pct=float(r.delta_ratio_pct), n_rxn=int(r.n_rxn),
                   n_null_ge=n_ge, fpr=n_ge / max(null.size, 1))
        out["top_positives"].append(rec)
        log(f"    {r.gene:8s}: |delta_ratio| = {r.score:.6g}% "
            f"(signed {r.delta_ratio_pct:+.4g}%, {int(r.n_rxn)} rxn) -> {n_ge:,} of the "
            f"null are >= it  (FPR {rec['fpr']:.3%})")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--channel", default="gem")
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--host", default=HOST)
    ap.add_argument("--suffix", default="",
                    help="the sweep tag's trailing part, e.g. `_lanes2` for a "
                         "lane-filtered de-novo run")
    ap.add_argument("--reps", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=20260825)
    a = ap.parse_args()

    tag = (f"woodruff_ratio_sweep_{a.channel}_{a.host}_fold{a.fold}_{a.element}"
           f"{a.suffix}")
    sweep = SWEEPS / f"{tag}.tsv"
    if not sweep.exists():
        raise SystemExit(f"{sweep.relative_to(ROOT)} does not exist -- run "
                         f"sweep_woodruff_ratio.py")
    df = pd.read_csv(sweep, sep="\t", float_precision="round_trip")
    census = pd.read_csv(GPR / "gene_census.tsv", sep="\t", dtype=str).fillna("")
    base = json.loads((SWEEPS / f"{tag}.BUILD.json").read_text())
    gof = pd.read_csv(GOF, dtype=str).fillna("")

    lines: list[str] = []

    def log(s=""):
        print(s)
        lines.append(s)

    df["score"] = df.delta_ratio_pct.abs().astype(float)
    bond = dict(zip(gof.gene, gof.carbon_bond_change))
    df["carbon_bond_change"] = df.gene.map(bond).fillna("")

    pos_sets = S.positive_sets(df, census)

    log(f"# {tag}")
    log(f"\n{base['measured_genes']:,} measured genes | {base['solved_genes']:,} "
        f"atom-mapped, {base['exact_zero_genes']:,} exact zeros | "
        f"{base['background_reactions']:,} background reactions "
        f"({base['weight_dict_convention']})")
    log(f"numerator   C({base['numerator']['src']} -> {base['numerator']['snk']}) = "
        f"{base['numerator']['base']:.9f}")
    log(f"denominator C({base['denominator']['src']} -> {base['denominator']['snk']}) = "
        f"{base['denominator']['base']:.9f}")
    log(f"host ratio  {base['host_ratio']:.9f}   ratio state {base['ratio_state']}")
    log(f"refused as host-deleted: {base['host_deleted_reactions_refused']} on "
        f"{base['genes_affected_by_refusal']}")
    lf = base.get("lane_filter")
    if lf:
        log(f"clone-side lane filter --min-lanes {lf['min_lanes']} (background left "
            f"whole): {lf['genes_before']:,} -> {lf['genes_after']:,} genes with a "
            f"reaction, mean reactions per clone {lf['mean_rxn_before']:.2f} -> "
            f"{lf['mean_rxn_after']:.2f}")
    else:
        log("clone-side lane filter: none")
    log("positive sets: " + ", ".join(f"{k} {len(v):,}" for k, v in pos_sets.items()))

    pairs = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    terminals = {base["numerator"]["src"], base["numerator"]["snk"],
                 base["denominator"]["snk"]}
    incident = set()
    for t in terminals:
        m = pairs[(pairs.substrate == t) | (pairs["product"] == t)]
        incident |= set(m.mnxr.astype(str))
    module = {g for g, r in zip(df.gene_norm, df.rxns)
              if r and (set(str(r).split(",")) & incident)}
    log(f"\ntautology module (a reaction incident to pyruvate, ethanol or oxaloacetate): "
        f"{len(module)} genes")

    skeleton = df.carbon_bond_change.isin(SKELETON)
    log(f"skeleton-changing genes (carbon_bond_change in {SKELETON}): "
        f"{int(skeleton.sum())} of {len(df):,}")

    report = {"tag": tag, "host": a.host, "channel": a.channel, "fold": a.fold,
              "host_ratio": base["host_ratio"], "populations": {}}
    for pop, sub0 in (("all_genes", df), ("skeleton_changing", df[skeleton])):
        log(f"\n{'=' * 78}\n### population {pop}  --  {len(sub0):,} genes, "
            f"{int((sub0.n_rxn > 0).sum()):,} atom-mapped\n{'=' * 78}")
        per_pop = {"n": int(len(sub0)), "sets": {}}
        for name, genes in pos_sets.items():
            sub = sub0.copy()
            sub["is_positive"] = sub.gene_norm.isin(genes)
            log(f"\n-- {name}: {int(sub.is_positive.sum())} of {len(genes)} in this "
                f"population " + "-" * 20)
            if sub.is_positive.sum() == 0:
                log("  no positive of this set survives the population filter -- "
                    "no ranking exists")
                per_pop["sets"][name] = {"skipped": "no positives in this population"}
                continue
            rec = S.analyse(sub, f"{pop}/{name}", log)
            struck = sub[~sub.gene_norm.isin(module)]
            log(f"  [tautology control: {len(sub) - len(struck)} module gene(s) struck]")
            rec["module_struck"] = S.analyse(struck, f"{pop}/{name}/no-module", log)
            rec["null_tail"] = null_tail(sub, name, log)
            signed = sub[sub.is_positive & (sub.n_rxn > 0)]
            rec["signed"] = dict(n=int(len(signed)),
                                 n_up=int((signed.delta_ratio_pct > 0).sum()),
                                 n_down=int((signed.delta_ratio_pct < 0).sum()),
                                 n_flat=int((signed.delta_ratio_pct == 0).sum()))
            log(f"  sign: {rec['signed']['n']} positives carry a non-zero response -- "
                f"{rec['signed']['n_up']} raise the ethanol share, "
                f"{rec['signed']['n_down']} lower it, {rec['signed']['n_flat']} flat")
            if name == "confirmed_clones" and sub.score.nunique() > 1:
                rec["resample"] = resample(sub, n_neg=100, reps=a.reps, seed=a.seed,
                                           log=log)
            per_pop["sets"][name] = rec
        log(f"\n  the top 20 of {pop} by |delta_ratio_pct|:")
        log(sub0.nlargest(20, "score")[["gene", "n_rxn", "delta_ratio_pct",
                                        "carbon_bond_change", "phenotype"]]
            .to_string(index=False, float_format=lambda v: f"{v:.6g}"))
        report["populations"][pop] = per_pop

    stem = f"SCORE_{tag}"
    (SWEEPS / f"{stem}.json").write_text(json.dumps(report, indent=2, default=float))
    (SWEEPS / f"{stem}.txt").write_text("\n".join(lines) + "\n")
    print(f"\n-> {(SWEEPS / stem).relative_to(ROOT)}.{{json,txt}}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
