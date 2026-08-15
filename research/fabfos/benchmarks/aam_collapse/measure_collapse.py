"""What the stoichiometric collapse moves, measured over the whole reaction universe.

    mamba run -n rdkit-scratch python research/fabfos/benchmarks/aam_collapse/measure_collapse.py \
        --lookups <dir> --bake data/fabfos/processed/metabolism_bake --outdir <dir>

WHY THIS EXISTS. `aam.worklist.ATOM_LIMIT` (600) is applied to a STOICHIOMETRIC
EXPANSION, so it measures how many times a molecule appears rather than how much
distinct chemistry a mapper must attend to. Nitrogenase hydrolyses 16 ATP and is
refused at over a thousand atoms. The fix is counting each distinct molecule once --
an exact reduction, not an approximation. This measures the fix before anything is
spent on a rebake.

WHAT IT PRODUCES, and the order matters: the superset assertion runs FIRST, before
any of the pleasing numbers, because it is the campaign's stop line written as a check.

  verdict_movement.tsv   the histogram under both measures, and the reaction-by-reaction
                         transitions between them
  yield_curve.tsv        banking rate by atom count under BOTH measures, joined to the
                         deployed bake -- the threshold's warrant
  recovered.tsv          every reaction the collapse admits, with both counts
  nitrogen.tsv           the N-fixation reactions by name, not as a count
  populations.tsv        what is still refused and why -- the partial lane's target sizes

WHAT THE CURVE RESTS ON. The deployed bake PREDATES the 600-atom cut -- it holds 14
reactions the cut now refuses -- so it can speak about the bins above the threshold,
which is the only place a threshold can actually be argued. That is what makes the
collapsed curve evidence rather than extrapolation, and it is why the support is every
buildable reaction rather than only the ones the current cap admits.

WHAT IT STILL CANNOT SHOW: a recovered reaction being MAPPABLE is not the same as its
being BANKED. This measures admission. Whether the mappers produce pairs for these
reactions is what the rebake answers, and no local artifact can stand in for it.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "src"))

from ecspr.bake.aam import worklist as W          # noqa: E402
from ecspr.bake import encoding as refs           # noqa: E402

# Bins for the yield curve. The lower edges are the ones the original threshold was
# argued over; the tail is kept coarse because it holds few reactions and no decision.
BINS = [(0, 100), (100, 200), (200, 300), (300, 400), (400, 500), (500, 600),
        (600, 800), (800, 1200), (1200, 1600), (1600, 10 ** 9)]


def bin_of(n):
    for lo, hi in BINS:
        if lo <= n < hi:
            return f"{lo}-{hi if hi < 10 ** 9 else 'inf'}"
    return "?"


def banked_reactions(bake: Path) -> set[str]:
    """MNXR ids the deployed bake actually holds atom pairs for.

    THE JOIN GOES THROUGH THE VOCABULARY. `atom_pairs.parquet` is keyed by vocabulary
    CODE, not by MNXR string -- reading the code as an id produces an empty join and a
    yield curve of zeros, which looks like a finding rather than a bug.
    """
    V = refs.load_vocab(bake / "vocab.parquet")
    pairs = refs.load_atom_pairs(bake / "atom_pairs.parquet")
    sym = V.symbols("rxn")
    return {str(sym[c]) for c in pd.unique(pairs["rxn"])}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lookups", required=True, type=Path,
                    help="a built `lookup::` directory (reactions/metabolites parquet)")
    ap.add_argument("--bake", required=True, type=Path,
                    help="the deployed metabolism_bake trio")
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("--collapsed-atom-limit", type=int, default=W.COLLAPSED_ATOM_LIMIT)
    ap.add_argument("--rebuild", action="store_true",
                    help="re-adjudicate rather than reusing the cached worklist")
    a = ap.parse_args(argv)
    a.outdir.mkdir(parents=True, exist_ok=True)

    rx = pd.read_parquet(a.lookups / "reactions.parquet")
    mets_full = pd.read_parquet(a.lookups / "metabolites.parquet",
                                columns=["mnxm", "name", "formula"])
    name_of = dict(zip(mets_full["mnxm"], mets_full["name"]))
    print(f"[measure] {len(rx):,} reactions, {len(name_of):,} metabolite names", flush=True)

    # ONE adjudication, not two. `atoms_collapsed` is recorded whenever a collapse was
    # attempted regardless of whether it was accepted, so the before-state is recoverable
    # from the same table -- and a second full pass would be twenty minutes of rdkit to
    # re-derive a column that is already there.
    cache = a.outdir / "worklist_both_measures.parquet"
    if cache.exists() and not a.rebuild:
        # Eight minutes of rdkit; everything after it is seconds. Cached so the
        # reporting half can be iterated on without re-adjudicating the universe.
        print(f"[measure] reusing {cache} (pass --rebuild to re-adjudicate)", flush=True)
        wl = pd.read_parquet(cache)
    else:
        wl = W.adjudicate(rx, name_of, W.ATOM_LIMIT, W.SMILES_LEN_LIMIT,
                          collapsed_atom_limit=a.collapsed_atom_limit)
        wl.to_parquet(cache, index=False)
    before = wl.assign(verdict_before=wl.apply(
        lambda r: ("too_long" if r["collapsed"] and r["chars"] > W.SMILES_LEN_LIMIT
                   else "oversize" if r["collapsed"]
                   else r["verdict"]), axis=1))
    wl["verdict_before"] = before["verdict_before"]

    # ---- the stop line, first -------------------------------------------------
    was = set(wl.loc[wl["verdict_before"] == "mappable", "mnxr"])
    now = set(wl.loc[wl["verdict"] == "mappable", "mnxr"])
    lost = was - now
    if lost:
        raise SystemExit(
            f"[measure] STOP: {len(lost):,} reactions lost `mappable`, e.g. "
            f"{sorted(lost)[:5]}. Coverage may only go up; a reaction under the "
            f"expanded cap must keep its expanded string byte for byte.")
    print(f"[measure] superset holds: {len(was):,} mappable before, {len(now):,} after, "
          f"{len(now - was):,} added, 0 lost", flush=True)

    # ---- verdict movement -----------------------------------------------------
    mv = Counter(zip(wl["verdict_before"], wl["verdict"]))
    rows = [dict(verdict_before=b, verdict_after=a_, n=n) for (b, a_), n in
            sorted(mv.items(), key=lambda kv: -kv[1])]
    pd.DataFrame(rows).to_csv(a.outdir / "verdict_movement.tsv", sep="\t", index=False)

    # ---- the recovered set ----------------------------------------------------
    rec = wl[wl["collapsed"]][["mnxr", "atoms", "chars", "atoms_collapsed",
                               "chars_collapsed", "verdict_before"]]
    rec = rec.sort_values("atoms", ascending=False)
    rec.to_csv(a.outdir / "recovered.tsv", sep="\t", index=False)

    # ---- the yield curve ------------------------------------------------------
    banked = banked_reactions(a.bake)
    print(f"[measure] the deployed bake banked {len(banked):,} reactions", flush=True)

    # SUPPORT IS EVERY BUILDABLE REACTION, not just the ones the current cap admits.
    # The deployed bake predates the 600-atom cut -- it holds 14 reactions the cut now
    # refuses -- so it can speak about the bins above the threshold, which is exactly
    # where a threshold has to be argued. Restricting the support to reactions under
    # the cap would produce a flat 98% curve that says nothing about where to cut.
    #
    # Reactions with no buildable SMILES are excluded, and that is not the same
    # restriction: they were never a mapper's to attempt, so their banking rate is a
    # fact about the CURATION lanes and would read here as a fact about size.
    sup = wl[wl["rxn_smiles"].notna()].copy()
    sup["banked"] = sup["mnxr"].isin(banked)
    # The collapsed count for EVERY buildable reaction, not only the refused ones the
    # adjudication needed it for -- the curve needs both measures over one population.
    cols = [W.collapse(s) for s in sup["rxn_smiles"]]
    sup["chars_c_all"] = [len(c) if c else None for c in cols]
    sup["atoms_c_all"] = [W.count_atoms(c) if (c and len(c) <= W.SMILES_LEN_LIMIT) else None
                          for c in cols]
    curve = []
    for measure, col in (("expanded", "atoms"), ("collapsed", "atoms_c_all")):
        d = sup[sup[col].notna()]
        for lo, hi in BINS:
            m = (d[col] >= lo) & (d[col] < hi)
            n = int(m.sum())
            curve.append(dict(measure=measure, bin=bin_of(lo), n_reactions=n,
                              n_banked=int(d.loc[m, "banked"].sum()),
                              rate=(round(float(d.loc[m, "banked"].mean()), 4) if n else None)))
        over = sup[sup[col].isna()]
        curve.append(dict(measure=measure, bin="over-char-cap", n_reactions=len(over),
                          n_banked=int(over["banked"].sum()),
                          rate=(round(float(over["banked"].mean()), 4) if len(over) else None)))
    pd.DataFrame(curve).to_csv(a.outdir / "yield_curve.tsv", sep="\t", index=False)
    sup.to_parquet(a.outdir / "support.parquet", index=False)

    # ---- what the EXPANDED cap already costs, which collapse only partly repays ----
    # A finding rather than a by-product: the deployed bake holds reactions the current
    # 600-atom cut refuses, so the chain regressed against its own predecessor BEFORE
    # this change. Collapse repays the part of that debt caused by repeats; the rest is
    # genuinely large distinct chemistry and is a question about ATOM_LIMIT itself.
    refused = sup[(sup["atoms"].isna()) | (sup["atoms"] > W.ATOM_LIMIT)]
    debt = refused[refused["banked"]].copy()
    debt["repaid_by_collapse"] = debt["mnxr"].isin(set(wl.loc[wl["collapsed"], "mnxr"]))
    debt[["mnxr", "atoms", "chars", "atoms_c_all", "chars_c_all",
          "verdict", "repaid_by_collapse"]].sort_values("atoms").to_csv(
        a.outdir / "deployed_bake_debt.tsv", sep="\t", index=False)
    print(f"[measure] the expanded cap refuses {len(debt):,} reactions the deployed bake "
          f"banked; the collapse repays {int(debt['repaid_by_collapse'].sum()):,} of them",
          flush=True)

    # ---- the nitrogen case, by name ------------------------------------------
    # A count of recovered reactions does not answer the question this scope exists for.
    n2 = set(mets_full.loc[mets_full["formula"] == "N2", "mnxm"])
    eqs = dict(zip(rx["mnxr"], rx["equation"]))
    fix = [r for r, e in eqs.items() if isinstance(e, str) and any(m + "@" in e for m in n2)]
    nit = wl[wl["mnxr"].isin(fix)].merge(
        sup[["mnxr", "atoms_c_all", "banked"]], on="mnxr", how="left")
    nit = nit.merge(rx[["mnxr", "equation"]], on="mnxr", how="left")
    nit[["mnxr", "classifs", "verdict_before", "verdict", "atoms", "chars",
         "atoms_c_all", "collapsed", "banked", "equation"]].to_csv(
        a.outdir / "nitrogen.tsv", sep="\t", index=False)
    moved = nit[nit["collapsed"]]
    print(f"[measure] {len(nit):,} reactions carry an N2 species; "
          f"{len(moved):,} move from refused to mappable: "
          f"{', '.join(f'{r.mnxr} (EC {r.classifs})' for r in moved.itertuples())}",
          flush=True)

    # ---- what is still refused ------------------------------------------------
    pops = Counter(wl.loc[wl["verdict"] != "mappable", "verdict"])
    prows = [dict(population=k, n=v) for k, v in sorted(pops.items(), key=lambda kv: -kv[1])]
    prows.append(dict(population="recovered_by_collapse", n=int(wl["collapsed"].sum())))
    prows.append(dict(population="collapse_attempted", n=int(wl["atoms_collapsed"].notna().sum())))
    pd.DataFrame(prows).to_csv(a.outdir / "populations.tsv", sep="\t", index=False)

    print("\n=== verdicts, before -> after")
    print(pd.DataFrame(rows).head(20).to_string(index=False))
    print("\n=== yield curve")
    print(pd.DataFrame(curve).to_string(index=False))
    print("\n=== still refused")
    print(pd.DataFrame(prows).to_string(index=False))
    print(f"\nwrote {a.outdir}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
