#!/usr/bin/env python3
"""Declare the Fuhrer axis panel -- targets, their size controls, and the shared
denominator -- from rules fixed before any ECSPr number exists.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \\
        research/fabfos/benchmarks/fuhrer/sinks/declare_axes.py
    ... --publish     # writes data/fabfos/benchmarks/fuhrer/{axes.tsv,axis_candidates.tsv}

THE ADMISSION RULES, IN ORDER, AND EVERY CANDIDATE THAT FAILS ONE IS RECORDED BESIDE THE
PANEL IN `axis_candidates.tsv` RATHER THAN DROPPED:

  R1 UNAMBIGUOUS AND PRESENT.  The ion's KEGG annotations resolve to exactly one MetaNetX
     identifier and that identifier carries carbon in the curated background. An ion whose
     mass fits six isomers is a mass, not a metabolite, and gets no axis.
  R2 SCORABLE.  At least MIN_POSITIVES deletions cross the paper's own reproducibility
     threshold of |z| > 2.765 on that ion. Below that the AUC is noise however it comes out.
  R3 A SIZE CONTROL EXISTS.  Some metabolite one reaction away from the sink, sharing the
     most carbon atoms with it of any neighbour, is itself a node. That neighbour is
     declared here, in the same file, as the axis's own control.
  R4 THE EIGHT WITH THE HIGHEST PUBLISHED AUC among those passing R1-R3.

**CAUTION** R4 SELECTS ON A STATISTIC DERIVED FROM NETWORK ADJACENCY, WHICH IS CLOSE TO THE
QUANTITY UNDER TEST. Table EV4's AUC ranks deletions by |z| and asks whether the metabolite's
own enzymes come out on top -- the same gene-metabolite association ECSPr predicts, measured
in the opposite direction. Choosing the panel on it therefore biases the panel toward
metabolites where adjacency already works. The fix is disclosure, not avoidance: the
genome-wide sweep sweeps every ion passing R1-R2 with no reference to the AUC at all, and is
the unbiased counterpart. Both numbers belong in the report and neither stands alone.

ONE COMPOUND, ONE AXIS. A compound often appears on several ions -- oxalate on three, in two
ionization modes -- and each ion is a separate measurement of it. The panel keeps the
highest-AUC ion per compound, so eight axes are eight metabolites rather than eight rows.

THE SOURCE IS THE PAPER'S, NOT A CHOICE. The screen grows on M9 glucose, so the numerator is
the conductance from D-glucose to the sink. The denominator is D-glucose to CO2, shared
across every axis exactly as the fang arm shares one denominator across four numerators:
carbon leaving the cell rather than being built into anything is what makes a ratio a
statement about routing to the axis.

`expected_dir` IS `-` ON EVERY TARGET AND THAT IS AN ARGUMENT, NOT A CONVENTION. This arm
deletes, so a gene whose loss cuts the conductance from glucose to the sink should leave less
of the sink. The measured z-score is signed and free to disagree, which is the point -- the
sign column exists so `analyse` can report agreement against the label set's own base rate.
"""
from __future__ import annotations

import argparse
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

import bake_pairs                                                        # noqa: E402
from ecspr.model.build import load_pairs                                 # noqa: E402

SCREEN = ROOT / "data/fabfos/runs/fuhrer_clones/parse/screen"
RESOLUTION = ROOT / "data/fabfos/runs/fuhrer_clones/parse/readout_resolution.tsv"
CHEM_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
OUT = ROOT / "data/fabfos/benchmarks/fuhrer"

HOST = "e_coli_bw25113"
ELEMENT = "C"
SOURCE_MNXM = "MNXM1364061"      # D-glucose, the medium's carbon source
SOURCE_NAME = "D-glucose"
CO2_MNXM = "MNXM13"
Z_THRESHOLD = 2.765              # Fuhrer 2017 Methods: 1% false-positive probability
MIN_POSITIVES = 20
N_AXES = 8
DENOMINATOR_AXIS = "denominator"
CITATION = "Fuhrer et al. Mol Syst Biol 13:907 (2017); doi:10.15252/msb.20167150"

FIELDS = ("dataset", "role", "axis", "src_mnxm", "src_name", "sink_mnxm", "sink_name",
          "element", "expected_dir", "citation", "note",
          "mode", "ion_index", "kegg_id", "published_auc", "n_positive", "n_measured")
CANDIDATE_FIELDS = ("mode", "ion_index", "kegg_id", "kegg_name", "mnxm", "node_gem",
                    "n_compound", "published_auc", "n_positive", "control_mnxm",
                    "verdict")


def mnx_names() -> dict[str, str]:
    out: dict[str, str] = {}
    with CHEM_PROP.open() as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            f = line.rstrip("\n").split("\t")
            out[f[0]] = f[1]
    return out


def curated_pairs() -> pd.DataFrame:
    """Atom pairs restricted to the reactions the curated host background carries."""
    df = pd.read_parquet(ROOT / f"data/fabfos/runs/{HOST}/gpr/gpr_gem.parquet",
                         columns=["mnxr", "in_atom_universe"])
    keep = df.in_atom_universe.fillna(False).infer_objects(copy=False).astype(bool)
    weights = set(df[keep].mnxr.dropna().astype(str))
    p = load_pairs(bake_pairs.atom_pairs(), element=ELEMENT)
    return p[p.mnxr.isin(weights)]


def size_controls(pairs: pd.DataFrame, sinks: list[str],
                  forbidden: set[str]) -> dict[str, tuple[str, int, int]]:
    """R3. For each sink, the one-reaction neighbour sharing the most carbon atoms with it.

    Computed off the ATOM-PAIR TABLE rather than from a hand-written neighbour list, for
    the same reason the tautology control is: a list written by hand encodes what its
    author remembered about the pathway, and this arm sweeps metabolites nobody here has
    an opinion about. `n_shared` is the count of mapped carbon atoms the reaction carries
    between the two, so the winner is the neighbour differing from the sink by the fewest
    carbons -- `one group away`, measured rather than asserted.

    The precursor direction is preferred: a substrate that becomes the sink is the sharper
    control, because it is what the sink is made FROM and a probe that cannot tell the two
    apart has ranked the module rather than the metabolite.
    """
    out: dict[str, tuple[str, int, int]] = {}
    carbons = (pd.concat([pairs.groupby("substrate").sub_idx.nunique(),
                          pairs.groupby("product").prod_idx.nunique()])
               .groupby(level=0).max())
    for sink in sinks:
        best: tuple[int, int, str] | None = None       # (precursor?, n_shared, mnxm)
        for into, other in ((True, pairs[pairs["product"] == sink]),
                            (False, pairs[pairs.substrate == sink])):
            col = "substrate" if into else "product"
            for mnxm, grp in other.groupby(col):
                if mnxm == sink or mnxm in forbidden:
                    continue
                key = (1 if into else 0, len(grp), str(mnxm))
                if best is None or key > best:
                    best = key
        if best is not None:
            out[sink] = (best[2], best[1], int(carbons.get(best[2], 0)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(ROOT)}")
    a = ap.parse_args()

    res = pd.read_csv(RESOLUTION, sep="\t", dtype=str).fillna("")
    roster = pd.read_csv(SCREEN / "gene_roster.tsv", sep="\t", dtype=str).fillna("")
    deletions = [g for g in roster.gene if g != "wt"]
    names = mnx_names()

    # R1
    cand = res[(res.resolution == "resolved") & (res.n_compound == "1")
               & (res.node_gem == "True")].copy()
    print(f"R1 unambiguous, cross-referenced, a curated node: {len(cand)} ions")

    # R2 -- one column read per mode, over the deletion columns only
    n_pos: dict[tuple[str, int], int] = {}
    n_measured = len(deletions)
    for mode in ("neg", "pos"):
        want = sorted(int(i) for i in cand[cand["mode"] == mode].ion_index)
        if not want:
            continue
        z = pd.read_parquet(SCREEN / f"zscore_{mode}.parquet", columns=deletions)
        arr = z.to_numpy()[[i - 1 for i in want]]
        for i, row in zip(want, arr):
            n_pos[(mode, i)] = int(np.sum(np.abs(row) > Z_THRESHOLD))
        del z, arr
    cand["n_positive"] = [n_pos[(m, int(i))]
                          for m, i in zip(cand["mode"], cand.ion_index)]
    print(f"R2 at least {MIN_POSITIVES} deletions past |z| > {Z_THRESHOLD} of "
          f"{n_measured:,} measured: {int((cand.n_positive >= MIN_POSITIVES).sum())} ions "
          f"(median positives {int(cand.n_positive.median())})")

    # R3
    pairs = curated_pairs()
    scorable = cand[cand.n_positive >= MIN_POSITIVES]
    forbidden = {SOURCE_MNXM, CO2_MNXM} | set(scorable.mnxm)
    controls = size_controls(pairs, sorted(set(scorable.mnxm)), forbidden)
    cand["control_mnxm"] = [controls.get(m, ("", 0, 0))[0] for m in cand.mnxm]
    print(f"R3 a one-reaction carbon neighbour that is itself a node: "
          f"{len(controls)} of {scorable.mnxm.nunique()} distinct sinks")

    def verdict(r) -> str:
        if r.n_positive < MIN_POSITIVES:
            return f"fail_R2_only_{r.n_positive}_positives"
        if not r.control_mnxm:
            return "fail_R3_no_carbon_neighbour"
        if not r.published_auc:
            return "fail_R4_no_published_auc"
        return "eligible"

    cand["verdict"] = [verdict(r) for r in cand.itertuples()]
    eligible = cand[cand.verdict == "eligible"].copy()
    eligible["auc"] = eligible.published_auc.astype(float)
    # R4 -- one axis per compound, on that compound's best-scoring ion
    eligible = (eligible.sort_values("auc", ascending=False)
                .drop_duplicates("mnxm").head(N_AXES))
    n_eligible = int((cand.verdict == "eligible").sum())
    cand.loc[cand.index.isin(eligible.index), "verdict"] = "DECLARED"
    print(f"R4 top {len(eligible)} of {n_eligible} eligible ions on "
          f"{cand[cand.verdict.isin(('eligible', 'DECLARED'))].mnxm.nunique()} distinct "
          f"compounds, one axis per compound, by published AUC\n")

    rows: list[dict] = []
    base = dict(dataset="fuhrer", src_mnxm=SOURCE_MNXM, src_name=SOURCE_NAME,
                element=ELEMENT, citation=CITATION)
    for r in eligible.itertuples():
        axis = f"{r.kegg_id.lower()}_{r.mode}{r.ion_index}"
        cm, shared, ccarbon = controls[r.mnxm]
        rows.append(dict(base, role="target", axis=axis, sink_mnxm=r.mnxm,
                         sink_name=names.get(r.mnxm, r.kegg_name), expected_dir="-",
                         note=(f"{r.kegg_name} ({r.kegg_id}) on {r.mode} ion {r.ion_index}, "
                               f"the only compound within 3 mDa of that mass. "
                               f"{r.n_positive} of {n_measured} deletions pass "
                               f"|z| > {Z_THRESHOLD}. Published AUC {r.published_auc}."),
                         mode=r.mode, ion_index=r.ion_index, kegg_id=r.kegg_id,
                         published_auc=r.published_auc, n_positive=r.n_positive,
                         n_measured=n_measured))
        rows.append(dict(base, role="control", axis=f"{axis}_precursor", sink_mnxm=cm,
                         sink_name=names.get(cm, cm), expected_dir="-",
                         note=(f"SIZE CONTROL for {axis}: the one-reaction carbon "
                               f"neighbour sharing the most atoms with it ({shared} "
                               f"mapped carbons; the neighbour carries {ccarbon}). Not a "
                               f"readout of this screen -- a competing sink the probe "
                               f"must rank differently if it is ranking the metabolite "
                               f"rather than the module around it."),
                         mode="", ion_index="", kegg_id="", published_auc="",
                         n_positive="", n_measured=""))
    rows.append(dict(base, role="control", axis=DENOMINATOR_AXIS, sink_mnxm=CO2_MNXM,
                     sink_name=names.get(CO2_MNXM, "CO2"), expected_dir="0",
                     note=("The ratio's shared denominator: carbon leaving the cell "
                           "rather than being built into any axis. One solve per clone "
                           "serves every axis, which is what makes the axes comparable."),
                     mode="", ion_index="", kegg_id="", published_auc="",
                     n_positive="", n_measured=""))

    panel = pd.DataFrame(rows, columns=list(FIELDS))
    print(panel[["role", "axis", "sink_mnxm", "sink_name", "published_auc",
                 "n_positive"]].to_string(index=False))
    print()
    for v, n in cand.verdict.value_counts().items():
        print(f"   {v:34} {n:>4}")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(ROOT)})")
        return 0
    OUT.mkdir(parents=True, exist_ok=True)
    for path, frame in ((OUT / "axes.tsv", panel),
                        (OUT / "axis_candidates.tsv",
                         cand[list(CANDIDATE_FIELDS)].sort_values(
                             ["verdict", "published_auc"], ascending=[True, False]))):
        path.unlink(missing_ok=True)
        frame.to_csv(path, sep="\t", index=False)
        print(f"-> {path.relative_to(ROOT)}: {len(frame)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
