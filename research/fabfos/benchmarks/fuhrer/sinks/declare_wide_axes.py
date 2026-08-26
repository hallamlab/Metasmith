#!/usr/bin/env python3
"""The genome-wide axis set: every metabolite the probe can reach and the screen can score,
selected without reference to the paper's own AUC.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \\
        research/fabfos/benchmarks/fuhrer/sinks/declare_wide_axes.py
    ... --publish     # writes data/fabfos/benchmarks/fuhrer/axes_wide.tsv

THIS IS THE UNBIASED COUNTERPART TO `declare_axes.py` AND THE TWO EXIST AS A PAIR. The
declared panel takes rules R1-R3 and then keeps the eight with the highest PUBLISHED AUC --
a statistic Fuhrer derived from network adjacency, which is close to the quantity under
test, so the panel is biased toward metabolites where adjacency already works. This file
applies R1 and R2 and stops. R4 is not applied at all, and R3 is not either: a size control
is a per-axis argument that does not scale to a hundred sinks, and the reaction-count
control inside the analyser covers every axis here.

ONE ION PER COMPOUND, CHOSEN BY POSITIVE COUNT rather than by AUC. A compound often sits on
several ions and each is a separate measurement of it; taking the ion with the most
deletions past |z| > 2.765 keeps the best-powered measurement without consulting the
statistic this arm is being compared against. Choosing by AUC here would reintroduce
exactly the bias the file exists to avoid.

WHAT THIS SET IS NOT. It is not "every metabolite in the screen". 7,534 ions become this
many for reasons that are counted in the output and belong in the report rather than in a
footnote: most ions carry no KEGG annotation at all, most annotated ones are chemically
ambiguous, and most unambiguous ones name a compound the curated background carries no
carbon for. A sweep that quietly considers only what survived reports coverage it never had.
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
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/fuhrer/sinks"))
import declare_axes as D                                              # noqa: E402

OUT = ROOT / "data/fabfos/benchmarks/fuhrer/axes_wide.tsv"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write {OUT.relative_to(ROOT)}")
    a = ap.parse_args()

    res = pd.read_csv(D.RESOLUTION, sep="\t", dtype=str).fillna("")
    roster = pd.read_csv(D.SCREEN / "gene_roster.tsv", sep="\t", dtype=str).fillna("")
    deletions = [g for g in roster.gene if g != "wt"]
    names = D.mnx_names()

    ions = res.ion_index.nunique() if False else len(res[["mode", "ion_index"]]
                                                     .drop_duplicates())
    print(f"{ions:,} ions measured")
    print(f"   {int((res.resolution == 'no_annotation').sum()):>6,} carry no KEGG "
          f"annotation within 3 mDa")
    ann = res[res.resolution != "no_annotation"]
    print(f"   {ann[['mode', 'ion_index']].drop_duplicates().shape[0]:>6,} are annotated, "
          f"{int((ann.n_compound.astype(int) > 1).sum()):,} of those rows on a mass that "
          f"fits more than one compound")
    solo = res[(res.resolution == "resolved") & (res.n_compound == "1")]
    print(f"   {len(solo):>6,} name exactly one cross-referenced compound")
    cand = solo[solo.node_gem == "True"].copy()
    print(f"   {len(cand):>6,} of those compounds carry carbon in the curated background "
          f"(R1)")

    n_pos: dict[tuple[str, int], int] = {}
    for mode in ("neg", "pos"):
        want = sorted(int(i) for i in cand[cand["mode"] == mode].ion_index)
        if not want:
            continue
        z = pd.read_parquet(D.SCREEN / f"zscore_{mode}.parquet", columns=deletions)
        arr = z.to_numpy()[[i - 1 for i in want]]
        for i, row in zip(want, arr):
            n_pos[(mode, i)] = int(np.sum(np.abs(row) > D.Z_THRESHOLD))
        del z, arr
    cand["n_positive"] = [n_pos[(m, int(i))] for m, i in zip(cand["mode"], cand.ion_index)]
    scorable = cand[cand.n_positive >= D.MIN_POSITIVES]
    print(f"   {len(scorable):>6,} have at least {D.MIN_POSITIVES} deletions past "
          f"|z| > {D.Z_THRESHOLD} (R2)")

    best = (scorable.sort_values(["n_positive", "ion_index"], ascending=[False, True])
            .drop_duplicates("mnxm"))
    print(f"   {len(best):>6,} distinct metabolites after keeping each compound's "
          f"best-powered ion\n")

    base = dict(dataset="fuhrer", src_mnxm=D.SOURCE_MNXM, src_name=D.SOURCE_NAME,
                element=D.ELEMENT, citation=D.CITATION)
    rows = [dict(base, role="target", axis=f"{r.kegg_id.lower()}_{r.mode}{r.ion_index}",
                 sink_mnxm=r.mnxm, sink_name=names.get(r.mnxm, r.kegg_name),
                 expected_dir="-",
                 note=(f"{r.kegg_name} ({r.kegg_id}) on {r.mode} ion {r.ion_index}. "
                       f"Admitted on reachability and power alone: R1 and R2 of "
                       f"sinks/declare_axes.py, with no reference to the published AUC."),
                 mode=r.mode, ion_index=r.ion_index, kegg_id=r.kegg_id,
                 published_auc=r.published_auc, n_positive=r.n_positive,
                 n_measured=len(deletions))
            for r in best.itertuples()]
    rows.append(dict(base, role="control", axis=D.DENOMINATOR_AXIS, sink_mnxm=D.CO2_MNXM,
                     sink_name=names.get(D.CO2_MNXM, "CO2"), expected_dir="0",
                     note="The ratio's shared denominator, as in axes.tsv.",
                     mode="", ion_index="", kegg_id="", published_auc="",
                     n_positive="", n_measured=""))
    panel = pd.DataFrame(rows, columns=list(D.FIELDS))
    have = panel[panel.published_auc != ""]
    print(f"{len(panel) - 1} target axes | positives per axis: median "
          f"{int(best.n_positive.median())}, range {int(best.n_positive.min())}-"
          f"{int(best.n_positive.max())} | {len(have)} carry a published AUC, "
          f"{len(panel) - 1 - len(have)} do not and are swept anyway")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(ROOT)})")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.unlink(missing_ok=True)
    panel.to_csv(OUT, sep="\t", index=False)
    print(f"\n-> {OUT.relative_to(ROOT)}: {len(panel)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
