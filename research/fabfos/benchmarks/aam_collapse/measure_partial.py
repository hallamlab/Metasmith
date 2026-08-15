"""What the partial lane would reach, sized before it is run.

    mamba run -n rdkit-scratch python research/fabfos/benchmarks/aam_collapse/measure_partial.py \
        --lookups <dir> --worklist <measure outdir>/worklist_both_measures.parquet \
        --outdir <dir>

THE LANE'S REAL TARGET SET CANNOT BE COMPUTED HERE, and saying so is half the point. It
is "reactions that were admitted and still ended with nothing", which is a fact about a
RUN -- you have to have asked the mappers. What CAN be measured locally is everything
else: the size-refused residual exactly, and the mapper-returned-nothing population
through a proxy.

THE PROXY, and why it is defensible. That population fails for one reason: RXNMapper's
transformer accepts at most 512 tokens, and the recorded set has median SMILES length
983 against the universe's 268, with 97.5% over 400 characters. So it is, to a good
approximation, "the long reactions" -- and reaction length is something this machine can
see. The table below sweeps the length threshold rather than picking one, so a reader can
put the cut where they think the token limit actually bites instead of inheriting a
guess.

WHAT THE NUMBERS MEAN. `forced` is (reaction, element) combinations conservation settles
with NO mapper at all -- free, exact, and available whether or not the rebake runs.
`reduced` is submissions that need one, so it is the lane's mapper cost as well as its
projected yield. `forced_pair_rows` is the row count the forced arm alone would add, and
it grows fast: an n-atom transfer with no unique bijection emits n^2 diluted candidates,
which is the doubly-stochastic completion behaving as designed and still a real number of
rows to carry.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "src"))

from ecspr.bake.aam import partial as P, worklist as W    # noqa: E402

# Sweep rather than pick. The recorded no-mapping set has median length 983.
PROXY_LENGTHS = (1600, 1200, 900, 600, 400)


def measure(label, targets, eq, formulas, smiles_of, ranks_of):
    uni, forced, tally = P.build(targets, eq, formulas, smiles_of, ranks_of,
                                 W.SMILES_LEN_LIMIT, W.ATOM_LIMIT)
    fre = len({(r[0], r[1]) for r in forced})
    return dict(population=label, n_reactions=len(targets),
                forced_reaction_elements=fre, reduced_submissions=len(uni),
                partial_reaction_elements=fre + len(uni),
                forced_pair_rows=sum(int(r[4]) for r in forced),
                over_atom_cap=int(tally.get("reduction over the atom cap", 0)),
                over_char_cap=int(tally.get("reduction over the character cap", 0)),
                no_balanced_reduction=sum(v for k, v in tally.items()
                                          if k.startswith("no balanced reduction")))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lookups", required=True, type=Path)
    ap.add_argument("--worklist", required=True, type=Path,
                    help="an adjudicated worklist, post-collapse")
    ap.add_argument("--outdir", required=True, type=Path)
    a = ap.parse_args(argv)
    a.outdir.mkdir(parents=True, exist_ok=True)

    wl = pd.read_parquet(a.worklist)
    rx = pd.read_parquet(a.lookups / "reactions.parquet", columns=["mnxr", "equation"])
    eq = dict(zip(rx["mnxr"], rx["equation"]))
    formulas, smiles_of, ranks_of = P.load_lookups(a.lookups)
    print(f"[partial] {len(wl):,} adjudicated reactions", flush=True)

    rows = []
    # EXACT: the population the worklist itself refuses, after the collapse has had its
    # second reading. No proxy involved -- this set is known.
    refused = list(wl.loc[wl["verdict"].isin(("oversize", "too_long")), "mnxr"])
    rows.append(measure("size-refused (exact)", refused, eq, formulas, smiles_of, ranks_of))

    # PROXY: the mapper-returned-nothing population, approached by reaction length.
    for lo in PROXY_LENGTHS:
        tgt = list(wl.loc[(wl["verdict"] == "mappable")
                          & (wl["rxn_smiles"].str.len() >= lo), "mnxr"])
        rows.append(measure(f"mappable, SMILES >= {lo} chars (proxy)", tgt,
                            eq, formulas, smiles_of, ranks_of))
        print(f"[partial] swept {lo}: {len(tgt):,} reactions", flush=True)

    df = pd.DataFrame(rows)
    df.to_csv(a.outdir / "partial_lane_yield.tsv", sep="\t", index=False)
    print("\n=== what the partial lane reaches")
    print(df.to_string(index=False))
    print(f"\nwrote {a.outdir / 'partial_lane_yield.tsv'}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
