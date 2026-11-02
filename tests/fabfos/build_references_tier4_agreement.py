#!/usr/bin/env python3
"""Measure this build's atom-pair table against the deployed tier-4 table.

    python tests/build_references_tier4_agreement.py \
        --pairs data/fabfos/processed/ensemble/<ver>/aam_pairs.parquet \
        --ledger data/fabfos/processed/ensemble/<ver>/ledger.parquet

WHY THERE IS A TARGET AT ALL. `atom_pairs_tier4.parquet` -- 2,455,235 correspondences
over 63,621 reactions, frozen 2026-07-20 -- already exists. It was produced by a chain of
hand-run scripts across several sessions with its accounting kept in markdown: a good
result with no reproducible producer. This graph is the producer. The point of building
it is not to discover the answer, it is to be able to get the answer again, so the test
that matters is whether it lands in the same place.

THE FLOOR IS THE DEPLOYED TABLE, not a ceiling. Two of the three gated numbers are
recalls AGAINST tier 4, and exceeding it is expected in one specific way: every one of
tier 4's 9,089 rescue-derived reactions is `mcs_only`, a single mapper at half weight,
because its crosswalk was built after its mappers had run. Here three mappers see the
completed reactions, so those rows gain corroboration and the weights will NOT match.
That is why weight is not compared and provenance is only reported.

WHAT IS COMPARED, IN INCREASING STRICTNESS

  1. REACTION RECALL. Did we produce any correspondence at all for each of tier 4's
     reactions? Gated.
  2. MOLECULE-LEVEL RECALL. Of tier 4's (reaction, element, substrate, product) claims
     over the reactions we share, how many do we also make? This says the same atoms of
     the same element went from the same molecule to the same molecule -- which is what
     the downstream conductance actually uses. Gated.
  3. EXACT ATOM-LEVEL RECALL. The same over the full 6-tuple, canonical rank included.
     REPORTED, NOT GATED, and the reason is structural: this build lays MetaCyc down
     FIRST, so on the ~15,539 reactions where curation and prediction both have an answer
     the curated indices replace the predicted ones. The two agree 98.6% at molecule level
     and 90.8% atom-to-atom, which caps this metric near 98% before anything is wrong.
     Reading a 92% here as a defect would be reading the layer order as a defect.

AND THE PART THAT MAKES A FAILURE ACTIONABLE: every tier-4 reaction we do NOT have is
broken down by the LEDGER's outcome for it. A miss with a named reason -- refused as
oversize, no lane would complete it, mapped and produced nothing -- is a result. A miss
whose reason is `banked` or absent from the ledger entirely is a bug, and the run has
one of those or it does not.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
DEFAULT_TIER4 = REPO / "data" / "fabfos" / "benchmark" / "reference_tier4" / "atom_pairs_tier4.parquet"

MOL_KEY = ["mnxr", "element", "substrate", "product"]
ATOM_KEY = MOL_KEY + ["sub_idx", "prod_idx"]

TIER4_ELEMENT_RXN = {"C": 61989, "N": 47532, "S": 15624, "P": 34922}


def _norm(d: pd.DataFrame) -> pd.DataFrame:
    d = d.copy()
    for c in ("sub_idx", "prod_idx"):
        d[c] = pd.to_numeric(d[c], errors="coerce").astype("Int64")
    for c in ("mnxr", "element", "substrate", "product"):
        d[c] = d[c].astype(str)
    return d


def _keyset(d: pd.DataFrame, cols) -> set:
    return set(map(tuple, d[cols].itertuples(index=False, name=None)))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pairs", required=True,
                    help="this build's stacked aam_pairs.parquet -- the evidence copy "
                         "under data/processed/ensemble/<version>/, since the product "
                         "itself is the ENCODED table")
    ap.add_argument("--tier4", default=str(DEFAULT_TIER4))
    ap.add_argument("--ledger", default=None,
                    help="this build's ledger.parquet. Without it the misses are a "
                         "number; with it they are a number with reasons")
    ap.add_argument("--min-reaction-recall", type=float, default=0.95)
    ap.add_argument("--min-molecule-recall", type=float, default=0.95)
    a = ap.parse_args()

    t4p = Path(a.tier4)
    if not t4p.exists():
        raise SystemExit(
            f"[tier4] {t4p} is absent. It is a VALIDATION artifact -- read by this gate "
            f"and by nothing in the build -- and it is dvc-tracked, so `dvc pull "
            f"data/fabfos/benchmark/reference_tier4.dvc` fetches it. Its absence must not be "
            f"read as agreement.")

    ours = _norm(pd.read_parquet(a.pairs, columns=ATOM_KEY + ["method", "source"]))
    t4 = _norm(pd.read_parquet(t4p, columns=ATOM_KEY + ["method", "source"]))

    t4_rxn, our_rxn = set(t4["mnxr"]), set(ours["mnxr"])
    shared = t4_rxn & our_rxn
    missing = t4_rxn - our_rxn
    extra = our_rxn - t4_rxn
    rxn_recall = len(shared) / len(t4_rxn)

    t4s = t4[t4["mnxr"].isin(shared)]
    os_ = ours[ours["mnxr"].isin(shared)]
    t4_mol, our_mol = _keyset(t4s, MOL_KEY), _keyset(os_, MOL_KEY)
    mol_recall = len(t4_mol & our_mol) / len(t4_mol) if t4_mol else 0.0
    t4_atom, our_atom = _keyset(t4s, ATOM_KEY), _keyset(os_, ATOM_KEY)
    atom_recall = len(t4_atom & our_atom) / len(t4_atom) if t4_atom else 0.0

    print("=" * 72)
    print("AGREEMENT WITH THE DEPLOYED TIER-4 TABLE")
    print("=" * 72)
    print(f"  tier 4        {len(t4):>10,} correspondences  {len(t4_rxn):>7,} reactions")
    print(f"  this build    {len(ours):>10,} correspondences  {len(our_rxn):>7,} reactions")
    print()
    print(f"  reaction recall     {rxn_recall:7.2%}  "
          f"({len(shared):,} of {len(t4_rxn):,})     gate >= "
          f"{a.min_reaction_recall:.0%}")
    print(f"  molecule recall     {mol_recall:7.2%}  "
          f"({len(t4_mol & our_mol):,} of {len(t4_mol):,})   gate >= "
          f"{a.min_molecule_recall:.0%}")
    print(f"  exact atom recall   {atom_recall:7.2%}  "
          f"({len(t4_atom & our_atom):,} of {len(t4_atom):,})   reported, not gated")
    print(f"  reactions we add    {len(extra):>7,}  (not in tier 4 at all)")

    print("\n  per element -- reactions carrying at least one correspondence:")
    print(f"    {'':<3}{'tier 4':>10}{'this build':>12}{'delta':>10}")
    for X, want in TIER4_ELEMENT_RXN.items():
        got = int(ours.loc[ours.element == X, "mnxr"].nunique())
        print(f"    {X:<3}{want:>10,}{got:>12,}{got - want:>+10,}")

    print("\n  provenance of what we produced (tier 4's, for comparison):")
    om = ours["method"].value_counts()
    tm = t4["method"].value_counts()
    for k in sorted(set(om.index) | set(tm.index)):
        print(f"    {k:<20} {int(om.get(k, 0)):>10,}   tier4 {int(tm.get(k, 0)):>10,}")

    unexplained = 0
    if a.ledger:
        led = pd.read_parquet(a.ledger, columns=["mnxr", "outcome", "verdict"])
        led = led.set_index("mnxr")
        rows = []
        for m in missing:
            if m in led.index:
                rows.append(str(led.at[m, "outcome"]))
            else:
                rows.append("NOT IN THE LEDGER")
        vc = pd.Series(rows, dtype=str).value_counts()
        print(f"\n  the {len(missing):,} tier-4 reactions we do not have, by our reason:")
        for k, n in vc.items():
            print(f"    {k:<24} {int(n):>8,}")
        unexplained = int(vc.get("banked", 0)) + int(vc.get("NOT IN THE LEDGER", 0))
        if unexplained:
            print(f"\n  {unexplained:,} of those have NO valid reason -- the ledger and "
                  f"the table disagree about them. That is a bug in this build, not a "
                  f"gap in its coverage.")
    else:
        print(f"\n  {len(missing):,} tier-4 reactions missing; pass --ledger to see why.")

    fails = []
    if rxn_recall < a.min_reaction_recall:
        fails.append(f"reaction recall {rxn_recall:.2%} < {a.min_reaction_recall:.0%}")
    if mol_recall < a.min_molecule_recall:
        fails.append(f"molecule recall {mol_recall:.2%} < {a.min_molecule_recall:.0%}")
    if unexplained:
        fails.append(f"{unexplained:,} misses the ledger cannot explain")

    print("\n" + "=" * 72)
    if fails:
        for f in fails:
            print(f"FAIL -- {f}", file=sys.stderr)
        return 1
    print("PASS -- this build reproduces the deployed table within the declared floor")
    return 0


if __name__ == "__main__":
    sys.exit(main())
