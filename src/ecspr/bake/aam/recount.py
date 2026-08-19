"""Exact per-element counts for every metabolite, taken from the STRUCTURE where there
is one -- and NULL, still, where the count is genuinely unknown.

WHAT THIS FIXES. `atom_pairs.count_element` refuses a formula carrying `*`, `(` or `)`,
because such a formula does not state a count -- `C70H131N3O9PS*2` says "and two residues
of unspecified composition". That refusal is right about the FORMULA and wrong about the
SPECIES: the same MetaNetX record almost always carries a SMILES whose explicit atoms are
countable exactly, and the residue is a `*` atom that contributes to no element. So the
70 carbons are known; only the residue is not. Refusing the whole species turns a known
count into an unknown one and takes the reaction's balance test with it.

WHY IT IS A TABLE AND NOT A FUNCTION CALL. Five lanes across two images need this, and the
alternative is each of them re-parsing ~100k structures per run to answer the same
question. It is also the SAME question `lookup::atom_ranks` answers for the structured
participants -- `len(ranks)` is this count -- so publishing it once is what lets the two
be checked against each other instead of drifting.

THE ROUTES, IN ORDER, and the order is the point:

  1. `smiles`       -- explicit atoms of a parsed structure. FIRST, because the structure
                       is what the mapper sees and what `atom_ranks` is keyed on. A count
                       taken from anywhere else can disagree with the number of ranks the
                       pair table indexes into, and then a pair row claims an atom that
                       has no node.
  2. `formula`      -- no structure, but a formula with no residue marker and no nesting.
  3. `star_formula` -- no structure, a `*` formula whose core is still countable once the
                       marker is stripped. The count is exact for the explicit part.
  4. `inchi`        -- no structure, no usable formula, but a SINGLE-component InChI whose
                       formula layer parses. Multi-component (`.`) is refused: the layer
                       sums over components and MetaNetX may mean only one of them.
  5. `none`         -- nothing said it. `n_atoms` is NULL, and it must stay NULL through
                       the parquet -- not 0, not NaN. A zero is a CLAIM, and it is the
                       claim that lets an unbalanced reaction pass a balance gate.

A RECOUNT IS NOT A BALANCE CLAIM. `n_residue` travels with every row for exactly that
reason: the explicit atoms are exact, the residue is not, so a balance test over these
counts must still require the residue slots to cancel across the two sides before it
believes the difference. That is `residue_slots_cancel`, and it is why this module ships
the residue count rather than folding it away.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .. import atom_pairs as AP

ELEMENTS = AP.ELEMENTS

SCHEMA = pa.schema([
    ("mnxm", pa.string()),
    ("element", pa.string()),
    # NULLABLE ON PURPOSE, and pyarrow keeps it so as long as nothing casts through
    # numpy. `pd.NA` in an Int32 extension column round-trips; a float column with NaN
    # does not, and a NaN sums to something.
    ("n_atoms", pa.int32()),
    ("n_residue", pa.int32()),
    ("source", pa.string()),
    ("disagrees_with_formula", pa.bool_()),
])

COLS = tuple(f.name for f in SCHEMA)

# `*<n>` -- the residue marker and its multiplicity. `*` alone means one.
_STAR = re.compile(r"\*(\d*)")
_INCHI_FORMULA = re.compile(r"^InChI=1S?/([^/]+)")
_POLYMER_N = re.compile(r"(?<![A-Z])n")


def counts_from_smiles(smiles):
    if not isinstance(smiles, str) or not smiles.strip():
        return None, None
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    mol = Chem.MolFromSmiles(smiles, sanitize=False)
    if mol is None:
        return None, None
    counts, residue = Counter(), 0
    for a in mol.GetAtoms():
        if a.GetAtomicNum() == 0 or a.GetSymbol() == "*":
            residue += 1
        else:
            counts[a.GetSymbol()] += 1
    return counts, residue


def counts_from_star_formula(formula):
    if not isinstance(formula, str) or "*" not in formula:
        return None, 0
    m = _STAR.search(formula)
    residue = int(m.group(1)) if m.group(1) else 1
    core = _STAR.sub("", formula)
    if "(" in core or ")" in core or _POLYMER_N.search(core):
        return None, residue
    counts = Counter()
    for sym, num in AP.FORMULA_TERM.findall(core):
        if sym:
            counts[sym] += int(num) if num else 1
    return (counts or None), residue


def counts_from_inchi(inchi):
    if not isinstance(inchi, str) or not inchi.startswith("InChI="):
        return None
    m = _INCHI_FORMULA.match(inchi)
    if not m or "." in m.group(1):
        return None
    core = m.group(1)
    if "*" in core or "(" in core or _POLYMER_N.search(core):
        return None
    counts = Counter()
    for sym, num in AP.FORMULA_TERM.findall(core):
        if sym:
            counts[sym] += int(num) if num else 1
    return counts or None


def recount(formula, smiles, inchi):
    counts, residue = counts_from_smiles(smiles)
    if counts is not None:
        return {X: int(counts.get(X, 0)) for X in ELEMENTS}, residue, "smiles"

    declared = {X: AP.count_element(formula, X) for X in ELEMENTS}
    if all(v is not None for v in declared.values()):
        return declared, 0, "formula"

    counts, residue = counts_from_star_formula(formula)
    if counts is not None:
        return {X: int(counts.get(X, 0)) for X in ELEMENTS}, residue, "star_formula"

    counts = counts_from_inchi(inchi)
    if counts is not None:
        return {X: int(counts.get(X, 0)) for X in ELEMENTS}, 0, "inchi"

    return {X: None for X in ELEMENTS}, None, "none"


def residue_slots_cancel(sub_residues, prod_residues):
    # The guard that keeps a recount from becoming a balance claim: an exact count
    # of the explicit atoms says nothing about what is inside a `*`, so the only
    # reactions whose element balance this table can settle are the ones where the
    # residues cancel. A None on either side is unknown, and so a refusal rather
    # than a zero.
    if any(r is None for r in sub_residues) or any(r is None for r in prod_residues):
        return None
    return sum(sub_residues) == sum(prod_residues)


def build(metabolites: Path):
    mt = pd.read_parquet(metabolites,
                         columns=["mnxm", "formula", "smiles", "inchi"])
    rows, tally = [], Counter()
    for mnxm, formula, smiles, inchi in mt.itertuples(index=False, name=None):
        counts, residue, source = recount(formula, smiles, inchi)
        tally[f"source={source}"] += 1
        declared = {X: AP.count_element(formula, X) for X in ELEMENTS}
        for X in ELEMENTS:
            n = counts[X]
            d = declared[X]
            disagrees = bool(source == "smiles" and d is not None and n != d)
            if disagrees:
                tally[f"disagrees={X}"] += 1
            if n is not None and d is None:
                tally[f"recovered={X}"] += 1
            rows.append((mnxm, X, n, residue, source, disagrees))
    return rows, tally


def cmd_build(args):
    rows, tally = build(args.metabolites)
    df = pd.DataFrame(rows, columns=list(COLS))
    df["n_atoms"] = df["n_atoms"].astype("Int32")
    df["n_residue"] = df["n_residue"].astype("Int32")
    pq.write_table(pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False),
                   args.out, compression="zstd")

    n_known = int(df["n_atoms"].notna().sum())
    lines = ["kind\tkey\tn"]
    for k, v in sorted(tally.items()):
        lines.append(f"tally\t{k}\t{v}")
    lines.append(f"product\trows\t{len(df)}")
    lines.append(f"product\tmetabolites\t{df['mnxm'].nunique()}")
    lines.append(f"product\tslots_with_a_count\t{n_known}")
    lines.append(f"product\tslots_unknown\t{len(df) - n_known}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("RECOUNT -- where each element count came from")
    print("=" * 60)
    for k, v in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<44} {v:>10,}")
    print(f"\n  {'(mnxm, element) slots':<44} {len(df):>10,}")
    print(f"  {'with a count':<44} {n_known:>10,}")
    print(f"  {'unknown -- NULL, not zero':<44} {len(df) - n_known:>10,}")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


def cmd_check(args):
    ec = pd.read_parquet(args.counts, columns=["mnxm", "element", "n_atoms", "source"])
    ar = pd.read_parquet(args.atom_ranks, columns=["mnxm", "element", "n_atoms"])
    j = ar.merge(ec, on=["mnxm", "element"], how="left", suffixes=("_rank", "_count"))
    bad = j[j["n_atoms_count"].isna() | (j["n_atoms_count"] != j["n_atoms_rank"])]
    print(f"[recount] {len(ar):,} atom_ranks rows checked, {len(bad):,} disagree")
    if len(bad):
        print(bad.head(20).to_string(index=False), file=sys.stderr)
        raise SystemExit(
            "the recount and atom_ranks disagree about how many atoms of an element a "
            "metabolite has. They are two readings of ONE structure, so this is not a "
            "tolerance to widen: every pair row keyed on these metabolites indexes into "
            "a rank list whose length the count contradicts.")
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--metabolites", required=True, type=Path)
    p.add_argument("--out", required=True)
    p.add_argument("--out-summary", required=True)

    p = sub.add_parser("check"); p.set_defaults(fn=cmd_check)
    p.add_argument("--counts", required=True, type=Path)
    p.add_argument("--atom-ranks", required=True, type=Path)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
