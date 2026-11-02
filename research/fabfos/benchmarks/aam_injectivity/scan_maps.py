"""Scan each mapper's cached SMILES for atom-map numbers reused within one side.

An atom map is a bijection between mapped substrate atoms and mapped product atoms, so
a map number may appear at most once per side. `atom_pairs.pairs_from_mapped` walks the
PRODUCT atoms and looks each one's map number up in a substrate index, so a number
repeated on the product side silently pairs one substrate atom with two product atoms --
in two different molecules, when the repeats sit in different templates.

Reads the aam_cache TSVs directly; no rdkit, no engine.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path

csv.field_size_limit(1 << 30)

MAPNUM = re.compile(r"\[[^\]]*?:(\d+)\]")

COLS = ("member", "mapped", "unmapped", "dup_substrate_side", "dup_product_side",
        "reactions_with_dup", "dup_atom_slots")


def scan(path: Path):
    n = unmapped = ds_rxn = dp_rxn = slots = 0
    hits = []
    with open(path) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            smi = r.get("mapped_rxn_smiles") or ""
            if not smi or ">>" not in smi:
                unmapped += 1
                continue
            n += 1
            lhs, rhs = smi.split(">>", 1)
            ds = sum(v - 1 for v in Counter(MAPNUM.findall(lhs)).values() if v > 1)
            dp = sum(v - 1 for v in Counter(MAPNUM.findall(rhs)).values() if v > 1)
            ds_rxn += ds > 0
            dp_rxn += dp > 0
            if ds or dp:
                hits.append((r["mnxr"], ds, dp))
                slots += ds + dp
    return n, unmapped, ds_rxn, dp_rxn, hits, slots


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cache", type=Path, required=True,
                    help="the bake chunk's aam_cache/ directory")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)

    a.out.mkdir(parents=True, exist_ok=True)
    rows = []
    for member in sorted(p.name for p in a.cache.iterdir() if p.is_dir()):
        f = a.cache / member / "cache.tsv"
        if not f.exists():
            continue
        n, unmapped, ds, dp, hits, slots = scan(f)
        rows.append((member, n, unmapped, ds, dp, len(hits), slots))
        with open(a.out / f"dup_{member}.tsv", "w") as fh:
            fh.write("mnxr\tdup_substrate\tdup_product\n")
            for m, x, y in sorted(hits):
                fh.write(f"{m}\t{x}\t{y}\n")
        print(f"{member:12s} mapped={n:7,} unmapped={unmapped:6,} "
              f"dup_sub={ds:6,} dup_prod={dp:6,} "
              f"rxn_with_dup={len(hits):6,} ({100*len(hits)/max(n,1):.2f}%) slots={slots:,}")

    with open(a.out / "map_number_duplication.tsv", "w") as fh:
        fh.write("\t".join(COLS) + "\n")
        for r in rows:
            fh.write("\t".join(str(x) for x in r) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
