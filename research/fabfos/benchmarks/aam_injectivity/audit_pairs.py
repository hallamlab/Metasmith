"""What a curated twin would recover from the injectivity violations in a baked chunk.

The violation count itself belongs to the bake and lives there --
`ecspr.bake.aam.layers.injectivity_violations`, reachable as
`python -m ecspr.bake.aam.layers audit`. That verb reads a layers-format table. This
script decodes a DEPLOYED chunk into the same shape so the number can be read off what
consumers actually load, then asks the question the bake does not: does MetaNetX already
hold this chemistry under a second id whose curated map the stack DID claim?

Two ids are twins when their participants differ by at most two metabolites and the
differing ones share a molecular formula -- an isomeric frame. `--strict-ec` also demands
a shared EC number, which refuses the analogues the participant test admits.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))
from ecspr.bake.aam.layers import injectivity_violations, load_coefficients  # noqa: E402

EQ_TERM = re.compile(r"(\d+(?:\.\d+)?)\s+(MNXM\w+)@")

# A twin search keyed on a participant shared by thousands of curated reactions degrades
# into a full scan; the rarest few participants pin the candidate set at the same recall.
PROBE_DEPTH = 3
PROBE_MAX_DEGREE = 4000
MAX_FRAME_SWAPS = 2


def decode(chunk: Path) -> pd.DataFrame:
    v = pd.read_parquet(chunk / "vocab.parquet")
    sym = {k: g.sort_values("code")["symbol"].to_numpy() for k, g in v.groupby("kind")}
    p = pd.read_parquet(chunk / "atom_pairs.parquet")
    return pd.DataFrame({
        "mnxr": sym["rxn"][p["rxn"].to_numpy()],
        "element": sym["element"][p["element"].to_numpy()],
        "substrate": sym["met"][p["tail_met"].to_numpy()],
        "product": sym["met"][p["head_met"].to_numpy()],
        "sub_idx": p["tail_rank"].to_numpy(),
        "prod_idx": p["head_rank"].to_numpy(),
        "pair_w": p["pair_w"].to_numpy().astype(float),
        "method": sym["method"][p["method"].to_numpy()],
        "source": sym["source"][p["source"].to_numpy()],
    })


def load_participants(reac_prop: Path):
    parts, ec = {}, {}
    for line in open(reac_prop):
        if line.startswith("#"):
            continue
        f = line.rstrip("\n").split("\t")
        if len(f) < 2 or not f[0].startswith("MNXR"):
            continue
        ms = frozenset(m for _c, m in EQ_TERM.findall(f[1]))
        if ms:
            parts[f[0]] = ms
        if len(f) > 3 and f[3].strip():
            ec[f[0]] = [x.strip() for x in f[3].split(";") if x.strip()]
    return parts, ec


def curated_twins(exposed, curated, parts, ec, formula, strict_ec: bool):
    inv = defaultdict(set)
    for r in curated:
        for m in parts.get(r, ()):
            inv[m].add(r)
    deg = {m: len(s) for m, s in inv.items()}
    found = {}
    for r in exposed:
        pr = parts.get(r)
        if pr is None:
            continue
        probes = sorted((m for m in pr if m in inv), key=lambda m: deg[m])[:PROBE_DEPTH]
        cands = set()
        for q in probes:
            if deg[q] <= PROBE_MAX_DEGREE:
                cands |= inv[q]
        if strict_ec:
            mine = set(ec.get(r, ()))
            cands = {t for t in cands if mine & set(ec.get(t, ()))} if mine else set()
        for t in sorted(cands):
            only_r, only_t = pr - parts[t], parts[t] - pr
            if not only_r or len(only_r) > MAX_FRAME_SWAPS or len(only_r) != len(only_t):
                continue
            fr = sorted(str(formula.get(x)) for x in only_r)
            ft = sorted(str(formula.get(x)) for x in only_t)
            if fr == ft and all(f and f != "None" for f in fr):
                found[r] = t
                break
    return found


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--chunk", type=Path, required=True)
    ap.add_argument("--reac-prop", type=Path, required=True)
    ap.add_argument("--metabolites", type=Path, required=True,
                    help="lookups/metabolites.parquet, for formula and name")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--strict-ec", action="store_true")
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)

    df = decode(a.chunk)
    mA, mB, bad = injectivity_violations(df, load_coefficients(a.reac_prop))
    n_rxn = df["mnxr"].nunique()
    aff = df.loc[bad, "mnxr"].unique()
    print(f"rows {len(df):,}  reactions {n_rxn:,}")
    print(f"A {int(mA.sum()):,} rows   B {int(mB.sum()):,} rows   "
          f"multiplicity explains {int(((mA | mB) & ~bad).sum()):,}")
    print(f"UNEXPLAINED {int(bad.sum()):,} rows ({100*bad.sum()/len(df):.2f}%), "
          f"{len(aff):,} reactions ({100*len(aff)/n_rxn:.2f}%), "
          f"weight {df.loc[bad, 'pair_w'].sum():,.0f} of {df['pair_w'].sum():,.0f} "
          f"({100*df.loc[bad, 'pair_w'].sum()/df['pair_w'].sum():.2f}%)")
    per_el = (df[bad].groupby("element")["mnxr"].nunique()
              / df.groupby("element")["mnxr"].nunique())
    print("affected fraction by element: "
          + "  ".join(f"{k} {100*v:.2f}%" for k, v in per_el.items()))
    print(df[bad].groupby("method").size().sort_values(ascending=False).to_string())

    mets = pd.read_parquet(a.metabolites).set_index("mnxm")
    parts, ec = load_participants(a.reac_prop)
    curated = set(df.loc[df["method"] == "curated", "mnxr"])
    exposed = sorted(set(aff) - curated)
    found = curated_twins(exposed, curated, parts, ec, mets["formula"].to_dict(),
                          a.strict_ec)
    label = "same EC + isomeric frame" if a.strict_ec else "isomeric frame"
    print(f"\nexposed (affected, no curated map): {len(exposed):,}")
    print(f"  recoverable from a curated twin ({label}): {len(found):,} "
          f"({100*len(found)/max(len(exposed), 1):.1f}%)")

    per_rxn = (df[bad].groupby("mnxr")
               .agg(rows=("pair_w", "size"), weight=("pair_w", "sum"))
               .sort_values("weight", ascending=False))
    per_rxn["curated_twin"] = [found.get(r, "") for r in per_rxn.index]
    per_rxn.to_csv(a.out / "affected_reactions.tsv", sep="\t")

    sink = (df[bad & mA].groupby("product")["pair_w"].agg(["sum", "size"])
            .sort_values("sum", ascending=False).head(40))
    sink.insert(0, "name", [mets["name"].get(i) for i in sink.index])
    sink.to_csv(a.out / "top_receivers.tsv", sep="\t")
    print(f"\nwrote {a.out/'affected_reactions.tsv'} and {a.out/'top_receivers.tsv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
