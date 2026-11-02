"""Did the re-bake lose any (mnxr, element) the deployed bake holds?

The scope's stop-line is that coverage may only go up. The injectivity trim REMOVES
correspondences, so where Indigo was the sole member for an (mnxr, element) the trim can
empty that key. This compares two chunks at both grains and names what moved.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def keys(chunk: Path):
    sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))
    from ecspr.bake.encoding import Vocab
    v = Vocab(pd.read_parquet(chunk / "vocab.parquet"))
    rxn = v.symbols("rxn")
    el = v.symbols("element")
    d = pd.read_parquet(chunk / "atom_pairs.parquet", columns=["rxn", "element"])
    k = {(str(rxn[a]), str(el[b])) for a, b in zip(d["rxn"], d["element"])}
    return k, {r for r, _e in k}, len(d)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--before", required=True)
    ap.add_argument("--after", required=True)
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)

    kb, rb, nb = keys(Path(a.before))
    ka, ra, na = keys(Path(a.after))
    lost, gained = kb - ka, ka - kb
    lost_r, gained_r = rb - ra, ra - rb

    print("=" * 68)
    print("COVERAGE DIFF -- the stop-line is that this may only go up")
    print("=" * 68)
    print(f"  pair rows        {nb:>10,} -> {na:>10,}   ({na - nb:+,})")
    print(f"  (mnxr, element)  {len(kb):>10,} -> {len(ka):>10,}   ({len(ka) - len(kb):+,})")
    print(f"  reactions        {len(rb):>10,} -> {len(ra):>10,}   ({len(ra) - len(rb):+,})")
    print(f"\n  keys LOST    : {len(lost):,}")
    print(f"  keys GAINED  : {len(gained):,}")
    print(f"  reactions LOST   : {len(lost_r):,}")
    print(f"  reactions GAINED : {len(gained_r):,}")
    if lost:
        s = pd.Series([e for _r, e in lost]).value_counts()
        print("\n  lost keys by element:")
        print(s.to_string())
        print("\n  first 20 lost keys:")
        for r, e in sorted(lost)[:20]:
            print(f"    {r}  {e}")
    if a.out:
        pd.DataFrame(sorted(lost), columns=["mnxr", "element"]).to_csv(
            a.out, sep="\t", index=False)
        print(f"\nwrote {a.out}")
    if lost:
        print("\n  STOP-LINE: keys were lost. This is a decision to bring back, not a "
              "variance to absorb -- either the trim's correctness is worth the coverage, "
              "or the key should be kept at reduced weight.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
