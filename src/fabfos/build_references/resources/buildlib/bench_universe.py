from pathlib import Path

import pandas as pd


def transport_mnxrs(reac_prop: str | Path) -> set:
    out = set()
    with open(reac_prop, errors="replace") as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) >= 6 and p[0] not in ("", "EMPTY") and p[5].strip() == "T":
                out.add(p[0])
    if not out:
        raise SystemExit(f"[universe] {reac_prop} flagged no reaction as transport -- "
                         f"either the column moved or this is not reac_prop.tsv")
    return out


def all_mnxrs(reac_prop: str | Path) -> set:
    out = set()
    with open(reac_prop, errors="replace") as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            mnxr = line.split("\t", 1)[0].strip()
            if mnxr.startswith("MNXR"):
                out.add(mnxr)
    if not out:
        raise SystemExit(f"[universe] {reac_prop} defines no MNXR -- this is not "
                         f"reac_prop.tsv")
    return out


def reac_prop_path(metanetx_dir: str | Path) -> Path:
    mnx = Path(metanetx_dir)
    rels = sorted(p for p in mnx.glob("*") if p.is_dir())
    if len(rels) != 1:
        raise SystemExit(f"[universe] expected one MetaNetX release under {mnx}, found "
                         f"{[p.name for p in rels]}")
    path = rels[0] / "reac_prop.tsv"
    if not path.exists():
        raise SystemExit(f"[universe] no reac_prop.tsv under {rels[0]}")
    return path


def atom_universe(vocab, pairs_path: str | Path, exclude=()) -> tuple:
    V = vocab if isinstance(vocab, pd.DataFrame) else pd.read_parquet(vocab)
    rxn_symbol = V[V["kind"] == "rxn"].set_index("code")["symbol"]
    codes = pd.read_parquet(pairs_path, columns=["rxn"])["rxn"].unique()
    unknown = set(codes) - set(rxn_symbol.index)
    if unknown:
        raise SystemExit(f"[universe] {len(unknown)} rxn codes in atom_pairs are absent "
                         f"from the vocab -- the two files are not one bake")
    mapped = set(rxn_symbol.loc[codes].tolist())
    exclude = set(exclude)
    universe = mapped - exclude
    stats = dict(n_mapped=len(mapped), n_excluded=len(mapped & exclude),
                 n_universe=len(universe), excluded_kind="transport")
    return universe, stats


def universe_line(stats: dict, tag: str) -> str:
    return (f"[{tag}] atom universe: {stats['n_universe']:,} reactions "
            f"({stats['n_mapped']:,} mapped by the bake, less {stats['n_excluded']:,} "
            f"{stats['excluded_kind']})")
