#!/usr/bin/env python3
"""Give the mapper tables that lack one an attribution block.

    mamba run -n ecspr python research/fabfos/migrations/add_attribution.py --report
    mamba run -n ecspr python research/fabfos/migrations/add_attribution.py --write

`genome_unit` names ONE PHYSICALLY SEPARATE COPY OF DNA INSIDE ONE CELL. It is the grain
evidence pools over, and `nomination_contributions` namespaces the ORF by it -- so a table
without one pools every ORF it holds as though they sat on one copy. Three tables in this
tree were written before the block existed, and each needs a different answer.

  nostoc NOS / ERY / RHI   ONE ORGANISM PER TABLE, so the unit is the organism and
                           `source` already carries it. Adding the column is NUMERICALLY
                           INERT: a constant column adds nothing to a groupby's partition,
                           and a constant ORF prefix changes no dedup. `--report` measures
                           that rather than asserting it.

  scadc_fosmids            ONE FOSMID INSERT IS ONE UNIT, and the insert is the head of the
                           ORF name -- `pool01_ATTGAGCC:megahit:k141_112:1-18882_1` is ORF 1
                           of insert `...:1-18882`. This is NOT inert: the table currently
                           pools 63,914 rows as one anonymous copy, so an insert-level unit
                           is a correction and `--report` prices it.

  scadc_metagenome         LEFT ALONE. The unit there is a MAG and this table carries no
                           binning at all -- `orf` names a contig, and contigs of one
                           genome are one cell's DNA, so a contig-level unit would claim a
                           separateness the data does not support. It stays without a
                           block, which is the honest state.

`source` IS NOT THE UNIT on a pipeline-written table -- scadc's is the engine's instance id
(`1-1-1.sLtui6SR1GPBQSMO-dYNQuWp0`), which is why it moves to `build_id` here.

**WARNING** NEVER WRITE THROUGH A FILE UNDER `data/`. See `_write`.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src/metasmith_libraries/resources/lib"))

import fabfos_evidence as fe  # noqa: E402
from ecspr.model.evidence import compute_E  # noqa: E402

NOSTOC = ROOT / "data/fabfos/nostoc/annotation"
RUNS = ROOT / "data/fabfos/runs"

#: `path -> how the unit is derived`. `source` means the table's own single-valued
#: `source`; `insert` means the head of the ORF name, up to its last underscore.
TARGETS = {
    **{NOSTOC / o / "gpr_4lane.parquet": ("source", "nostoc_community", o)
       for o in ("NOS", "ERY", "RHI")},
    RUNS / "scadc_fosmids/gpr/gpr_4lane.parquet": ("insert", "scadc", "scadc_fosmids"),
    RUNS / "scadc_fosmids/gpr/gpr_7lane.parquet": ("insert", "scadc", "scadc_fosmids"),
}

EXTENSIONS = ("attribution",)


def _write(df, dest: Path):
    """**WARNING** DVC's cache type here is `hardlink,symlink`, so a checked-out file is
    one inode with many names. `to_parquet` truncates that inode, which edits the cache
    itself and silently rewrites every other checkout of the same content. Writing a
    sibling and renaming over the name breaks the link instead of following it."""
    tmp = dest.with_name(dest.name + ".migrating")
    df.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, dest)
    return dest


def units_for(df: pd.DataFrame, how: str) -> pd.Series:
    if how == "source":
        u = df["source"].astype(str)
        if u.nunique() != 1:
            raise SystemExit(f"a `source` unit needs one source, found {u.nunique()}")
        return u
    if how == "insert":
        return df["orf"].astype(str).str.rsplit("_", n=1).str[0]
    raise SystemExit(f"unknown derivation {how!r}")


def convert(path: Path, how: str, host: str, source: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if fe.extensions_of(df):
        raise SystemExit(f"{path} already carries {fe.extensions_of(df)}")
    out = df.copy()
    out["genome_unit"] = units_for(df, how)
    # The engine's instance id is provenance, not identity: it keeps its own column.
    out["build_id"] = df["source"].astype(str)
    out["host"] = host
    out["source"] = source
    return out[fe.schema_for(EXTENSIONS)]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--report", action="store_true",
                    help="price the change: how many units, and what it does to E")
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args()
    if not (a.report or a.write):
        ap.error("pick --report or --write")

    for path, (how, host, source) in TARGETS.items():
        if not path.exists():
            print(f"[skip] {path} is not checked out")
            continue
        out = convert(path, how, host, source)
        n = out["genome_unit"].nunique()
        line = (f"[{path.relative_to(ROOT)}] {len(out):,} rows, {n:,} unit(s) "
                f"by {how}")
        if a.report:
            # THE PRICE OF THE CHANGE, measured rather than argued. Pooling groups on
            # `genome_unit`, so a table that had none pooled as one anonymous copy.
            before = compute_E(pd.read_parquet(path), "before")
            after = compute_E(out, "after")
            common = before.index.intersection(after.index)
            moved = int((before[common] != after[common]).sum())
            line += (f" | E moves on {moved:,} of {len(common):,} reactions"
                     f" ({'INERT' if moved == 0 else 'a correction'})")
        print(line, flush=True)
        if a.write:
            fe.validate_gpr(out, out["lane_set"].iloc[0], None, source, EXTENSIONS)
            _write(out, path)
            print(f"  -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
