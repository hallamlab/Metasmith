# The scadc runs' lane files, laid out the way `reparse_run.py` reads them.
#
# TWO LAYOUTS EXIST, and only one is the pipeline's. A host run holds its six files flat
# under `annotations/lanes/` with the names the transform stages; the two scadc runs
# predate that and hold one directory per lane, with the UniRef subject title INLINE in
# the hit table and the ProteinBERT ids in a sibling index rather than in the parquet.
#
# THIS MATERIALISES, IT DOES NOT REPARSE. Teaching the reparse driver a second layout
# would put the difference between two archives inside the thing that is supposed to
# call the shipped mapper exactly as the transform calls it. So the shape is fixed here,
# once, and everything downstream sees one layout.
#
# The files it writes are views of the originals -- symlinked where the format already
# matches, rewritten only where it does not. Nothing here decides what a call is worth.
#
#     mamba run -n msm python research/fabfos/migrations/scadc_lanes_adapter.py \
#         --run scadc_fosmids
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(REPO / "src" / "metasmith_libraries" / "resources" / "lib"))
import fabfos_evidence as fe                                          # noqa: E402

# The 12 standard blast6 fields, then the subject title, then the bit-score ratio. The
# mapper's `_BLAST6_BSR_COLS` is this minus `stitle`, which it reads from a sibling file
# -- so the title is SPLIT OUT rather than dropped: it is the only source of
# `intermediate_name` for this lane.
_SCADC_BLAST6 = fe._BLAST6_BSR_COLS[:12] + ["stitle", "bsr"]

# Where each lane's file lives under `annotations/`, and what it is called in `lanes/`.
_LANES = {
    "clean": ("clean/*.clean.tsv", "clean.tsv"),
    "kofam": ("kofam/*.kofam.csv", "kofamscan.csv"),
    "uniref50": ("uniref50/*.uniref50.blast6.tsv", "diamond_uniref50.tsv"),
    "pbert": ("proteinbert/*.pbert.parquet", "proteinbert_embeddings.parquet"),
}


def _one(root: Path, pattern: str) -> Path:
    hits = sorted(root.glob(pattern))
    if len(hits) != 1:
        raise SystemExit(f"expected one {pattern} under {root}, found {hits}")
    return hits[0]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", required=True)
    ap.add_argument("--out", type=Path,
                    help="default: the run's own annotations/lanes/")
    a = ap.parse_args()

    ann = REPO / "data/fabfos/runs" / a.run / "annotations"
    if not ann.is_dir():
        raise SystemExit(f"no {ann}")
    out = a.out or (ann / "lanes")
    out.mkdir(parents=True, exist_ok=True)

    for lane, (pattern, name) in _LANES.items():
        src = _one(ann, pattern)
        dst = out / name
        if lane == "uniref50":
            hits = pd.read_csv(src, sep="\t", dtype=str, header=None,
                               names=_SCADC_BLAST6)
            # One row per subject, not one per hit: the mapper joins the titles by
            # `sseqid`, and a 1.1 GB hit table carries the same title thousands of times.
            (hits[["sseqid", "stitle"]].drop_duplicates("sseqid")
             .rename(columns={"stitle": "description"})
             .to_csv(out / "diamond_uniref50_descriptions.tsv", sep="\t", index=False))
            hits[fe._BLAST6_BSR_COLS].to_csv(dst, sep="\t", index=False)
            print(f"{lane:9s} {len(hits):>9,} hits, "
                  f"{hits['sseqid'].nunique():,} subjects -> {dst.name} + descriptions")
        elif lane == "pbert":
            idx = _one(ann, "proteinbert/*.pbert.index.csv")
            emb = pd.read_parquet(src)
            ids = pd.read_csv(idx)
            if len(ids) != len(emb):
                raise SystemExit(
                    f"{idx.name} has {len(ids):,} ids and {src.name} has {len(emb):,} "
                    f"vectors. They are positional, so a length mismatch mislabels "
                    f"every embedding rather than some of them")
            pd.concat([ids[["sequence_id"]].reset_index(drop=True),
                       emb.reset_index(drop=True)], axis=1).to_parquet(dst, index=False)
            print(f"{lane:9s} {len(emb):>9,} vectors x {emb.shape[1]} dims -> {dst.name}")
        else:
            # Already the shape the mapper parses; a symlink keeps one copy on disk and
            # makes it obvious that nothing was rewritten.
            if dst.is_symlink() or dst.exists():
                dst.unlink()
            dst.symlink_to(src.resolve())
            print(f"{lane:9s} {src.name} -> {dst.name} (symlink)")

    print(f"\n{out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
