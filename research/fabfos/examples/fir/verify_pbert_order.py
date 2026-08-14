#!/usr/bin/env python3
"""Prove the legacy ProteinBERT rows are in FASTA ORDER, by content.

    python verify_pbert_order.py <orfs_dir> <annot1> <fresh_dir> <samples...>

THE ROW-COUNT AUDIT CANNOT SETTLE THIS. Row count is invariant under every
permutation, so `rows(parquet) == records(faa)` passes on a stack whose rows
belong to different ORFs than the repack will assign them. And the producer
never promised fasta order in the first place: `proteinbert.py`'s combiner
concatenates the embedder's chunk `.npy` and `.csv` files in LEXICOGRAPHIC
filename order and guarantees only stack-row i == index-row i -- with
`chunk_10` sorting before `chunk_2`. The index that would have recorded the
order was not retained for these files.

If the order is wrong, every downstream artifact is a full, non-empty,
schema-valid, four-channel GPR table in which the embedding lane's ORF
attributions are silently wrong. Nothing later catches it.

So: re-embed a sample through the SAME pinned image the legacy pass used, and
for each fresh row find its nearest legacy row by cosine. Order is preserved
exactly when that argmax is the identity permutation. A near-miss is not a pass
-- a permuted stack still matches perfectly, just at the wrong index.

`<fresh_dir>` holds `<sample>.parquet` written by the pinned embedder; the
sbatch beside this file produces them.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

ORFS = Path(sys.argv[1])
ANNOT1 = Path(sys.argv[2])
FRESH = Path(sys.argv[3])
SAMPLES = sys.argv[4:]

LEGACY = ANNOT1 / "annotation-proteinbert_embeddings"


def load(p: Path) -> np.ndarray:
    t = pq.read_table(p, columns=[f"dim_{i}" for i in range(512)])
    return np.column_stack([c.to_numpy(zero_copy_only=False) for c in t.columns]
                           ).astype(np.float32)


def norm(x):
    return x / np.clip(np.linalg.norm(x, axis=1, keepdims=True), 1e-9, None)


def main() -> int:
    bad = 0
    for s in SAMPLES:
        fresh_p = FRESH / f"{s}.parquet"
        if not fresh_p.exists():
            print(f"{s}: NO FRESH EMBEDDING -- the re-embed did not produce "
                  f"{fresh_p}", file=sys.stderr)
            bad += 1
            continue
        a = norm(load(fresh_p))
        b = norm(load(LEGACY / f"{s}.parquet"))
        if a.shape != b.shape:
            print(f"{s}: FRESH {a.shape} vs LEGACY {b.shape}", file=sys.stderr)
            bad += 1
            continue
        sim = a @ b.T
        nearest = np.argmax(sim, axis=1)
        ident = np.arange(len(a))
        n_ok = int((nearest == ident).sum())
        diag = float(np.mean(sim[ident, ident]))
        best = float(np.mean(sim[ident, nearest]))
        verdict = "OK" if n_ok == len(a) else "PERMUTED"
        if n_ok != len(a):
            bad += 1
            off = ident[nearest != ident][:5]
            print(f"{s}: {verdict} -- {len(a) - n_ok} of {len(a)} rows match a "
                  f"DIFFERENT legacy row; e.g. fresh {list(off)} -> legacy "
                  f"{list(nearest[off])}", file=sys.stderr)
        print(f"{s:12s} n={len(a):>6,}  identity-matches {n_ok}/{len(a)}  "
              f"mean cos(diag)={diag:.5f}  mean cos(best)={best:.5f}  {verdict}",
              flush=True)

    if bad:
        print(f"\n{bad} sample(s) FAILED. The repack's positional alignment is not "
              f"valid; the legacy embeddings cannot be reused as they stand.",
              file=sys.stderr)
        return 1
    print(f"\nall {len(SAMPLES)} sample(s): legacy row i IS fasta record i.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
