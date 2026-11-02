#!/usr/bin/env python3
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
