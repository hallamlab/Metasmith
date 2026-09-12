#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

CHUNK = 1024


def fasta_to_legacy_row(n: int, chunk: int = CHUNK) -> np.ndarray:
    if n <= 0:
        return np.zeros(0, dtype=np.int64)
    n_chunks = (n + chunk - 1) // chunk
    order = sorted(range(1, n_chunks + 1), key=str)
    size = {k: min(k * chunk, n) - (k - 1) * chunk for k in order}
    offset, acc = {}, 0
    for k in order:
        offset[k] = acc
        acc += size[k]
    assert acc == n, f"chunk sizes sum to {acc}, not {n}"
    out = np.empty(n, dtype=np.int64)
    for k in range(1, n_chunks + 1):
        lo = (k - 1) * chunk
        out[lo:lo + size[k]] = offset[k] + np.arange(size[k])
    return out


def _load(p: Path) -> np.ndarray:
    t = pq.read_table(p, columns=[f"dim_{i}" for i in range(512)])
    return np.column_stack([c.to_numpy(zero_copy_only=False) for c in t.columns]
                           ).astype(np.float32)


def _norm(x):
    return x / np.clip(np.linalg.norm(x, axis=1, keepdims=True), 1e-9, None)


def main() -> int:
    orfs, annot1, fresh = (Path(a) for a in sys.argv[1:4])
    samples = sys.argv[4:]
    legacy_dir = annot1 / "annotation-proteinbert_embeddings"
    bad = 0
    for s in samples:
        fp = fresh / f"{s}.parquet"
        if not fp.exists():
            print(f"{s}: no fresh embedding at {fp}", file=sys.stderr)
            bad += 1
            continue
        a = _norm(_load(fp))
        b = _norm(_load(legacy_dir / f"{s}.parquet"))
        if a.shape != b.shape:
            print(f"{s}: fresh {a.shape} vs legacy {b.shape}", file=sys.stderr)
            bad += 1
            continue
        perm = fasta_to_legacy_row(len(a))
        aligned = b[perm]
        diag = np.einsum("ij,ij->i", a, aligned)
        sim = a @ b.T
        nearest = np.argmax(sim, axis=1)
        hit = int((nearest == perm).sum())
        n_chunks = (len(a) + CHUNK - 1) // CHUNK
        ok = hit == len(a) or float(diag.mean()) > 0.995
        print(f"{s:12s} n={len(a):>6,} chunks={n_chunks:>3}  "
              f"argmax==perm {hit}/{len(a)}  mean cos(perm)={diag.mean():.5f}  "
              f"min={diag.min():.4f}  {'OK' if ok else 'MISMATCH'}", flush=True)
        if not ok:
            bad += 1
            off = np.arange(len(a))[nearest != perm][:5]
            print(f"    e.g. fresh {list(off)} -> perm says {list(perm[off])}, "
                  f"argmax says {list(nearest[off])}", file=sys.stderr)

    if bad:
        print(f"\n{bad} sample(s) do not follow the lexicographic-chunk mapping. "
              f"The legacy embeddings cannot be re-indexed from the record count "
              f"alone and must be re-embedded.", file=sys.stderr)
        return 1
    print(f"\nall {len(samples)} sample(s): legacy row `fasta_to_legacy_row(n)[i]` "
          f"IS fasta record i. The permutation is recoverable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
