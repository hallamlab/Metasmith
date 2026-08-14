#!/usr/bin/env python3
"""The legacy ProteinBERT rows are NOT in fasta order, and the reason is exact.

    python pbert_permutation.py <orfs_dir> <annot1> <fresh_dir> <samples...>

MEASURED, 2026-08-05. Re-embedding four assemblies through the pinned
`external_proteinbert:2024.03.28` image and nearest-neighbour matching each
fresh row against the legacy stack:

    SRR1029109     203 ORFs   203/203 identity        cos 0.99877   OK
    SRR16201313    223 ORFs   223/223 identity        cos 0.99894   OK
    DRR106440    5,006 ORFs  4,955/5,006 identity     cos(diag) == cos(best)
    DRR315842   49,522 ORFs  2,032/49,522 identity    cos(diag) 0.480 vs 0.998

The last one is a real permutation, not a tie: the diagonal is not the best
match. And the embedder's own output names it -- the chunk files come back as

    _t.1, _t.10, _t.11, ... _t.19, _t.2, _t.20, ... _t.9

and `proteinbert.py`'s combiner stacks `sorted(in_dir.glob("*.npy"))`. That is
LEXICOGRAPHIC, so chunk 10 is stacked before chunk 2. A sample small enough for
one chunk is unaffected, which is why the 203- and 223-ORF assemblies pass and
why nothing ever noticed.

THE PERMUTATION IS THEREFORE KNOWN, not merely present. The embedder writes
fixed 1,024-sequence chunks in fasta order; the combiner concatenates them in
lexicographic chunk-name order. Both halves are deterministic, so the mapping
from fasta record to legacy row can be computed from the record count alone --
no index, no re-embedding.

This module computes that mapping and, given fresh embeddings, PROVES it. It is
a gate, not a report: the repack applies the mapping, so a mapping that is
merely plausible produces a full, confident, wrong table.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

CHUNK = 1024          # `pbert run --model_batch 1024`, echoed as "sequences to load"


def fasta_to_legacy_row(n: int, chunk: int = CHUNK) -> np.ndarray:
    """`out[i]` = the legacy stack row holding fasta record `i`.

    Chunk k (1-based) holds records [(k-1)*chunk, min(k*chunk, n)) and is stacked
    at the offset its position in LEXICOGRAPHIC chunk order implies.
    """
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
        # Compare fresh record i against legacy row perm[i]. Under the
        # hypothesis this is the same protein, so the cosine is ~1 and the
        # nearest legacy row IS perm[i].
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
