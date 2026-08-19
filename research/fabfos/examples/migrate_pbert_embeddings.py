"""Convert a legacy (embeddings parquet, index csv) pair into one self-addressing table.

    python migrate_pbert_embeddings.py <orfs.faa> <emb.parquet> <index.csv> <out.parquet>

`annotation::proteinbert_embeddings` now carries `sequence_id` beside its 512 floats;
the pairs this replaces named the rows in a separate file and were paired by position.
That pairing was WRONG for anything the embedder split into more than one chunk -- it
writes fixed 1,024-sequence chunks named `<stem>.1`, `<stem>.2`, ... and the old
combiner stacked `sorted(glob("*.npy"))`, so chunk 10 landed before chunk 2.

SO THIS DOES NOT MERELY ZIP THE TWO FILES. It proves the pairing first, and it can do
that without re-embedding: a deterministic embedder gives byte-identical sequences
byte-identical vectors, so every group of ORFs sharing a sequence md5 must share a
row. Two candidate pairings are scored against that -- the file's own order, and the
lexicographic-chunk permutation `fir/pbert_permutation.fasta_to_legacy_row` recovers
from the record count alone -- and the two coincide below ten chunks, which is why a
small artifact has only one candidate. Exactly one may pass. An artifact with no
duplicated sequence decides nothing either way and is refused rather than guessed at,
because a guess here produces a complete, schema-valid, confidently wrong table.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent / "fir"))
from pbert_permutation import CHUNK, fasta_to_legacy_row  # noqa: E402


def iter_fasta(path: Path):
    name, seq = None, []
    for line in path.read_text().splitlines():
        if line.startswith(">"):
            if name is not None:
                yield name, "".join(seq)
            name, seq = line[1:].split()[0], []
        else:
            seq.append(line.strip())
    if name is not None:
        yield name, "".join(seq)


def duplicate_groups(records) -> list[list[int]]:
    by_md5: dict[str, list[int]] = {}
    for i, (_, seq) in enumerate(records):
        by_md5.setdefault(hashlib.md5(seq.encode()).hexdigest(), []).append(i)
    return [g for g in by_md5.values() if len(g) > 1]


# Share of duplicate groups whose members all land on the same vector. Compared by
# COSINE, not elementwise: some of these artifacts were stored at reduced precision, so
# two rows that are the same embedding differ by ~2e-4 absolute while agreeing to seven
# decimals in direction -- and direction is the only thing the kNN lane reads.
def agreement(emb: np.ndarray, rows: np.ndarray, groups: list[list[int]]) -> float:
    ok = 0
    for g in groups:
        v = emb[rows[g]]
        v = v / np.clip(np.linalg.norm(v, axis=1, keepdims=True), 1e-9, None)
        if float((v @ v[0]).min()) > 0.999:
            ok += 1
    return ok / len(groups)


def main() -> int:
    faa, emb_p, idx_p, out_p = (Path(a) for a in sys.argv[1:5])

    records = list(iter_fasta(faa))
    ids = [n for n, _ in records]
    df = pd.read_parquet(emb_p)
    dims = [c for c in df.columns if c.startswith("dim_")]
    if not dims:
        dims = list(df.columns)
    emb = df[dims].to_numpy(dtype=np.float32)

    idx = pd.read_csv(idx_p)
    id_col = next((c for c in ("sequence_id", "id") if c in idx.columns), None)
    if id_col is None:
        print(f"{idx_p} has no id column: {list(idx.columns)}", file=sys.stderr)
        return 2
    if list(idx[id_col]) != ids:
        print(f"{idx_p} does not name the same sequences, in order, as {faa}",
              file=sys.stderr)
        return 2
    if len(emb) != len(ids):
        print(f"{emb_p} has {len(emb)} rows and {faa} has {len(ids)} records",
              file=sys.stderr)
        return 2

    identity = np.arange(len(ids), dtype=np.int64)
    lex = fasta_to_legacy_row(len(ids))
    n_chunks = (len(ids) + CHUNK - 1) // CHUNK

    # WHEN THERE IS ONLY ONE CANDIDATE, THERE IS NOTHING TO CHOOSE BETWEEN. The bug is
    # that the embedder's chunk files sort lexicographically rather than numerically,
    # so the only rival to the file's own order is `fasta_to_legacy_row`. Below TEN
    # chunks -- `.1` through `.9` -- the two orders coincide and that rival IS the
    # identity, which is why a small artifact is safe by arithmetic rather than by
    # evidence. Demanding duplicate sequences here would refuse a migration that
    # cannot be wrong, and this is the common case: an assembly's ORF set clears ten
    # chunks only above 9,216 sequences.
    if np.array_equal(lex, identity):
        out = pd.concat([
            pd.DataFrame({"sequence_id": ids}),
            pd.DataFrame(emb, columns=[f"dim_{i}" for i in range(len(dims))]),
        ], axis=1)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(out_p, index=False)
        print(f"wrote {out_p} -- {len(out):,} x {len(dims)}; {n_chunks} embedder "
              f"chunk(s), so lexicographic and numeric chunk order coincide and the "
              f"file's own order is the only candidate pairing")
        return 0

    groups = duplicate_groups(records)
    if not groups:
        print(f"{faa} spans {n_chunks} embedder chunks, so the chunk order is "
              f"genuinely ambiguous, and it has no two records sharing a sequence to "
              f"settle it. Re-embed rather than migrate.", file=sys.stderr)
        return 2

    # Two genuine candidates, because the early return above already handled the case
    # where the lexicographic permutation collapses to the identity.
    candidates = {"as written": identity, "lexicographic-chunk": lex}
    scores = {k: agreement(emb, v, groups) for k, v in candidates.items()}
    for k, v in scores.items():
        print(f"{k:22s} {v:6.1%} of {len(groups)} duplicate-sequence groups agree")

    passing = [k for k, v in scores.items() if v > 0.99]
    if len(passing) != 1:
        print(f"{len(passing)} candidate pairings pass; exactly one must. Migrating "
              f"on a tie or on none would attribute embeddings to the wrong ORFs.",
              file=sys.stderr)
        return 1

    rows = candidates[passing[0]]
    out = pd.concat([
        pd.DataFrame({"sequence_id": ids}),
        pd.DataFrame(emb[rows], columns=[f"dim_{i}" for i in range(len(dims))]),
    ], axis=1)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_p, index=False)
    print(f"wrote {out_p} -- {len(out):,} x {len(dims)} via the '{passing[0]}' pairing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
