"""Convert a legacy (embeddings parquet, index csv) pair into one self-addressing table.

    python migrate_pbert_embeddings.py <orfs.faa> <emb.parquet> <index.csv> <out.parquet>

`annotation::proteinbert_embeddings` now carries `sequence_id` beside its 512 floats;
the pairs this replaces named the rows in a separate file and were paired by position.

TWO INDEX CONVENTIONS ARE IN THE WILD AND NEITHER FILE SAYS WHICH IT IS. Some indexes
list the ORFs in the FASTA's order, as the embedder wrote them. Others were rewritten
into the STACK's order by an earlier repair -- one such even carries a redundant
`global_row` column that is only its own row number. The two are indistinguishable by
inspection, and reading one as the other attributes every embedding to another
protein while producing a full, schema-valid table. So the index's id ORDER is never
trusted here, only its id SET; the pairing is decided by evidence.

THE EVIDENCE NEEDS NO RE-EMBEDDING: a deterministic embedder gives byte-identical
sequences byte-identical vectors, so every group of ORFs sharing a sequence md5 must
share a row. Two candidate pairings are scored against that -- the index already being
in stack order, and the index being in FASTA order with the stack assembled by
`sorted(glob("*.npy"))`, which puts chunk 10 before chunk 2 (`fasta_to_legacy_row`).
Exactly one may pass.

Below TEN chunks the two candidates can collapse into one, and then no evidence is
needed because there is nothing to choose between. An artifact that IS ambiguous and
has no duplicated sequence is refused rather than guessed at, because a guess here
produces a complete, schema-valid, confidently wrong table.
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
    index_ids = list(idx[id_col])
    if set(index_ids) != set(ids) or len(index_ids) != len(ids):
        print(f"{idx_p} does not name the same {len(ids)} sequences as {faa} "
              f"({len(index_ids)} rows, {len(set(index_ids) & set(ids))} shared). This "
              f"is a different ORF set, not a different order.", file=sys.stderr)
        return 2
    if len(emb) != len(ids):
        print(f"{emb_p} has {len(emb)} rows and {faa} has {len(ids)} records",
              file=sys.stderr)
        return 2

    identity = np.arange(len(ids), dtype=np.int64)
    lex = fasta_to_legacy_row(len(ids))
    n_chunks = (len(ids) + CHUNK - 1) // CHUNK

    # TWO INDEX CONVENTIONS ARE IN THE WILD AND THE FILE DOES NOT SAY WHICH IT IS.
    # Some indexes list the ORFs in the FASTA's order, which is what the embedder's
    # own output gives; others were rewritten into the STACK's order by an earlier
    # repair, and one of those even carries a redundant `global_row` column that is
    # just its own row number. Both look identical to a reader. So the id ORDER of the
    # index is not trusted here -- only its id SET, checked above -- and the pairing
    # is settled by evidence below.
    #
    #   "index position"       row i of the stack belongs to index row i, i.e. the
    #                          index is already in stack order
    #   "lexicographic-chunk"  the index is in FASTA order and the stack was assembled
    #                          by `sorted(glob("*.npy"))`, so chunk 10 landed before
    #                          chunk 2 -- `fasta_to_legacy_row` is that permutation
    #
    # `row_of[i]` is the stack row holding the protein named at index row i.
    by_index = identity
    by_lex = lex[[{n: i for i, n in enumerate(ids)}[n] for n in index_ids]]

    # WHEN THERE IS ONLY ONE CANDIDATE, THERE IS NOTHING TO CHOOSE BETWEEN. Below TEN
    # chunks -- `.1` through `.9` -- lexicographic and numeric chunk order coincide, so
    # the permutation is the identity and both candidates collapse to the same map.
    # Demanding duplicate sequences there would refuse a migration that cannot be
    # wrong, and that is the common case: an ORF set clears ten chunks only above
    # 9,216 sequences.
    if np.array_equal(by_index, by_lex):
        # THE SHORTCUT RESTS ON `CHUNK`, WHICH THE ARTIFACT DOES NOT RECORD. 1,024 is
        # what `proteinbert.py` pins today, but this script exists for artifacts that
        # predate that pin, and a legacy set embedded at a smaller batch has more
        # chunks than the arithmetic thinks -- which would collapse the two candidates
        # wrongly and write a confident, wrong table with no gate at all. So when the
        # FASTA offers duplicate sequences, they are checked anyway: the shortcut is
        # allowed to save work, never to skip evidence that is there.
        chosen, why = by_index, (
            f"{n_chunks} embedder chunk(s) at CHUNK={CHUNK}, so lexicographic and "
            f"numeric chunk order coincide and there is only one candidate pairing")
        groups = duplicate_groups(records)
        if groups:
            at = {n: i for i, n in enumerate(index_ids)}
            gi = [[at[ids[j]] for j in g] for g in groups]
            score = agreement(emb, chosen, gi)
            if score <= 0.99:
                print(f"the single candidate pairing agrees with only {score:.1%} of "
                      f"{len(groups)} duplicate-sequence groups. The record count says "
                      f"{n_chunks} chunk(s) at CHUNK={CHUNK}; the embeddings say "
                      f"otherwise, so that batch size is wrong for this artifact and "
                      f"the pairing is not determined. Re-embed rather than migrate.",
                      file=sys.stderr)
                return 1
            why += f", confirmed on {len(groups)} duplicate-sequence groups"
    else:
        groups = duplicate_groups(records)
        if not groups:
            print(f"{faa} spans {n_chunks} embedder chunks, so the chunk order is "
                  f"genuinely ambiguous, and it has no two records sharing a sequence "
                  f"to settle it. Re-embed rather than migrate.", file=sys.stderr)
            return 2
        # score against the FASTA's duplicate groups, expressed as index positions
        at = {n: i for i, n in enumerate(index_ids)}
        gi = [[at[ids[j]] for j in g] for g in groups]
        candidates = {"index position": by_index, "lexicographic-chunk": by_lex}
        scores = {k: agreement(emb, v, gi) for k, v in candidates.items()}
        for k, v in scores.items():
            print(f"{k:22s} {v:6.1%} of {len(groups)} duplicate-sequence groups agree")
        passing = [k for k, v in scores.items() if v > 0.99]
        if len(passing) != 1:
            print(f"{len(passing)} candidate pairings pass; exactly one must. "
                  f"Migrating on a tie or on none would attribute embeddings to the "
                  f"wrong ORFs.", file=sys.stderr)
            return 1
        chosen, why = candidates[passing[0]], f"via the '{passing[0]}' pairing"

    out = pd.concat([
        pd.DataFrame({"sequence_id": index_ids}),
        pd.DataFrame(emb[chosen], columns=[f"dim_{i}" for i in range(len(dims))]),
    ], axis=1)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_p, index=False)
    print(f"wrote {out_p} -- {len(out):,} x {len(dims)}; {why}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
