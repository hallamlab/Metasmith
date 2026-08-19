"""Rebuild `ref::label_transfer_landmarks` from the scrambled pool, with no re-embedding.

    PYTHONPATH="$PWD/src" mamba run -n msm python \
        research/fabfos/annotation_lanes/pbert/rebuild_landmarks.py [--esmc]

THE PERMUTATION IS COMPUTED, NOT SEARCHED FOR. `pbert` writes fixed 1,024-sequence
chunks in FASTA order named `<stem>.1`, `<stem>.2`, ... with no zero padding, and the
old assemble step stacked `sorted(glob("*.npy"))` while its index stayed in FASTA order.
Both halves are deterministic, so the map from index position to stack row follows from
the record count alone -- `fir/pbert_permutation.fasta_to_legacy_row`, measured against
re-embedded assemblies on 2026-08-05. 222,019 accessions is 217 chunks, and
`sorted(range(1, 218), key=str)` is not the identity.

THIS SUPERSEDES `repair_pool_index.py`, which recovered the same permutation by matching
duplicate-item signatures block by block. That reached 214,016 of 222,019 accessions and
left 8 blocks it could not separate. The two agree on 100.00% of what the signature
method covered, so the analytic map is the signature result plus the blocks it could not
resolve -- and neither had to trust the other to get there.

Three gates, all of which must pass before anything is written. Two are the checks that
identified the defect, run in reverse; the third is held out from both derivations:

  * every sampled group of accessions with an identical sequence lands on an identical
    embedding (39/400 as shipped, 400/400 permuted)
  * the permutation agrees with `repair_pool_index.py`'s independent recovery wherever
    that one spoke
  * every DH10B ORF byte-identical to a Swiss-Prot entry unique in the set sits at
    cosine > 0.999 to that entry's row (3/554 as shipped, 554/554 permuted)

`--esmc` does the same collapse for the ESM-C set, whose stack and index WERE written
from the same arrays in one pass and so need no permutation. It runs the duplicate-
sequence gate anyway, because "was never scrambled" is a claim about a transform rather
than about the bytes on disk, and this artifact predates anyone checking.

INPUT   data/fabfos/processed/reference_label_pool{,_esmc}/pool{,_esmc}/
        data/fabfos/originals/swissprot/2026_02/uniprot_sprot.fasta.gz
        data/fabfos/runs/e_coli_dh10b/  -- the held-out anchors
OUT     data/fabfos/processed/label_transfer_landmarks{,_esmc}/landmarks{,_esmc}/
"""
from __future__ import annotations

import collections
import gzip
import hashlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "research/fabfos/examples/fir"))
from pbert_permutation import fasta_to_legacy_row  # noqa: E402

POOL = REPO / "data/fabfos/processed/reference_label_pool/pool"
POOL_ESMC = REPO / "data/fabfos/processed/reference_label_pool_esmc/pool_esmc"
SPROT = REPO / "data/fabfos/originals/swissprot/2026_02/uniprot_sprot.fasta.gz"
DH10B = REPO / "data/fabfos/runs/e_coli_dh10b"
REPAIRED = HERE / "cache/pool_index_repaired.parquet"
OUT = REPO / "data/fabfos/processed/label_transfer_landmarks/landmarks"
OUT_ESMC = REPO / "data/fabfos/processed/label_transfer_landmarks_esmc/landmarks_esmc"

ALPHABET = set("ACDEFGHIKLMNPQRSTUVWXY")


def tokenisable(seq: str) -> str:
    return "".join(c if c in ALPHABET else "X" for c in seq.upper())


def md5(seq: str) -> str:
    return hashlib.md5(tokenisable(seq).encode()).hexdigest()


def read_fasta_gz(path: Path, keep: set[str] | None = None):
    out, name, buf = {}, None, []
    with gzip.open(path, "rt") as fh:
        for line in fh:
            if line.startswith(">"):
                if name is not None and (keep is None or name in keep):
                    out[name] = "".join(buf)
                parts = line[1:].split("|")
                name = parts[1] if len(parts) >= 3 else line[1:].split(None, 1)[0]
                buf = []
            else:
                buf.append(line.strip())
    if name is not None and (keep is None or name in keep):
        out[name] = "".join(buf)
    return out


def read_fasta(path: Path):
    out, name, buf = {}, None, []
    for line in path.read_text().splitlines():
        if line.startswith(">"):
            if name is not None:
                out[name] = "".join(buf)
            name, buf = line[1:].split()[0], []
        else:
            buf.append(line.strip())
    if name is not None:
        out[name] = "".join(buf)
    return out


def unit(a: np.ndarray) -> np.ndarray:
    return a / np.clip(np.linalg.norm(a, axis=1, keepdims=True), 1e-9, None)


def main() -> int:
    esmc = "--esmc" in sys.argv[1:]
    pool, out = (POOL_ESMC, OUT_ESMC) if esmc else (POOL, OUT)
    stack_name = "emb_esmc.npy" if esmc else "emb_pbert.npy"

    idx = pd.read_parquet(pool / "orf_index.parquet")
    acc = idx["orf"].to_numpy()
    n = len(acc)
    emb = np.load(pool / stack_name, mmap_mode="r")
    if emb.shape[0] != n:
        print(f"index {n} rows, stack {emb.shape[0]}", file=sys.stderr)
        return 2
    # The ESM-C transform wrote its stack and its ids from the same arrays, so the only
    # candidate is the identity -- and it still has to pass the gates below.
    perm = np.arange(n) if esmc else fasta_to_legacy_row(n)
    print(f"{n:,} accessions, {(n + 1023) // 1024} chunks, "
          f"permutation is {'the identity' if (perm == np.arange(n)).all() else 'non-trivial'}")

    seqs = read_fasta_gz(SPROT, keep=set(acc.tolist()))
    if len(seqs) != n:
        print(f"{len(seqs):,} of {n:,} accessions have a Swiss-Prot sequence",
              file=sys.stderr)
        return 2
    pos = {a: i for i, a in enumerate(acc)}
    by_seq: dict[str, list[int]] = collections.defaultdict(list)
    for a, s in seqs.items():
        by_seq[md5(s)].append(pos[a])

    # gate 1 -- identical sequences, identical embeddings
    groups = [g for g in by_seq.values() if len(g) > 1]
    rng = np.random.default_rng(0)
    sample = [groups[i] for i in rng.choice(len(groups), min(400, len(groups)),
                                            replace=False)]

    def agree(rows_of):
        ok = 0
        for g in sample:
            v = unit(np.asarray(emb[np.sort(rows_of[g])], dtype=np.float32))
            ok += float((v @ v[0]).min()) > 0.999
        return ok

    before, after = agree(np.arange(n)), agree(perm)
    print(f"duplicate-sequence groups: {before}/{len(sample)} agree as shipped, "
          f"{after}/{len(sample)} permuted")

    # gate 2 -- against the independent signature recovery. It ran on the ProteinBERT
    # stack only; there is nothing for it to say about ESM-C.
    if REPAIRED.exists() and not esmc:
        rep = pd.read_parquet(REPAIRED)
        m = dict(zip(rep["orf"], rep["row"]))
        covered = np.array([m.get(a, -1) for a in acc], dtype=np.int64)
        seen = covered >= 0
        same = int((covered[seen] == perm[seen]).sum())
        print(f"signature recovery covered {int(seen.sum()):,}/{n:,}; the analytic "
              f"permutation agrees on {same:,} ({100.0 * same / int(seen.sum()):.2f}%)")
        gate2 = same == int(seen.sum())
    else:
        print("the signature cross-check does not apply here")
        gate2 = True

    # gate 3 -- held out from both derivations
    unique_row = {k: v[0] for k, v in by_seq.items() if len(v) == 1}
    qids = pd.read_csv(DH10B / ("annotation_alts/esmc/dh10b.esmc.index.csv" if esmc
                                else "annotations/proteinbert/dh10b.pbert.index.csv"))["sequence_id"].to_numpy()
    qdf = pd.read_parquet(DH10B / ("annotation_alts/esmc/dh10b.esmc.parquet" if esmc
                                   else "annotations/proteinbert/dh10b.pbert.parquet"))
    qcols = [c for c in qdf.columns if c.startswith("dim_")] or list(qdf.columns)
    q = qdf[qcols].to_numpy(dtype=np.float32)
    qpos = {o: i for i, o in enumerate(qids)}
    anchors = [(qpos[o], unique_row[md5(s)])
               for o, s in read_fasta(DH10B / "annotations/dh10b.faa").items()
               if o in qpos and md5(s) in unique_row]
    qi = np.array([a for a, _ in anchors])
    pi = np.array([b for _, b in anchors])

    def anchor_hits(rows):
        return int((np.einsum("ij,ij->i", unit(q[qi]),
                              unit(np.asarray(emb[rows], dtype=np.float32))) > 0.999).sum())

    hit_before, hit_after = anchor_hits(pi), anchor_hits(perm[pi])
    print(f"DH10B anchors: {hit_before}/{len(anchors)} at cosine > 0.999 as shipped, "
          f"{hit_after}/{len(anchors)} permuted")

    if not (after == len(sample) and gate2 and hit_after == len(anchors)):
        print("a gate did not pass; nothing written. A permutation that is merely "
              "plausible produces a complete, confident, wrong reference.",
              file=sys.stderr)
        return 1

    out.mkdir(parents=True, exist_ok=True)
    table = pd.concat([
        idx[["orf", "mnxr_list"]].rename(columns={"orf": "accession"}).reset_index(drop=True),
        pd.DataFrame(np.asarray(emb[perm], dtype=np.float32),
                     columns=[f"dim_{i}" for i in range(emb.shape[1])]),
    ], axis=1)
    table.to_parquet(out / "landmarks.parquet", index=False)

    src = (pool / "pool_source.txt").read_text().rstrip("\n")
    (out / "source.txt").write_text(
        src + "\n"
        f"assembly\tcollapsed from {pool.parent.name}/{pool.name} by "
        f"annotation_lanes/pbert/rebuild_landmarks.py, index-to-stack map "
        f"{'identity' if esmc else 'fasta_to_legacy_row (lexicographic chunk order)'}, "
        f"gated on {before}->{after}/{len(sample)} duplicate-sequence groups and "
        f"{hit_before}->{hit_after}/{len(anchors)} held-out DH10B anchors\n")
    print(f"wrote {out}/landmarks.parquet -- {len(table):,} x {emb.shape[1]} "
          f"({(out / 'landmarks.parquet').stat().st_size / 2**20:.0f} MiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
