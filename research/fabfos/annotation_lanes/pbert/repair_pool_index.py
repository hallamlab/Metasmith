"""THE QUESTION -- the pbert lane transfers a label from the wrong protein. Why?

Because `ref::reference_label_pool`'s index does not describe its embedding stack.
The pool is 222,019 Swiss-Prot accessions in the release's FASTA order and a
222,019-row ProteinBERT stack, and `reference_label_pool.py`'s ASSEMBLE step pairs
them by row -- but the embedder writes the stack in SHARDS of 1,024, and the two
`sorted(glob(...))` calls that read the shards back do not put the .npy files in
the order the .csv index describes. Every row is then labelled with some other
protein's reactions, and nothing raises: the length check passes, the label merge
passes, and the lane emits a full, confident, wrong table. It is exactly the failure
that transform's own docstring warns about, arriving through the one door it does
not guard.

Three independent observations pin it, none of which needs the embedder:
  * 328 of 400 groups of pool accessions carrying IDENTICAL sequences have
    DIFFERENT pool embeddings. A deterministic embedder cannot do that.
  * For a DH10B ORF whose sequence is byte-identical to a Swiss-Prot K-12 entry,
    the cosine to that entry's pool row is 0.26 -- background -- while an exact
    match (1.000) sits elsewhere in the stack.
  * Those exact matches land at a displacement from the accession's index position
    that is constant in blocks of 1,024 and changes by multiples of 1,024 between
    blocks. A shard permutation, not noise.

THE REPAIR, and why it needs no re-embedding. The permutation moves whole shards, so
recovering it means matching 217 index blocks to 217 stack blocks. The signature that
identifies a block is which of its 1,024 positions hold a DUPLICATED item: in the
index, an accession whose sequence appears elsewhere in the pool; in the stack, a row
whose embedding appears elsewhere. Under the true pairing those two patterns are the
same 1,024-bit vector, and they are nearly unique -- the median best match agrees on
1,023 of 1,024 positions while the runner-up agrees on 774.

Validated on held-out anchors the matching never saw: DH10B ORFs whose sequence is
unique in the pool and byte-identical to a Swiss-Prot entry, so the row holding their
embedding is known independently. 442 of 443 land exactly where the recovered map
says. The seven blocks the signature cannot separate are dropped rather than guessed.

SUPERSEDED by rebuild_landmarks.py, which computes the same permutation from the record
count instead of searching for it -- the two agree on 100.00% of the 214,016 accessions
this one resolved, and the analytic map also covers the 8 blocks the signature could not
separate. Kept because it is the independent derivation that gate, and because the
signature argument is what identified the defect in the first place.
INPUT   data/fabfos/processed/reference_label_pool/pool/{orf_index.parquet,emb_pbert.npy}
        data/fabfos/originals/swissprot/2026_02/uniprot_sprot.fasta.gz
ENV     PYTHONPATH="$PWD/src" mamba run -n msm python \
            research/fabfos/annotation_lanes/pbert/repair_pool_index.py
OUT     research/fabfos/annotation_lanes/pbert/cache/pool_index_repaired.parquet
        research/fabfos/annotation_lanes/pbert/pool_repair_dh10b.tsv   (committed)
"""
from __future__ import annotations

import collections
import gzip
import hashlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _knn  # noqa: E402
from _common import ANN, CACHE, HERE, SHIPPED_POOL as POOL, iter_fasta, load_cohort, load_query, md5, orf_to_accession  # noqa: E402

SWISSPROT = POOL.parents[2] / "originals/swissprot/2026_02/uniprot_sprot.fasta.gz"
BLOCK = 1024
ALPHABET = set("ACDEFGHIKLMNPQRSTUVWXY")


def tokenisable(s: str) -> str:
    """The pool builder's recoding, so our md5 keys the sequence that was embedded."""
    return "".join(c if c in ALPHABET else "X" for c in s.upper())


def pool_sequences(accs) -> dict[str, str]:
    want = set(accs)
    out, acc, buf = {}, None, []
    with gzip.open(SWISSPROT, "rt") as fh:
        for line in fh:
            if line.startswith(">"):
                if acc in want:
                    out[acc] = "".join(buf)
                parts = line[1:].split("|")
                acc = parts[1] if len(parts) >= 3 else line[1:].split(None, 1)[0]
                buf = []
            else:
                buf.append(line.strip())
    if acc in want:
        out[acc] = "".join(buf)
    return out


def main():
    CACHE.mkdir(exist_ok=True)
    idx = pd.read_parquet(POOL / "orf_index.parquet")
    accs = idx["orf"].to_numpy()
    n = len(accs)
    seqs = pool_sequences(accs)
    if len(seqs) != n:
        raise SystemExit(f"{n - len(seqs)} pool accessions absent from Swiss-Prot")

    sh = [hashlib.md5(tokenisable(seqs[a]).encode()).digest() for a in accs]
    cs = collections.Counter(sh)
    S = np.array([cs[h] > 1 for h in sh])
    uniq = np.array([cs[h] == 1 for h in sh])

    emb = np.load(POOL / "emb_pbert.npy", mmap_mode="r")
    eh = [hashlib.md5(np.asarray(emb[r]).tobytes()).digest() for r in range(n)]
    ce = collections.Counter(eh)
    D = np.array([ce[h] > 1 for h in eh])
    print(f"[repair] duplicated: {S.sum():,} sequences, {D.sum():,} embeddings", flush=True)

    nb = (n + BLOCK - 1) // BLOCK
    tail = n - (nb - 1) * BLOCK
    # A shard permutation only ever puts a block boundary at a multiple of BLOCK, or
    # at that offset by `tail` once the short last shard has gone by.
    starts = np.array(sorted(set(list(range(0, n - BLOCK + 1, BLOCK)) +
                                 list(range(tail, n - BLOCK + 1, BLOCK)))))
    W = np.stack([D[s:s + BLOCK] for s in starts])

    scored = []
    for b in range(nb - 1):
        sc = (W == S[b * BLOCK:(b + 1) * BLOCK]).sum(1)
        o = np.argsort(-sc)
        scored.append((b, int(starts[o[0]]), int(sc[o[0]]), int(sc[o[1]])))
    block_start, taken = {}, set()
    for b, s, best, second in scored:
        if best >= BLOCK - 24 and best - second >= 50 and s not in taken:
            block_start[b], _ = s, taken.add(s)
    print(f"[repair] blocks matched on signature: {len(block_start)}/{nb - 1}", flush=True)

    rows, orfs, labels = [], [], []
    lab = dict(zip(idx["orf"], idx["mnxr_list"]))
    for b, s in block_start.items():
        for o in range(BLOCK):
            i = b * BLOCK + o
            rows.append(s + o)
            orfs.append(accs[i])
            labels.append(lab[accs[i]])
    rep = pd.DataFrame({"role": "reference", "row": rows, "orf": orfs, "mnxr_list": labels})
    rep = rep.sort_values("row").reset_index(drop=True)
    rep.to_parquet(CACHE / "pool_index_repaired.parquet", index=False)

    # ---- validation on anchors the matching never saw --------------------
    E = _knn._norm(np.asarray(emb, dtype=np.float32))
    q, qid = load_query()
    truth = set(load_cohort()["orf"])
    keep = np.array([i for i, o in enumerate(qid) if o in truth])
    Q = _knn._norm(q[keep])
    qid = qid[keep]
    o2a = orf_to_accession()
    posn = {a: i for i, a in enumerate(accs)}
    ok = tot = 0
    for s in range(0, len(Q), 256):
        sim = Q[s:s + 256] @ E.T
        r = np.argmax(sim, 1)
        v = sim[np.arange(len(r)), r]
        for j in range(len(r)):
            acc = o2a.get(qid[s + j])
            i = posn.get(acc, -1)
            if i < 0 or not uniq[i] or v[j] <= 0.999:
                continue
            b = i // BLOCK
            if b not in block_start:
                continue
            tot += 1
            ok += int(block_start[b] + (i - b * BLOCK) == int(r[j]))
    print(f"[repair] held-out anchors {ok}/{tot} ({ok/max(tot,1):.1%}) land where the map says")
    print(f"[repair] repaired pool covers {len(rep):,} of {n:,} accessions "
          f"({len(rep)/n:.1%}); {nb - 1 - len(block_start)} blocks dropped as unresolved")

    pd.DataFrame([dict(pool_accessions=n, blocks=nb, block_size=BLOCK, tail=tail,
                       blocks_resolved=len(block_start), accessions_kept=len(rep),
                       anchors=tot, anchors_correct=ok,
                       identical_seq_groups=int(sum(1 for c in cs.values() if c > 1)),
                       duplicated_sequences=int(S.sum()), duplicated_embeddings=int(D.sum()))]
                 ).to_csv(HERE / "pool_repair_dh10b.tsv", sep="\t", index=False)
    print(f"wrote {CACHE/'pool_index_repaired.parquet'} and {HERE/'pool_repair_dh10b.tsv'}")


if __name__ == "__main__":
    main()
