"""The all-vs-all identity the dedup clusters on, cached.

Shared module. Draws no figure.

WHAT IT IS
  One blastn of the 669 pieces against themselves at the dedup's own settings --
  `evalue 1000, perc_identity 50`, deliberately permissive because the similarity
  that matters is one long contiguous match and a strict evalue drops the HSPs
  that make it up -- reduced to a matrix by `fabfos_recovery._similarity`.

  Two normalisations, both from that one blast:

    symmetric     nident / max(qlen, slen)   the CLUSTERING metric
    containment   nident / qlen              the ABSORB metric

  The clustering metric is symmetric and penalises length disagreement, so a
  fragment does not read as identical to the thing that contains it. Containment
  asks the opposite question on purpose and is the wrong matrix to cluster on.
  Both are computed here so a figure never has to pick the convenient one.

  `_similarity` is imported, not restated. Two definitions of identity -- one in
  the pipeline, one in the figure of the pipeline -- is a difference that shows
  up as a slightly different curve and as nothing else.

INPUT   pieces.py (which reads only ./data)
ENV     mamba run -n figure-net python main/figures/inserts/identity.py
        needs blastn on PATH; FABFOS_BLAST_BIN points at it (default: the
        `fabfos-bio` env, which is where this repo's blast 2.17 lives)
OUT     cache/identity/  the blast table and the two matrices, keyed by the piece
        set's size and the length floor so a re-run under different settings
        cannot silently reuse the wrong one
"""
import argparse
import os
from pathlib import Path

import numpy as np

import pieces
from pieces import fr, CACHE

BLAST_BIN = Path(os.environ.get(
    "FABFOS_BLAST_BIN", "/home/tony/lib/miniforge3/envs/fabfos-bio/bin"))
WORK = CACHE / "identity"


def _with_blast_on_path():
    if not (BLAST_BIN / "blastn").exists():
        raise SystemExit(
            f"no blastn at {BLAST_BIN}. Set FABFOS_BLAST_BIN, or create the env:\n"
            f"  mamba create -n fabfos-bio -c conda-forge -c bioconda blast")
    os.environ["PATH"] = f"{BLAST_BIN}:{os.environ['PATH']}"


def hits(threads=12, force=False):
    """-> the blast table path. Runs the all-vs-all once and caches it.

    Written as `ava_hits.tsv` inside the piece work directory, under the name and
    beside the two files the pipeline's own `dedup` puts there -- so that
    directory IS a dedup work directory and anything that reads one can be
    pointed straight at it.
    """
    work = pieces.build()
    out = work / "ava_hits.tsv"
    # A table cached before the format widened has the right name and the wrong
    # columns, and the reader raises on it rather than reducing it to an empty
    # matrix. Re-blast instead of asking the user to delete a scratch file.
    if out.exists() and not force:
        first = out.read_text().split("\n", 1)[0]
        if len(first.split("\t")) == len(fr.AVA_COLS):
            return out
        print("[identity] cached all-vs-all predates the query-coordinate "
              "columns; re-running", flush=True)
    _with_blast_on_path()
    WORK.mkdir(parents=True, exist_ok=True)
    rows = fr.blast_hsps(work / "pooled.fna", work / "pooled.fna", WORK / "blast",
                         threads=threads, evalue=1000, perc_identity=50,
                         outfmt=fr.AVA_HSP_FMT)
    tmp = out.with_suffix(".partial")
    with open(tmp, "w") as fh:
        for h in rows:
            fh.write("\t".join(h[k] for k in fr.AVA_COLS) + "\n")
    tmp.rename(out)
    print(f"[identity] {len(rows):,} HSPs -> {out.parent.name}/{out.name}", flush=True)
    return out


def matrices(threads=12, force=False):
    """-> (labels, symmetric, containment).

    `labels` are the pipeline's `C#####` keys in `piece_meta.json` order, which is
    the order every other table here is built against.
    """
    meta, _seqs = pieces.load()
    # `sum` names the reduction, not a version: the matrices changed when
    # `_similarity` started summing non-overlapping HSPs, and a cache keyed only
    # on piece count would have served the old ones under the new metric.
    cache = WORK / f"sim_n{len(meta)}_min{pieces.MIN_LEN}_sum.npz"
    if cache.exists() and not force:
        z = np.load(cache, allow_pickle=False)
        return list(z["labels"]), z["symmetric"], z["containment"]

    hsps = fr.read_blast_tsv(hits(threads=threads, force=force),
                             outfmt=fr.AVA_HSP_FMT)
    labels = list(meta.keys())
    index = {c: i for i, c in enumerate(labels)}
    sym = fr._similarity(hsps, index, "symmetric")
    cont = fr._similarity(hsps, index, "containment")
    WORK.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(cache, labels=np.array(labels), symmetric=sym,
                        containment=cont)
    print(f"[identity] {sym.shape[0]}x{sym.shape[0]} -> {cache.name}", flush=True)
    return labels, sym, cont


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--threads", type=int, default=12)
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    labels, sym, cont = matrices(threads=a.threads, force=a.force)
    off = ~np.eye(len(labels), dtype=bool)
    print(f"{len(labels)} pieces; symmetric identity off-diagonal: "
          f"max {sym[off].max():.4f}, "
          f"pairs >= 0.99: {int((sym[off] >= 0.99).sum() // 2):,}")


if __name__ == "__main__":
    main()
