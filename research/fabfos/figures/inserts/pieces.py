"""The dedup's own input, rebuilt from `./data`: the 669 pieces and what was done to each.

Shared module. Draws no figure.

WHY THIS EXISTS
  Two of the figures here are about the CLUSTERING, so their input is not the 170
  inserts but the pieces the clustering ran on -- and the shipped run does not
  include them. What it does include is enough to put them back exactly:
  `membership.csv` names all 669 by a fully-qualified id, and every assembly and
  assembly graph they were cut from is in `assembly/`. So this module re-runs the
  pipeline's own `rectify` over the shipped junction blast and takes the pieces
  out of the far end, rather than approximating them from the ids.

  It re-runs rather than slices for one reason: `action` -- kept / trimmed /
  split / circularised -- is what the length figure counts, and it is a property
  of the CUT, not of the coordinates. Reading it off the id range (does
  start-end span the whole contig?) is wrong on 10 of 669, and wrong in the
  direction that matters: it labels untouched graph-circular contigs as trimmed,
  because their stated join overlap makes the piece shorter than the raw record.

  Nothing here is reimplemented. Every step is `fabfos_recovery`'s, called the
  way `dedup` calls it, so the figure cannot drift onto a second definition of
  what a piece is.

NO BACKBONE BLAST
  `rectify` is given `backbone=None`, so no blast runs. The backbone footprints
  come from the shipped `junctions.tsv` -- blast's own table, verbatim -- and the
  only thing a live backbone would add is the graph-adjacency half of the closure
  evidence. Closure is not recomputed here at all: `inserts.csv` ships `ends` and
  that column is the one to use.

INPUT   data/fabfos/runs/scadc_fosmids/assembly/assemblies/          70 FASTA + graphs
        data/fabfos/runs/scadc_fosmids/sequences/inserts/            junctions, membership
ENV     mamba run -n figure-net python main/figures/inserts/pieces.py --verify
OUT     cache/pieces/  the rebuilt piece set, in the pipeline's own C##### keys
        `--verify` re-derives the 170 centroid sequences and diffs them against
        the shipped `inserts.fna`. That check is this module's whole licence.
"""
import argparse
import json
import sys
from pathlib import Path

from _common import ASSEMBLY, CACHE, INSERT_META, INSERTS, REPO

ASSEMBLIES = ASSEMBLY / "assemblies"
JUNCTIONS = INSERT_META / "junctions.tsv"
MEMBERSHIP = INSERT_META / "membership.csv"
INSERTS_FNA = INSERTS / "inserts.fna"

MIN_LEN = 10_000
MIN_PIECE = 1_000
MIN_HSP = 50

sys.path.insert(0, str(REPO / "src"))
from fabfos.algorithm import fabfos_recovery as fr        # noqa: E402


def pools():
    out = {}
    for fna in sorted(ASSEMBLIES.glob("*.fna")):
        pool, assembler = fna.name[:-len(".fna")].rsplit(".", 1)
        if assembler == "megahit":
            graph, paths = fna.with_suffix(".fastg"), None
        elif assembler == "spades":
            graph, paths = fna.with_suffix(".gfa"), fna.with_suffix(".paths")
        else:
            raise ValueError(f"no graph convention for assembler {assembler!r}")
        for p in (graph, *( [paths] if paths else [] )):
            if not p.exists():
                raise FileNotFoundError(f"{fna.name}: missing graph {p}")
        out.setdefault(pool, {})[assembler] = (fna, graph, paths)
    return out


def build(work=None, force=False):
    work = Path(work or CACHE / "pieces")
    if not force and (work / "piece_meta.json").exists():
        return work
    work.mkdir(parents=True, exist_ok=True)

    split_all = work / "split_contigs.fna"
    records = []
    for pool, byasm in pools().items():
        out_split = work / "split" / f"{pool}.fna"
        out_split.parent.mkdir(parents=True, exist_ok=True)
        fr.rectify(
            assemblies=[(fna, asm, pool) for asm, (fna, _g, _p) in byasm.items()],
            junctions=JUNCTIONS,
            out_split=out_split,
            backbone=None,
            graphs={asm: (g, p) for asm, (_f, g, p) in byasm.items()},
            min_piece=MIN_PIECE, min_hsp=MIN_HSP,
            work=work / "rectify" / pool,
        )
        records += fr.read_fasta(out_split)
    fr.write_fasta(split_all, records)
    fr.dedup_prep(split_all, work, min_contig_len=MIN_LEN)
    return work


def load(work=None):
    work = build(work)
    meta = json.loads((work / "piece_meta.json").read_text())
    seqs = {n: s for n, _d, s in fr.read_fasta(work / "pooled.fna")}
    return meta, seqs


def by_piece_id(work=None):
    meta, seqs = load(work)
    return ({m["piece"]: seqs[k] for k, m in meta.items()},
            {m["piece"]: m["action"] for k, m in meta.items()})


def verify(work=None):
    import pandas as pd

    seqs, actions = by_piece_id(work)
    mem = pd.read_csv(MEMBERSHIP)
    shipped = set(mem["contig"])
    rebuilt = set(seqs)
    print(f"pieces rebuilt {len(rebuilt):,}   membership names {len(shipped):,}   "
          f"identical: {rebuilt == shipped}")

    bad = 0
    centroids = fr.read_fasta(INSERTS_FNA)
    for name, _desc, seq in centroids:
        if seqs.get(name) != seq:
            bad += 1
            if bad <= 5:
                print(f"  MISMATCH {name}: shipped {len(seq)} bp, "
                      f"rebuilt {len(seqs.get(name, '')) or 'absent'}")
    print(f"centroids checked {len(centroids)}   exact {len(centroids) - bad}   "
          f"mismatch {bad}")

    counts = {}
    for a in actions.values():
        counts[a] = counts.get(a, 0) + 1
    print("action  " + "  ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    ok = (rebuilt == shipped) and bad == 0
    print("VERIFIED" if ok else "FAILED")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--work", type=Path, default=None)
    ap.add_argument("--force", action="store_true", help="rebuild even if cached")
    ap.add_argument("--verify", action="store_true",
                    help="diff the rebuilt centroids against inserts.fna")
    a = ap.parse_args()
    build(a.work, force=a.force)
    raise SystemExit(verify(a.work) if a.verify else 0)


if __name__ == "__main__":
    main()
