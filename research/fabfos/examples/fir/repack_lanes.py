#!/usr/bin/env python3
"""Repack the legacy per-assembly kofam hits into the shards.

    SLURM_ARRAY_TASK_ID=<i> SHARD_N=<n> python repack_lanes.py <gpr_root> <annot1>

Writes, per shard, the one file the driver supplies to the planner as an
already-computed lane product parented to that shard's ORFs:

    lanes/shard_XXXX.kofam.csv          annotation::kofamscan_results

ONLY KOFAM. The same legacy pass produced 87 GB of ProteinBERT embeddings and
they are not reusable: the index recording their row order was not retained, and
a content check (`verify_pbert_order.py`, 2026-08-05) found the order is not the
fasta's -- 2,032 of 49,522 rows aligned on a 49-chunk assembly. The lane is
re-run instead, which costs ~10 min a shard and removes the assumption entirely.

kofam survives that same test because it never made the assumption: its rows are
per (gene, KO) hit, KEYED by gene id rather than positional, so they are selected
here against the id set the shard actually holds -- read from the shard fasta,
not inferred. A sample split across two shards therefore sends each hit to
whichever shard owns its gene, with no offset arithmetic anywhere.

Resumable per shard, so a wall-clock kill costs one shard rather than the pass.
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict
from pathlib import Path

GPR = Path(sys.argv[1])
ANNOT1 = Path(sys.argv[2])
SHARD_I = int(os.environ.get("SLURM_ARRAY_TASK_ID", "0"))
SHARD_N = int(os.environ.get("SHARD_N", "1"))

SHARDS = GPR / "shards"
LANES = GPR / "lanes"
KOFAM_SRC = ANNOT1 / "annotation-kofamscan_results"

DELIM = "::"
KOFAM_HEADER = "gene_name,KO,thrshld,score,E-value,best"


def read_parts(path: Path) -> dict[str, list[tuple[str, int, int]]]:
    """shard -> [(sample, start, n)] in write order."""
    out: dict[str, list[tuple[int, str, int, int]]] = defaultdict(list)
    with path.open() as fh:
        next(fh)
        for line in fh:
            shard, order, sample, start, n = line.rstrip("\n").split("\t")
            out[shard].append((int(order), sample, int(start), int(n)))
    return {k: [(s, st, n) for _o, s, st, n in sorted(v)] for k, v in out.items()}


def shard_ids(faa: Path) -> set[str]:
    """The shard's prefixed ORF ids, read from the shard itself."""
    ids = set()
    with faa.open("rb") as fh:
        for line in fh:
            if line[:1] == b">":
                ids.add(line[1:].split()[0].decode())
    return ids


def repack_kofam(shard: str, parts, out_csv: Path, ids: set[str]) -> tuple[int, int]:
    tmp = out_csv.with_suffix(".csv.part")
    kept = seen = 0
    done: set[str] = set()
    with tmp.open("w") as out:
        out.write(KOFAM_HEADER + "\n")
        for sample, _start, _n in parts:
            if sample in done:
                continue        # one pass per sample even when it has two slices
            done.add(sample)
            src = KOFAM_SRC / f"{sample}.txt"
            with src.open() as fh:
                head = fh.readline().rstrip("\n").strip()
                if head != KOFAM_HEADER:
                    raise SystemExit(
                        f"REFUSING {shard}: {src.name} header is {head!r}, not the "
                        f"{KOFAM_HEADER!r} the lane's parser expects. A drift here "
                        f"silently renames every column and empties the lane.")
                for line in fh:
                    gene, _sep, rest = line.partition(",")
                    seen += 1
                    pid = f"{sample}{DELIM}{gene}"
                    if pid in ids:
                        # `rest` carries whatever line ending the legacy file
                        # had, and a file with no trailing newline would
                        # concatenate the next sample's first row onto this
                        # one. That is loud here (the parser declares 6 fields
                        # and would see 11) but only after all four lanes have
                        # run, so it is cheaper to normalise than to diagnose.
                        if not rest.endswith("\n"):
                            rest += "\n"
                        out.write(f"{pid},{rest}")
                        kept += 1
    if kept == 0:
        raise SystemExit(
            f"REFUSING {shard}: none of the {seen:,} legacy kofam rows for its "
            f"{len(done)} member sample(s) name an ORF this shard holds. The gene "
            f"ids do not join, which means the legacy annotation describes a "
            f"different ORF call than the corpus this campaign is sharding.")
    # Rename LAST, so `out_csv.exists()` means "complete" and never "a task died
    # halfway through this shard". Without it the resume check can never fire and
    # -- worse -- the driver looks for a file the pass never produced under the
    # name it promised.
    tmp.rename(out_csv)
    return kept, seen


def main() -> int:
    parts_by_shard = read_parts(GPR / "parts.tsv")
    shards = sorted(parts_by_shard)
    mine = shards[SHARD_I::SHARD_N]
    LANES.mkdir(parents=True, exist_ok=True)

    for shard in mine:
        kof = LANES / f"{shard}.kofam.csv"
        if kof.exists():
            continue
        if not (SHARDS / f"{shard}.done").exists():
            raise SystemExit(f"REFUSING {shard}: the shard fasta is not marked done")
        parts = parts_by_shard[shard]
        ids = shard_ids(SHARDS / f"{shard}.faa")
        kept, seen = repack_kofam(shard, parts, kof, ids)
        print(f"[{SHARD_I}] {shard}: {len(ids):,} ORFs, {len(parts)} slices, "
              f"{kept:,} of {seen:,} kofam hits kept", flush=True)
    print(f"[{SHARD_I}] done: {len(mine)} shard(s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
