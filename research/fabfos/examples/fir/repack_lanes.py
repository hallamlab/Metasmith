#!/usr/bin/env python3
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
    out: dict[str, list[tuple[int, str, int, int]]] = defaultdict(list)
    with path.open() as fh:
        next(fh)
        for line in fh:
            shard, order, sample, start, n = line.rstrip("\n").split("\t")
            out[shard].append((int(order), sample, int(start), int(n)))
    return {k: [(s, st, n) for _o, s, st, n in sorted(v)] for k, v in out.items()}


def shard_ids(faa: Path) -> set[str]:
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
                continue
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
