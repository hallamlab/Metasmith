#!/usr/bin/env python3
"""Pool 2,844 per-assembly ORF fastas into even shards, reversibly and resumably.

    python reshard.py --orfs-dir <in> --output <out> [--orfs-per-shard 100000]

Supersedes `examples/cyanoverse_reshard_orfs.py`, which was written for the
same job and never run. Three things changed, and each removes a failure the
corpus scale would otherwise have caused.

**Sequential fill, not bin-packing.** The old packer refused to split an
assembly, so the 492 MB sample (~1M ORFs, 200x the median) got a shard ten
times the size of every other -- precisely the shard that would exceed the
join's memory and the walltime cap. Filling shards in order and spilling the
remainder into the next one gives shards that are all exactly the target size,
makes the shard count `ceil(total / target)` rather than an emergent property
of the packing, and bounds every lane's per-task cost by construction.

Splitting is safe *here* because every lane is per-sequence: CLEAN's maxsep
distance, DIAMOND's bitscore ratio (its e-value scales with the DATABASE, not
the query file), kofam's profile scores and the embedding kNN vote are all
computed for one ORF against a fixed reference. Splitting a sample across two
shards is a rearrangement of the same measurement, not a different one. It
would NOT be safe for a lane that normalised within its input file.

**No 59-million-row manifest.** The old script accumulated one manifest row per
ORF in a Python list and wrote it at the end -- several GB resident, and a crash
lost the whole 30 GB pass. It is also unnecessary: the ORF ids in shard order
ARE the shard fasta's headers, which is what the repack needs, and the sample an
ORF came from is in its own id. What gets written instead is `parts.tsv`, one
row per (shard, sample, slice) -- a few thousand rows, and the exact contract
the repack reads to know which slice of a sample's legacy kofam/pbert artifacts
belongs to which shard.

**Resumable.** The survey (counting 59M records across 29 GB) is cached, and a
finished shard carries a `.done` marker with its record count, so a re-run skips
it. A killed job costs one shard, not the pass.

WHAT MAKES POOLING SAFE IS THE PREFIX, AND NOTHING ELSE. Independent assemblies
genuinely emit the same ORF id -- `k141_1_1` is not distinctive -- so every
header is rewritten `>{sample}::{original}`. Without it the rows do not merely
lose their labels, they silently *collide*, and the collision looks like a
normal table.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

DELIM = "::"
DEFAULT_ORFS_PER_SHARD = 100_000
ORFS_GLOB = "*.faa"


def count_records(path: Path) -> int:
    n = 0
    with path.open("rb") as fh:
        for line in fh:
            if line[:1] == b">":
                n += 1
    return n


def survey(orfs_dir: Path, cache: Path) -> list[tuple[str, Path, int]]:
    if cache.exists():
        out = []
        with cache.open() as fh:
            next(fh)
            for line in fh:
                s, p, n = line.rstrip("\n").split("\t")
                out.append((s, Path(p), int(n)))
        print(f"survey: {len(out)} assemblies from cache {cache}")
        return out

    files = sorted(orfs_dir.glob(ORFS_GLOB))
    if not files:
        raise SystemExit(f"no {ORFS_GLOB} under {orfs_dir}")
    out: list[tuple[str, Path, int]] = []
    seen: set[str] = set()
    for i, f in enumerate(files):
        sample = f.name[: -len(".faa")]
        if DELIM in sample:
            raise SystemExit(f"sample id contains {DELIM!r}, cannot prefix safely: {f}")
        if sample in seen:
            raise SystemExit(f"duplicate sample id {sample!r} from {f}")
        seen.add(sample)
        out.append((sample, f, count_records(f)))
        if i % 200 == 0:
            print(f"  surveyed {i}/{len(files)}", flush=True)
    cache.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache.with_suffix(f".tmp.{os.getpid()}")
    with tmp.open("w") as fh:
        fh.write("sample\tpath\tn_orfs\n")
        for s, p, n in out:
            fh.write(f"{s}\t{p}\t{n}\n")
    tmp.rename(cache)
    print(f"survey: {len(out)} assemblies -> {cache}")
    return out


def plan_parts(items: list[tuple[str, Path, int]], per_shard: int):
    shards: list[list[tuple[str, Path, int, int]]] = []
    cur: list[tuple[str, Path, int, int]] = []
    load = 0
    for sample, path, n in items:
        off = 0
        while off < n:
            take = min(n - off, per_shard - load)
            cur.append((sample, path, off, take))
            off += take
            load += take
            if load >= per_shard:
                shards.append(cur)
                cur, load = [], 0
        if n == 0:
            cur.append((sample, path, 0, 0))
    if cur:
        shards.append(cur)
    return shards


def write_shard(parts, out_fasta: Path) -> int:
    tmp = out_fasta.with_suffix(".faa.part")
    written = 0
    with tmp.open("w") as out:
        for sample, path, start, take in parts:
            if take == 0:
                continue
            seen = -1
            emitting = False
            got = 0
            with path.open("r") as fh:
                for line in fh:
                    if line.startswith(">"):
                        seen += 1
                        if got >= take:
                            break
                        emitting = seen >= start
                        if emitting:
                            out.write(f">{sample}{DELIM}{line[1:].rstrip(chr(10))}\n")
                            got += 1
                            written += 1
                    elif emitting:
                        out.write(line if line.endswith("\n") else line + "\n")
            if got != take:
                raise SystemExit(
                    f"REFUSING: {path.name} yielded {got} of the {take} records the "
                    f"survey promised at offset {start}. The input changed under the "
                    f"run and the parts table no longer describes the shards.")
    tmp.rename(out_fasta)
    return written


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--orfs-dir", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--orfs-per-shard", type=int, default=DEFAULT_ORFS_PER_SHARD)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--only", default=None,
                   help="shard index range 'A:B' to write (for array jobs)")
    a = p.parse_args(argv)

    orfs_dir = Path(a.orfs_dir).expanduser().resolve()
    output = Path(a.output).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)

    items = survey(orfs_dir, output / "survey.tsv")
    total = sum(n for _, _, n in items)
    print(f"{len(items)} assemblies, {total:,} ORFs total")
    if not total:
        raise SystemExit("every input fasta is empty; nothing to shard")

    shards = plan_parts(items, a.orfs_per_shard)
    loads = [sum(t for _, _, _, t in s) for s in shards]
    split = sum(1 for s in shards for _, _, st, _ in s if st > 0)
    print(f"{len(shards)} shards of at most {a.orfs_per_shard:,} ORFs")
    print(f"  ORFs/shard  min {min(loads):,}  max {max(loads):,}")
    print(f"  slices/shard  min {min(len(s) for s in shards)}  max {max(len(s) for s in shards)}")
    print(f"  {split} continuation slice(s) -- assemblies straddling a boundary")

    parts_tsv = output / "parts.tsv"
    if not a.dry_run and not parts_tsv.exists():
        tmp = parts_tsv.with_suffix(f".tmp.{os.getpid()}")
        with tmp.open("w") as fh:
            fh.write("shard\torder\tsample\tstart\tn\n")
            for i, s in enumerate(shards):
                for j, (sample, _path, st, n) in enumerate(s):
                    fh.write(f"shard_{i:04d}\t{j}\t{sample}\t{st}\t{n}\n")
        tmp.rename(parts_tsv)
        print(f"parts -> {parts_tsv}")

    if a.dry_run:
        print("\n--dry-run: no shard written")
        return 0

    shard_dir = output / "shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    lo, hi = (0, len(shards))
    if a.only:
        lo, hi = (int(x) for x in a.only.split(":"))
        hi = min(hi, len(shards))

    for i in range(lo, hi):
        out_fasta = shard_dir / f"shard_{i:04d}.faa"
        done = shard_dir / f"shard_{i:04d}.done"
        if done.exists():
            continue
        n = write_shard(shards[i], out_fasta)
        if n != loads[i]:
            raise SystemExit(f"REFUSING: shard_{i:04d} wrote {n}, planned {loads[i]}")
        done.write_text(f"{n}\n")
        print(f"  shard_{i:04d}  {len(shards[i])} slices  {n:,} ORFs", flush=True)

    print(f"\nshards -> {shard_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
