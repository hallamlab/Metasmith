#!/usr/bin/env python3
"""Reshard N per-assembly ORF fastas into a few pooled shards, reversibly.

    python examples/cyanoverse_reshard_orfs.py --orfs-dir <in> --output <out>
    python examples/cyanoverse_reshard_orfs.py --orfs-dir <in> --output <out> --dry-run

The Cyanoverse corpus is 2,844 per-assembly ORF fastas
(`v2/orfs_and_metabuli/sequences-open_reading_frames/<SRA_ID>.faa` on the
Projects Globus collection). `fabfos.pipelines.annotation` will happily take all
2,844 as samples -- metasmith fans out over them -- but each sample pays a
`kofamscan` and a `diamond` startup against a 17 GB UniRef50 database, and at
2,844 samples that startup cost, not the alignment, is the run. Pooling into a
few big shards amortises it.

WHAT MAKES POOLING SAFE IS THE PREFIX, AND NOTHING ELSE. Once two assemblies'
ORFs share a fasta, the only thing standing between "per-assembly GPR tables"
and "one undifferentiated table" is that every record still says which assembly
it came from. So every header is rewritten `>{sample}::{original}` and the pair
is also written to a manifest. Two independent assemblies genuinely do emit the
same ORF id -- `k141_1_1` is not distinctive -- so without the prefix the rows
do not merely lose their labels, they silently *collide*, and the collision
looks like a normal table. The prefix is the whole design; the shard count is
just arithmetic.

`::` is the delimiter because sample ids here are SRA accessions (`DRR001142`)
and cannot contain it; the script refuses any sample id that does, rather than
producing a manifest whose first field is ambiguous.

SHARD SIZE IS AN ESTIMATE, NOT A MEASUREMENT. The default targets 100,000 ORFs
per shard, reasoned from the one prior real GPR run -- 183 fosmid inserts pooled
into a single ~69k-ORF shard, which completed as one job.

Scale, measured off the staged corpus (2,844 files, 30.61 GB) rather than
guessed: at ~517 B/ORF -- calibrated on the three pinned E. coli proteomes, and
an OVER-estimate of per-record size since prodigal headers are shorter than
NCBI's -- that is **~59 million ORFs**, so the 100k default implies ~590 shards.
Fewer, larger shards (250k -> ~240, 500k -> ~120) trade array width for
per-shard wall-clock. Nobody has timed a shard of this shape against UniRef50;
re-tune once the first shard's wall-clock is known, and run `--dry-run` first,
which reports the real packing from actual record counts rather than this
byte-based estimate.

The corpus is very unevenly sized -- median 2.4 MB but max 492 MB, ~200x -- so
one assembly alone exceeds any sane per-shard target. `pack_by_size` gives such
an assembly its own shard rather than splitting it.

Bin-packing is by ORF count, largest-first, and assemblies are never split
across shards -- an assembly split in two would have its GPR rows produced by
two independent lane runs, which is a different measurement, not a
rearrangement of the same one.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

DELIM = "::"
DEFAULT_ORFS_PER_SHARD = 100_000
ORFS_GLOB = "*.faa"


def count_records(path: Path) -> int:
    """Number of fasta records, counted by '>' at line start, streaming."""
    n = 0
    with path.open("rb") as fh:
        for line in fh:
            if line.startswith(b">"):
                n += 1
    return n


def survey(orfs_dir: Path) -> list[tuple[str, Path, int]]:
    """(sample, path, n_orfs) per input fasta, refusing anything unusable."""
    files = sorted(orfs_dir.glob(ORFS_GLOB))
    if not files:
        raise SystemExit(f"no {ORFS_GLOB} under {orfs_dir}")

    out: list[tuple[str, Path, int]] = []
    seen: set[str] = set()
    for f in files:
        sample = f.name[: -len(".faa")] if f.name.endswith(".faa") else f.stem
        if DELIM in sample:
            raise SystemExit(f"sample id contains the {DELIM!r} delimiter, cannot prefix safely: {f}")
        if sample in seen:
            raise SystemExit(f"duplicate sample id {sample!r} from {f}")
        seen.add(sample)
        n = count_records(f)
        if n == 0:
            print(f"  WARNING empty (0 records), still carried: {f.name}", file=sys.stderr)
        out.append((sample, f, n))
    return out


def pack_by_size(items: list[tuple[str, Path, int]], per_shard: int) -> list[list[tuple[str, Path, int]]]:
    """Largest-first bin-packing into shards of AT MOST per_shard ORFs.

    The shard count falls out of the cap and is not chosen. An assembly larger
    than per_shard gets a shard to itself rather than being split; see the
    module docstring on why splitting is not a rearrangement.
    """
    shards: list[list[tuple[str, Path, int]]] = []
    loads: list[int] = []
    for item in sorted(items, key=lambda t: t[2], reverse=True):
        target = None
        for i, load in enumerate(loads):
            if load + item[2] <= per_shard:
                target = i
                break
        if target is None:
            shards.append([item])
            loads.append(item[2])
        else:
            shards[target].append(item)
            loads[target] += item[2]
    return shards


def pack_into_n(items: list[tuple[str, Path, int]], n: int) -> list[list[tuple[str, Path, int]]]:
    """Longest-processing-time packing into EXACTLY n shards.

    Separate from `pack_by_size` because a cap and a count are different asks
    and one cannot serve both: capping at ceil(total/n) overflows to n+1 bins
    the moment two large assemblies will not share one (measured -- 3 E. coli
    proteomes, --shards 2, gave 3). Assigning each assembly largest-first to
    the currently emptiest shard honours n exactly and balances as a side
    effect.
    """
    if n < 1:
        raise SystemExit("--shards must be >= 1")
    if n > len(items):
        raise SystemExit(f"--shards {n} exceeds the {len(items)} assemblies available")
    shards: list[list[tuple[str, Path, int]]] = [[] for _ in range(n)]
    loads = [0] * n
    for item in sorted(items, key=lambda t: t[2], reverse=True):
        i = loads.index(min(loads))
        shards[i].append(item)
        loads[i] += item[2]
    return shards


def write_shard(shard: list[tuple[str, Path, int]], out_fasta: Path, manifest_rows: list[str]) -> int:
    """Concatenate a shard's fastas, prefixing every header. Returns records written."""
    written = 0
    with out_fasta.open("w") as out:
        for sample, path, _ in shard:
            with path.open("r") as fh:
                for line in fh:
                    if line.startswith(">"):
                        original = line[1:].rstrip("\n")
                        orf_id = original.split()[0] if original.split() else ""
                        out.write(f">{sample}{DELIM}{original}\n")
                        manifest_rows.append(f"{sample}{DELIM}{orf_id}\t{sample}\t{orf_id}\t{out_fasta.name}")
                        written += 1
                    else:
                        out.write(line)
    return written


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--orfs-dir", required=True, help=f"directory of per-assembly ORF fastas ({ORFS_GLOB})")
    p.add_argument("--output", required=True, help="directory to write shards/ + manifest.tsv into")
    p.add_argument("--orfs-per-shard", type=int, default=DEFAULT_ORFS_PER_SHARD,
                    help=f"target ORFs per shard (default {DEFAULT_ORFS_PER_SHARD}; an estimate, see docstring)")
    p.add_argument("--shards", type=int, default=None,
                    help="pack into exactly this many shards instead (overrides --orfs-per-shard)")
    p.add_argument("--dry-run", action="store_true", help="survey and report the packing, write nothing")
    a = p.parse_args(argv)

    orfs_dir = Path(a.orfs_dir).expanduser().resolve()
    output = Path(a.output).expanduser().resolve()

    print(f"=== surveying {orfs_dir} ===")
    items = survey(orfs_dir)
    total = sum(n for _, _, n in items)
    print(f"{len(items)} assemblies, {total:,} ORFs total")
    if not total:
        raise SystemExit("every input fasta is empty; nothing to shard")

    if a.shards:
        shards = pack_into_n(items, a.shards)
        how = f"exactly {a.shards} shard(s)"
    else:
        shards = pack_by_size(items, a.orfs_per_shard)
        how = f"shards of at most {a.orfs_per_shard:,} ORFs"
    loads = [sum(n for _, _, n in s) for s in shards]
    print(f"packing into {len(shards)} shard(s) -- {how}")
    print(f"  assemblies/shard  min {min(len(s) for s in shards)}  max {max(len(s) for s in shards)}")
    print(f"  ORFs/shard        min {min(loads):,}  max {max(loads):,}")

    if a.dry_run:
        print("\n--dry-run: nothing written")
        return 0

    shard_dir = output / "shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    manifest_rows: list[str] = []
    written_total = 0
    for i, shard in enumerate(shards):
        out_fasta = shard_dir / f"shard_{i:04d}.faa"
        written_total += write_shard(shard, out_fasta, manifest_rows)
        print(f"  wrote {out_fasta.name}  {len(shard)} assemblies  {loads[i]:,} ORFs")

    manifest = output / "manifest.tsv"
    with manifest.open("w") as fh:
        fh.write("prefixed_id\tsample\torf_id\tshard\n")
        fh.write("\n".join(manifest_rows) + "\n")

    # the survey counted headers; the writer re-counted them as it copied. If
    # those disagree, a file changed underneath us mid-run and the manifest no
    # longer describes the shards -- which is exactly the silent-wrong-table
    # failure this script exists to prevent.
    if written_total != total:
        raise SystemExit(
            f"REFUSING: surveyed {total:,} ORFs but wrote {written_total:,}. "
            "Inputs changed during the run; shards and manifest disagree."
        )

    print(f"\n{len(shards)} shards -> {shard_dir}")
    print(f"manifest ({len(manifest_rows):,} rows) -> {manifest}")
    print(f"\nnext: python -m fabfos.pipelines.annotation --orfs-dir {shard_dir} --output <out>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
