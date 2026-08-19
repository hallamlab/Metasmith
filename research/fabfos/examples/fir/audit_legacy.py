#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

import pyarrow.parquet as pq

ORFS = Path(sys.argv[1])
ANNOT1 = Path(sys.argv[2])
OUT = Path(sys.argv[3])
SHARD_I = int(os.environ.get("SLURM_ARRAY_TASK_ID", "0"))
SHARD_N = int(os.environ.get("SHARD_N", "1"))

KOFAM = ANNOT1 / "annotation-kofamscan_results"
UNIREF = ANNOT1 / "annotation-diamond_uniref50_results"
PBERT = ANNOT1 / "annotation-proteinbert_embeddings"

KOFAM_HEADER = "gene_name,KO,thrshld,score,E-value,best"
UNIREF_NCOL_LEGACY = 12
UNIREF_NCOL_CURRENT = 14
PBERT_DIMS = 512

COLS = ["sample", "faa_records", "pbert_rows", "pbert_dims", "kofam_header_ok",
        "kofam_rows", "uniref_ncol", "verdict"]


def faa_records(p: Path) -> int:
    n = 0
    with p.open("rb") as fh:
        for line in fh:
            if line[:1] == b">":
                n += 1
    return n


def line_count(p: Path) -> int:
    n = 0
    with p.open("rb") as fh:
        for _ in fh:
            n += 1
    return n


def first_line(p: Path) -> str:
    with p.open("r", errors="replace") as fh:
        return fh.readline().rstrip("\n")


def audit(sample: str) -> dict:
    r = {c: "" for c in COLS}
    r["sample"] = sample
    problems = []

    faa = ORFS / f"{sample}.faa"
    n_faa = faa_records(faa)
    r["faa_records"] = n_faa

    pb = PBERT / f"{sample}.parquet"
    if not pb.exists():
        problems.append("pbert_missing")
    else:
        md = pq.ParquetFile(pb)
        r["pbert_rows"] = md.metadata.num_rows
        dims = [n for n in md.schema_arrow.names if n.startswith("dim_")]
        r["pbert_dims"] = len(dims)
        if md.metadata.num_rows != n_faa:
            problems.append(f"pbert_rows{md.metadata.num_rows}!=faa{n_faa}")
        if len(dims) != PBERT_DIMS:
            problems.append(f"pbert_dims={len(dims)}")

    kf = KOFAM / f"{sample}.txt"
    if not kf.exists():
        problems.append("kofam_missing")
    else:
        head = first_line(kf)
        ok = head.strip() == KOFAM_HEADER
        r["kofam_header_ok"] = int(ok)
        r["kofam_rows"] = max(0, line_count(kf) - 1)
        if not ok:
            problems.append("kofam_header")

    un = UNIREF / f"{sample}.tsv"
    if not un.exists():
        problems.append("uniref_missing")
    else:
        head = first_line(un)
        ncol = len(head.split("\t")) if head else 0
        r["uniref_ncol"] = ncol
        if ncol not in (0, UNIREF_NCOL_LEGACY, UNIREF_NCOL_CURRENT):
            problems.append(f"uniref_ncol={ncol}")

    r["verdict"] = "OK" if not problems else ";".join(problems)
    return r


def main() -> int:
    samples = sorted(p.stem for p in ORFS.glob("*.faa"))
    mine = samples[SHARD_I::SHARD_N]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out = OUT.parent / f"{OUT.name}.{SHARD_I:03d}.tsv"

    done = set()
    if out.exists():
        with out.open() as fh:
            for line in fh:
                f = line.split("\t")
                if f and f[0] not in ("sample",):
                    done.add(f[0])
    fresh = not out.exists()
    with out.open("a") as fh:
        if fresh:
            fh.write("\t".join(COLS) + "\n")
            fh.flush()
        for i, s in enumerate(mine):
            if s in done:
                continue
            try:
                r = audit(s)
            except Exception as e:                    # noqa: BLE001
                r = {c: "" for c in COLS}
                r["sample"] = s
                r["verdict"] = f"ERROR:{type(e).__name__}:{e}"
            fh.write("\t".join(str(r[c]) for c in COLS) + "\n")
            fh.flush()
            if i % 25 == 0:
                print(f"[{SHARD_I}] {i}/{len(mine)} {s} {r['verdict']}", flush=True)
    print(f"[{SHARD_I}] done: {len(mine)} sample(s) -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
