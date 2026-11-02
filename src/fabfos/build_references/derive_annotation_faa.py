#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
GENOMES = REPO / "data" / "fabfos" / "runs"
ORIGINALS = REPO / "data" / "fabfos" / "originals" / "genomes"

HOSTS = {
    "e_coli_dh10b": "dh10b",
    "e_coli_epi300": "epi300",
    "e_coli_k12": "k12",
}

NO_ORF_IDS = ("kofam_brite.csv",)


def _rows(path: Path, delim: str):
    with path.open(newline="") as fh:
        yield from csv.reader(fh, delimiter=delim, quoting=csv.QUOTE_NONE)


def lane_ids(path: Path) -> set[str]:
    name = path.name
    if name.endswith(NO_ORF_IDS):
        return set()

    if name.endswith(".detail.txt"):
        ids = set()
        for line in path.read_text().splitlines():
            if line.startswith("#") or not line.strip():
                continue
            fields = line.split()
            if fields[0] == "*":
                fields = fields[1:]
            if fields:
                ids.add(fields[0].strip('"'))
        return ids

    if name.endswith(".blast6.tsv"):
        return {r[0].strip('"') for r in _rows(path, "\t") if r}

    delim = "\t" if path.suffix == ".tsv" else ","
    rows = _rows(path, delim)
    header = next(rows, None)
    if header is None:
        return set()
    cols = [h.strip().lower() for h in header]

    if "contig" in cols and "orf" in cols:
        c, o = cols.index("contig"), cols.index("orf")
        return {f"{r[c].strip()}_{r[o].strip()}".strip('"') for r in rows if len(r) > max(c, o)}
    for key in ("query id", "orf", "sequence_id"):
        if key in cols:
            i = cols.index(key)
            return {r[i].strip().strip('"') for r in rows if len(r) > i and r[i].strip()}
    raise SystemExit(f"{path}: no ORF-id column in header {header!r} -- teach this "
                     f"reader about it rather than letting it read zero ids")


def union_for(host: str) -> tuple[set[str], list[tuple[Path, int]]]:
    root = GENOMES / host / "annotations"
    union: set[str] = set()
    per_table = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix in (".faa", ".npy", ".parquet", ".md"):
            continue
        ids = lane_ids(path)
        per_table.append((path.relative_to(root), len(ids)))
        union |= ids
    return union, per_table


def read_fasta(path: Path) -> list[tuple[str, str]]:
    records: list[tuple[str, list[str]]] = []
    for line in path.read_text().splitlines():
        if line.startswith(">"):
            records.append((line[1:].strip(), []))
        elif records:
            records[-1][1].append(line.strip())
    return [(h, "".join(s)) for h, s in records]


def write_fasta(path: Path, records: list[tuple[str, str]], width: int = 60) -> None:
    out = []
    for name, seq in records:
        out.append(f">{name}")
        out.extend(seq[i:i + width] for i in range(0, len(seq), width))
    path.write_text("\n".join(out) + "\n")


def build_epi300() -> list[tuple[str, str]]:
    run = (GENOMES / "e_coli_epi300" / "annotation_alts" / "raw"
           / "52c.ecoli_mp3.2024-09-27-18-24" / "epi300" / "epi300")
    faa = run / "orf_prediction" / "epi300.faa"
    mapping = run / "preprocessed" / "epi300.mapping.txt"
    if not (faa.exists() and mapping.exists()):
        raise SystemExit(
            f"epi300's generating ORF fasta is gone ({faa}). It was lifted into "
            f"`annotations/epi300.faa` and the run directory deleted; restore it from "
            f"the retired `data/processed/genomes.dvc` pin (git commit `9f18c1f`) "
            f"to rebuild.")

    rename = {}
    for row in _rows(mapping, "\t"):
        if len(row) >= 2:
            rename[row[0].strip()] = row[1].strip()

    out, unmapped = [], []
    for header, seq in read_fasta(faa):
        token = header.split()[0]
        contig = token.rsplit("-G", 1)[0]
        lane_id = rename.get(contig)
        if lane_id is None:
            unmapped.append(token)
        else:
            out.append((lane_id, seq))
    if unmapped:
        raise SystemExit(f"{len(unmapped)} epi300 ORF headers have no row in "
                         f"{mapping.name}: {unmapped[:5]}")
    return out


def build_k12(union: set[str]) -> list[tuple[str, str]]:
    faa = ORIGINALS / "e_coli_k12" / "genome" / "NC_000913.3.faa"
    by_versionless = {u.rsplit(".", 1)[0]: u for u in union}
    if len(by_versionless) != len(union):
        raise SystemExit("two k12 lane ids share a versionless accession; the join "
                         "below would silently pick one of them")

    out, seen = [], set()
    for header, seq in read_fasta(faa):
        acc = None
        for field in header.split("["):
            if field.startswith("protein_id="):
                acc = field.split("=", 1)[1].rstrip("] ")
                break
        if acc is None:
            continue
        lane_id = by_versionless.get(acc.rsplit(".", 1)[0])
        if lane_id is None or lane_id in seen:
            continue
        seen.add(lane_id)
        out.append((lane_id, seq))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="verify coverage and write nothing")
    ap.add_argument("--verbose", action="store_true",
                    help="print each lane table's id count")
    a = ap.parse_args()

    problems = []
    for host, prefix in HOSTS.items():
        target = GENOMES / host / "annotations" / f"{prefix}.faa"
        union, per_table = union_for(host)
        print(f"\n{host}  ({prefix}.faa)")
        if a.verbose:
            for rel, n in per_table:
                print(f"    {n:>6,}  {rel}")
        print(f"  lane union: {len(union):,} ORF ids over "
              f"{sum(1 for _, n in per_table if n):,} tables")

        if target.exists():
            action = "present"
            records = read_fasta(target)
        elif a.check:
            problems.append(f"{host}: {target.relative_to(REPO)} is absent")
            print("  ABSENT -- --check writes nothing")
            continue
        else:
            records = build_epi300() if host == "e_coli_epi300" else build_k12(union)
            action = "built"

        ids = {h.split()[0] for h, _ in records}
        if len(ids) != len(records):
            problems.append(f"{host}: {len(records) - len(ids)} duplicate ids in the .faa")
        covered = len(union & ids)
        pct = 100.0 * covered / len(union) if union else 0.0
        print(f"  {action}: {len(records):,} sequences; covers {covered:,}/{len(union):,} "
              f"({pct:.2f}%), {len(ids - union):,} not referenced by any lane")

        if covered != len(union):
            missing = sorted(union - ids)
            problems.append(f"{host}: {len(missing):,} lane ORFs are not in the .faa, "
                            f"e.g. {missing[:5]}")
            if action == "built":
                print("  REFUSED to write -- coverage is not total")
            continue

        if action == "built" and not a.check:
            write_fasta(target, records)
            print(f"  wrote {target.relative_to(REPO)}")

    print()
    for p in problems:
        print(f"FAIL: {p}")
    if not problems:
        print("every host's annotations/ carries a .faa accounting for 100% of its "
              "lane ORFs")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
