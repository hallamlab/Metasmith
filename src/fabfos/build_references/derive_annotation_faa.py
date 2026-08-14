#!/usr/bin/env python3
"""Give every host `annotations/` the ORF fasta its own lane tables are keyed on.

    python build_references/derive_annotation_faa.py            # build what is missing, verify all
    python build_references/derive_annotation_faa.py --check     # verify only, write nothing

An `annotations/<host>/` directory ships four lane tables -- clean, kofam, uniref50,
proteinbert -- all keyed on one ORF set. Without that ORF set beside them the tables
join to nothing: the id space is the only thing tying a row back to a sequence, and
each host's is different (`ECDH10B_0001`, `C1_1`, `NP_414542.1`).

THE BAR IS TOTAL COVERAGE, and it is measured rather than asserted: the union of ORF
ids over every lane table must be a SUBSET of the ids in the `.faa`. A partial result
is refused, because a `.faa` covering 97% of a lane looks exactly like a correct one
until someone counts the rows that silently dropped out of a join.

THE `.faa` IS NOT INTERCHANGEABLE WITH THE PUBLISHED PROTEOME. `originals/genomes/`
holds each host's current NCBI annotation; the lanes were produced by the deployed
method over a DIFFERENT ORF call. Their id spaces overlap by 0% for dh10b and epi300 --
so copying the original over is not a fix, it is a file that joins to nothing. For
dh10b it would be destructive: that host already ships the file that generated its
lanes, covering them 4,126/4,126, and this script leaves it untouched.

WHERE EACH HOST'S FILE COMES FROM
---------------------------------
  dh10b   already present -- the generating file. Verified, never rewritten.
  epi300  LIFTED from the MetaPathways run under `annotation_alts/raw/`, which is
          literally the ORF set the epi300 lanes were built from. Its headers are in
          the run's own `epi300-C<n>-G1` space; `preprocessed/epi300.mapping.txt` is
          what carries them back to the `C1_<n>` space the lanes use.
  k12     RECONSTRUCTED from `originals/genomes/.../NC_000913.3.faa`, which is NOT the
          generating file -- no k12 proteome in the lane id space survives anywhere in
          this tree, so one had to be rebuilt. dh10b and epi300 both ship their real
          generating file; k12 does not, and the difference matters when a sequence is
          used to explain a lane's call. The originals have demonstrably moved since
          the lanes ran: the lanes key on `NP_416485.4` and the current proteome
          publishes `NP_416485.5`. Matching is on the versionless accession, so that
          ORF carries the NEWER sequence, not the one the lane scored -- and nothing
          says the other 4,297 are unchanged, only that their version did not move.

Once epi300's file is lifted, `annotation_alts/raw/` is deletable and this script can
no longer rebuild it -- by design. The lifted file is the artifact; the 101 MB run
directory was only ever its container.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
GENOMES = REPO / "data" / "fabfos"
ORIGINALS = REPO / "data" / "originals" / "genomes"

# host -> the prefix its files are named by. `annotations/<prefix>.faa` is the target.
HOSTS = {
    "e_coli_dh10b": "dh10b",
    "e_coli_epi300": "epi300",
    "e_coli_k12": "k12",
}

# Lane tables that carry no ORF id at all, named so that "no ids here" is a declared
# fact rather than an empty read. `kofam_brite` is keyed on the KO, not on the ORF.
NO_ORF_IDS = ("kofam_brite.csv",)


# --------------------------------------------------------------------------- reading

def _rows(path: Path, delim: str):
    """One list of fields per PHYSICAL line.

    Quoting is deliberately off. `k12.clean.tsv` carries rows whose Query ID field
    begins with a stray `"` -- CLEAN echoed a FASTA description containing a comma and
    the writer quoted it, but the quote never closes on that line. Honouring quotes
    makes csv swallow the following lines into one field, and k12's clean lane then
    reports 670 ORFs instead of 4,300. Tokens are unquoted individually below.
    """
    with path.open(newline="") as fh:
        yield from csv.reader(fh, delimiter=delim, quoting=csv.QUOTE_NONE)


def lane_ids(path: Path) -> set[str]:
    """Every ORF id a lane table references.

    THE `contig,orf` SCHEMA IS THE TRAP. `kofam.csv` and `pbert.index.csv` store the id
    SPLIT on its first underscore, so k12's `NP_414542.1` arrives as `NP` + `414542.1`
    and epi300's `C1_7` as `C1` + `7`. Reading column 0 alone yields a two-element set
    -- `{"NP"}`, `{"C1"}` -- and every coverage number computed from it is meaningless
    while looking entirely plausible. The id is `f"{contig}_{orf}"`, and `orf` is kept
    as the literal token: coercing `0002` to an int would give `ECDH10B_2`.
    """
    name = path.name
    if name.endswith(NO_ORF_IDS):
        return set()

    if name.endswith(".detail.txt"):
        # kofamscan's detail format: `# ...` comments, then `[*] <gene> <KO> ...` where
        # the leading `*` marks a hit over threshold and is absent below it.
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
    """The union of ORF ids over every table under `annotations/`, plus the per-table
    counts, so a lane that suddenly contributes nothing is visible instead of absorbed."""
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
    """(header line without `>`, sequence) in file order."""
    records: list[tuple[str, list[str]]] = []
    for line in path.read_text().splitlines():
        if line.startswith(">"):
            records.append((line[1:].strip(), []))
        elif records:
            records[-1][1].append(line.strip())
    return [(h, "".join(s)) for h, s in records]


def write_fasta(path: Path, records: list[tuple[str, str]], width: int = 60) -> None:
    """Write, never open an existing file for writing -- everything already under
    `annotations/` is a read-only hardlink into the shared DVC cache."""
    out = []
    for name, seq in records:
        out.append(f">{name}")
        out.extend(seq[i:i + width] for i in range(0, len(seq), width))
    path.write_text("\n".join(out) + "\n")


# --------------------------------------------------------------------------- builders

def build_epi300() -> list[tuple[str, str]]:
    """Lift the MetaPathways ORF fasta, re-headered into the lane id space.

    `orf_prediction/epi300.faa`, NOT the `epi300.qced.faa` beside it -- the qced file is
    a filtered subset and does not cover the lane union.

    The run treated each ORF as its own contig, so a header is `epi300-C<n>-G1`:
    strip the `-G<n>` gene suffix and the remainder is the mapping file's column 0,
    whose column 1 is the `C1_<n>` id every epi300 lane keys on.
    """
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
    """Reconstruct k12's ORF set from the published proteome, keyed by the lane's id.

    A RECONSTRUCTION, NOT THE GENERATING FILE. Direct evidence that the originals have
    moved since the lanes ran: the lanes carry `NP_416485.4` and the current proteome
    carries `NP_416485.5`, so joining on the bare accession leaves exactly one ORF
    uncovered. The versionless accession closes it -- and means that protein's SEQUENCE
    here may not be the one the lane actually scored.

    ONLY WHAT THE LANES REFERENCE IS WRITTEN. The proteome carries 4,318 records against
    a 4,298 lane union; shipping the extra 20 would make the file a superset rather than
    the ORF set, and every "is this ORF annotated?" question then has two answers.
    """
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



# --------------------------------------------------------------------------- driver

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
