#!/usr/bin/env python3
"""Reformat a raw UniRef90 blast6+stitle+bsr table into the bsr-csv schema.

    python build_references/derive_uniref90_bsr_csv.py IN.blast6.tsv OUT.csv \
        --id-column contig --split last-underscore
    python build_references/derive_uniref90_bsr_csv.py --selftest METAG.blast6.tsv \
        data/fabfos/runs/scadc_metagenome/annotation_alts/uniref90/metag.uniref90.csv

A `uniref90/` under `data/fabfos/runs/<run>/annotation_alts/` ships either form: the
7-column table `<id>,orf,ref_id,description,bsr,evalue,percent_identity`, or the
14-column NCBI blast6 with `stitle` and a trailing BSR it is reformatted from. The
metagenome copy is the 7-column form, dh10b's is the blast6, and this script is the
bridge. `--selftest` is what makes it trustworthy: it re-derives a shipped table from
its own blast6 and refuses unless the result matches byte for byte.

That is also why the two forms are not both kept. The host's 7-column copy was deleted
in favour of its blast6 precisely because this script reproduces it exactly -- one file
plus a checkable derivation beats two files that can drift.

WHAT THE REFORMAT IS
--------------------
  qseqid   split on the last separator into (id, orf)
  sseqid   drop the `UniRefNN_` prefix                     -> ref_id
  stitle   drop the leading `UniRefNN_ID` token            -> description
  pident, evalue, bsr                                       verbatim

TWO THINGS IT DELIBERATELY DOES NOT DO
--------------------------------------
`lib::fabfos_evidence.read_uniref50` performs the same steps and then two more,
because it is feeding the GPR mapper rather than writing a benchmark table:

  * **no best-hit dedup.** That function keeps one row per ORF; the shipped tables
    keep every hit (`metag.uniref90.csv` carries 6,029,338 rows over far fewer ORFs).
    Deduping here would silently discard ~90% of a table whose whole purpose is to be
    the fuller record.
  * **no `_BOILERPLATE_RE`.** That function strips the `n=`/`Tax=`/`RepID=` tail out
    of the description; the shipped tables retain it. Stripping would produce a table
    that looks right and does not match.

Both are verified by `--selftest`, not asserted here.

THE SEPARATOR IS PER-COHORT AND THERE IS NO SAFE DEFAULT
--------------------------------------------------------
The three cohorts key their ORFs differently, and each lane in a cohort agrees with
its siblings:

  metagenome  `k141_657858-20`   contig `k141_657858`, orf `20`   -- last `-`
  fosmids     `C096_21`          fosmid `C096`,        orf `21`   -- last `_`
  host        `ECDH10B_0002`     contig `ECDH10B`,     orf `0002` -- last `_`

The host case is the one to be careful with. `ECDH10B` is a locus-tag prefix, not a
contig -- DH10b is a single replicon, so that column is constant -- and the ordinal is
ZERO-PADDED. `orf` is written as the literal token, never coerced to int, so
`f"{id}_{orf}"` reconstructs the id every other host lane keys on
(`dh10b.deepec.tsv`, `dh10b.esmc.index.csv`). Coercing would give `ECDH10B_2` and
every join against those would come back empty.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

BLAST6_BSR_COLS = [
    "qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
    "qstart", "qend", "sstart", "send", "evalue", "bitscore", "stitle", "bsr",
]

SPLITTERS = {"last-dash": "-", "last-underscore": "_"}

_UNIREF_PREFIX = re.compile(r"^UniRef\d+_")


def clean_stitle(stitle: str) -> str:
    """Drop the leading `UniRefNN_ID` token, keep everything after it.

    Not a regex on the accession: `stitle` is `<sseqid> <description>` verbatim out
    of DIAMOND, so the token to remove is exactly the first whitespace-delimited
    field. Matching on `UniRef\\d+_\\S+` instead would also eat a description that
    happens to start with one.
    """
    _, _, rest = stitle.partition(" ")
    return rest


def derive(src: Path, sep: str, id_column: str) -> tuple[list[str], list[list[str]]]:
    header = [id_column, "orf", "ref_id", "description", "bsr", "evalue",
              "percent_identity"]
    rows: list[list[str]] = []
    unsplit = 0
    with src.open(newline="") as fh:
        for lineno, line in enumerate(csv.reader(fh, delimiter="\t",
                                                 quoting=csv.QUOTE_NONE), 1):
            if len(line) != len(BLAST6_BSR_COLS):
                raise SystemExit(
                    f"{src}:{lineno}: {len(line)} columns, expected "
                    f"{len(BLAST6_BSR_COLS)}. This reformat wants blast6 WITH stitle "
                    f"and a trailing BSR; a 12-column table is the plain outfmt 6 and "
                    f"carries neither.")
            r = dict(zip(BLAST6_BSR_COLS, line))
            ident, found, orf = r["qseqid"].rpartition(sep)
            if not found:
                # Not fatal on its own -- report the count and let the caller judge,
                # because a wholly unsplit column is a wrong `--split`, while one or
                # two is a genuinely odd id.
                unsplit += 1
                ident, orf = r["qseqid"], ""
            rows.append([
                ident, orf,
                _UNIREF_PREFIX.sub("", r["sseqid"]),
                clean_stitle(r["stitle"]),
                r["bsr"], r["evalue"], r["pident"],
            ])
    if unsplit:
        print(f"  WARNING: {unsplit} of {len(rows)} qseqids contain no {sep!r} and "
              f"were left whole with an empty orf. Wrong --split?", file=sys.stderr)
    return header, rows


def write(dest: Path, header: list[str], rows: list[list[str]]) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def selftest(src: Path, shipped: Path, sep: str, id_column: str) -> int:
    """Re-derive a shipped table from its own source and diff. The whole warrant."""
    header, rows = derive(src, sep, id_column)
    import io
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    got = buf.getvalue()
    want = shipped.read_text()
    if got == want:
        print(f"SELFTEST OK: {len(rows):,} rows re-derived from {src.name} match "
              f"{shipped.name} exactly")
        return 0
    g, wl = got.splitlines(), want.splitlines()
    print(f"SELFTEST FAILED: {len(g):,} derived rows vs {len(wl):,} shipped",
          file=sys.stderr)
    for i, (a, b) in enumerate(zip(g, wl)):
        if a != b:
            print(f"  first difference at line {i + 1}:\n    derived: {a}\n"
                  f"    shipped: {b}", file=sys.stderr)
            break
    return 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("source", type=Path, help="the 14-column blast6+stitle+bsr table")
    ap.add_argument("dest", type=Path,
                    help="the bsr-csv to write, or -- with --selftest -- the shipped "
                         "table to compare against")
    ap.add_argument("--split", choices=sorted(SPLITTERS), required=True,
                    help="how qseqid splits into (id, orf). Per-cohort; see the "
                         "module docstring.")
    ap.add_argument("--id-column", default="contig",
                    help="name of the first column: 'contig' for metagenome and host, "
                         "'fosmid' for fosmids")
    ap.add_argument("--selftest", action="store_true",
                    help="re-derive `dest` from `source` and refuse unless it matches")
    a = ap.parse_args()

    sep = SPLITTERS[a.split]
    if not a.source.exists():
        raise SystemExit(f"no source table at {a.source}")
    if a.selftest:
        return selftest(a.source, a.dest, sep, a.id_column)

    header, rows = derive(a.source, sep, a.id_column)
    write(a.dest, header, rows)
    n_ids = len({r[0] for r in rows})
    n_orfs = len({(r[0], r[1]) for r in rows})
    print(f"{len(rows):,} hits over {n_orfs:,} ORFs on {n_ids:,} {a.id_column}(s) "
          f"-> {a.dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
