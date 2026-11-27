#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

COLUMNS = [
    "virus_id", "evidence", "sample", "mag",
    "host_contig", "host_contig_length", "host_start", "host_end",
    "virus_start", "virus_end",
    "spacer_id", "spacer_hits", "spacer_length", "alignment_length", "mismatches",
    "effective_mismatches", "percent_identity", "evalue", "self_hit",
]
NA = "NA"


def split_source_contig(source_contig: str) -> tuple[str, str]:
    stem, sep, contig = source_contig.partition("~")
    assert sep, (
        f"source contig [{source_contig}] has no `<assembly stem>~` prefix; "
        "splitContigsForAmr writes one on every contig batch header"
    )
    return stem, contig


def fasta_lengths(path: Path) -> dict[str, int]:
    lengths, name = {}, None
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                name = line[1:].split()[0]
                lengths[name] = 0
            elif name is not None:
                lengths[name] += len(line.strip())
    return lengths


def read_tsv(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def read_arrays(path: Path) -> dict[str, tuple[str, int, int]]:
    rows = read_tsv(path) if Path(path).stat().st_size else []
    return {r["CRISPR"]: (r["Contig"], int(r["Start"]), int(r["End"])) for r in rows}


def _overlaps(a_start, a_end, b_start, b_end) -> bool:
    return a_start <= b_end and b_start <= a_end


def provirus_links(provenance: list[dict], mags: dict[str, dict]) -> list[dict]:
    by_contig = {}
    for mag, m in mags.items():
        for contig, length in m["contigs"].items():
            by_contig[(m["sample"], m["assembly"], contig)] = (mag, length)

    rows = []
    for p in provenance:
        stem, contig = split_source_contig(p["source_contig"])
        hit = by_contig.get((p["sample"], stem, contig))
        if hit is None:
            continue
        mag, length = hit
        start, end = int(p["start"]), int(p["end"])
        whole = start <= 1 and end >= length
        rows.append({
            "virus_id": p["frozen_id"],
            "evidence": "binned_viral_contig" if whole else "provirus",
            "sample": p["sample"], "mag": mag,
            "host_contig": contig, "host_contig_length": length,
            "host_start": start, "host_end": end,
            "virus_start": 1, "virus_end": end - start + 1,
        })
    return rows


def spacer_links(hits: list[dict], provenance: list[dict], mags: dict[str, dict],
                 arrays: dict[str, dict]) -> list[dict]:
    origin = {}
    for p in provenance:
        stem, contig = split_source_contig(p["source_contig"])
        origin[p["frozen_id"]] = (p["sample"], stem, contig, int(p["start"]), int(p["end"]))

    best = {}
    for h in hits:
        # cctyper.py tags every spacer header `<bin stem>|<array>|<spacer>`.
        mag, array, _ = h["spacer_id"].split("|", 2)
        m = mags.get(mag)
        if m is None:
            continue
        contig, a_start, a_end = arrays.get(mag, {}).get(array, (NA, None, None))
        virus = h["contig_id"]
        spacer_len, aln_len, mm = int(h["spacer_length"]), int(h["alignment_length"]), int(h["mismatches"])
        # The search is ungapped, so a mismatch at a spacer end trims the alignment
        # rather than counting as a mismatch; the trimmed bases are mismatches over
        # the full spacer length.
        effective = mm + spacer_len - aln_len

        o = origin.get(virus)
        self_hit = (
            o is not None and a_start is not None
            and (o[0], o[1], o[2]) == (m["sample"], m["assembly"], contig)
            and _overlaps(o[3], o[4], a_start, a_end)
        )
        s_start, s_end = sorted((int(h["contig_start"]), int(h["contig_end"])))
        row = {
            "virus_id": virus, "evidence": "spacer", "sample": m["sample"], "mag": mag,
            "host_contig": contig, "host_contig_length": m["contigs"].get(contig, NA),
            "host_start": NA if a_start is None else a_start,
            "host_end": NA if a_end is None else a_end,
            "virus_start": s_start, "virus_end": s_end,
            "spacer_id": h["spacer_id"], "spacer_length": spacer_len,
            "alignment_length": aln_len, "mismatches": mm, "effective_mismatches": effective,
            "percent_identity": h["percent_identity"], "evalue": h["evalue"],
            "self_hit": str(self_hit).lower(),
        }
        rank = (self_hit, effective, -aln_len, float(h["evalue"]))
        key = (virus, mag)
        prev = best.get(key)
        spacers = (prev[2] if prev else set()) | {h["spacer_id"]}
        if prev is None or rank < prev[0]:
            best[key] = (rank, row, spacers)
        else:
            best[key] = (prev[0], prev[1], spacers)

    rows = []
    for _, row, spacers in best.values():
        rows.append(row | {"spacer_hits": len(spacers)})
    return rows


def write(rows: list[dict], out: Path) -> None:
    rows = sorted(rows, key=lambda r: (r["virus_id"], r["mag"], r["evidence"]))
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, delimiter="\t", restval=NA, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--provenance", type=Path, required=True)
    ap.add_argument("--spacer-hits", type=Path, required=True)
    ap.add_argument("--mags", type=Path, required=True,
                    help="TSV: mag, sample, assembly, fasta, arrays")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    mags, arrays = {}, {}
    for r in read_tsv(a.mags):
        mags[r["mag"]] = {"sample": r["sample"], "assembly": r["assembly"],
                          "contigs": fasta_lengths(Path(r["fasta"]))}
        arrays[r["mag"]] = read_arrays(Path(r["arrays"])) if r["arrays"] != NA else {}

    provenance = read_tsv(a.provenance)
    stems = {(m["sample"], m["assembly"]) for m in mags.values()}
    crossed = sum(1 for p in provenance
                  if any(s == p["sample"] for s, _ in stems)
                  and (p["sample"], split_source_contig(p["source_contig"])[0]) not in stems)
    if crossed:
        print(f"WARNING: {crossed} frozen viruses come from an assembly other than their "
              "sample's MAGs' and cannot be placed on a MAG contig", flush=True)

    rows = provirus_links(provenance, mags) + spacer_links(read_tsv(a.spacer_hits), provenance, mags, arrays)
    write(rows, a.out)
    print(f"{len(rows)} virus-host links over {len(mags)} MAGs", flush=True)


if __name__ == "__main__":
    main()
