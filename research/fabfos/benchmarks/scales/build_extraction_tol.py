#!/usr/bin/env python3
"""Decode the SCALEs ethanol-tolerance supplement into a benchmark extraction table.

Source
------
`data/fabfos/originals/benchmarks/scales/1-s2.0-S109671761200119X-mmc1.xlsx`,
sheet ``Gene_fitnesses`` (the first worksheet): one row per E. coli gene with a
b-number, a symbol, and the SCALEs gene fitness Wgene under 15 g/L and 30 g/L
exogenous ethanol.

  Woodruff LBA, Pandhal J, Ow SY, Karimpour-Fard A, Weiss SJ, Wright PC, Gill RT.
  "Genome-scale identification and characterization of ethanol tolerance genes in
  Escherichia coli." Metab Eng 15 (2013) 124-133.
  Host E. coli BW25113 delta-recA::FRT; MOPS minimal medium + 2 g/L dextrose, 37 C.

The clone table is transcribed from Table 1 of the main PDF, cross-checked against
Table S1 (primer list) of `...-mmc2.docx`; b-numbers for the clone genes are looked
up in the same workbook, with a small explicit synonym map for the two symbols the
2012 workbook still carries under their y-gene names.

Decoding note: this reads the .xlsx with stdlib zipfile + ElementTree so it has no
environment dependency. Cells carrying ``t="s"`` hold an INTEGER INDEX into
``xl/sharedStrings.xml``, not a string -- reading ``<v>`` naively yields integers
where gene symbols belong. ``t="inlineStr"`` is handled too (absent in this file).

Refusal check
-------------
The paper states (Section 3.2): "only 158 or 487 of the ~4300 genes in E. coli
[were] enriched (Wgene > 1) in the 15 g/L and 30 g/L selection, respectively."
This script recomputes both counts from the workbook and exits non-zero unless
both reproduce exactly. The threshold is strict ``> 1`` and is not tunable.
"""

from __future__ import annotations

import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

DATASET = "scales_tol"
SHEET = "xl/worksheets/sheet1.xml"  # workbook.xml: sheetId 1, name "Gene_fitnesses"
SOURCE_TABLE = "mmc1.xlsx!Gene_fitnesses"

# The paper's own arithmetic. Not a tuning knob.
EXPECT_15 = 158
EXPECT_30 = 487

# Table 1 of the main PDF, verified line-for-line against Table S1 of mmc2.docx.
# The nine confirmed clones are rows 1-9 and the five unconfirmed are rows 10-14;
# see `_CONFIRMED_EVIDENCE` for how that partition is pinned by the text.
CLONE_TABLE = [
    (1, ["lpcA"], "D-sedoheptulose 7-phosphate isomerase"),
    (2, ["tilS"], "tRNA(Ile)-lysidine synthetase"),
    (3, ["yhfT", "yhfU"], "predicted inner membrane protein; predicted protein"),
    (4, ["fadE"], "acyl-CoA dehydrogenase"),
    (5, ["serA"], "a-ketoglutarate reductase / D-3-phosphoglycerate dehydrogenase"),
    (6, ["zur"], "Zur transcriptional repressor"),
    (7, ["arnC", "arnB"], "undecaprenyl phosphate-L-Ara4FN transferase; UDP-L-Ara4O C-4 transaminase"),
    (8, ["yicE"], "YicE NCS2 transporter"),
    (9, ["yaeQ", "yaeJ"], "conserved protein; peptidyl-tRNA hydrolase"),
    (10, ["yafK"], "conserved protein"),
    (11, ["rof", "yaeP"], "modulator of Rho-dependent termination; conserved protein"),
    (12, ["yafJ"], "predicted amidotransferase"),
    (13, ["trmA"], "tRNA(m5U54) methyltransferase"),
    (14, ["yijD", "fabR"], "conserved inner membrane protein; FabR repressor"),
]

CONFIRMED_CLONES = {1, 2, 3, 4, 5, 6, 7, 8, 9}

_CONFIRMED_EVIDENCE = {
    1: "named tolerant; largest effect (5.9-fold); one of the 5 profiled clones",
    2: "named tolerant (translation: tilS, yaeJ); one of the 5 profiled clones",
    3: "one of the 5 most improved clones sent for transcriptional profiling (yhfUT)",
    4: "named tolerant (fatty acid oxidation: fadE); one of the 5 profiled clones",
    5: "named tolerant (serine biosynthesis: serA); one of the 5 profiled clones",
    6: "named tolerant (regulation of transcription: zur)",
    7: "named tolerant (LPS biosynthesis: lpcA, arnB, arnC)",
    8: "named tolerant (transport: yicE)",
    9: "named tolerant (translation: tilS, yaeJ)",
    10: "named as not improved; neighbour of a tolerance gene",
    11: "named as not improved; neighbour of a tolerance gene",
    12: "named as not improved; neighbour of a tolerance gene",
    13: "not among the nine; one of the two distinct non-neighbour clones that failed",
    14: "not among the nine; one of the two distinct non-neighbour clones that failed",
}

# Symbols Table 1 uses that the 2012 workbook still lists under y-gene names.
# arnB = yfbE = b2253, arnC = yfbF = b2254; both sit inside clone 7's stated
# genomic window 2,363,701-2,366,090.
SYMBOL_SYNONYMS = {"arnB": "yfbE", "arnC": "yfbF"}


def find_repo_root(start: Path) -> Path:
    """Walk up from `start` until a directory containing `data/fabfos` is found."""
    for candidate in [start, *start.parents]:
        if (candidate / "data" / "fabfos").is_dir():
            return candidate
    raise SystemExit(f"could not locate a repo root containing data/fabfos above {start}")


def _cell_text(cell, shared: list[str]) -> str:
    kind = cell.get("t")
    if kind == "inlineStr":
        inline = cell.find(NS + "is")
        return "" if inline is None else "".join(t.text or "" for t in inline.iter(NS + "t"))
    value = cell.find(NS + "v")
    if value is None or value.text is None:
        return ""
    if kind == "s":
        return shared[int(value.text)]
    return value.text


def read_sheet(xlsx: Path, sheet: str) -> list[dict[str, str]]:
    """Return one dict per sheet row, keyed by column letter, shared strings resolved."""
    with zipfile.ZipFile(xlsx) as z:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in z.namelist():
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            shared = ["".join(t.text or "" for t in si.iter(NS + "t")) for si in root.findall(NS + "si")]
        data = ET.fromstring(z.read(sheet)).find(NS + "sheetData")

    rows = []
    for row in data.findall(NS + "row"):
        cells = {}
        for cell in row.findall(NS + "c"):
            column = "".join(ch for ch in (cell.get("r") or "") if ch.isalpha())
            cells[column] = _cell_text(cell, shared)
        rows.append(cells)
    return rows


def classify(f15: float, f30: float) -> str:
    hot15, hot30 = f15 > 1, f30 > 1
    if hot15 and hot30:
        return "tolerant_both"
    if hot15:
        return "tolerant_15"
    if hot30:
        return "tolerant_30"
    return "neutral"


def main() -> None:
    repo = find_repo_root(Path(__file__).resolve().parent)
    xlsx = repo / "data/fabfos/originals/benchmarks/scales/1-s2.0-S109671761200119X-mmc1.xlsx"
    out_dir = repo / "data/fabfos/benchmarks/_extractions" / DATASET
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = read_sheet(xlsx, SHEET)
    header = rows[0]
    expected_header = ["Gene b-number", "Gene name", "15 g/L selection Gene fitness", "30 g/L selection Gene fitness"]
    got_header = [header.get(c, "").strip() for c in "ABCD"]
    if got_header != expected_header:
        sys.exit(f"unexpected header row: {got_header!r} (expected {expected_header!r})")

    records = []
    n_missing_bnum = n_missing_symbol = n_unparsable = 0
    for cells in rows[1:]:
        bnum = cells.get("A", "").strip().lower()
        symbol = cells.get("B", "").strip()
        try:
            f15 = float(cells.get("C", ""))
            f30 = float(cells.get("D", ""))
        except ValueError:
            n_unparsable += 1
            continue
        if not bnum:
            n_missing_bnum += 1
        if not symbol:
            n_missing_symbol += 1
        records.append((symbol, bnum, f15, f30))

    n15 = sum(1 for _, _, f15, _ in records if f15 > 1)
    n30 = sum(1 for _, _, _, f30 in records if f30 > 1)

    print(f"rows read (data rows, header excluded): {len(records)}")
    print(f"rows with no b-number: {n_missing_bnum}")
    print(f"rows with no gene symbol: {n_missing_symbol}")
    print(f"rows with unparsable fitness (dropped): {n_unparsable}")
    print(f"genes with fitness > 1 at 15 g/L: {n15} (paper says {EXPECT_15})")
    print(f"genes with fitness > 1 at 30 g/L: {n30} (paper says {EXPECT_30})")

    if (n15, n30) != (EXPECT_15, EXPECT_30):
        sys.exit(
            "REFUSING to write: the paper's arithmetic does not reproduce.\n"
            f"  got {n15}/{n30} above 1 at 15/30 g/L, expected {EXPECT_15}/{EXPECT_30}.\n"
            "  Investigate which sheet, column or rows are being read -- do not move the threshold."
        )

    # b-numbers that two symbols share; worth flagging rather than silently deduplicating.
    seen: dict[str, list[str]] = {}
    for symbol, bnum, _, _ in records:
        if bnum:
            seen.setdefault(bnum, []).append(symbol)
    shared_bnums = {b: syms for b, syms in seen.items() if len(syms) > 1}
    print(f"b-numbers carried by more than one row: {len(shared_bnums)}")

    extraction = out_dir / "extraction.tsv"
    with extraction.open("w", encoding="utf-8") as fh:
        fh.write("dataset\tgene\tgene_norm\tbnum\tfitness_15\tfitness_30\tphenotype\tsource_table\tnote\n")
        for symbol, bnum, f15, f30 in records:
            notes = []
            if not bnum:
                notes.append("no b-number in source workbook")
            elif bnum in shared_bnums:
                others = [s for s in shared_bnums[bnum] if s != symbol]
                notes.append("b-number shared with " + ",".join(others))
            fh.write(
                "\t".join(
                    [
                        DATASET,
                        symbol,
                        symbol.lower(),
                        bnum,
                        repr(f15),
                        repr(f30),
                        classify(f15, f30),
                        SOURCE_TABLE,
                        "; ".join(notes),
                    ]
                )
                + "\n"
            )

    by_symbol = {symbol.lower(): bnum for symbol, bnum, _, _ in records}
    clones = out_dir / "clones.tsv"
    n_unmapped = 0
    with clones.open("w", encoding="utf-8") as fh:
        fh.write("clone\tgenes\tbnums\ttested\tconfirmed\tnote\n")
        for clone_id, genes, product in CLONE_TABLE:
            bnums = []
            for gene in genes:
                key = SYMBOL_SYNONYMS.get(gene, gene).lower()
                bnum = by_symbol.get(key, "")
                if not bnum:
                    n_unmapped += 1
                bnums.append(bnum)
            note = f"{product}; {_CONFIRMED_EVIDENCE[clone_id]}"
            for gene in genes:
                if gene in SYMBOL_SYNONYMS:
                    note += f"; {gene} listed in workbook as {SYMBOL_SYNONYMS[gene]}"
            fh.write(
                "\t".join(
                    [
                        str(clone_id),
                        ",".join(genes),
                        ",".join(bnums),
                        "TRUE",
                        "TRUE" if clone_id in CONFIRMED_CLONES else "FALSE",
                        note,
                    ]
                )
                + "\n"
            )

    n_confirmed = len(CONFIRMED_CLONES)
    print(f"clones written: {len(CLONE_TABLE)} ({n_confirmed} confirmed, {len(CLONE_TABLE) - n_confirmed} not)")
    if n_unmapped:
        print(f"clone genes with no b-number found in the workbook: {n_unmapped}")
    print(f"wrote {extraction}")
    print(f"wrote {clones}")


if __name__ == "__main__":
    main()
