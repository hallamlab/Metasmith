#!/usr/bin/env python3
"""Lay out the files ASPIRE's sankey and metadata scripts read, from the port's own tables.

Upstream's scripts find their inputs in a shared output tree: seqkit stats keyed by fastq
file name, a sample manifest mapping those names to samples, a metadata sheet carrying a
Color column, and the count tables. This writes that tree from the sample sheet, the read
fate and the count tables, so the scripts run unmodified.

`blank` applies analysis.min_level_size afterwards: a label value held by fewer samples is
emptied in the tables the analyses read, so each analysis skips that level and keeps the
sample.
"""

import argparse
from pathlib import Path

import pandas as pd

PALETTE = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
           "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]


def read_sheet(path):
    sheet = pd.read_csv(path, sep="\t", dtype=str)
    return sheet, sheet.columns[0], list(sheet.columns[1:])


def with_color(sheet, label):
    levels = sorted(sheet[label].dropna().unique())
    colors = {lvl: PALETTE[i % len(PALETTE)] for i, lvl in enumerate(levels)}
    out = sheet.copy()
    out["Color"] = out[label].map(colors).fillna("#d3d3d3")
    return out


def write_common(out, sheet, sid_col, label, fate):
    out.mkdir(parents=True, exist_ok=True)
    (out / "stats").mkdir(exist_ok=True)
    with_color(sheet, label).to_csv(out / "metadata.tsv", sep="\t", index=False)
    rows, stats = [], []
    for sid in sheet[sid_col]:
        r1, r2 = f"{sid}_R1.fastq.gz", f"{sid}_R2.fastq.gz"
        rows.append((sid, r1, r2))
        raw = int(fate.loc[sid, "raw"]) if sid in fate.index else 0
        stats += [(r1, raw), (r2, raw)]
    pd.DataFrame(rows).to_csv(out / "manifest.tsv", sep="\t", header=False, index=False)
    pd.DataFrame(stats, columns=["file", "num_seqs"]).to_csv(
        out / "stats" / "fastq_stats.tsv", sep="\t", index=False)


def one_row(values, name):
    return pd.DataFrame([values], index=pd.Index([name], name="#OTU ID"))


def cmd_sankey(args):
    sheet, sid_col, labels = read_sheet(args.sheet)
    fate = pd.read_csv(args.fate, sep="\t", dtype={"sample": str}).set_index("sample")
    label = args.label or labels[0]
    out = args.out
    write_common(out, sheet, sid_col, label, fate)
    pd.DataFrame(
        [(f"{sid}.filtered.fasta.gz", int(fate.loc[sid, "filtered"])) for sid in fate.index],
        columns=["file", "num_seqs"],
    ).to_csv(out / "stats" / "filtered_fastqs.tsv", sep="\t", index=False)
    (out / "ASVs").mkdir(exist_ok=True)
    decon = fate["table_filtered"] - fate["removed_contaminant"]
    for name, series in (("ASV_counts", fate["mapped"]), ("ASV_target.decon", decon),
                         ("ASV_target.micro", fate["kept"])):
        one_row(series.to_dict(), "all").to_csv(out / "ASVs" / f"{name}.tsv", sep="\t")


def cmd_metadata(args):
    sheet, sid_col, labels = read_sheet(args.sheet)
    fate = pd.read_csv(args.fate, sep="\t", dtype={"sample": str}).set_index("sample")
    label = args.label or labels[0]
    out = args.out
    write_common(out, sheet, sid_col, label, fate)
    (out / "ASVs").mkdir(exist_ok=True)
    (out / "mito" / "ASVs").mkdir(parents=True, exist_ok=True)
    clean = pd.read_csv(args.clean, sep="\t", index_col=0)
    clean.to_csv(out / "ASVs" / "ASV_target.micro.tsv", sep="\t")
    removed = pd.read_csv(args.removed, sep="\t", index_col=0)
    mito = removed.loc[removed["reason"] == "mitochondrial"].drop(columns="reason")
    mito.to_csv(out / "mito" / "ASVs" / "ASV_target.mito.tsv", sep="\t")


def cmd_blank(args):
    sheet, sid_col, labels = read_sheet(args.sheet)
    rare = {}
    for label in labels:
        counts = sheet[label].value_counts()
        rare[label] = set(counts[counts < args.min_level_size].index)
    for path in args.tables:
        table = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
        for label, values in rare.items():
            if label in table.columns and values:
                table.loc[table[label].isin(values), label] = ""
        table.to_csv(path, sep="\t", index=False)
    for label, values in rare.items():
        if values:
            print(f"[{label}] blanked, held by fewer than {args.min_level_size} samples: {sorted(values)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("sankey", "metadata"):
        p = sub.add_parser(name)
        p.add_argument("--sheet", type=Path, required=True)
        p.add_argument("--fate", type=Path, required=True)
        p.add_argument("--label", default=None, help="default: the sheet's first label")
        p.add_argument("--out", type=Path, required=True)
        if name == "metadata":
            p.add_argument("--clean", type=Path, required=True)
            p.add_argument("--removed", type=Path, required=True)
    p = sub.add_parser("blank")
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--min-level-size", type=int, required=True)
    p.add_argument("tables", type=Path, nargs="+")
    args = ap.parse_args()
    {"sankey": cmd_sankey, "metadata": cmd_metadata, "blank": cmd_blank}[args.cmd](args)


if __name__ == "__main__":
    main()
