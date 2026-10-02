#!/usr/bin/env python3
"""Lay out the files ASPIRE's sankey and metadata scripts read, from the port's own tables.

Upstream's scripts find their inputs in a shared output tree: seqkit stats keyed by fastq
file name, a sample manifest mapping those names to samples, a metadata sheet carrying a
Color column, and the count tables. This writes that tree from the sample sheet, the read
fate and the count tables, so the scripts run unmodified.

`mito` writes the removed counts' mitochondrial rows as a count table and prints how many.

`force_keep` merges every label's indicator summary into one ASV list for SpiecEasi's
--force-keep-asvs, keeping the rows run_spieceasi.R's own reader would keep from one summary.

`isa_overlays` prepares graph_network's indicator overlays in a directory of label summaries.
The script requires two; a lone label is paired with a `<label>_twin` copy of itself, in the
summaries and in the metadata. It writes the metadata the script should read and prints the
overlay labels, or nothing when no label has a summary.

`labels` prints the sheet labels an analysis runs over, one line each with a directory-safe
name and a secondary label (the next analysable label, or itself when it is the only one),
and records the rest with a reason. A label needs at least two non-empty levels, counted over
--keep-samples when it is given.

`recolor` rewrites a table's Color column for another label, since upstream's scripts read
one group-to-colour mapping and the metadata carries the first label's. plot_diversity.py
cannot colour an empty group, so diversity drops the samples a label leaves empty and takes
a secondary label only when it is complete over the rest.

`blank` applies analysis.min_level_size afterwards: a label value held by fewer samples is
emptied in the tables the analyses read, so each analysis skips that level and keeps the
sample.
"""

import argparse
import re
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
    mito_rows(args.removed).to_csv(out / "mito" / "ASVs" / "ASV_target.mito.tsv", sep="\t")


def mito_rows(removed_path):
    removed = pd.read_csv(removed_path, sep="\t", index_col=0)
    return removed.loc[removed["reason"] == "mitochondrial"].drop(columns="reason")


def cmd_mito(args):
    mito = mito_rows(args.removed)
    mito.to_csv(args.out, sep="\t")
    print(len(mito))


def cmd_force_keep(args):
    keep = set()
    for path in sorted(args.results.glob("*_indicator_species_summary.tsv")):
        table = pd.read_csv(path, sep="\t", dtype=str)
        if table.empty:
            continue
        if "ASV" not in table.columns:
            keep.update(table.iloc[:, 0].dropna())
            continue
        rows = pd.Series(True, index=table.index)
        if "significant" in table.columns:
            rows &= table["significant"].str.lower().isin(["true", "t", "1", "yes"])
        for q in ("q.value", "q_value"):
            if q in table.columns:
                rows &= pd.to_numeric(table[q], errors="coerce") < 0.05
                break
        keep.update(table.loc[rows, "ASV"].dropna())
    pd.DataFrame({"ASV_ID": sorted(keep)}).to_csv(args.out, sep="\t", index=False)
    print(len(keep))


def cmd_isa_overlays(args):
    suffix = "_indicator_species_summary.tsv"
    labels = sorted(p.name[: -len(suffix)] for p in args.dir.glob(f"*{suffix}"))
    md = pd.read_csv(args.metadata, sep="\t", dtype=str, keep_default_na=False)
    if len(labels) == 1:
        twin = f"{labels[0]}_twin"
        (args.dir / f"{twin}{suffix}").write_bytes((args.dir / f"{labels[0]}{suffix}").read_bytes())
        if labels[0] in md.columns:
            md[twin] = md[labels[0]]
        labels.append(twin)
    md.to_csv(args.out, sep="\t", index=False)
    print(",".join(labels))


def cmd_labels(args):
    _sheet, sid_col, labels = read_sheet(args.sheet)
    table = pd.read_csv(args.table, sep="\t", dtype=str, keep_default_na=False)
    if args.keep_samples is not None:
        keep = set(pd.read_csv(args.keep_samples, sep="\t", index_col=0, dtype=str).index)
        table = table.loc[table[sid_col].isin(keep)]
    kept, skipped = [], []
    for label in labels:
        if label not in table.columns:
            skipped.append((label, "absent from the analysis table"))
            continue
        n = len({v for v in table[label] if v})
        if n < 2:
            skipped.append((label, f"{n} non-empty level(s)"))
        else:
            kept.append(label)
    pd.DataFrame(skipped, columns=["label", "reason"]).to_csv(args.skipped, sep="\t", index=False)
    for i, label in enumerate(kept):
        print(f"{label}\t{re.sub(r'[^A-Za-z0-9._-]', '_', label)}\t{kept[(i + 1) % len(kept)]}")


def cmd_recolor(args):
    table = pd.read_csv(args.table, sep="\t", dtype=str, keep_default_na=False)
    if args.drop_unlabelled:
        table = table.loc[table[args.label] != ""]
    levels = sorted(v for v in table[args.label].unique() if v)
    colors = {lvl: PALETTE[i % len(PALETTE)] for i, lvl in enumerate(levels)}
    table["Color"] = table[args.label].map(colors).fillna("#d3d3d3")
    table.to_csv(args.out, sep="\t", index=False)
    if args.secondary and args.secondary != args.label and args.secondary in table.columns \
            and (table[args.secondary] != "").all():
        print(args.secondary)


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
    p = sub.add_parser("mito")
    p.add_argument("--removed", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("force_keep")
    p.add_argument("--results", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("isa_overlays")
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--metadata", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("labels")
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--table", type=Path, required=True)
    p.add_argument("--skipped", type=Path, required=True)
    p.add_argument("--keep-samples", type=Path, default=None,
                   help="a table whose first column lists the samples to judge the labels on")
    p = sub.add_parser("recolor")
    p.add_argument("--label", required=True)
    p.add_argument("--drop-unlabelled", action="store_true", help="drop rows with an empty --label")
    p.add_argument("--secondary", default=None,
                   help="print this label back when every kept row has a value for it")
    p.add_argument("table", type=Path)
    p.add_argument("out", type=Path)
    p = sub.add_parser("blank")
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--min-level-size", type=int, required=True)
    p.add_argument("tables", type=Path, nargs="+")
    args = ap.parse_args()
    {"sankey": cmd_sankey, "metadata": cmd_metadata, "mito": cmd_mito, "force_keep": cmd_force_keep,
     "isa_overlays": cmd_isa_overlays, "labels": cmd_labels,
     "recolor": cmd_recolor, "blank": cmd_blank}[args.cmd](args)


if __name__ == "__main__":
    main()
