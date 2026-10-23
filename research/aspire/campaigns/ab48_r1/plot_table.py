#!/usr/bin/env python3
"""Cut the lab V4 ASV table to the samples Kalen's sheet marks Plot=Y, under his IDs and dates.

    python research/aspire/campaigns/ab48_r1/plot_table.py [--study lab_v4_r1] [--out data/aspire/spirulina_v4_plot]

Reads ASV_Metadata.csv from data/aspire/hallam_16s_inputs and the study's curated counts.
Writes asv_counts.tsv, samples.tsv (each Plot=Y row and what became of it) and
id_reconciliation.tsv (his IDs against our libraries, both ways).
"""

import argparse
import re
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DATA = REPO / "data" / "aspire"
SHEETS = DATA / "hallam_16s_inputs"
STUDIES = ["lab_r1", "lab_r2", "lab_v4_r1"]
# The sheet's IDs come from a spreadsheet that cut long names to 15 characters.
TRUNCATED_AT = 15


def norm(name: str) -> str:
    return re.sub(r"_S\d+$", "", name).replace("_", "-").lower()


def counts_columns(study: str) -> list[str]:
    path = next((DATA / study / "aspire-counts_clean").iterdir())
    return list(pd.read_csv(path, sep="\t", index_col=0, nrows=0).columns)


def libraries() -> pd.DataFrame:
    cw = pd.read_csv(SHEETS / "sample_crosswalk.tsv", sep="\t", dtype=str).fillna("")
    cw = cw[["asv_table_id", "cohort", "display_label", "date", "source", "is_control"]]
    seen = set(cw.asv_table_id)
    extra = sorted({c for s in STUDIES for c in counts_columns(s)} - seen)
    nostoc = pd.read_csv(SHEETS / "Nostoc_Anabaena_Metadata.csv", dtype=str, encoding="utf-8-sig")
    named = dict(zip(nostoc.File_ID, nostoc.ID))
    cw = pd.concat([cw, pd.DataFrame({"asv_table_id": extra, "display_label": [named.get(e, e) for e in extra],
                                      "cohort": "(not in crosswalk)"})])
    cw = cw.fillna("")
    for s in STUDIES:
        cw[s] = cw.asv_table_id.isin(counts_columns(s))
    return cw.reset_index(drop=True)


def library_keys(row) -> set[str]:
    return {norm(row.display_label), norm(row.asv_table_id), norm(row.asv_table_id.removeprefix("s"))}


def reconcile() -> tuple[pd.DataFrame, pd.DataFrame]:
    sheet = pd.read_csv(SHEETS / "ASV_Metadata.csv", dtype=str, encoding="utf-8-sig").fillna("")
    libs = libraries()
    keys = [library_keys(r) for r in libs.itertuples()]
    by_key = {}
    for i, ks in enumerate(keys):
        for k in ks:
            by_key.setdefault(k, set()).add(i)

    matched, how = [], []
    for sid in sheet.ID:
        n = norm(sid)
        hit = by_key.get(n, set())
        kind = "exact" if hit else ""
        if not hit and len(sid) == TRUNCATED_AT:
            stem = n.rstrip("-")
            hit = {i for i, ks in enumerate(keys) if any(k.startswith(stem) for k in ks)}
            kind = "truncated" if hit else ""
        if not hit:
            inside = re.compile(rf"(^|-){re.escape(n)}(-|$)")
            hit = {i for i, ks in enumerate(keys) if any(inside.search(k) for k in ks)}
            kind = "contained" if len(hit) == 1 else ""
            hit = hit if kind else set()
        matched.append(sorted(hit))
        how.append(kind)
    sheet["match"] = how
    sheet["library_index"] = matched

    # A truncated ID that fits several libraries, or a library two sheet rows claim, is not resolved.
    claims = pd.Series([i for m in matched for i in m]).value_counts()
    def resolve(row):
        if len(row.library_index) == 1 and claims[row.library_index[0]] == 1:
            return row.library_index[0]
        exact = [i for i in row.library_index if norm(row.ID) in keys[i]]
        return exact[0] if len(exact) == 1 else None
    sheet["library"] = sheet.apply(resolve, axis=1)
    lib_cols = ["asv_table_id", "cohort", "display_label", "date", *STUDIES]
    for c in lib_cols:
        sheet[c] = sheet.library.map(lambda i: libs.at[int(i), c] if pd.notna(i) else "")
    sheet.loc[sheet.library.isna(), STUDIES] = False

    unclaimed = libs.drop(index=sheet.library.dropna().astype(int))
    unclaimed = unclaimed[unclaimed[STUDIES].any(axis=1)]
    return sheet.drop(columns=["library_index"]), unclaimed


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--study", default="lab_v4_r1")
    ap.add_argument("--out", type=Path, default=DATA / "spirulina_v4_plot")
    args = ap.parse_args()

    sheet, unclaimed = reconcile()
    counts = pd.read_csv(next((DATA / args.study / "aspire-counts_clean").iterdir()), sep="\t", index_col=0)
    tax = pd.read_csv(next((DATA / args.study / "amplicon-asv_taxonomy").iterdir()), sep="\t", index_col=0)

    plot = sheet[sheet.Plot == "Y"].sort_values(["Date", "ID"]).copy()
    first = ~plot.asv_table_id.duplicated() | (plot.asv_table_id == "")
    plot["status"] = "in table"
    plot.loc[plot.asv_table_id == "", "status"] = "no library in our runs"
    plot.loc[(plot.asv_table_id != "") & ~plot.asv_table_id.isin(counts.columns), "status"] = f"not in {args.study}"
    owner = plot[first & (plot.asv_table_id != "")].set_index("asv_table_id").ID
    dup = ~first & (plot.asv_table_id != "")
    plot.loc[dup, "status"] = "same library as " + plot.loc[dup, "asv_table_id"].map(owner)
    kept = plot[plot.status == "in table"]

    table = counts[kept.asv_table_id.tolist()]
    table.columns = kept.ID.tolist()
    table = table[table.sum(axis=1) > 0]
    table.insert(0, "Taxon", tax["Taxon"].reindex(table.index))
    table.index.name = "ASV_ID"
    plot["reads"] = plot.asv_table_id.map(counts.sum()).fillna(0).astype(int)

    args.out.mkdir(parents=True, exist_ok=True)
    table.to_csv(args.out / "asv_counts.tsv", sep="\t")
    plot[["ID", "Source", "Date", "Type", "Batch", "status", "asv_table_id", "cohort", "reads"]].to_csv(
        args.out / "samples.tsv", sep="\t", index=False)
    recon = pd.concat([
        sheet.assign(side="sheet", resolved=sheet.library.notna())[
            ["side", "ID", "Plot", "Date", "match", "resolved", "asv_table_id", "cohort", *STUDIES]],
        unclaimed.assign(side="ours", ID="", Plot="", Date="", match="", resolved=False)[
            ["side", "ID", "Plot", "Date", "match", "resolved", "asv_table_id", "cohort", *STUDIES]],
    ])
    recon.to_csv(args.out / "id_reconciliation.tsv", sep="\t", index=False)

    print(f"{args.study}: {counts.shape[1]} samples, {len(counts)} ASVs -> {table.shape[1] - 1} samples, {len(table)} ASVs")
    print(plot.status.value_counts().to_string())
    unresolved = sheet[sheet.library.isna()]
    print(f"sheet rows without a library: {len(unresolved)} of {len(sheet)}")
    print(f"our libraries the sheet lacks: {len(unclaimed)}")


if __name__ == "__main__":
    main()
