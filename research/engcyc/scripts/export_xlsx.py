"""Write the manifest and every metadata table into one Excel workbook for sharing.

    python export_xlsx.py <out.xlsx>

Sheet one is the manifest. Each metadata table follows on a sheet named by its dataset_id,
and the manifest's metadata_table cell links to it. A column whose cells exceed Excel's
32,767-character limit is left out and listed on the about sheet.
"""
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.hyperlink import Hyperlink

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "data/engcyc/manifest.tsv"
METADATA = ROOT / "data/engcyc/metadata"
HEADERLESS = {"amp_saanich_jgi_itags.tsv"}
EXCEL_CELL_LIMIT = 32767
NUMBER = re.compile(r"^-?(0|[1-9]\d{0,14})(\.\d+)?$")
WRAP_COLUMNS = {"description", "existing_results", "notes", "locations"}

FONT = Font(name="Arial", size=10)
HEAD = Font(name="Arial", size=10, bold=True, color="FFFFFF")
HEAD_FILL = PatternFill("solid", fgColor="1F4E5A")
LINK = Font(name="Arial", size=10, color="0563C1", underline="single")


def cell_value(v):
    return float(v) if "." in v and NUMBER.match(v) else int(v) if NUMBER.match(v) else v


def write_sheet(ws, df, header=True, wrap=()):
    if header:
        ws.append([str(c) for c in df.columns])
        for c in ws[1]:
            c.font, c.fill = HEAD, HEAD_FILL
        ws.freeze_panes = "A2"
    for row in df.itertuples(index=False):
        ws.append([cell_value(v) for v in row])
    for col_idx, col in enumerate(df.columns, start=1):
        letter = get_column_letter(col_idx)
        longest = max([len(str(col))] + [len(str(v)) for v in df[col].head(500)])
        wide = str(col) in wrap
        ws.column_dimensions[letter].width = 60 if wide else min(max(10, longest + 2), 45)
        for c in ws[letter][1 if header else 0:]:
            c.font = FONT
            if wide:
                c.alignment = Alignment(wrap_text=True, vertical="top")
    if header and len(df):
        ws.auto_filter.ref = ws.dimensions


def main(out):
    manifest = pd.read_csv(MANIFEST, sep="\t", dtype=str, keep_default_na=False)
    wb = Workbook()
    ws = wb.active
    ws.title = "manifest"
    write_sheet(ws, manifest, wrap=WRAP_COLUMNS)
    ws.column_dimensions[get_column_letter(list(manifest.columns).index("size_bytes") + 1)].width = 18
    for c in ws[get_column_letter(list(manifest.columns).index("size_bytes") + 1)][1:]:
        c.number_format = "#,##0"

    dropped = []
    link_col = list(manifest.columns).index("metadata_table") + 1
    for i, table in enumerate(manifest.metadata_table, start=2):
        if not table:
            continue
        header = table not in HEADERLESS
        df = pd.read_csv(METADATA / table, sep="\t", dtype=str, keep_default_na=False,
                         header=0 if header else None)
        too_long = [c for c in df.columns if df[c].map(len).max() > EXCEL_CELL_LIMIT]
        dropped += [(table, str(c)) for c in too_long]
        name = table.removesuffix(".tsv")
        write_sheet(wb.create_sheet(name), df.drop(columns=too_long), header=header)
        link = ws.cell(row=i, column=link_col)
        link.hyperlink = Hyperlink(ref=link.coordinate, location=f"'{name}'!A1")
        link.font = LINK

    commit = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                            capture_output=True, text=True).stdout.strip()
    about = wb.create_sheet("about")
    lines = [
        ["Engcyc candidate datasets"],
        [f"Exported {dt.date.today().isoformat()} from branch engcyc/dev, commit {commit}."],
        ["The manifest sheet lists one dataset per row. Its metadata_table cell links to that dataset's sheet."],
        ["Each dataset sheet is a copy of the metadata that already existed for it, with its original columns."],
        ["A dataset with an empty metadata_table had no metadata to copy."],
        ["The source of truth is the DVC-pinned TSVs under data/engcyc/. Edits made here do not flow back."],
    ]
    lines += [[f"Left out: column {c} of {t}, because its cells exceed Excel's {EXCEL_CELL_LIMIT:,}-character limit."]
              for t, c in dropped]
    for line in lines:
        about.append(line)
    about["A1"].font = Font(name="Arial", size=12, bold=True)
    for row in about.iter_rows(min_row=2):
        row[0].font = FONT
    about.column_dimensions["A"].width = 110
    wb.save(out)
    print(f"{out}: {len(wb.sheetnames)} sheets, dropped {dropped}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
