#!/usr/bin/env python3
"""Inject report_data.json into report.tmpl.html and write report.html.

The report is published as an artifact rather than committed as prose, so nothing
it states may be typed by hand. The template holds the argument and the layout; the
numbers arrive from the round's own tables through `report_data.py`.

    build_report.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

HERE = Path(__file__).resolve().parent
MARK = "/*__DATA__*/"


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    tmpl = (HERE / "report.tmpl.html").read_text()
    data = (HERE / "report_data.json").read_text()
    if MARK not in tmpl:
        raise SystemExit(f"template has no {MARK} placeholder")
    if "</script" in data:
        raise SystemExit("data would close the script tag it is embedded in")
    out = HERE / "report.html"
    out.write_text(tmpl.replace(MARK, data))
    print(f"wrote {out.name}: {len(out.read_text())} bytes")


if __name__ == "__main__":
    main()
