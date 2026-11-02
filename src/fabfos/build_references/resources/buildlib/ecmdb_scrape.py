# ECMDB's concentration browse, scraped into one table.
#
# THE SCRAPE IS THE ACQUISITION, not a parse of something downloadable. ECMDB serves
# `ecmdb.json.zip` -- 3,760 metabolite cards with identifiers and structures -- and that
# file carries NO concentrations. The measurements exist only as a paginated HTML browse
# at /concentrations, 25 growth-condition rows per page, with no API behind it. So the
# acquire transform fetches the bulk file verbatim for the crosswalk AND runs this for the
# data, and neither substitutes for the other.
#
# That makes this the same shape as the KEGG acquisition, which saves REST responses as
# the endpoints returned them: the originals tier holds what the server gave, and the
# server gives HTML.
#
# THE ROW HAS SEVEN CELLS, NOT NINE. Two of them pack two fields apiece behind an <hr/> --
# strain/media, and growth-status/growth-system. Reading the table as one field per cell
# returns zero rows, silently, because no row then matches the expected width.
#
# A ROW WHOSE UNIT DOES NOT PARSE IS KEPT WITH A NULL VALUE, never guessed. A mis-scaled
# concentration is a silent three-order error in every ratio it touches, and the aggregator
# downstream would have no way to see it. The raw string is kept beside the parsed value so
# the refusal is checkable.
#
# The measurements are not ECMDB's own. The citation column names the primary studies by
# PMID -- 19561621 (Bennett 2009), 17379776 (Ishii 2007) and others -- which is what makes
# this citable as a panel rather than as one database's opinion.
from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

BROWSE = "https://ecmdb.ca/concentrations?page={page}"
UA = {"User-Agent": "Mozilla/5.0 (research; metasmith bake direction lane)"}

# A floor, not a target. The browse returns HTTP 200 with an empty table when it is
# unhappy, exactly as KEGG's REST does, so an empty result is checked rather than trusted.
MIN_ROWS = 900

ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
TAG = re.compile(r"<[^>]+>")
M2M = re.compile(r"/compounds/(M2MDB\d+)")
HR = re.compile(r"<hr\s*/?>", re.I)
CONC = re.compile(r"^\s*([0-9.eE+-]+)\s*(?:±|\+/-)?\s*([0-9.eE+-]+)?\s*(uM|mM|nM|M)\s*$")

TO_MM = {"M": 1e3, "mM": 1.0, "uM": 1e-3, "nM": 1e-6}


def get(url: str, tries: int = 3) -> str:
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=60) as fh:
                return fh.read().decode("utf-8", "replace")
        except Exception as e:                                  # noqa: BLE001
            if i == tries - 1:
                raise
            print(f"  retry {i + 1} on {url}: {e}", file=sys.stderr)
            time.sleep(3 * (i + 1))
    raise AssertionError("unreachable")


def text(s: str) -> str:
    return html.unescape(TAG.sub(" ", s)).replace("\xa0", " ").strip()


def split_packed(cell: str) -> list[str]:
    # Split the two-field cells on their <hr/>. Joining them into one string instead
    # would make the dedup key depend on the page's formatting.
    return [text(part) for part in HR.split(cell)]


def parse_page(doc: str) -> list[dict]:
    rows = []
    for match in ROW.finditer(doc):
        cells = CELL.findall(match.group(1))
        if len(cells) != 7:
            continue
        accession = text(cells[0])
        if not accession.startswith("ECMDB"):
            continue
        m2m = M2M.search(cells[0])
        strain = split_packed(cells[3]) + ["", ""]
        growth = split_packed(cells[4]) + ["", ""]
        rows.append(dict(
            ecmdb=accession, m2m=(m2m.group(1) if m2m else None),
            name=text(cells[1]), conc_raw=text(cells[2]),
            strain=strain[0], media=strain[1],
            growth=growth[0], system=growth[1],
            temperature=text(cells[5]), citation=text(cells[6]),
        ))
    return rows


def parse_concentration(raw: str):
    # "2510± 183 uM" -> (2.51, 0.183) in mM. (None, None) when it does not parse.
    m = CONC.match(raw.replace("&plusmn;", "±"))
    if not m:
        return None, None
    value, sd, unit = m.group(1), m.group(2), m.group(3)
    scale = TO_MM[unit]
    try:
        return float(value) * scale, (float(sd) * scale if sd else None)
    except ValueError:
        return None, None


def scrape(max_pages: int, sleep: float) -> list[dict]:
    seen, rows = set(), []
    for page in range(1, max_pages + 1):
        got = parse_page(get(BROWSE.format(page=page)))
        if not got:
            print(f"[ecmdb] page {page}: 0 rows -- end of the browse")
            break
        fresh = []
        for r in got:
            key = (r["ecmdb"], r["conc_raw"], r["media"], r["growth"], r["citation"])
            if key not in seen:
                seen.add(key)
                fresh.append(r)
        rows.extend(fresh)
        print(f"[ecmdb] page {page}: {len(got)} rows ({len(fresh)} new), "
              f"{len(rows)} total", flush=True)
        if not fresh:
            # The browse repeats its last page rather than 404ing past the end.
            break
        time.sleep(sleep)
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True)
    ap.add_argument("--pages", type=int, default=60)
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--min-rows", type=int, default=MIN_ROWS)
    a = ap.parse_args(argv)

    rows = scrape(a.pages, a.sleep)
    for r in rows:
        r["conc_mM"], r["sd_mM"] = parse_concentration(r["conc_raw"])

    parsed = sum(1 for r in rows if r["conc_mM"] is not None)
    metabolites = len({r["ecmdb"] for r in rows})
    print(f"[ecmdb] {len(rows)} rows, {parsed} parsed to mM, "
          f"{metabolites} distinct metabolites")
    if len(rows) < a.min_rows:
        print(f"[ecmdb] under the {a.min_rows}-row floor -- the browse answers an unhappy "
              f"request with an empty table and HTTP 200, so refusing to write a short "
              f"snapshot rather than pinning one", file=sys.stderr)
        return 1
    Path(a.out).write_text(json.dumps(rows, indent=1))
    print(f"[ecmdb] -> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
