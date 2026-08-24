"""Scrape ECMDB's concentration browse into one table, and join it to MNXM.

ECMDB's bulk `ecmdb.json` carries the metabolite cards (identifiers, structures) and NO
concentrations; the measurements live only in the paginated `/concentrations` browse, 25
growth-condition rows per page. So the bulk file is the crosswalk and the browse is the
data, and both are needed.

The join to MetaNetX goes ECMDB accession -> kegg/chebi/hmdb -> MNXM through
`chem_xref.tsv`. It is deliberately not by name: ECMDB names are display strings
("Adenosine monophosphate", "cis-Aconitic acid") and MetaNetX carries several accessions
per name.
"""
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


def _get(url: str, tries: int = 3) -> str:
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=60) as fh:
                return fh.read().decode("utf-8", "replace")
        except Exception as e:
            if i == tries - 1:
                raise
            print(f"  retry {i+1} on {url}: {e}", file=sys.stderr)
            time.sleep(3 * (i + 1))
    raise AssertionError


CELL = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
TAG = re.compile(r"<[^>]+>")
M2M = re.compile(r"/compounds/(M2MDB\d+)")
HR = re.compile(r"<hr\s*/?>", re.I)


def _text(s: str) -> str:
    return html.unescape(TAG.sub(" ", s)).replace("\xa0", " ").strip()


def _split(cell: str) -> list[str]:
    # Two of the browse's seven cells pack two fields separated by <hr/>: strain/media
    # and growth-status/growth-system. Splitting on the rule keeps them apart; joining
    # them into one string would make the dedup key depend on formatting.
    return [_text(part) for part in HR.split(cell)]


def parse_page(doc: str) -> list[dict]:
    rows = []
    for rm in ROW.finditer(doc):
        cells = CELL.findall(rm.group(1))
        if len(cells) != 7:
            continue
        acc = _text(cells[0])
        if not acc.startswith("ECMDB"):
            continue
        m2m = M2M.search(cells[0])
        strain = _split(cells[3]) + ["", ""]
        growth = _split(cells[4]) + ["", ""]
        rows.append(dict(ecmdb=acc, m2m=(m2m.group(1) if m2m else None),
                         name=_text(cells[1]), conc_raw=_text(cells[2]),
                         strain=strain[0], media=strain[1],
                         growth=growth[0], system=growth[1],
                         temperature=_text(cells[5]), citation=_text(cells[6])))
    return rows


CONC = re.compile(r"^\s*([0-9.eE+-]+)\s*(?:±|\+/-)?\s*([0-9.eE+-]+)?\s*(uM|mM|nM|M)\s*$")


def parse_conc(raw: str):
    # "2510± 183 uM" -> (2.51, 0.183) in mM. A row whose unit or value does not parse is
    # returned as None rather than guessed: a mis-scaled concentration is a silent
    # three-order-of-magnitude error in every ratio it touches.
    m = CONC.match(raw.replace("&plusmn;", "±"))
    if not m:
        return None, None
    val, sd, unit = m.group(1), m.group(2), m.group(3)
    scale = {"M": 1e3, "mM": 1.0, "uM": 1e-3, "nM": 1e-6}[unit]
    try:
        v = float(val) * scale
        s = float(sd) * scale if sd else None
    except ValueError:
        return None, None
    return v, s


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--pages", type=int, default=60)
    ap.add_argument("--sleep", type=float, default=0.4)
    a = ap.parse_args(argv)

    seen, rows = set(), []
    for page in range(1, a.pages + 1):
        doc = _get(BROWSE.format(page=page))
        got = parse_page(doc)
        if not got:
            print(f"[ecmdb] page {page}: 0 rows -- stopping")
            break
        fresh = [r for r in got if (key := (r["ecmdb"], r["conc_raw"], r["media"],
                                            r["growth"], r["citation"])) not in seen
                 and not seen.add(key)]
        rows.extend(fresh)
        print(f"[ecmdb] page {page}: {len(got)} rows ({len(fresh)} new), "
              f"{len(rows)} total", flush=True)
        if not fresh:
            break
        time.sleep(a.sleep)

    for r in rows:
        r["conc_mM"], r["sd_mM"] = parse_conc(r["conc_raw"])
    Path(a.out).write_text(json.dumps(rows, indent=1))
    ok = sum(1 for r in rows if r["conc_mM"] is not None)
    print(f"[ecmdb] {len(rows)} rows, {ok} parsed to mM, "
          f"{len({r['ecmdb'] for r in rows})} distinct metabolites -> {a.out}")


if __name__ == "__main__":
    main()
