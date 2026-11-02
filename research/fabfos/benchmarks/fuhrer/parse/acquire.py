#!/usr/bin/env python3
"""Acquire the Fuhrer 2017 metabolome screen -- the article and its four supplements from
a local download, the screen matrices from EBI BioStudies -- into
`data/fabfos/originals/benchmarks/fuhrer/`.

    mamba run -n msm python research/fabfos/benchmarks/fuhrer/parse/acquire.py
    ... --publish        # actually copies and downloads

THIS SCRIPT EXISTS BECAUSE RE-ACQUISITION HAS TO BE POSSIBLE. The eydallin arm's
acquisition transform points at the wrong article identifier and cannot be re-run, so its
raw folder is only reproducible by hand. Everything this arm consumes is named here.

TWO CAVEATS ABOUT THE BIOSTUDIES ENDPOINT, both of which present as "not found" rather
than as an error:
  - The API and the FTP host reject a bare curl/urllib User-Agent. Send a browser one.
  - Accessions are bucketed by LAST THREE DIGITS (`S-BSST/015/S-BSST2015/`), but
    accessions below 1000 are not in that tree at all -- they live under a NAME-RANGE
    bucket, `S-BSST/S-BSST0-99/S-BSST5/`. Guessing numeric buckets returns 404 forever.
    `api/v1/studies/<acc>/info` reports the real `relPath`; that is the only reliable
    way to find it.

THE TWO RAW-INTENSITY MATRICES ARE DELIBERATELY NOT FETCHED. `rawdata_neg_all.tsv` and
`rawdata_pos_all.tsv` are 543 MB between them and carry four columns per gene (technical
x biological duplicates). The paper's own unit of analysis is the modified z-score, this
arm re-derives nothing upstream of it, and `sample_id_all.xls` -- the gene key for those
two matrices only -- is skipped for the same reason. The gap is intentional and recorded
in the arm's README, not silent.

FILE SIZES ARE THE ACQUISITION RECORD. Every remote file's byte count is asserted against
the manifest below, which was read from the study's own file listing. A short read from a
truncated transfer produces a complete-looking matrix, so a size mismatch deletes the
partial file and exits.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]

OUT = REPO / "data/fabfos/originals/benchmarks/fuhrer"
DOWNLOADS = Path.home() / "downloads"

ACCESSION = "S-BSST5"
FIRE = f"https://ftp.ebi.ac.uk/biostudies/fire/S-BSST/S-BSST0-99/{ACCESSION}/Files"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Publisher filenames, kept exactly: the filename is the acquisition record.
ARTICLE = (
    "44320_2017_msb.20167150.pdf",              # Fuhrer et al. Mol Syst Biol 13:907
    "44320_2017_BFMSB167150_MOESM2_ESM.xlsx",   # Table EV1A strains, EV1B ion annotations
    "44320_2017_BFMSB167150_MOESM3_ESM.xlsx",   # Table EV2 annotation summary
    "44320_2017_BFMSB167150_MOESM4_ESM.zip",    # Table EV3 per-gene detail pages
    "44320_2017_BFMSB167150_MOESM5_ESM.xlsx",   # Table EV4 per-ion published AUC
)

SCREEN = {
    "zscore_neg.tsv": 90485873,
    "zscore_pos.tsv": 124636326,
    "sample_id_zscore.xls": 192512,
    "neg_ionMz.xls": 154112,
    "pos_ionMz.xls": 203264,
    "neg_kegg_all_3mD.xls": 1417216,
    "pos_kegg_all_3mD.xls": 1333760,
}

SKIPPED = {
    "rawdata_neg_all.tsv": 229189456,
    "rawdata_pos_all.tsv": 314326485,
    "sample_id_all.xls": 622592,
}


def fetch(name: str, expect: int, dest: Path) -> None:
    req = urllib.request.Request(f"{FIRE}/{name}", headers={"User-Agent": UA})
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(req, timeout=300) as r, tmp.open("wb") as fh:
        shutil.copyfileobj(r, fh, length=1 << 20)
    got = tmp.stat().st_size
    if got != expect:
        tmp.unlink()
        raise SystemExit(f"!! {name}: {got} bytes, manifest says {expect} -- transfer "
                         f"truncated, partial file removed")
    tmp.replace(dest)
    print(f"   {name}: {got:,} bytes")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"copy and download into {OUT.relative_to(REPO)}")
    a = ap.parse_args()

    missing = [n for n in ARTICLE if not (DOWNLOADS / n).exists()]
    if missing:
        raise SystemExit(f"!! not in {DOWNLOADS}: {missing}\n"
                         f"   the article and its supplements are behind a publisher "
                         f"page and are not fetchable from here")

    have = {p.name: p.stat().st_size for p in OUT.glob("**/*") if p.is_file()}
    want = {**{n: (DOWNLOADS / n).stat().st_size for n in ARTICLE}, **SCREEN}
    todo = {n: s for n, s in want.items() if have.get(n) != s}

    print(f"{len(want)} files wanted, {len(want) - len(todo)} already present and the "
          f"right size, {len(todo)} to acquire "
          f"({sum(SCREEN[n] for n in todo if n in SCREEN) / 1e6:.0f} MB from BioStudies)")
    print(f"not fetched by design: {', '.join(SKIPPED)} "
          f"({sum(SKIPPED.values()) / 1e6:.0f} MB)")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(REPO)})")
        return 0

    (OUT / "screen").mkdir(parents=True, exist_ok=True)
    for name in ARTICLE:
        if name in todo:
            shutil.copy2(DOWNLOADS / name, OUT / name)
            print(f"   {name}: {want[name]:,} bytes (from {DOWNLOADS})")
    for name in SCREEN:
        if name in todo:
            fetch(name, SCREEN[name], OUT / "screen" / name)

    total = sum(p.stat().st_size for p in OUT.glob("**/*") if p.is_file())
    print(f"\n-> {OUT.relative_to(REPO)}: "
          f"{len(list(OUT.glob('**/*')))} entries, {total / 1e6:.0f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
