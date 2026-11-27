#!/usr/bin/env python3
"""Republish a geoquery 0.7 dataset (TSV tables) in the layout geoquery_run.py writes, without rerunning it.

    python republish_07.py --src mmap_canada_mining --out mmap_canada_mining.new \\
        --produced 2026-11-19 --image geoquery:0.7@sha256:944cd4a4120c
"""
import argparse, shutil, tarfile, tempfile
from datetime import date
from pathlib import Path

import pandas as pd

from geoquery_run import INTEGRATED, publish, type_dir


def _tsv(p: Path) -> pd.DataFrame:
    return pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False)


def results_from_07(src: Path, results: Path) -> tuple[Path, str]:
    def dest(type_name: str, name: str) -> Path:
        d = type_dir(results, type_name)
        d.mkdir(parents=True, exist_ok=True)
        return d / name

    (aoi_table,) = [p for p in (src / "aois").iterdir() if p.name != "aois.geojson"]
    shutil.copyfile(src / "aois" / "aois.geojson", dest("aois", "aois.geojson"))

    tables = {p.stem: p for p in src.glob("*/*.tsv") if p.parent.name not in ("_logs", "integrated")}
    coverage = _tsv(src / "integrated" / "coverage.tsv")
    for source in coverage.columns.drop("aoi"):
        if source in tables:
            t = _tsv(tables[source])
        else:  # publish() counts a source with no table as found nowhere and copies nothing
            t = pd.DataFrame({"aoi": coverage.aoi, "source": source, "family": "none", "status": coverage[source]})
        t.to_csv(dest("source_table", f"{source}.csv"), index=False)

    for name in ("observations_long", "features_wide"):
        shutil.copyfile(src / "integrated" / INTEGRATED[name], dest(name, INTEGRATED[name]))
    coverage.to_csv(dest("coverage", INTEGRATED["coverage"]), index=False)

    gaps = _tsv(src / "_logs" / "gaps.tsv").rename(
        columns={"product": "source", "stem": "aoi", "reason": "gap", "detail": "reason"})
    gaps.to_csv(dest("gaps", "gaps.csv"), index=False)

    (archive,) = (src / "_logs" / "runs").glob("*.tar.gz")
    run = archive.name.removesuffix(".tar.gz")
    meta = results / "_metadata"
    with tarfile.open(archive) as tar:
        tar.extractall(meta, filter="data")
    (meta / run / "index.yml").rename(meta / "index.yml")
    (meta / run).rename(meta / "logs.latest")
    return aoi_table, run


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", type=Path, required=True, help="the 0.7 dataset folder")
    ap.add_argument("--out", type=Path, required=True, help="folder to write; its name is the dataset id")
    ap.add_argument("--produced", type=date.fromisoformat, required=True, help="date of the 0.7 run")
    ap.add_argument("--image", required=True, help="geoquery image the 0.7 run used")
    a = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        aoi_table, run = results_from_07(a.src.resolve(), Path(tmp))
        print(publish(Path(tmp), aoi_table, a.out.resolve(), run, a.image, a.produced))


if __name__ == "__main__":
    main()
