#!/usr/bin/env python3
"""What each run under data/fabfos/runs/ actually has, measured rather than declared.

    python src/fabfos/build_references/runs_census.py            # print
    python src/fabfos/build_references/runs_census.py --write    # also write census.tsv

`denovo` is the column worth explaining. A run can hold an `annotations/` tree AND a
de-novo GPR table and still have no de-novo evidence, because the two can be about
different ORF sets -- three hosts carry a 2024 lane run whose ORF ids overlap their own
current table by exactly zero. So this measures the overlap instead of testing for the
directory, and a run counts only when its lanes and its table name the same proteins.

`gem_adapted_from` follows the borrow to the run that owns the curated model, so a table
that came from another host's GEM says whose. `self` means the model is that run's own.
"""
from __future__ import annotations

import argparse
import ast
import csv
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[3]
RUNS = REPO / "data" / "fabfos" / "runs"
GEM_SOURCE_AT = (Path(__file__).resolve().parent / "transforms" / "benchmark"
                 / "host_gpr_gem.py")

# scadc_metagenome's de-novo table is ~20M rows; its lanes are larger still. Reading the
# orf column alone and sampling the lane side keeps the census a few seconds rather than
# a few minutes, and an overlap verdict does not need every row to be certain.
LANE_SAMPLE = 200_000


def gem_source_map() -> dict[str, str]:
    """The transform's own host -> GEM-owner map, read without importing the transform."""
    tree = ast.parse(GEM_SOURCE_AT.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "GEM_SOURCE" for t in node.targets):
            return ast.literal_eval(node.value)
    raise SystemExit(f"no GEM_SOURCE assignment in {GEM_SOURCE_AT}")


def normalise(ids: pd.Series) -> set[str]:
    # `fabfos_evidence.read_uniref50` folds a trailing `-<n>` onto `_<n>`; the lanes and
    # the table disagree on that suffix alone for the metagenome, which would otherwise
    # read as a total miss.
    return set(ids.astype(str).str.replace(r"-(\d+)$", r"_\1", regex=True))


def clean_table(run: Path) -> Path | None:
    """The CLEAN lane, under either layout: `lanes/clean.tsv` or `clean/<x>.clean.tsv`.

    A run derived from another host's table -- AG1 from DH1, LW06 from BW25113 -- runs no
    lanes of its own but keeps its parent's ORF namespace, so the parent's lanes are the
    ones that back it. Following the borrow is what makes its verdict mean the same thing
    as a host's.
    """
    lanes = run / "annotations" / "lanes" / "clean.tsv"
    if lanes.exists():
        return lanes
    old = sorted((run / "annotations" / "clean").glob("*.clean.tsv"))
    if old:
        return old[0]
    borrow = next((run / "gpr").glob("BUILD_borrow.json"), None)
    if borrow:
        parent = json.loads(borrow.read_text()).get("borrowed_from")
        if parent:
            return clean_table(run.parent / parent)
    return None


def denovo_table(run: Path) -> Path | None:
    for name in ("gpr_denovo.parquet", "gpr_4lane.parquet"):
        p = run / "gpr" / name
        if p.exists():
            return p
    return None


def lane_orfs(path: Path) -> set[str]:
    # QUOTE_NONE: a CLEAN table is plain TSV, and at least one host's carries a lone
    # double quote inside an EC annotation that the default dialect reads as an unclosed
    # string and then runs off the end of the file.
    df = pd.read_csv(path, sep="\t", usecols=[0], nrows=LANE_SAMPLE, quoting=csv.QUOTE_NONE)
    return normalise(df.iloc[:, 0])


def table_orfs(path: Path) -> set[str]:
    return normalise(pq.read_table(path, columns=["orf"]).column("orf").to_pandas())


def build_json(run: Path) -> dict:
    for p in sorted((run / "gpr").glob("BUILD_*.json")):
        try:
            return json.loads(p.read_text())
        except json.JSONDecodeError:
            continue
    return {}


def gem_chain(run_name: str, gem_source: dict[str, str], meta: dict) -> str:
    """The run whose curated model this table came from, following one borrow."""
    if run_name in gem_source:
        owner = gem_source[run_name]
        return "self" if owner == run_name else owner
    host = meta.get("host") or (meta.get("population") or {}).get("host")
    if not host:
        return ""
    owner = gem_source.get(host, host)
    return host if owner == host else f"{host} -> {owner}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--write", action="store_true",
                    help=f"write {(RUNS / 'census.tsv').relative_to(REPO)}")
    a = ap.parse_args()

    gem_source = gem_source_map()
    rows = []
    for run in sorted(p for p in RUNS.glob("*") if p.is_dir()):
        lanes, table = clean_table(run), denovo_table(run)
        denovo, note = "no", "no lane table" if not lanes else "no de-novo table"
        if lanes and table:
            l, t = lane_orfs(lanes), table_orfs(table)
            shared = len(l & t)
            denovo = "yes" if shared and shared >= 0.5 * len(l) else "no"
            note = f"{shared:,}/{len(l):,} lane ORFs in a {len(t):,}-ORF table"
        meta = build_json(run)
        has_gem = (run / "gpr" / "gpr_gem.parquet").exists()
        rows.append({
            "run": run.name,
            "denovo": denovo,
            "gem": "yes" if has_gem else "no",
            # A run with no GEM has nothing to have adapted one from. w3110 records its
            # own name as the host of its de-novo build, which is not a GEM provenance.
            "gem_adapted_from": gem_chain(run.name, gem_source, meta) if has_gem else "",
        })
        print(f"  {run.name:<20} denovo={denovo:<4} {note}")

    df = pd.DataFrame(rows)
    print()
    print(df.to_string(index=False))
    if a.write:
        out = RUNS / "census.tsv"
        df.to_csv(out, sep="\t", index=False)
        print(f"\nwrote {out.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
