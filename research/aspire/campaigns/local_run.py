#!/usr/bin/env python3
"""Run one ASPIRE transform locally in docker on a campaign's retrieved intermediates.

    python research/aspire/campaigns/local_run.py <row> [<row> ...] [--src DIR] [--home AGENT_HOME]

No solver and no Nextflow: each row goes through `metasmith run` against a scratch data library
built over the files in --src, plus the image and script resources. Each requirement is bound
by type to the one item of that type, so the rows run in dependency order and a row's products
join the library for the rows after it. The source files are a campaign's analysis tables
retrieved from sockeye; a row whose input no earlier row produced and --src lacks fails by name.
"""

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
MLIB = REPO / "src" / "metasmith_libraries"
sys.path.insert(0, str(REPO / "src"))

from metasmith.models.direct_run import RunTransform  # noqa: E402
from metasmith.python_api import DataInstanceLibrary  # noqa: E402

# The retrieved file names a campaign's results carry, by type. A survey slot takes the sheet
# and an abundance slot the curated counts, as the planner wires them in an ASPIRE study.
SOURCES = {
    "aspire::study_metadata": "study_metadata.tsv",
    "aspire::analysis_counts": "analysis_counts.tsv",
    "aspire::analysis_metadata": "analysis_metadata.tsv",
    "aspire::analysis_asv_meta": "analysis_asv_meta.tsv",
    "aspire::counts_clean": "counts_clean.tsv",
    "aspire::counts_removed": "counts_removed.tsv",
    "aspire::read_fate": "read_fate.tsv",
    "aspire::sample_measurements": "sample_measurements.tsv",
    "aspire::mag_collection": "mag_collection",
    "amplicon::asv_taxonomy": "asv_taxonomy.tsv",
}
STANDS_IN = {"amplicon::survey": "aspire::study_metadata",
             "amplicon::abundance_table": "aspire::analysis_counts"}
TYPE_FILES = ("env.yml", "lib.yml", "aspire.yml", "amplicon.yml", "sequences.yml")


def build_library(work: Path, src: Path, produced: dict[str, str]) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(work / "inputs.xgdb")
    lib.Purge()
    for t in TYPE_FILES:
        lib.AddTypeLibrary(MLIB / "data_types" / t)
    lib.AddItem(MLIB / "resources" / "env" / "aspire.env", "env::aspire.env")
    lib.AddItem(MLIB / "resources" / "lib" / "aspire", "lib::aspire")
    study = lib.AddItem(src / SOURCES["aspire::study_metadata"], "aspire::study_metadata")
    for dtype, name in SOURCES.items():
        if dtype != "aspire::study_metadata" and (src / name).exists():
            lib.AddItem(src / name, dtype, parents={study})
    for dtype, path in produced.items():
        if Path(path).exists():
            lib.AddItem(Path(path), dtype, parents={study})
    lib.Save()
    return lib


def bindings(lib: DataInstanceLibrary, transform: Path) -> list[tuple[str, str]]:
    from metasmith.python_api import TransformInstanceLibrary
    tlib = TransformInstanceLibrary.ResolveParentLibrary(transform)
    inst = tlib.GetTransform(transform.relative_to(Path(tlib.location).resolve()))
    by_props = {}
    for path, dtype in lib.manifest.items():
        by_props.setdefault(frozenset(lib.GetType(dtype).properties), []).append((dtype, path))
    out = []
    for name in inst.BindableNames():
        dep = inst._dep_names[name]
        hits = by_props.get(frozenset(dep.properties), [])
        if not hits:
            for want, stand_in in STANDS_IN.items():
                if frozenset(lib.GetType(want).properties) == frozenset(dep.properties):
                    hits = by_props.get(frozenset(lib.GetType(stand_in).properties), [])
        assert hits, f"[{transform.stem}] needs [{name}] and nothing in the library is that type"
        assert len(hits) == 1, f"[{transform.stem}] [{name}] is ambiguous: {hits}"
        out.append((name, str(hits[0][1])))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("rows", nargs="+")
    ap.add_argument("--src", type=Path, default=REPO / "cache" / "aspire" / "t6_cyano")
    ap.add_argument("--work", type=Path, default=REPO / "cache" / "aspire" / "local_runs")
    ap.add_argument("--home", type=Path, default=Path(os.environ.get("AGENT_HOME", Path.home() / "msm.M0mGIjKq")))
    ap.add_argument("--cpus", type=int, default=4)
    ap.add_argument("--memory", type=int, default=8)
    args = ap.parse_args()

    args.work.mkdir(parents=True, exist_ok=True)
    ledger = args.work / "produced.json"
    produced = json.loads(ledger.read_text()) if ledger.exists() else {}
    failed = []
    for row in args.rows:
        transform = (MLIB / "transforms" / "aspire" / f"{row}.py").resolve()
        lib = build_library(args.work, args.src.resolve(), produced)
        out = args.work / row
        shutil.rmtree(out, ignore_errors=True)
        os.environ["METASMITH_WORK_ROOT"] = str(out)
        result = RunTransform(transform, lib, bindings(lib, transform), work_dir=out,
                              agent_home=args.home, cpus=args.cpus, memory=args.memory)
        for group in result.manifest:
            for dep, path in group.items():
                produced[_dtype_of(lib, dep)] = str(path)
        ledger.write_text(json.dumps(produced, indent=1))
        print(f"[{row}] success={result.success}", flush=True)
        if not result.success:
            failed.append(row)
    return 1 if failed else 0


def _dtype_of(lib: DataInstanceLibrary, dep) -> str:
    want = frozenset(dep.properties)
    for ns, tlib in lib.types.items():
        for name, endpoint in tlib.types.items():
            if frozenset(endpoint.properties) == want:
                return f"{ns}::{name}"
    raise KeyError(f"no type with properties {sorted(map(str, want))}")


if __name__ == "__main__":
    raise SystemExit(main())
