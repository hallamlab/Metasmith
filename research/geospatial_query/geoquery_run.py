#!/usr/bin/env python3
"""Run geoquery on one AOI table with metasmith and publish one CSV per source, with gaps and a README.

Needs metasmith (conda install -c conda-forge -c bioconda -c hallamlab metasmith) and Docker, by default.
--home, --runtime, --config and --params send the steps elsewhere, e.g. a Slurm cluster over ssh:

    python geoquery_run.py --aois my_aois.kml --out my_aois
    python geoquery_run.py --aois my_aois.kml --out my_aois --home ssh://cluster/scratch/me/geoquery \\
        --runtime apptainer --setup "module load apptainer" --config slurm --params slurm.yml
"""
import argparse, dataclasses, json, hashlib, re, shutil, subprocess, tarfile
from datetime import date
from pathlib import Path

import pandas as pd

from geoquery.aggregate import versions

TEMPLATE = "geoquery_from_aoi_table"
DICTIONARIES = ("feature_descriptions.csv", "class_descriptions.csv")
INTEGRATED = {"observations_long": "observations_long.parquet",
              "features_wide": "features_wide.parquet",
              "coverage": "coverage.csv"}
GAP_REASONS = {
    "not_applicable": "The source does not cover this AOI, for example a US-only source and a Canadian AOI. "
                      "geoquery did not query it.",
    "no_hits": "The source answered but holds no data for this AOI, for example no station or no image "
               "tile nearby.",
    "failed": "The source did not answer: it needs a login, its address is gone, or it timed out. "
              "A later run can succeed.",
}
# Caveats a reader of one source's table needs and the data cannot show.
NOTES = {"phac_wastewater": (
    "`phac_wastewater` loads are SARS-CoV-2 N2 gene copies/mL, the unit PHAC's own dashboard puts on "
    "this series (\"Viral load (copies/mL)\"). PHAC applies the same label to solid-fraction samples, "
    "which are often measured per gram. PHAC's open CSV ends 2024-06-28.")}


def type_dir(results: Path, type_name: str) -> Path:
    return results / f"geoquery-{type_name}"


def _files(results: Path, type_name: str) -> list[Path]:
    return sorted(p.resolve() for p in type_dir(results, type_name).glob("*"))


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def publish(results: Path, aoi_table: Path, out: Path, run: str, image: str, produced: date):
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    ledger = []

    def record(rel: str, stem: str, product: str, source: str):
        dst = out / rel
        assert dst.stat().st_size > 0, f"zero-byte output {rel}"
        ledger.append(dict(path=rel, stem=stem, product=product, run=run, source=source,
                           bytes=dst.stat().st_size, sha256=_sha256(dst)))

    def put(src: Path, rel: str, stem: str, product: str):
        (out / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, out / rel)
        record(rel, stem, product, src.name)

    put(aoi_table, f"aois/{aoi_table.name}", out.name, "aoi_table")
    (aois,) = _files(results, "aois")
    put(aois, "aois/aois.geojson", out.name, "aois")
    norm = pd.read_json(aois).get("features")
    pd.DataFrame([f["properties"] for f in norm]).to_csv(out / "aois.csv", index=False)
    record("aois.csv", out.name, "aois", aois.name)

    found, tables, frames = {}, [], []
    for t in _files(results, "source_table"):
        d = pd.read_csv(t, dtype=str)
        frames.append(d)
        src = d.source.iloc[0]
        family = re.sub(r"[^a-z0-9]+", "_", d.family.iloc[0].lower()).strip("_")
        found[src] = int((d.status == "found").sum())
        if found[src]:
            tables.append(f"{family}/{src}.csv")
            put(t, tables[-1], src, src)
    for name, fname in INTEGRATED.items():
        (f,) = _files(results, name)
        put(f, f"integrated/{fname}", out.name, name)

    (g,) = _files(results, "gaps")
    gaps = pd.read_csv(g, dtype=str).rename(columns={"reason": "detail", "gap": "reason"})
    (out / "_logs" / "runs").mkdir(parents=True)
    gaps[["aoi", "source", "reason", "detail"]].to_csv(out / "_logs" / "gaps.csv", index=False)
    record("_logs/gaps.csv", out.name, "gaps", g.name)
    pd.DataFrame(GAP_REASONS.items(), columns=["reason", "description"]).to_csv(
        out / "_logs" / "gaps_descriptions.csv", index=False)
    record("_logs/gaps_descriptions.csv", out.name, "gaps", "publish.py")
    versions(pd.concat(frames, ignore_index=True)).to_csv(out / "_logs" / "versions.csv", index=False)
    record("_logs/versions.csv", out.name, "versions", "publish.py")
    for d in DICTIONARIES:
        put(Path(__file__).parent / d, d, out.name, "dictionary")
    logs = (results / "_metadata" / "logs.latest").resolve()
    with tarfile.open(out / "_logs" / "runs" / f"{run}.tar.gz", "w:gz") as tar:
        tar.add(logs, arcname=run)
        tar.add(results / "_metadata" / "index.yml", arcname=f"{run}/index.yml")
    record(f"_logs/runs/{run}.tar.gz", run, "run_logs", "logs.latest")

    empty = sorted(s for s, n in found.items() if n == 0)
    counts = gaps.reason.value_counts()
    readme = [
        f"# {out.name}",
        "",
        f"Geospatial features for {len(norm)} areas of interest (AOIs) from {len(found)} public data sources. "
        f"geoquery produced it on {produced} (run `{run}`, image `{image}`).",
        "",
        "Start with `integrated/features_wide.parquet` for one row per AOI, or open a source's CSV below.",
        "",
        "## Files",
        "",
        "- `aois.csv`: the AOIs, one row each. `aois/` holds the AOI file as delivered and as normalised "
        "(GeoJSON, EPSG:4326).",
        "- `<family>/<source>.csv`: one table per source that found data, grouped by feature family.",
        "- `integrated/observations_long.parquet`: every value, one row each.",
        "- `integrated/features_wide.parquet`: one row per AOI, one column per feature, named `feature[class]` "
        "for a classed feature. Values are text. A `<column>_unit` column follows any column whose unit "
        "differs between AOIs.",
        "- `integrated/coverage.csv`: one row per AOI, one column per source: `found`, `unavailable` or "
        "`not-attempted`.",
        "- `_logs/gaps.csv`: every AOI and source with no data, and why. `_logs/gaps_descriptions.csv` explains "
        "each reason.",
        "- `_logs/versions.csv`: per source, the time asked for, the data times returned, and the release "
        "and address read.",
        "- `feature_descriptions.csv`: per feature, the upstream field it is read from, what geoquery does to it, "
        "and the source's own definition quoted verbatim with its URL, or blank with the reason. "
        "`class_descriptions.csv`: the same for each class value.",
        "- `_logs/ledger.csv`: size and sha256 of every file. `_logs/runs/`: the pipeline's run logs.",
        "",
        "## Columns in the source tables",
        "",
        "- `aoi`, `source`, `feature`: what was measured, where, and by which source.",
        "- `class`: the source's own category name for a feature reported per category, blank otherwise.",
        "- `value`: the value as text. `value_num`: the same value as a number, blank when it is not one.",
        "- `SI_unit`: the unit of that row's value, blank for categories, names and flags.",
        "- `family`, `observation_type`: the feature family and how the source observes (satellite, survey, "
        "station and so on).",
        "- `time`: the date of the data. `licence`: the source's licence. `native_res_m`: the source's "
        "resolution in metres, or `vector`.",
        "- `release`, `item`, `url`: the source edition, the item read (tile, file or record) and the address "
        "queried.",
        "- `status`, `reason`: `found`, or why the source has no data for that AOI.",
        "",
        "Each source returns its default moment, usually its latest data or a long-term average. Raster reads "
        "cap at 320 pixels a side, so a large AOI reads coarser than the source's native resolution.",
        "",
        "## Gaps",
        "",
        *[f"- `{k}` ({counts.get(k, 0)}): {v}" for k, v in GAP_REASONS.items()],
        "",
        *[f"{NOTES[s]}\n" for s in sorted(NOTES) if found.get(s)],
        "## Tables",
        "",
        *[f"- `{p}`" for p in sorted(tables)],
        "",
        f"Sources with no data for any AOI ({len(empty)}): {', '.join(empty) or 'none'}.",
        "",
    ]
    (out / "README.md").write_text("\n".join(readme))
    record("README.md", out.name, "readme", "publish.py")
    pd.DataFrame(ledger).to_csv(out / "_logs" / "ledger.csv", index=False)
    return dict(tables=len(tables), empty=len(empty), gaps=len(gaps))


def run(aoi_table: Path, out: Path, work: Path, sources: list[str] | None, plan_only: bool, timeout: float,
        home: str | None, runtime: str, setup: list[str], config: str | None, params: Path | None):
    from metasmith.python_api import Agent, Plan, Runtime, Source, SshSource, Template
    from metasmith.gui.stdlib import clone_stdlib

    # The installed package ships the library uncompiled; a clone is the compiled copy the GUI also plans from.
    work.mkdir(parents=True, exist_ok=True)
    root = Path(clone_stdlib(work)["path"])
    (template,) = [t for t in Template.Discover(root) if t.name == TEMPLATE]
    (case,) = template.cases
    adapters = [json.loads(v)["adapter"] for v in template.input_library["values"].values()]
    unknown = set(sources or []) - set(adapters)
    if unknown:
        raise SystemExit(f"unknown sources: {sorted(unknown)}")
    adapters = [a for a in adapters if not sources or a in sources]

    host, remote = "", ""
    if home and home.startswith("ssh://"):
        host, _, remote = home.removeprefix("ssh://").partition("/")
        remote = f"/{remote}"
    if host:
        agent = Agent(home=SshSource(host=host, path=Path(remote)).AsSource(), runtime=Runtime[runtime.upper()],
                      setup_commands=setup)
        given_table = f"{remote}/inputs/{aoi_table.name}"
    else:
        agent = Agent(home=Source.FromLocal(Path(home) if home else work / "msm_home"),
                      runtime=Runtime[runtime.upper()], setup_commands=setup)
        given_table = aoi_table
    if not plan_only:
        agent.Deploy()
        if host:
            # Tasks that replay the home's setup_commands fail their relay check; the client still runs them.
            subprocess.run(["ssh", host, f"sed -i '/^setup_commands:/,$d' {remote}/lib/agent.yml"
                                         f" && mkdir -p {remote}/inputs"], check=True)
            subprocess.run(["rsync", "-c", str(aoi_table), f"{host}:{given_table}"], check=True)
    givens = agent.PoolGivens()
    table = givens.Add(given_table, case.sample_type, name=f"{out.name}/aoi_table")
    for a in adapters:
        givens.Value(f"{out.name}/source/{a}", {"adapter": a}, "geoquery::source", parents=[table])
    inputs = givens.Build(work / "inputs.xgdb", type_library_paths=[root / "data_types" / "geoquery.yml"])
    task = Plan([dataclasses.replace(case, input_library=inputs)],
                template.transform_libraries, template.resource_libraries)
    if not task.ok:
        raise SystemExit(f"plan failed, dropped targets: {sorted(task.plan.dropped_targets)}")
    print(f"plan {task.GetKey()}: {len(adapters)} sources, {len(task.plan.steps)} transforms")
    if plan_only:
        return
    agent.StageWorkflow(task, on_exist="update")
    presets = agent.GetNxfConfigPresets()
    agent.RunWorkflow(task, config_file=presets.get(config, Path(config)) if config else None, params=params)
    result = agent.WaitForWorkflow(task, timeout_s=timeout, poll_s=10.0)
    print(f"run {result['status']} after {result['elapsed_s']:.0f}s")
    if result["status"] != "completed":
        raise SystemExit("\n".join(result["tail"]))
    results = Path(agent.GetResultSource(task).GetPath())
    if host:
        local = work / "results"
        subprocess.run(["rsync", "-aL", "--delete", f"{host}:{results}/", f"{local}/"], check=True)
        results = local
    missing = [t for t in case.target_types if not any(type_dir(results, t.removeprefix("geoquery::")).glob("*"))]
    if missing:  # a run that launched nothing can still report completed
        raise SystemExit(f"run produced no {missing}; see {results}/_metadata/logs.latest")
    env = (root / "resources" / "env" / "geoquery.env").read_text()
    image = re.search(r"^container: docker://(\S+)", env, re.M).group(1)
    print(f"published {out}: {publish(results, aoi_table, out, task.GetKey(), image, date.today())}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--aois", type=Path, required=True, help="AOI table, KML or GeoJSON")
    ap.add_argument("--out", type=Path, required=True, help="dataset folder to write; its name is the dataset id")
    ap.add_argument("--work", type=Path, help="metasmith work folder (default: <out>_work)")
    ap.add_argument("--sources", help="comma-separated subset of sources (default: all)")
    ap.add_argument("--plan-only", action="store_true", help="plan and stop")
    ap.add_argument("--timeout", type=float, default=4 * 3600, help="seconds to wait for the run")
    ap.add_argument("--home", help="metasmith agent home: a local path or ssh://<host>/<path> (default: <work>/msm_home)")
    ap.add_argument("--runtime", choices=["docker", "apptainer"], default="docker", help="container runtime on the home's host")
    ap.add_argument("--setup", action="append", default=[], help="shell line to run before metasmith on the home's host; repeatable")
    ap.add_argument("--config", help="Nextflow config: a metasmith preset name (local, slurm) or a config file (default: local)")
    ap.add_argument("--params", type=Path, help="YAML or JSON of Nextflow params for that config, e.g. slurmAccount")
    a = ap.parse_args()
    out = a.out.resolve()
    run(a.aois.resolve(), out, (a.work or out.with_name(out.name + "_work")).resolve(),
        a.sources.split(",") if a.sources else None, a.plan_only, a.timeout,
        a.home, a.runtime, a.setup, a.config, a.params.resolve() if a.params else None)


if __name__ == "__main__":
    main()
