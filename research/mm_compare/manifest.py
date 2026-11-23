#!/usr/bin/env python3
"""Write data/mm_compare/manifest.tsv: one row per (dataset, sample, subject, method) output, with its path and provenance.

    manifest.py <repo root>

Paths are relative to the repo root. A PGDB or 4-lane row whose `path` is a per-sample table names its
subject in the table's `pgdb` or `mag` column.
"""
import csv
import json
import sys
from pathlib import Path

STRAINS = ("k12", "dh10b", "dh1", "w3110", "bw25113", "epi300")
CARVEME_E5 = "E5 run Zze0nvLA (batch 1, finished 2026-10-05); CarveMe 1.6.6 annotate BiGG, carve, gap-fill"
CARVEME_ECOLI = "E5 e5_gem replayed on fir, job 65074111 (MEMOTE 65078731), 2026-10-14; CarveMe 1.6.6"
PGDB = "Ryan, MetaPathways; chinook /Manuscripts/Science/2025-03-07_Engcyc/{sample}; resume_key {key}"
GPR_CAMI = "fabfos 4-lane (clean+kofam+pbert+uniref50), run {key}, metasmith 0.22.1 image, fir, 2026-10"
GPR_ECOLI = "fabfos 4-lane (clean+kofam+pbert+uniref50), run {key}, 2026-08-18 regen"


def rows(repo: Path):
    mm = repo / "data" / "mm_compare"
    rel = lambda p: str(p.relative_to(repo))
    samples = list(csv.DictReader(open(mm / "sample_map.tsv"), delimiter="\t"))
    mags = {}
    for r in csv.DictReader(open(mm / "inventory" / "carveme_models.tsv"), delimiter="\t"):
        if r["dataset"] == "cami_hmp":
            mags.setdefault(r["sample"], []).append(r["subject"])
    pgdbs = {}
    for r in csv.DictReader(open(mm / "inventory" / "pgdb_models.tsv"), delimiter="\t"):
        pgdbs.setdefault(r["sample"], []).append(r["subject"])
    gpr_key = (repo / "data" / "scratch" / "mm_compare_gpr" / "RUN_KEY")
    gpr_key = gpr_key.read_text().strip() if gpr_key.exists() else ""

    for s in samples:
        ryan, e5 = s["ryan_sample"], s["e5_sample"]
        key = json.loads((mm / "ryan_pgdb_pwy" / ryan / "compact-results.json").read_text())["resume_key"]
        for p in pgdbs.get(ryan, []):
            yield ("cami_hmp", ryan, e5, p, "pathway_tools:ryan_metapathways", rel(mm / "ryan_pgdb_tables" / f"{ryan}.reactions.tsv.gz"),
                   PGDB.format(sample=ryan, key=key[:12]))
        for m in mags.get(e5, []):
            yield ("cami_hmp", ryan, e5, m, "carveme", rel(mm / "e5_hmp_carveme" / e5 / m / "carveme_sbml.xml"), CARVEME_E5)
        gpr = mm / "gpr_4lane" / f"{e5}.parquet"
        if gpr.exists():
            for m in mags.get(e5, []):
                yield ("cami_hmp", ryan, e5, m, "fabfos_4lane", rel(gpr), GPR_CAMI.format(key=gpr_key))
    poolcheck = mm / "gpr_4lane" / "poolcheck.parquet"
    if poolcheck.exists():
        e5 = "toy_hmp_gastrooral_sample_4"
        ryan = next(s["ryan_sample"] for s in samples if s["e5_sample"] == e5)
        yield ("cami_hmp", ryan, e5, "1-21-1.f3cadf8affb7669b-AufqCiBP",
               "fabfos_4lane_poolcheck", rel(poolcheck), GPR_CAMI.format(key=gpr_key))

    for st in STRAINS:
        yield ("ecoli", st, st, st, "carveme", rel(mm / "ecoli_carveme" / st / "carveme_sbml.xml"), CARVEME_ECOLI)
        build = repo / "data" / "fabfos" / "runs" / f"e_coli_{st}" / "gpr"
        b = json.loads((build / "BUILD_denovo.json").read_text())
        yield ("ecoli", st, st, st, "fabfos_4lane", rel(build / "gpr_denovo.parquet"),
               GPR_ECOLI.format(key=Path(b["source"]).name.split("__")[0]))
        for route in sorted(d for d in (mm / "ecoli_pgdb").iterdir() if d.is_dir()):
            tables = route / f"{st}.reactions.tsv.gz"
            if tables.exists():
                yield ("ecoli", st, st, st, f"pathway_tools:{route.name}", rel(tables),
                       f"see {rel(route / 'PROVENANCE')}")


def main(repo: Path):
    out = repo / "data" / "mm_compare" / "manifest.tsv"
    out.unlink(missing_ok=True)
    with open(out, "w") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["dataset", "sample", "e5_sample", "subject", "method", "path", "provenance"])
        n = 0
        for r in rows(repo):
            w.writerow(r)
            n += 1
    print(n, "rows", file=sys.stderr)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
