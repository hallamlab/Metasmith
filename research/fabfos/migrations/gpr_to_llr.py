# Every committed GPR table onto the current schema, in one pass.
#
# THE PROBLEM   `load_gpr` has required `llr` since the calibration landed, and all 58
#               committed GPR parquets carried `raw_score`. Nothing in the tree ran on
#               committed data. The same pass adds `genome_unit`, `gene` and
#               `llr_route`, so a table is rewritten once rather than four times.
#
# FOUR ROUTES, and which one a table takes is a property of what still exists beside it:
#
#   reparse    The lane files are pinned, so the mapper can be re-run and the weight is
#              MEASURED. `annotation_lanes/calibration/reparse_run.py` does it, calling
#              the shipped mapper the way the transform does.
#   derive     No proteome of its own; the table is projected from a host that has one.
#              `build_references/derive_{ag1,lw06}_denovo.py`.
#   assert     Every row is on an assertion channel, so its weight is a DECLARED
#              constant and the migration is exact. `to_unified` alone.
#   migrate    Neither lane files nor a projection. `to_unified` assigns
#              UNCALIBRATED_LLR and the rows say `llr_route = migrated`, which is what
#              stops a placeholder being read as a measurement.
#
# **CAUTION** THE NUMBERS MOVE. A calibrated call is worth 3 to 9 nats where the retired
# constant was worth 1, so every committed measurement produced on `raw_score` will rank
# differently. That is the point of the migration, not a regression in it.
#
# **WARNING** `--apply` overwrites DVC-pinned tables in place. Run without it first: the
# default writes to `--out` and prints the same report.
#
#     mamba run -n msm python research/fabfos/migrations/gpr_to_llr.py --out /tmp/mig
#     mamba run -n msm python research/fabfos/migrations/gpr_to_llr.py --apply
from __future__ import annotations

import argparse
import glob
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(REPO / "src" / "metasmith_libraries" / "resources" / "lib"))
import fabfos_evidence as fe                                          # noqa: E402

GPR_GLOBS = (
    "data/fabfos/runs/*/gpr/*.parquet",
    "data/fabfos/nostoc/annotation/*/*.parquet",
    "data/fabfos/nostoc/ecspr/networks/*/gpr.parquet",
    "data/fabfos/benchmarks/*/gpr*.parquet",
)

# NOT EVIDENCE TABLES. Reaction SETS -- mnxr x channel x origin, no nominator and no
# weight -- from an older host-plus-insert union arm. The schema describes nominations,
# so there is nothing here for it to describe. Named rather than filtered by column, so
# a NEW table that happens to lack a nominator is a failure rather than a silent skip.
NOT_EVIDENCE = {
    "data/fabfos/runs/e_coli_epi300/gpr/gpr_epi300_clone2.parquet",
    "data/fabfos/runs/e_coli_epi300/gpr/gpr_union_epi300_183.parquet",
}

# BUILD PRODUCTS. `nostoc_ecspr.py --compose` writes these from the three
# `nostoc/annotation/*/gpr_4lane.parquet` it takes as input, so migrating them by hand
# would produce a network that no longer matches its own members. Migrate the members,
# then re-run the composer -- which is T12's job, not this one's.
REGENERATE = "data/fabfos/nostoc/ecspr/networks/*/gpr.parquet"

# WHICH COLUMN HOLDS THE GENE, per table, because there is no rule that covers them all
# and guessing would populate one ontology from another's namespace:
#
#   gpr_gem            `orf` is a BiGG REACTION id on a ruleless row; `feature_name` is
#                      the model gene where the model gave a rule.
#   gpr_manual         `orf` IS the gene symbol; `feature_name` is prose
#                      ("ADP-sugar pyrophosphatase").
#   gpr_insertion      `orf` is the construct's gene.
#   aska denovo        `orf` is a long ORF id; `feature_name` carries the symbol.
#   eydallin clones    `orf` is the gene, and `feature_name` is null throughout.
#
# A de-novo host table takes no entry: the mapper reads `[gene=...]` out of the FASTA
# header, so the reparse route populates the column at its source.
GENE_COLUMN = {
    "gpr_gem.parquet": "feature_name",
    "gpr_manual.parquet": "orf",
    "gpr_insertion.parquet": "orf",
    "data/fabfos/runs/aska/gpr/gpr_denovo.parquet": "feature_name",
    "data/fabfos/runs/eydallin_clones/gpr/gpr_denovo.parquet": "orf",
}

# The runs whose lane files are pinned beside them, and how to reach each one. A run
# with no entry has no `annotations.dvc` at all -- its lanes were never pinned, so no
# amount of pulling recovers them.
REPARSE = {
    "e_coli_bw25113": {}, "e_coli_dh1": {}, "e_coli_dh10b": {},
    "e_coli_epi300": {}, "e_coli_k12": {}, "e_coli_w3110": {},
    # A cohort has no proteome under originals/genomes -- its ORFs are clone inserts --
    # so it names its FASTA and stops after the mapper. The blocks its table carries are
    # the collector's, and the collector cannot rebuild a cohort's condition columns;
    # `--graft-onto` carries the reparsed weight onto them by grain key instead.
    "eydallin_clones": {"orfs": "annotations/eydallin_clones.faa",
                        "graft": "gpr/gpr_denovo.parquet"},
}

# THE TWO SCADC RUNS ARE NOT REPARSABLE AS A MIGRATION, and this is the finding rather
# than an omission. Their lane files ARE pinned, and `migrations/scadc_lanes_adapter.py`
# puts them in the layout the mapper reads -- but the committed
# `scadc_fosmids/gpr_4lane.parquet` holds 63,914 rows and the same lanes through the
# current mapper produce 28,298. The cut-offs moved under it: `PBERT_NN_MIN` is new and
# abstains on 2,698 of 5,395 ORFs, `CLEAN_MIN_SCORE` drops 4,513 of 6,587 CLEAN calls,
# and kofam now needs a family threshold that these files write as `-`.
#
# Re-running the mapper would therefore REPLACE these tables, not reweight them, and
# replacing a published table with a different measurement is a decision about the
# study, not about the schema. They migrate with `llr_route = migrated`, which says
# exactly what their weight is worth. The 19.7M-row metagenome is the same case and
# would additionally cost a 1.44M x 222k similarity pass.

# A run with no proteome of its own, and the host whose table it is projected from. Both
# borrow because they are absent from NCBI. The derivation is a WITHHOLDING -- the genes
# the genotype broke -- so it must run against the REPARSED source or it would project
# a calibrated strain from an unweighted one.
DERIVE = {
    "e_coli_ag1": ("e_coli_dh1", "derive_ag1_denovo.py"),
    # BW25113, not K-12: LW06 is a Keio-parent derivative and the script's own PARENT
    # says so. Borrowing K-12 would project a strain from a genome it is not descended
    # from, and the genotype check that catches it runs on the proteome, not here.
    "e_coli_lw06": ("e_coli_bw25113", "derive_lw06_denovo.py"),
}


def _write(df: pd.DataFrame, dest: Path) -> None:
    """Write beside the target, then rename over it.

    **WARNING** NEVER WRITE THROUGH A DVC-CHECKED-OUT FILE. This repo's cache links with
    `hardlink,symlink`, so a table in `data/` is one inode with 19 or 24 names -- the
    cache entry and every other checkout that shares its content. Opening it for writing
    would edit the CACHE, silently corrupting every other file pointing at the same
    content and leaving `dvc status` insisting everything is fine.

    Renaming replaces the directory ENTRY instead. The old inode keeps its other links,
    the cache is untouched, and `dvc add` re-links the new content afterwards. This is
    what `dvc unprotect` does, without needing dvc on PATH.
    """
    import os
    tmp = dest.with_name(dest.name + ".migrating")
    df.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, dest)


def gene_column_for(rel: str) -> "str | None":
    if rel in GENE_COLUMN:
        return GENE_COLUMN[rel]
    return GENE_COLUMN.get(Path(rel).name)


def classify(rel: str, df: pd.DataFrame) -> str:
    if rel in NOT_EVIDENCE:
        return "skip"
    if Path(rel).match(REGENERATE):
        return "regenerate"
    run = Path(rel).parts[3] if rel.startswith("data/fabfos/runs/") else None
    if run in REPARSE and Path(rel).name in (
            "gpr_denovo.parquet", "gpr_denovo_mapper.parquet"):
        return "reparse"
    if run in DERIVE and Path(rel).name == "gpr_denovo.parquet":
        return "derive"
    if set(df["channel"].map(fe.normalise_channel)) <= set(fe.ASSERTION_CHANNELS):
        return "assert"
    return "migrate"


def convert(path: Path, rel: str) -> pd.DataFrame:
    """One table through `to_unified`, on the blocks it already carries."""
    df = pd.read_parquet(path)
    ext = fe.extensions_of(df)
    out = fe.to_unified(df, ext, gene=gene_column_for(rel))
    if len(out) != len(df):
        raise SystemExit(f"[migrate] {rel}: {len(df):,} rows in, {len(out):,} out")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path,
                    default=Path(__file__).resolve().parent / "out")
    ap.add_argument("--apply", action="store_true",
                    help="overwrite the DVC-pinned tables in place")
    ap.add_argument("--only", default="", help="substring filter on the path")
    ap.add_argument("--threads", type=int, default=8)
    a = ap.parse_args()

    files = sorted({f for g in GPR_GLOBS for f in glob.glob(str(REPO / g))})
    files = [f for f in files if a.only in f]

    # THE REPARSES FIRST, one per run rather than one per table: both of a run's tables
    # come out of the same mapper invocation, and running it twice would produce two
    # tables whose pbert lane disagrees -- the kNN vote is not bit-reproducible.
    reparsed: dict[str, Path] = {}
    ledger = []
    for run, spec in REPARSE.items():
        if a.only and a.only not in f"data/fabfos/runs/{run}/":
            continue
        dest = a.out / "reparse"
        argv = [sys.executable,
                str(REPO / "research/fabfos/annotation_lanes/calibration/reparse_run.py"),
                "--host", run, "--out", str(dest), "--threads", str(a.threads)]
        if "orfs" in spec:
            graft = f"data/fabfos/runs/{run}/{spec['graft']}"
            argv += ["--orfs", str(REPO / "data/fabfos/runs" / run / spec["orfs"]),
                     "--graft-onto", str(REPO / graft)]
            gcol = gene_column_for(graft)
            if gcol:
                argv += ["--gene-column", gcol]
        print(f"\n=== reparse {run}", flush=True)
        subprocess.run(argv, check=True)
        reparsed[run] = dest / run

    # THEN THE DERIVATIONS, off the reparsed sources rather than the committed ones. A
    # borrow that read the pinned table would project a calibrated strain from an
    # unweighted one and say nothing about it.
    derived: dict[str, Path] = {}
    for run, (src_run, script) in DERIVE.items():
        if a.only and a.only not in f"data/fabfos/runs/{run}/":
            continue
        if src_run not in reparsed:
            print(f"\n=== derive {run}: SKIPPED, {src_run} was not reparsed this run",
                  flush=True)
            continue
        out_dir = a.out / "derive"
        print(f"\n=== derive {run} from the reparsed {src_run}", flush=True)
        subprocess.run(
            [sys.executable, str(REPO / "src/fabfos/build_references" / script),
             "--from", str(reparsed[src_run] / "gpr_denovo.parquet")], check=True)
        built = REPO / "src/fabfos/build_references/out" / run / "gpr_denovo.parquet"
        if not built.exists():
            raise SystemExit(f"[derive] {script} wrote no {built}")
        (out_dir / run).mkdir(parents=True, exist_ok=True)
        (out_dir / run / "gpr_denovo.parquet").write_bytes(built.read_bytes())
        derived[run] = out_dir / run

    for f in files:
        rel = str(Path(f).relative_to(REPO))
        df = pd.read_parquet(f)
        route = classify(rel, df)
        n_in = len(df)
        if route in ("skip", "regenerate"):
            ledger.append(dict(table=rel, route=route, rows_in=n_in, rows_out=None))
            print(f"{route:<10} {n_in:>10,}  {rel}")
            continue
        if route in ("reparse", "derive"):
            run = Path(rel).parts[3]
            built = (reparsed if route == "reparse" else derived).get(run)
            if built is None:
                raise SystemExit(f"[{route}] nothing was built for {run}")
            src = built / Path(rel).name
            if not src.exists():
                raise SystemExit(f"[{route}] {run} produced no {Path(rel).name}")
            out = pd.read_parquet(src)
        else:
            out = convert(Path(f), rel)
        if len(out) != n_in:
            raise SystemExit(f"[{route}] {rel}: {n_in:,} rows in, {len(out):,} out. "
                             f"A migration that changes the row set is not a migration")
        routes = sorted(set(out["llr_route"]))
        dest = Path(f) if a.apply else a.out / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        _write(out, dest)
        ledger.append(dict(table=rel, route=route, rows_in=n_in, rows_out=len(out),
                           llr_routes=routes,
                           llr_median=round(float(out["llr"].median()), 4),
                           genes=int(out["gene"].notna().sum())))
        print(f"{route:<10} {n_in:>10,}  {rel}\n"
              f"{'':22}llr med {out['llr'].median():.4g}  {'/'.join(routes)}  "
              f"{int(out['gene'].notna().sum()):,} rows name a gene")

    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "MIGRATION.json").write_text(json.dumps(dict(
        applied=bool(a.apply), n_tables=len(ledger), tables=ledger), indent=2))
    counts: dict = {}
    for e in ledger:
        counts[e["route"]] = counts.get(e["route"], 0) + 1
    print(f"\n{len(ledger)} tables: {counts}")
    print(f"ledger -> {a.out / 'MIGRATION.json'}"
          + ("" if a.apply else f"\ntables -> {a.out} (not applied; pass --apply)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
