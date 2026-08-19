from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


def repo_root() -> Path:
    for d in Path(__file__).resolve().parents:
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit("could not locate repo root (no ancestor contains data/fabfos)")


ROOT = repo_root()
BENCH = ROOT / "data" / "fabfos" / "benchmarks"
EXTRACT = BENCH / "_extractions"

SIDECARS = {
    "scales_tol": (
        "measured_fitness.tsv",
        EXTRACT / "scales_tol" / "extraction.tsv",
        ("gene", "gene_norm", "bnum", "fitness_15", "fitness_30", "phenotype"),
        "SCALEs ethanol-tolerance fitness, Woodruff et al. Metab Eng 15:124-133 (2013), "
        "doi:10.1016/j.ymben.2012.10.007. Host BW25113 delta-recA, MOPS minimal + 2 g/L "
        "glucose. Fitness is freq_final/freq_initial; the paper's threshold is > 1.",
    ),
    "scales_prod": (
        "measured_fitness.tsv",
        EXTRACT / "scales_prod" / "extraction.tsv",
        ("rank", "gene", "gene_norm", "fitness_prod", "fitness_wt15", "fitness_wt30",
         "named", "phenotype"),
        "SCALEs ethanol-production gene fitness at batch 8, Woodruff et al. Metab Eng "
        "17:1-11 (2013), doi:10.1016/j.ymben.2013.01.006. Host LW06 in AMX minimal. "
        "Recovered from an embedded chart cache; 4,100 of 4,103 rows are anonymous "
        "because the source workbook survives only as a dead OLE link.",
    ),
}

PRIOR = {("eydallin", "measured_glycogen.tsv")}


def carry(src: Path, cols: tuple[str, ...], dst: Path, header_note: str) -> int:
    rows = [l.rstrip("\n").split("\t") for l in src.open()]
    have = rows[0]
    missing = [c for c in cols if c not in have]
    if missing:
        raise SystemExit(f"{src}: no column(s) {missing}; found {have}")
    idx = [have.index(c) for c in cols]
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("w") as fh:
        fh.write(f"# {header_note}\n")
        fh.write("\t".join(cols) + "\n")
        for r in rows[1:]:
            fh.write("\t".join(r[i] for i in idx) + "\n")
    return len(rows) - 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="report what is present and missing; write nothing")
    a = ap.parse_args()

    problems = []
    for study, name in sorted(PRIOR):
        p = BENCH / study / "Y" / name
        state = "present" if p.exists() else "MISSING"
        print(f"  prior sidecar {study}/Y/{name}: {state}")
        if not p.exists():
            problems.append(
                f"{study}/Y/{name} is gone -- a study-tier publish replaced the folder "
                f"and no transform can regenerate it. Restore it from a backup or from "
                f"git history before continuing.")

    for study, (name, src, cols, note) in sorted(SIDECARS.items()):
        d = BENCH / study
        if not d.is_dir():
            problems.append(f"{study}: no study folder at {d} -- has the tier been built?")
            continue
        dst = d / "Y" / name
        if a.check:
            print(f"  {study}/Y/{name}: "
                  f"{'present' if dst.exists() else 'absent'} (source {src.name})")
            continue
        if not src.exists():
            problems.append(f"{study}: no extraction at {src}")
            continue
        if dst.exists():
            dst.unlink()
        n = carry(src, cols, dst, note)
        print(f"  wrote {dst.relative_to(ROOT)}  ({n:,} rows, {len(cols)} columns)")

    for p in problems:
        print(f"FAIL: {p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
