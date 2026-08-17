"""Put the continuous readouts back under `<study>/Y/`, after the tier has published.

    python research/fabfos/benchmarks/scales/write_y_sidecars.py [--check]

THE TIER PUBLISHES BY `rmtree` THEN `copytree`, so every study folder is replaced
wholesale on a study-tier build and anything in `Y/` that the tier did not write is
gone. `Y/expectations.tsv` and `Y/DEFAULT.md` come back; a sidecar does not. This script
is the other half of that arrangement -- run it after `--publish` -- and it is also why
`data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv` has to be backed up before any
full build rather than merely noticed afterwards.

WHY A SIDECAR AT ALL. `expectations.tsv` is categorical by construction: the tier's
answer key is a direction per (condition, element, metabolite), because that is what a
conductance readout can be scored against. Both SCALEs arms publish a CONTINUOUS fitness,
and the classifier arm's whole value is that its ~4,000 non-enriching genes are measured
negatives rather than absences -- a fact that a `flat` label preserves the existence of
but not the magnitude of. The sidecar carries the magnitude, in the shape the eydallin
phenotype sidecar established, and the folder-shape check tolerates it because that check
inspects the top level of a study folder only.

The two arms carry different quantities and the files say so in their own headers:

  scales_tol   fitness at 15 and 30 g/L exogenous ethanol, BW25113 delta-recA in MOPS
               minimal + 2 g/L glucose. 4,225 genes, of which 158 clear fitness 1 at
               15 g/L and 487 at 30 g/L -- the paper's own counts, which the extraction
               reproduces as a refusal.
  scales_prod  batch-8 production gene fitness, LW06 in AMX minimal. 4,103 ranks, of
               which 3 are nameable.

NEVER JOIN THE TWO BY VALUE. The later paper recalculated the earlier selections; the
ranges differ sixfold and a trial join matched 337 of 4,103. The two files sit in
different directories for that reason.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


def repo_root() -> Path:
    """Nearest ancestor of this file that contains `data/fabfos`."""
    for d in Path(__file__).resolve().parents:
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit("could not locate repo root (no ancestor contains data/fabfos)")


ROOT = repo_root()
BENCH = ROOT / "data" / "fabfos" / "benchmarks"
EXTRACT = BENCH / "_extractions"

# study -> (sidecar name, source extraction, columns to carry, what it is)
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

# The sidecar that predates this work and that no transform can regenerate. Listed so a
# full build's blast radius is named in code rather than remembered.
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
        # A published sidecar may be a read-only DVC hardlink into a cache several
        # worktrees share; replacing the link is the only safe write.
        if dst.exists():
            dst.unlink()
        n = carry(src, cols, dst, note)
        print(f"  wrote {dst.relative_to(ROOT)}  ({n:,} rows, {len(cols)} columns)")

    for p in problems:
        print(f"FAIL: {p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
