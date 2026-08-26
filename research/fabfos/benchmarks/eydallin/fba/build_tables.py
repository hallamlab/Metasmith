#!/usr/bin/env python3
"""The Eydallin 2007 deletion mutants as the three tables `ecspr fba` reads.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/eydallin/fba/build_tables.py [--probe]

THE ARM THE 2010 CLONES COULD NOT BE. `benchmarks/eydallin_fba/` measures the
gain-of-function library, where every clone is a chromosomal *E. coli* ORF the host already
carries -- a curated carve is a bit-exact no-op and the arm is vacuous by construction.
These 65 are deletions of the same pathway's genes, with a SIGNED phenotype
(glycogen_excess against glycogen_deficient), which is the two-sided question flux balance
can actually be asked.

It is also the one conductance arm that beat its own controls, so it is the arm the
comparison most needs on the page.

TWO SINKS, BECAUSE THE CONDUCTANCE ARM HAS TWO. `sweeps/sweep_aska_ratio.py` reads
C(glucose -> glycogen) / C(glycogen -> pyruvate); the quotient is what makes the readout
two-sided under Rayleigh. Flux balance has no such theorem to work around, but the two
metabolites are still measured separately so a movement in glycogen can be told from a
movement in the whole carbon budget.

**CAUTION** 27 OF THE 65 MUTANTS DELETE A GENE THIS FRAME DOES NOT MODEL. Their conditions
are emitted anyway and come out identical to the baseline. That keeps the population the 65
the paper measured rather than the 38 the model happens to cover, and it marks them
`is_control` -- a real perturbation that cannot move the measurement is what a noise floor
is made of.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

import fba_arm as D                                              # noqa: E402

CENSUS = D.ROOT / "data/fabfos/runs/eydallin_clones/gpr/lof/mutant_census.tsv"

#: The two hubs `sweeps/sweep_aska_ratio.py` reads, by MNXM so both arms aim at one
#: molecule. Glycogen is the readout; pyruvate is the ratio's denominator and is carried
#: here as a control for the same reason it is carried there.
SINKS = (("glycogen", "target", "MNXM738130", "glycogen"),
         ("pyruvate", "control", "MNXM23", "pyruvate"))


def cohort() -> D.Cohort:
    census = pd.read_csv(CENSUS, sep="\t").drop_duplicates("gene")
    return D.Cohort(
        name="eydallin2007",
        frame=D.ROOT / "data/fabfos/originals/genomes/e_coli_k12/GEM/iML1515.json",
        host_gpr=D.ROOT / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet",
        cohort_gpr=D.ROOT / "data/fabfos/runs/eydallin_clones/gpr/lof/gpr_gem.parquet",
        condition_ids=tuple(census.condition_id),
        sinks=SINKS,
        out=HERE / "cache",
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--probe", action="store_true",
                    help="after writing, check each sink is reachable at all")
    ap.add_argument("--gapfill", action="store_true",
                    help="probe with the carve repaired. Read the carved set first")
    a = ap.parse_args()
    c = cohort()
    D.build(c)
    if a.probe:
        D.probe(c, do_gapfill=a.gapfill)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
