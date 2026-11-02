#!/usr/bin/env python3
"""The Fuhrer deletion screen as the three tables `ecspr fba` reads.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/fuhrer/fba/build_tables.py [--probe]

THE POINT IS THAT THIS IS THE SAME COHORT, NOT A SIMILAR ONE. `sweeps/sweep_fuhrer_ratio.py`
reads the same host GPR table and the same deletion table and asks a conductance question of
them; this writes down the same 3,806 constructions as an `ecspr::conditions` table, so a
mutant measured both ways is one mutant. The shared reader is `benchmarks/fba_deletion.py`,
and everything general is documented there.

WHY FLUX BALANCE CAN BITE HERE AND COULD NOT ON THE EYDALLIN CLONES. Every clone in the 2010
library is a chromosomal ORF the host already carries, so a curated carve is a bit-exact
no-op and that arm is vacuous by construction. A deletion is the opposite: the gene's
reactions leave the model, biomass can go to zero, and the answer is a property of the
network rather than of the convention. 1,348 of the 3,806 screened deletions remove at least
one reaction of iML1515.

THE FRAME IS iML1515 AND NOTHING IS BORROWED. Fuhrer screened the Keio collection, whose
parent is BW25113, and iML1515 is a BW25113-curated model. The 2010 Eydallin arm had to
borrow DH1's model for AG1; the numbers here are not comparable to that arm's for a reason
that has nothing to do with the method.

**CAUTION** THE HOST GPR IS CURRENT AND THE COHORT GPR IS NOT.
`data/fabfos/runs/fuhrer_clones/gpr/` is one of the tables `tests/fabfos/test_gpr_schema.py`
still reports on a pre-schema layout, so its `llr` is a migration constant rather than a
calibrated weight. That does not reach this arm: the deletion is read off the cohort table as
a SET of ORFs and the carve runs on `min_lanes`, which is a lane count and reads no weight at
all. A `--carve-threshold` run would read one, and must wait for the rebuild.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

import fba_arm as D                                              # noqa: E402

AXES = D.ROOT / "data/fabfos/benchmarks/fuhrer/axes.tsv"
CENSUS = D.ROOT / "data/fabfos/runs/fuhrer_clones/gpr/mutant_census.tsv"

#: The denominator of the conductance arm's ratio. Not a readout of the screen, and carried
#: for the same reason it is carried there: every axis shares it, so a flux to CO2 measured
#: on the same models is the one column that says whether an axis moved or the whole carbon
#: budget did.
DENOMINATOR_AXIS = "denominator"


def cohort(roles=("target", "control")) -> D.Cohort:
    axes = pd.read_csv(AXES, sep="\t", dtype=str).fillna("")
    axes = axes[axes.role.isin(roles) | (axes.axis == DENOMINATOR_AXIS)]
    census = pd.read_csv(CENSUS, sep="\t").drop_duplicates("gene")
    screened = census[census.screened.astype(bool)]
    return D.Cohort(
        name="fuhrer",
        frame=D.ROOT / "data/fabfos/originals/genomes/e_coli_k12/GEM/iML1515.json",
        host_gpr=D.ROOT / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet",
        cohort_gpr=D.ROOT / "data/fabfos/runs/fuhrer_clones/gpr/gpr_gem.parquet",
        condition_ids=tuple(screened.condition_id),
        sinks=tuple((r["axis"], r["role"], r["sink_mnxm"], r["sink_name"])
                    for r in axes.to_dict("records")),
        out=HERE / "cache",
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--roles", nargs="+", default=["target", "control"],
                    help="which declared axes to emit. The precursor controls are the "
                         "sharpest control this panel has: a probe ranking a target and "
                         "its own immediate precursor alike has ranked the module rather "
                         "than the metabolite")
    ap.add_argument("--probe", action="store_true",
                    help="after writing, check each sink is reachable at all")
    ap.add_argument("--gapfill", action="store_true",
                    help="probe with the carve repaired. Read the carved set first: it is "
                         "this host's own genotype")
    a = ap.parse_args()
    c = cohort(tuple(a.roles))
    D.build(c)
    if a.probe:
        D.probe(c, do_gapfill=a.gapfill)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
