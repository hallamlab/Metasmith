#!/usr/bin/env python3
"""The SCADC fosmid inserts as the tables `ecspr fba` reads, over the conductance arm's
own conditions table.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/scadc/fba/build_tables.py [--probe]

THE ONE COHORT WHERE NOTHING HAD TO BE REWRITTEN. `research/fabfos/examples/scadc_ecspr_arm.py`
built its constructions through `ecspr conditions`, so
`data/fabfos/runs/scadc_fosmids/ecspr/conditions.parquet` is already the arm-neutral table
both arms want. This reads that file rather than rebuilding an equivalent one, which is the
difference between two arms measuring one construction and two arms measuring two
constructions that were written to agree.

THE THIRD SHAPE OF PERTURBATION. Eydallin's clones add a reaction the host already has and
are vacuous; Fuhrer and Eydallin 2007 delete one. A fosmid insert adds reactions the host
GENUINELY LACKS -- 93.4% of the MNXR the inserts assert are absent from the EPI300 GEM -- so
this is the cohort that exercises the MetaNetX insertion, its four admission rules and the
cycle closure. It is also therefore the only one where `--mnx` is not optional: without it
every insert measures the same untouched carve and the arm silently becomes the vacuous
case.

**CAUTION** THE FOUR SINKS ARE CENTRAL-METABOLISM HUBS -- acetyl-CoA, malonyl-CoA,
oxaloacetate and AMP -- chosen because a carbon-flow probe needs a sink the host can reach.
Maximum flux to such a hub is bounded by uptake rather than by the pathway, so run `--probe`
and read what the unperturbed host already carries before reading any insert's delta as a
result.

**CAUTION** THE STAGED NAMES MATTER. A conditions table names its tables by BASENAME, and
the fosmid pool and three host tables are all `gpr_4lane.parquet` or `gpr_gem.parquet` where
they live. `stage()` copies them under the names the committed conditions table uses;
`load_corpus` refuses a collision rather than picking one.

**CAUTION** SCADC HAS NO LABEL SET. There is no `Y/` for these inserts, so this arm cannot
report accuracy against a phenotype. What it can report is CONCORDANCE between the two arms
and null clearance for each, which is a real comparison without ground truth.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

import fba_arm as D                                                   # noqa: E402
from ecspr.model import tests as test_mod                             # noqa: E402

CACHE = HERE / "cache"
TABLES = CACHE / "tables"

SRC_HOST = D.ROOT / "data/fabfos/runs/e_coli_epi300/gpr/gpr_gem.parquet"
SRC_FOSMIDS = D.ROOT / "data/fabfos/runs/scadc_fosmids/gpr/gpr_4lane.parquet"
CONDITIONS = D.ROOT / "data/fabfos/runs/scadc_fosmids/ecspr/conditions.parquet"
ECSPR_TESTS = D.ROOT / "data/fabfos/runs/scadc_fosmids/ecspr/tests.parquet"

#: The names the committed conditions table gives them.
T_HOST = "epi300_gem.parquet"
T_FOSMIDS = "scadc_fosmids_4lane.parquet"

BASELINE_ID = "epi300_host"


def stage() -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    for src, name in ((SRC_HOST, T_HOST), (SRC_FOSMIDS, T_FOSMIDS)):
        shutil.copyfile(src, TABLES / name)
        n = len(pd.read_parquet(TABLES / name))
        print(f"[stage] {name}: {n:,} rows from {src.relative_to(D.ROOT)}")


def sinks() -> tuple:
    """The sinks the conductance arm measured, read off its own committed tests table.

    NOT RE-DECLARED HERE. The terminals were resolved against a bake; taking them from the
    committed table means this arm reads no bake product and still aims at exactly the
    molecules the other arm aimed at.
    """
    t = pd.read_parquet(ECSPR_TESTS)
    hubs = sorted({h for row in t.sink_hub.dropna().astype(str)
                   for h in row.split("|") if h})
    return tuple((h, "target", h, h) for h in hubs)


def cohort() -> D.Cohort:
    return D.Cohort(
        name="scadc",
        frame=D.ROOT / "data/fabfos/originals/genomes/e_coli_dh10b/GEM/iECDH10B_1368.json",
        host_gpr=TABLES / T_HOST,
        cohort_gpr=SRC_FOSMIDS,
        condition_ids=(),
        sinks=sinks(),
        baseline_id=BASELINE_ID,
        conditions_path=CONDITIONS,
        extra_gpr=(TABLES / T_FOSMIDS,),
        # AN INSERT IS ONE genome_unit AND 9 TO 81 ORFs, and the null draws ORFs -- see
        # `ecspr draw --size-from`. Sizing the observation the same way is what lets the
        # size-matched p find a stratum at all; on unit count it found none and every
        # `p_stratum` came back NaN.
        size_column=("orf", T_FOSMIDS),
        out=CACHE,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--probe", action="store_true")
    a = ap.parse_args()
    stage()
    c = cohort()
    D.build(c)
    # An insert is not a control the way a gene with no reaction is: whether it changes
    # the model depends on the insertion rules, which is a run-time answer. The census
    # counts the empty differences, and that count is what says how many were vacuous.
    if a.probe:
        D.probe(c)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
