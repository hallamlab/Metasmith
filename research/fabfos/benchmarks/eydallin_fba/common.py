"""Paths and the measured phenotype for the constructed-model FBA arm.

THE LOADERS LEFT. Reading a GPR table, carving a frame with it, bridging MetaNetX onto that
frame and closing the cycles insertion creates are now `ecspr.model.fba` and
`ecspr.model.mnx`, because the conductance arm has to read the same tables and a second
copy of the reader is a second answer. What stays here is what is specific to this study:
where its files are, and what the 2010 screen measured.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
CACHE = HERE / "cache"
for _d in (OUT, CACHE):
    _d.mkdir(parents=True, exist_ok=True)

MNX = ROOT / "data/fabfos/originals/metanetx/4.5"

RUNS = ROOT / "data/fabfos/runs"
GEM_DH1 = ROOT / "data/fabfos/originals/genomes/e_coli_dh1/GEM/iECDH1ME8569_1439.json"

HOST_GEM = RUNS / "e_coli_ag1/gpr/gpr_gem.parquet"
HOST_DENOVO = RUNS / "e_coli_ag1/gpr/gpr_denovo.parquet"
CLONE_GEM = RUNS / "eydallin_clones/gpr/gpr_gem.parquet"
CLONE_DENOVO = RUNS / "eydallin_clones/gpr/gpr_denovo.parquet"
CLONE_CENSUS = RUNS / "eydallin_clones/gpr/clone_gem_census.tsv"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"

GLYCOGEN_MET = "glycogen_c"
DEMAND = "DM_glycogen_c"


def load_model():
    import cobra
    return cobra.io.load_json_model(str(GEM_DH1))


def measured() -> pd.DataFrame:
    """Fig. 1, keyed on a lowercased condition_id.

    The clone tables write `eydallin:ppK` and this table writes `eydallin:ppk`, so an exact
    join loses one condition.  Lowercasing recovers it and changes nothing else.
    """
    d = pd.read_csv(MEASURED, sep="\t")
    d["_k"] = d.condition_id.str.lower()
    return d
