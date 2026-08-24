"""Paths, loaders and the MetaNetX bridge for the constructed-model FBA arm.

Reuses laser/vs_gem's MetaNetX machinery rather than restating it: parse_mnx_equation,
load_reac_prop and the four-tier MetBridge live there and are already exercised against
this same MetaNetX 4.5 pin.  Only the caches are local, because that module's cache dir
is gitignored and empty in this worktree.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
CACHE = HERE / "cache"
for _d in (OUT, CACHE):
    _d.mkdir(parents=True, exist_ok=True)

MNX = ROOT / "data/fabfos/originals/metanetx/4.5"
REAC_PROP = MNX / "reac_prop.tsv"
PROP_CACHE = CACHE / "chem_prop_slim.parquet"
BIGG_CACHE = CACHE / "biggM_bridge.parquet"

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
HET_PREFIX = "HET_"

_LASER = ROOT / "research/fabfos/benchmarks/laser/vs_gem"


def laser_bridge():
    """parse_mnx_equation / load_reac_prop / MetBridge, with our caches patched in."""
    sys.path.insert(0, str(_LASER))
    import common as LC
    LC.CACHE = CACHE
    LC.REAC_PROP = REAC_PROP
    import bridge as LB
    LB.BIGG_CACHE = BIGG_CACHE
    import fba_scaffold as FS
    FS.C = LC
    return FS, LB


def bigg_bridge() -> pd.DataFrame:
    return pd.read_parquet(BIGG_CACHE)


def chem_prop() -> pd.DataFrame:
    return pd.read_parquet(PROP_CACHE)


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


def host_gem_bigg() -> set:
    """Model reaction ids the AG1 host GPR asserts.  The AG1 genotype edits bind here:
    a reaction the borrow withheld (relA1 -> GTPDPK) is simply absent.

    Reads every row.  `projection_via` records how the row reached MetaNetX, not whether
    the reaction is in the model: 159 rows travel `embedded` and 13 `kegg`, and filtering
    to `bigg` drops 78 reactions the model does carry.  All rows name a model reaction id
    in `intermediate_id`, and together they cover 2,754 of the model's 2,755.
    """
    g = pd.read_parquet(HOST_GEM, columns=["intermediate_id"])
    return set(g.intermediate_id.astype(str))


def host_denovo_mnxr(min_lanes: int = 1) -> set:
    """MNXR the de-novo lanes assert for the host proteome.

    min_lanes >= 2 is the documented fix for the ProtBERT lane's promiscuity (REPORT.md);
    at 1 every single-lane claim counts.
    """
    g = pd.read_parquet(HOST_DENOVO, columns=["orf", "mnxr", "channel"])
    n = g.groupby("mnxr").channel.nunique()
    return set(n[n >= min_lanes].index.astype(str))


def clone_gem_rxns() -> dict:
    """lowercased condition_id -> sorted model reaction ids."""
    g = pd.read_parquet(CLONE_GEM, columns=["condition_id", "intermediate_id"])
    g["_k"] = g.condition_id.str.lower()
    return {k: sorted(set(v)) for k, v in
            g.groupby("_k").intermediate_id.apply(list).items()}


def clone_denovo_mnxr(min_lanes: int = 1) -> dict:
    """lowercased condition_id -> sorted MNXR, filtered by how many lanes agree."""
    g = pd.read_parquet(CLONE_DENOVO, columns=["condition_id", "mnxr", "channel"])
    g["_k"] = g.condition_id.str.lower()
    n = g.groupby(["_k", "mnxr"]).channel.nunique().reset_index()
    n = n[n.channel >= min_lanes]
    return {k: sorted(set(v)) for k, v in n.groupby("_k").mnxr.apply(list).items()}


def model_mnxr_map() -> dict:
    """Model reaction id -> MNXR, from the host GEM GPR's own crosswalk.

    Every row carries an MNXR whatever its `projection_via`, so no filter applies.
    """
    g = pd.read_parquet(HOST_GEM, columns=["intermediate_id", "mnxr"])
    g = g[g.mnxr.notna()]
    return dict(zip(g.intermediate_id.astype(str), g.mnxr.astype(str)))
