from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402

BIGG_CACHE = C.CACHE / "biggM_bridge.parquet"
MODELS = {
    "e_coli_k12": C.ROOT / "data/fabfos/originals/genomes/e_coli_k12/GEM/iML1515.json",
    "e_coli_dh10b": C.ROOT / "data/fabfos/originals/genomes/e_coli_dh10b/GEM/iECDH10B_1368.json",
}


def bigg_bridge(force: bool = False) -> pd.DataFrame:
    if BIGG_CACHE.exists() and not force:
        return pd.read_parquet(BIGG_CACHE)
    x = pd.read_csv(C.CHEM_XREF, sep="\t", comment="#", header=None,
                    names=["source", "mnxm", "description"], dtype=str,
                    low_memory=False)
    x = x[x.source.str.startswith("biggM:", na=False)].copy()
    x["bigg"] = x.source.str[len("biggM:"):]
    out = x[["bigg", "mnxm"]].drop_duplicates()
    out.to_parquet(BIGG_CACHE, index=False)
    return out


_COMP = re.compile(r"_([a-z]{1,2})$")


def strip_compartment(bigg_id: str) -> str:
    return _COMP.sub("", bigg_id)


def load_model_json(host_dir: str) -> dict:
    return json.loads(MODELS[host_dir].read_text())


def biomass_reaction(model: dict) -> dict:
    cands = [r for r in model["reactions"]
             if r.get("objective_coefficient", 0) or "BIOMASS" in r["id"].upper()]
    obj = [r for r in cands if r.get("objective_coefficient", 0)]
    if obj:
        return obj[0]
    core = [r for r in cands if "core" in r["id"].lower()]
    return (core or cands)[0]


def biomass_precursors(host_dir: str) -> tuple[list, dict]:
    model = load_model_json(host_dir)
    rxn = biomass_reaction(model)
    subs = [m for m, coef in rxn["metabolites"].items() if coef < 0]
    br = bigg_bridge()
    by_bigg = br.groupby("bigg").mnxm.apply(list).to_dict()
    universe = C.atom_universe("C")
    mnxms, unmapped, off_universe = [], [], []
    for m in subs:
        cands = by_bigg.get(strip_compartment(m), [])
        if not cands:
            unmapped.append(m)
            continue
        inu = [c for c in cands if c in universe]
        if inu:
            mnxms.extend(inu)
        else:
            off_universe.append(m)
    diag = dict(reaction=rxn["id"], n_substrates=len(subs),
                n_mapped=len(subs) - len(unmapped),
                n_in_carbon_universe=len(subs) - len(unmapped) - len(off_universe),
                unmapped=unmapped, off_universe=off_universe)
    return sorted(set(mnxms)), diag


if __name__ == "__main__":
    for h in MODELS:
        p, d = biomass_precursors(h)
        print(h, json.dumps({k: v for k, v in d.items()
                             if k not in ("unmapped", "off_universe")}))
        print("   ", len(p), "distinct MNXM precursors in the C atom universe")
        print("    unmapped:", d["unmapped"])
        print("    off-universe:", d["off_universe"])
