"""MetaNetX readers and the equation parser the reconstruction lane is built on.

Lifted from `research/fabfos/benchmarks/laser/vs_gem/fba_scaffold.py` and
`bridge.py` in curation round 5. `parse_mnx_equation` came across unchanged;
`load_reac_prop`, `bigg_bridge` and `strip_compartment` took their input paths as
arguments, where the research versions read module-level constants pointing five
directories up at one checkout's `data/`. A transform can only be handed a path,
which is the whole reason the lift was needed.
"""
from __future__ import annotations

import re

import pandas as pd

# One term of a MetaNetX equation: a stoichiometric coefficient, an MNXM, and the
# compartment it is in.
_TERM = re.compile(r"^\s*(\d+(?:\.\d+)?)\s+(MNXM\S+?)@(MNXD\d+)\s*$")
_COMPARTMENT_SUFFIX = re.compile(r"_([a-z]{1,2})$")

REAC_PROP_COLUMNS = ["mnxr", "equation", "reference", "classifs", "is_balanced", "is_transport"]
CHEM_PROP_COLUMNS = ["mnxm", "name", "reference", "formula", "charge", "mass",
                     "inchi", "inchikey", "smiles"]


def parse_mnx_equation(eq: str) -> dict[str, float] | None:
    """Stoichiometry of a single-compartment MetaNetX equation, or None.

    None means "not usable here", not "malformed": a transport reaction spans two
    compartments and has no single-compartment stoichiometry to give, and a term
    this does not recognise makes the whole equation untrustworthy rather than
    partially usable.
    """
    if not isinstance(eq, str) or "=" not in eq:
        return None
    lhs, rhs = eq.split("=", 1)
    out: dict[str, float] = {}
    comps = set()
    for side, sign in ((lhs, -1.0), (rhs, 1.0)):
        for term in side.split("+"):
            term = term.strip()
            if not term:
                continue
            m = _TERM.match(term)
            if not m:
                return None
            coef, mnxm, comp = float(m.group(1)), m.group(2), m.group(3)
            comps.add(comp)
            out[mnxm] = out.get(mnxm, 0.0) + sign * coef
    if len(comps) > 1:
        return None
    return {k: v for k, v in out.items() if v != 0.0} or None


def load_reac_prop(path, needed: set[str] | None = None) -> dict[str, dict]:
    frame = pd.read_csv(path, sep="\t", comment="#", header=None,
                        names=REAC_PROP_COLUMNS, dtype=str, low_memory=False)
    if needed is not None:
        frame = frame[frame.mnxr.isin(needed)]
    return {
        r.mnxr: dict(equation=r.equation, is_balanced=r.is_balanced,
                     is_transport=r.is_transport)
        for r in frame.itertuples(index=False)
    }


def load_chem_prop(path, needed: set[str] | None = None) -> pd.DataFrame:
    frame = pd.read_csv(path, sep="\t", comment="#", header=None,
                        names=CHEM_PROP_COLUMNS, dtype=str, low_memory=False)
    if needed is not None:
        frame = frame[frame.mnxm.isin(needed)]
    return frame


def bigg_bridge(chem_xref_path) -> pd.DataFrame:
    """BiGG metabolite id -> MNXM, from MetaNetX's own cross-reference table."""
    x = pd.read_csv(chem_xref_path, sep="\t", comment="#", header=None,
                    names=["source", "mnxm", "description"], dtype=str, low_memory=False)
    x = x[x.source.str.startswith("biggM:", na=False)].copy()
    x["bigg"] = x.source.str[len("biggM:"):]
    return x[["bigg", "mnxm"]].drop_duplicates()


def strip_compartment(bigg_id: str) -> str:
    return _COMPARTMENT_SUFFIX.sub("", bigg_id)


def biomass_reaction(model):
    """The reaction a cobra model grows on.

    The objective when one is set, and otherwise the reaction whose id says
    biomass -- a model arriving with neither has no growth to maximise and the
    caller has to say so rather than silently optimising nothing.
    """
    objective = [r for r in model.reactions if r.objective_coefficient]
    if objective:
        return objective[0]
    named = [r for r in model.reactions if "BIOMASS" in r.id.upper()]
    if not named:
        return None
    core = [r for r in named if "core" in r.id.lower()]
    return (core or named)[0]
