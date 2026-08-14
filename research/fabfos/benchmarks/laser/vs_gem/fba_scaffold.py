"""The FBA scaffold: one model per host that every design is realised on by bound
flips alone.

Two reasons it is built this way, both measured rather than stylistic:

* **Adding 117 open demand reactions to unmodified iML1515 raises max growth from
  0.877 to 1.384**, because the drains unblock dead-end-limited reactions. Gated
  at (0,0) with only the reaction under maximisation opened, growth is exactly
  0.877. A scaffold is therefore a correctness requirement, not a speed trick --
  though it is also 20x faster than copying the model (0.046s vs 0.89s per LP).
* Heterologous reactions insert **reversible by default**. Inserting a lycopene
  condition's novel reactions with `lb=0` where MetaNetX declares them unbalanced
  gives max production of every new metabolite = 0.0, because MetaNetX writes the
  carotenoid desaturases in the reductive direction. LASER's own added-reaction
  table overrides this where it has an opinion.

The scaffold is built over the union of ALL 472 pooled add reactions, not just the
ones the real designs use, so a counterfactual design never forces a model rebuild.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge  # noqa: E402
import common as C  # noqa: E402

DEMAND_PREFIX = "DM_bench_"
HET_PREFIX = "HET_"
BIG = 1000.0

_EQ = re.compile(r"^\s*(\d+(?:\.\d+)?)\s+(MNXM\S+?)@(MNXD\d+)\s*$")


def parse_mnx_equation(eq: str):
    """`1 MNXM1@MNXD1 + 2 MNXM3@MNXD1 = 1 MNXM7@MNXD1` -> {mnxm: coef}, one
    compartment only (MNXD1 = cytosol-equivalent). Returns None if any term fails
    to parse or the reaction spans compartments, because a half-parsed
    stoichiometry inserted into a GEM is worse than no insertion."""
    if not isinstance(eq, str) or "=" not in eq:
        return None
    lhs, rhs = eq.split("=", 1)
    out, comps = {}, set()
    for side, sign in ((lhs, -1.0), (rhs, 1.0)):
        for term in side.split("+"):
            term = term.strip()
            if not term:
                continue
            m = _EQ.match(term)
            if not m:
                return None
            coef, mnxm, comp = float(m.group(1)), m.group(2), m.group(3)
            comps.add(comp)
            out[mnxm] = out.get(mnxm, 0.0) + sign * coef
    if len(comps) > 1:
        return None
    return {k: v for k, v in out.items() if v != 0.0} or None


def load_reac_prop(needed: set) -> dict:
    rp = pd.read_csv(C.REAC_PROP, sep="\t", comment="#", header=None,
                     names=["mnxr", "equation", "reference", "classifs",
                            "is_balanced", "is_transport"], dtype=str,
                     low_memory=False)
    rp = rp[rp.mnxr.isin(needed)]
    return {r.mnxr: dict(equation=r.equation, is_balanced=r.is_balanced,
                         is_transport=r.is_transport)
            for r in rp.itertuples(index=False)}


# ---------------------------------------------------------------------------
# Metabolite bridge: MNXM -> a model species
# ---------------------------------------------------------------------------

class MetBridge:
    """Four tiers, in order: direct `biggM:` xref, InChIKey-14, formula+charge,
    then create. The formula+charge tier is the risky one and stays last; every
    hit it makes is recorded so the count is auditable.

    The FPP crosswalk is why the InChIKey tier is load-bearing rather than
    decorative: `biggM:frdp` resolves to MNXM1363834 but `reac_prop` writes
    MNXM1363833, so without it FPP orphans and every isoprenoid graft is severed
    from central metabolism.
    """

    def __init__(self, model, prop: pd.DataFrame):
        self.model = model
        self.tiers = {"direct": 0, "inchikey": 0, "formula_charge": 0,
                      "created": 0, "reused": 0}
        br = bridge.bigg_bridge()
        mnxm_by_bigg = br.groupby("bigg").mnxm.apply(list).to_dict()

        self.by_mnxm = {}
        self.by_ikey = {}
        self.by_fc = {}
        p = prop.set_index("mnxm")
        self.formula = p.formula.to_dict()
        self.charge = p.charge.to_dict()
        self.ikey = p.inchikey.to_dict()

        for met in model.metabolites:
            if met.compartment != "c":
                continue
            base = bridge.strip_compartment(met.id)
            for mnxm in mnxm_by_bigg.get(base, []):
                self.by_mnxm.setdefault(mnxm, met)
                k = self._ikey14(mnxm)
                if k:
                    self.by_ikey.setdefault(k, met)
                fc = self._fc(mnxm)
                if fc:
                    self.by_fc.setdefault(fc, met)

    def _ikey14(self, mnxm):
        v = self.ikey.get(mnxm)
        return str(v)[:14] if isinstance(v, str) and v else None

    def _fc(self, mnxm):
        f, c = self.formula.get(mnxm), self.charge.get(mnxm)
        if isinstance(f, str) and f and f != "*" and c is not None and \
                not pd.isna(c):
            return (f, str(c))
        return None

    def get(self, mnxm: str, create: bool = True):
        import cobra
        m = self.by_mnxm.get(mnxm)
        if m is not None:
            self.tiers["direct"] += 1
            return m, "direct"
        k = self._ikey14(mnxm)
        if k and k in self.by_ikey:
            m = self.by_ikey[k]
            self.by_mnxm[mnxm] = m
            self.tiers["inchikey"] += 1
            return m, "inchikey"
        fc = self._fc(mnxm)
        if fc and fc in self.by_fc:
            m = self.by_fc[fc]
            self.by_mnxm[mnxm] = m
            self.tiers["formula_charge"] += 1
            return m, "formula_charge"
        if not create:
            return None, "absent"
        mid = f"{mnxm}_c"
        if mid in self.model.metabolites:
            self.tiers["reused"] += 1
            m = self.model.metabolites.get_by_id(mid)
        else:
            f = self.formula.get(mnxm)
            m = cobra.Metabolite(mid, compartment="c",
                                 name=str(mnxm),
                                 formula=(f if isinstance(f, str) and f != "*"
                                          else None))
            self.model.add_metabolites([m])
            self.tiers["created"] += 1
        self.by_mnxm[mnxm] = m
        return m, "created"


# ---------------------------------------------------------------------------
# LASER's own direction opinion
# ---------------------------------------------------------------------------

LASER_ADDED = (C.ROOT / "data/originals/benchmarks/laser/inputs/"
               "Ecoli iJO1366 LASER Added Reactions.txt")


def laser_directions() -> dict:
    """{frozenset(reactant bigg ids), frozenset(product bigg ids)} -> direction.
    33 rows over 28 genes touching 53 of the 235 conditions -- used as a
    precedence-2 overlay on the MetaNetX route and as a free stoichiometry
    cross-check."""
    if not LASER_ADDED.exists():
        return {}
    df = pd.read_csv(LASER_ADDED, sep="\t", dtype=str).fillna("")
    out = {}
    for r in df.itertuples(index=False):
        subs = frozenset(x.strip() for x in r.Reactants.split("+") if x.strip())
        prods = frozenset(x.strip() for x in r.Products.split("+") if x.strip())
        out[(subs, prods)] = r._1 if hasattr(r, "_1") else ""
    return out
