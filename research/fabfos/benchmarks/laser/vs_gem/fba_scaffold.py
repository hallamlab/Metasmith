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


class MetBridge:
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


LASER_ADDED = (C.ROOT / "data/fabfos/originals/benchmarks/laser/inputs/"
               "Ecoli iJO1366 LASER Added Reactions.txt")


def laser_directions() -> dict:
    if not LASER_ADDED.exists():
        return {}
    df = pd.read_csv(LASER_ADDED, sep="\t", dtype=str).fillna("")
    out = {}
    for r in df.itertuples(index=False):
        subs = frozenset(x.strip() for x in r.Reactants.split("+") if x.strip())
        prods = frozenset(x.strip() for x in r.Products.split("+") if x.strip())
        out[(subs, prods)] = r._1 if hasattr(r, "_1") else ""
    return out
