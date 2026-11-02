"""Match a MetaNetX metabolite to one already in a cobra model.

Lifted from `research/fabfos/benchmarks/laser/vs_gem/fba_scaffold.py::MetBridge`
in curation round 5, with the BiGG cross-reference taken as an argument rather
than read from a module constant.

The three tiers exist because a model's metabolite ids are its own dialect.
Direct is a BiGG id MetaNetX already cross-references. InChIKey block 1 is the
structural skeleton, which matches across dialects -- but NOT across protonation
states, so an acid and its anion have different block-1 keys and are correctly
kept apart. Formula-and-charge is the last resort and will occasionally join two
isomers; it is counted separately so a caller can see how much of a mapping rests
on it.
"""
from __future__ import annotations

import pandas as pd

import mnx


class MetBridge:
    def __init__(self, model, chem_prop: pd.DataFrame, chem_xref_path):
        self.model = model
        self.tiers = {"direct": 0, "inchikey": 0, "formula_charge": 0,
                      "created": 0, "reused": 0}
        bridge = mnx.bigg_bridge(chem_xref_path)
        mnxm_by_bigg = bridge.groupby("bigg").mnxm.apply(list).to_dict()

        p = chem_prop.set_index("mnxm")
        self.formula = p.formula.to_dict()
        self.charge = p.charge.to_dict()
        self.ikey = p.inchikey.to_dict()

        self.by_mnxm: dict[str, object] = {}
        self.by_ikey: dict[str, object] = {}
        self.by_fc: dict[tuple[str, str], object] = {}
        for met in model.metabolites:
            if met.compartment != "c":
                continue
            base = mnx.strip_compartment(met.id)
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
        if isinstance(f, str) and f and f != "*" and c is not None and not pd.isna(c):
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
            m = cobra.Metabolite(
                mid, compartment="c", name=str(mnxm),
                formula=(f if isinstance(f, str) and f != "*" else None),
            )
            self.model.add_metabolites([m])
            self.tiers["created"] += 1
        self.by_mnxm[mnxm] = m
        return m, "created"
