from __future__ import annotations

SIGMA_CEILING = 100.0


class EquilibratorMember:
    def __init__(self):
        from equilibrator_api import ComponentContribution
        self.cc = ComponentContribution()
        self._cache: dict[str, object] = {}

    def _compound(self, inchikey: str, inchi: str | None):
        if inchikey in self._cache:
            return self._cache[inchikey]
        cc, cpd = self.cc, None
        for meth, arg in (("get_compound_by_inchi", inchi),
                          ("search_compound_by_inchi_key", inchikey),
                          ("get_compound_by_inchi_key", inchikey)):
            fn = getattr(cc, meth, None)
            if fn is None or arg is None:
                continue
            try:
                cpd = fn(arg)
            except Exception:
                cpd = None
            if isinstance(cpd, (list, tuple)):
                cpd = cpd[0] if cpd else None
            if cpd is not None:
                break
        self._cache[inchikey] = cpd
        return cpd

    def dgr(self, stoich: dict[str, float], props: dict[str, dict]):
        from equilibrator_api import Reaction
        rxn_dict = {}
        for mnxm, coeff in stoich.items():
            p = props.get(mnxm)
            if not p or "inchikey" not in p:
                return None, None, None, "no_props"
            cpd = self._compound(p["inchikey"], p.get("inchi"))
            if cpd is None:
                return None, None, None, "unresolved"
            rxn_dict[cpd] = rxn_dict.get(cpd, 0.0) + coeff
        try:
            rxn = Reaction(rxn_dict)
            dg = self.cc.standard_dg_prime(rxn)
            uses_gc = bool(self.cc.is_using_group_contribution(rxn))
            val = float(dg.value.m_as("kJ/mol"))
            err = float(dg.error.m_as("kJ/mol"))
        except Exception as e:
            return None, None, None, f"error:{type(e).__name__}"
        if not (err < SIGMA_CEILING):
            return None, None, uses_gc, "uninformative"
        return val, err, uses_gc, "ok"
