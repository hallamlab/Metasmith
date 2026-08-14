"""eQuilibrator member: component-contribution dGr'0 per MNXR reaction.

Routed by InChIKey (never MNXM accessions -- eQuilibrator's compound cache is
frozen at an older MetaNetX, so accessions silently miss). Reports per reaction:
  (dg, sigma, uses_gc, reason)
where uses_gc is is_using_group_contribution() -- provenance only, kept so the
calibration (T3) can restrict to the MEASURED (reactant-contribution) arm, which
is the only arm without the group-conserving structural-zero artifact.

Runs under the `equilibrator` env (python 3.11). Reaction orientation is the MNXR
equation orientation (substrates negative), so dg's sign is aligned with the whole
annotator by construction.
"""
from __future__ import annotations

# eQuilibrator returns a degenerate ~1e4-1e5 kJ/mol uncertainty for a compound it
# holds but cannot constrain (near-null eigenvalue). That is not a measurement:
# any |dG'| relevant to a conductance ratio is well under ~50 kJ/mol (ratio
# 1e-9..1e9), so a sigma past this physical ceiling carries no directional
# information and the reaction is treated as eQ-silent, not as a vote. This is a
# validity boundary, not a tuned knob.
SIGMA_CEILING = 100.0  # kJ/mol


class EquilibratorMember:
    def __init__(self):
        from equilibrator_api import ComponentContribution
        self.cc = ComponentContribution()
        self._cache: dict[str, object] = {}

    def _compound(self, inchikey: str, inchi: str | None):
        """Resolve one compound via InChIKey, with an InChI fallback. Cached.
        Returns an eQuilibrator Compound or None (compound not in its cache)."""
        if inchikey in self._cache:
            return self._cache[inchikey]
        cc, cpd = self.cc, None
        # Exact InChI first (stereo-specific); the InChIKey routes fall back but
        # a connectivity-block match would merge anomers, so they are last resort.
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
            if isinstance(cpd, (list, tuple)):   # some lookups return match lists
                cpd = cpd[0] if cpd else None
            if cpd is not None:
                break
        self._cache[inchikey] = cpd
        return cpd

    def dgr(self, stoich: dict[str, float], props: dict[str, dict]):
        """dGr'0 for one MNXR from {mnxm: signed_coeff}. props is MNXM->{inchikey,inchi,..}.

        Returns (dg_kJ, sigma_kJ, uses_gc, reason). dg is None when the reaction is
        eQ-silent: reason in {ok, no_props, unresolved, error}.
        """
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
        if not (err < SIGMA_CEILING):           # NaN or degenerate -> no information
            return None, None, uses_gc, "uninformative"
        return val, err, uses_gc, "ok"
