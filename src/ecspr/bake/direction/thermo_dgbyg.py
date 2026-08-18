"""dGbyG member: learned-GNN dGr'0 per MNXR reaction, with the wildcard guard.

dGbyG is a 100-head deep ensemble; standard_dGr_prime returns (mean, std) where
std is a genuine per-head spread (native output already dG'0 at pH 7 / I 0.25 /
298.15 K, so no ChemAxon/Legendre transform).

THE GUARD. dGbyG silently accepts R-group wildcards: RDKit reports '*' as atomic
number 0, which one-hots to a valid feature vector, so the model returns a
confident number for a compound it never trained on (confirmed: '*C(=O)O' ->
(-360, 26)). Any reaction with a wildcard atom in any compound ABSTAINS here --
returning that number would be strictly worse than silence.

Routed by SMILES from chem_prop. Runs under the `dgbyg` env, which must be
invoked with LD_LIBRARY_PATH=$CONDA_PREFIX/lib (env rdkit needs the env's newer
libstdc++, not the system one).
"""
from __future__ import annotations


def _has_wildcard(smiles: str) -> bool | None:
    """True/False if parseable; None if RDKit cannot parse the SMILES at all."""
    from rdkit import Chem
    m = Chem.MolFromSmiles(smiles)
    if m is None:
        return None
    return any(a.GetAtomicNum() == 0 for a in m.GetAtoms())


class DgbygMember:
    def __init__(self):
        # import here so the module imports cleanly outside the dgbyg env
        from dGbyG.api import Compound, Reaction
        self._Compound = Compound
        self._Reaction = Reaction

    def dgr(self, stoich: dict[str, float], props: dict[str, dict]):
        """dGr'0 for one MNXR from {mnxm: signed_coeff}. props is MNXM->{smiles,..}.

        Returns (dg_kJ, sigma_kJ, wildcard, reason). dg is None when abstaining:
        reason in {ok, no_smiles, unparseable, wildcard, unbalanced, error}.
        wildcard is True iff the abstention was caused by an R-group atom.
        """
        rxn_dict = {}
        for mnxm, coeff in stoich.items():
            p = props.get(mnxm)
            if not p or "smiles" not in p:
                return None, None, False, "no_smiles"
            wc = _has_wildcard(p["smiles"])
            if wc is None:
                return None, None, False, "unparseable"
            if wc:
                return None, None, True, "wildcard"      # the guard
            cpd = self._Compound(p["smiles"], "smiles")
            rxn_dict[cpd] = rxn_dict.get(cpd, 0.0) + coeff
        try:
            rxn = self._Reaction(rxn_dict)
            if not rxn.is_balanced:
                # dGbyG multiplies unbalanced reactions by NaN; treat as abstain.
                return None, None, False, "unbalanced"
            mu, sd = rxn.standard_dGr_prime
            mu, sd = float(mu), float(sd)
        except Exception as e:
            return None, None, False, f"error:{type(e).__name__}"
        return mu, sd, False, "ok"
