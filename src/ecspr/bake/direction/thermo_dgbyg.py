from __future__ import annotations


def _has_wildcard(smiles: str) -> bool | None:
    from rdkit import Chem
    m = Chem.MolFromSmiles(smiles)
    if m is None:
        return None
    return any(a.GetAtomicNum() == 0 for a in m.GetAtoms())


class DgbygMember:
    def __init__(self):
        from dGbyG.api import Compound, Reaction
        self._Compound = Compound
        self._Reaction = Reaction

    def dgr(self, stoich: dict[str, float], props: dict[str, dict]):
        rxn_dict = {}
        for mnxm, coeff in stoich.items():
            p = props.get(mnxm)
            if not p or "smiles" not in p:
                return None, None, False, "no_smiles"
            wc = _has_wildcard(p["smiles"])
            if wc is None:
                return None, None, False, "unparseable"
            if wc:
                return None, None, True, "wildcard"
            cpd = self._Compound(p["smiles"], "smiles")
            rxn_dict[cpd] = rxn_dict.get(cpd, 0.0) + coeff
        try:
            rxn = self._Reaction(rxn_dict)
            if not rxn.is_balanced:
                return None, None, False, "unbalanced"
            mu, sd = rxn.standard_dGr_prime
            mu, sd = float(mu), float(sd)
        except Exception as e:
            return None, None, False, f"error:{type(e).__name__}"
        return mu, sd, False, "ok"
