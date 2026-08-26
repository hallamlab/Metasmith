"""Every redox couple in models.tsv, checked against the n_e/n_h its rows declare.

`_gate_couple` compares HEAVY atoms only. This compares hydrogen and formal charge, which
is where the flavin couples are wrong.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, "src")
from rdkit import Chem, RDLogger                                        # noqa: E402
from rdkit.Chem import rdMolDescriptors                                 # noqa: E402

RDLogger.DisableLog("rdApp.*")
HERE = Path(__file__).resolve()
ROOT = HERE.parents[4]
D = ROOT / "src" / "ecspr" / "bake" / "direction"


def models() -> dict:
    out = {}
    for line in (D / "models.tsv").read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        p = line.split("\t")
        if p and p[0].startswith("MODEL:"):
            out[p[0]] = p[2]
    return out


def counts(smi: str):
    m = Chem.MolFromSmiles(smi)
    h = sum(a.GetTotalNumHs() + (1 if a.GetSymbol() == "H" else 0) for a in m.GetAtoms())
    return rdMolDescriptors.CalcMolFormula(m), Chem.GetFormalCharge(m), h


def main() -> int:
    mods = models()
    # A `#` is a comment ONLY at line start -- SMILES and term strings carry `#`.
    import io
    text = "\n".join(l for l in (D / "substitutions.tsv").read_text().splitlines()
                     if not l.startswith("#"))
    subs = pd.read_csv(io.StringIO(text), sep="\t")
    subs = subs[subs["kind"] == "carrier"]

    seen, bad = {}, 0
    for r in subs.itertuples(index=False):
        cid = str(r.couple_id)
        seen.setdefault(cid, {})[str(r.state)] = (str(r.terms), r.n_e, r.n_h)
    print(f"{'couple':16s} {'ox':22s} {'red':22s} {'dH':>3s} {'n_h':>4s} "
          f"{'q_ox':>5s} {'q_red':>6s} verdict")
    for cid, half in sorted(seen.items()):
        if {"ox", "red"} - set(half):
            continue
        (t_ox, n_e, n_h), (t_red, _, _) = half["ox"], half["red"]
        k_ox = t_ox.split("*")[-1].strip()
        k_red = t_red.split("*")[-1].strip()
        if k_ox not in mods or k_red not in mods:
            continue
        f_ox, q_ox, h_ox = counts(mods[k_ox])
        f_red, q_red, h_red = counts(mods[k_red])
        dh = h_red - h_ox
        ok = (dh == int(n_h)) and (q_ox + int(n_h) - int(n_e) == q_red)
        bad += not ok
        print(f"{cid:16s} {f_ox:22s} {f_red:22s} {dh:>3d} {int(n_h):>4d} "
              f"{q_ox:>+5d} {q_red:>+6d} {'ok' if ok else 'INCONSISTENT'}")
    print(f"\n{bad} inconsistent couple(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
