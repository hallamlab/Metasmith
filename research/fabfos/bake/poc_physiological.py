"""Does a measured concentration change the direction call, and is uniform 1 mM enough?

The bake's ratio is exp(dG'o/RT) -- eQuilibrator's `standard_dg_prime`, every participant
at 1 M. Three conventions are compared here against reactions whose physiological direction
in E. coli is not in doubt:

  STD    dG'o                     1 M all round      -- what the bake ships
  PHYS   dG'o + RT sum(nu ln 1mM) 1 mM all round     -- eQuilibrator's own second offer
  CONC   dG'o + RT sum(nu ln c_i) measured c_i       -- ECMDB, defaulting to 1 mM

PHYS is in the table because it is the cheap fix, and the question of whether it is
sufficient has to be answered before the expensive one is proposed. It corrects only
MOLECULARITY (how many solutes each side has); it cannot see SKEW (that [Pi] runs 50x
[G1P]), because it sets every participant equal by construction.

POLYMERS AND PSEUDO-METABOLITES ARE HELD AT UNIT ACTIVITY AND DROPPED. That is the same
assertion `substitute.py`'s ZERO_OFFSET_KINDS already makes about the standard term --
glycogen appears on both sides of a phosphorylase in the chemistry and cancels, and
MetaNetX writing only one side of it is the polymer-budget defect, not a real asymmetry.
Pricing the polymer as a millimolar solute instead is what the `--polymer-mM` flag does,
and it is offered because it CHANGES THE ANSWER for exactly these reactions.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from ecspr.bake.direction.refdata import load_mnxm_names, load_mnxm_props, load_mnxr_stoich

RT = 8.314e-3 * 298.15

# Held at unit activity: a macromolecular acceptor whose free-solute concentration is not
# the quantity the enzyme sees. MetaNetX writes each of these on one side only.
UNIT_ACTIVITY = {
    "MNXM738130": "Glycogen", "MNXM8348": "Branching glycogen",
    "MNXM727735": "Amylose", "MNXM725902": "Starch",
    "BIOMASS": "Biomass",
}
# Excluded from the concentration correction because eQuilibrator's transformed (prime)
# potentials already account for them: water is the solvent, and the proton is what the
# prime transform is a transform over.
IMPLICIT = {"WATER", "MNXM1"}

# (mnxr, label, physiological direction as reac_prop writes it, why)
PROBES = [
    ("MNXR153519", "FBA aldolase",              "forward", "glycolysis, FBP -> DHAP + GAP"),
    ("MNXR191943", "GAPDH",                     "reverse", "written as the reductive direction"),
    ("MNXR145952", "PFK",                       "forward", "committed step, ATP-driven"),
    ("MNXR146574", "TPI",                       "reverse", "glycolysis runs DHAP -> GAP"),
    ("MNXR97932",  "enolase",                   "forward", "glycolysis, 2PG -> PEP"),
    ("MNXR102547", "phosphoglycerate mutase",   "reverse", "glycolysis runs 3PG -> 2PG"),
    ("MNXR191159", "PGK",                        "forward", "glycolysis, substrate-level ATP"),
    ("MNXR197738", "PGI",                        "reverse", "glycolysis runs G6P -> F6P"),
    ("MNXR147046", "FBP + H2O = F6P + Pi",      "reverse", "written backwards; FBPase hydrolyses"),
    ("MNXR145036", "glycogen phosphorylase",    "reverse", "glgP/malP are degradative"),
    ("MNXR145038", "glycogen phosphorylase (branched)", "reverse", "same enzyme class"),
    ("MNXR147516", "starch phosphorylase",      "reverse", "same enzyme class"),
    ("MNXR133777", "purine nucleoside phosphorylase", "reverse", "deoD degrades nucleosides"),
    ("MNXR145046", "glycogen synthase",         "forward", "glgA is anabolic, ADP-glucose driven"),
]


def verdict(dg, dead_band):
    if dg is None or (isinstance(dg, float) and math.isnan(dg)):
        return "silent"
    if abs(dg) < dead_band:
        return "reversible"
    return "forward" if dg < 0 else "reverse"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--reac-prop", required=True)
    ap.add_argument("--chem-prop", required=True)
    ap.add_argument("--conc", required=True, help="conc_mnxm.tsv from build_concentration_table")
    ap.add_argument("--default-mM", type=float, default=1.0,
                    help="concentration for a participant ECMDB does not measure. 1 mM is "
                         "eQuilibrator's own physiological default, so CONC degrades to "
                         "PHYS on a reaction it knows nothing about rather than to STD.")
    ap.add_argument("--polymer-mM", type=float, default=None,
                    help="price the unit-activity set as a solute at this concentration "
                         "instead. Offered because it changes the phosphorylase calls.")
    ap.add_argument("--dead-band", type=float, default=RT * math.log(10.0),
                    help="|dG'| below this reads as reversible; one decade of conductance")
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)

    conc = {}
    with open(a.conc) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            conc[r["mnxm"]] = float(r["conc_mM"])
    print(f"[conc] {len(conc)} measured concentrations")

    stoich = load_mnxr_stoich(a.reac_prop)
    props = load_mnxm_props(a.chem_prop)
    names = load_mnxm_names(a.chem_prop)

    from equilibrator_api import Q_, ComponentContribution, Reaction
    cc = ComponentContribution()
    cache = {}

    def compound(mnxm):
        if mnxm in cache:
            return cache[mnxm]
        p = props.get(mnxm) or {}
        cpd = None
        for meth, arg in (("get_compound_by_inchi", p.get("inchi")),
                          ("search_compound_by_inchi_key", p.get("inchikey")),
                          ("get_compound_by_inchi_key", p.get("inchikey"))):
            fn = getattr(cc, meth, None)
            if fn is None or arg is None:
                continue
            try:
                got = fn(arg)
            except Exception:
                got = None
            if isinstance(got, (list, tuple)):
                got = got[0] if got else None
            if got is not None:
                cpd = got
                break
        cache[mnxm] = cpd
        return cpd

    rows = []
    for mnxr, label, truth, why in PROBES:
        s = stoich.get(mnxr)
        if s is None:
            print(f"{mnxr} {label}: no stoichiometry", file=sys.stderr)
            continue
        st, _bal, _tr = s
        held = {m: c for m, c in st.items() if m in UNIT_ACTIVITY}
        rxn_dict, unresolved, defaulted, seen = {}, [], [], []
        for mnxm, coeff in st.items():
            if mnxm in UNIT_ACTIVITY and a.polymer_mM is None:
                continue
            cpd = compound(mnxm)
            if cpd is None:
                unresolved.append(names.get(mnxm, mnxm))
                continue
            rxn_dict[cpd] = rxn_dict.get(cpd, 0.0) + coeff
            if mnxm in IMPLICIT:
                continue
            if mnxm in UNIT_ACTIVITY:
                seen.append((cpd, a.polymer_mM))
            elif mnxm in conc:
                seen.append((cpd, conc[mnxm]))
            else:
                seen.append((cpd, a.default_mM))
                defaulted.append(names.get(mnxm, mnxm))
        if unresolved:
            rows.append(dict(mnxr=mnxr, label=label, truth=truth, why=why,
                             std=None, phys=None, cnc=None,
                             note="unresolved: " + ", ".join(unresolved[:3])))
            continue

        rxn = Reaction(rxn_dict)
        try:
            std = float(cc.standard_dg_prime(rxn).value.m_as("kJ/mol"))
            sig = float(cc.standard_dg_prime(rxn).error.m_as("kJ/mol"))
            phys = float(cc.physiological_dg_prime(rxn).value.m_as("kJ/mol"))
        except Exception as e:
            rows.append(dict(mnxr=mnxr, label=label, truth=truth, why=why,
                             std=None, phys=None, cnc=None,
                             note=f"error: {type(e).__name__}: {e}"))
            continue
        for cpd, c in seen:
            rxn.set_abundance(cpd, Q_(c, "mM"))
        cnc = float(cc.dg_prime(rxn).value.m_as("kJ/mol"))

        rows.append(dict(mnxr=mnxr, label=label, truth=truth, why=why,
                         std=std, sigma=sig, phys=phys, cnc=cnc,
                         held=";".join(f"{names.get(m,m)}" for m in held),
                         n_default=len(defaulted),
                         defaulted=";".join(sorted(set(defaulted))[:4]),
                         note=""))

    hdr = (f"{'MNXR':<12} {'reaction':<34} {'truth':<9} "
           f"{'STD':>8} {'PHYS':>8} {'CONC':>8}   {'std':<10} {'phys':<10} {'conc':<10}")
    print("\n" + hdr)
    print("-" * len(hdr))
    score = {"std": 0, "phys": 0, "cnc": 0}
    n = 0
    for r in rows:
        if r["std"] is None:
            print(f"{r['mnxr']:<12} {r['label'][:34]:<34} {r['truth']:<9} "
                  f"{'--':>8} {'--':>8} {'--':>8}   {r['note'][:44]}")
            continue
        n += 1
        v = {k: verdict(r[k], a.dead_band) for k in ("std", "phys", "cnc")}
        for k in score:
            score[k] += (v[k] == r["truth"])
        mark = lambda k: ("OK " if v[k] == r["truth"] else "XX ") + v[k]
        print(f"{r['mnxr']:<12} {r['label'][:34]:<34} {r['truth']:<9} "
              f"{r['std']:8.2f} {r['phys']:8.2f} {r['cnc']:8.2f}   "
              f"{mark('std'):<10} {mark('phys'):<10} {mark('cnc'):<10}")
        if r["held"]:
            print(f"{'':<12} held at unit activity: {r['held']}")
        if r["n_default"]:
            print(f"{'':<12} {r['n_default']} participant(s) defaulted to "
                  f"{a.default_mM} mM: {r['defaulted']}")
    print("-" * len(hdr))
    print(f"scored {n} reactions   STD {score['std']}/{n}   "
          f"PHYS {score['phys']}/{n}   CONC {score['cnc']}/{n}"
          f"   (dead band {a.dead_band:.2f} kJ/mol"
          f"{'' if a.polymer_mM is None else f', polymer at {a.polymer_mM} mM'})")

    if a.out:
        keys = ("mnxr", "label", "truth", "why", "std", "sigma", "phys", "cnc",
                "held", "n_default", "defaulted", "note")
        with open(a.out, "w") as fh:
            w = csv.DictWriter(fh, fieldnames=keys, delimiter="\t", extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"-> {a.out}")


if __name__ == "__main__":
    main()
