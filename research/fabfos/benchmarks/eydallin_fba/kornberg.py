"""The medium the Eydallin screens actually ran in, as an exchange-bound set.

Both the 2010 ASKA screen and the 2007 Keio screen used Kornberg medium, verbatim from
the 2010 Methods (data/fabfos/originals/benchmarks/eydallin/eydallin2010_fulltext.jats.xml):

    "cells were grown at 37C with rapid gyratory shaking in liquid Kornberg medium
     (1.1% K2HPO4, 0.85% KH2PO4, 0.6% yeast extract from Duchefa, Haarlem, the
     Netherlands) supplemented with 50 mM glucose and 1 mM MgCl2 ... The culture medium
     was not supplemented with IPTG."

and, on the character of the nitrogen source (3.1.5):

    "Yeast extract (the amino acid source of the Kornberg medium employed in this work)
     is deficient in various amino acids. ... Mutants impaired in cysteine biosynthesis
     entering the stationary phase display a glycogen-excess phenotype when they are
     cultured in the Kornberg medium."

Glycogen was measured at the onset of stationary phase; WT held 45 nmol glucose/mg protein.

Three things this translates into, and one it does not:

  phosphate/K      1.1% K2HPO4 + 0.85% KH2PO4 is ~125 mM Pi and ~188 mM K+.  Not limiting,
                   and left at the model's own ceiling rather than given a number.
  yeast extract    0.6% is the ONLY nitrogen source, so EX_nh4_e closes.  Its carbon is
                   given a budget rather than a per-compound rate, split evenly across the
                   panel and divided by each compound's carbon count -- the same formula
                   laser/vs_gem/run_fba.py::carbon_exchanges uses for its carbon sources.
  cysteine         excluded from the panel.  The paper's own explanation for its cysI,
                   cysP and serB hits is that the medium is cysteine-deficient, so leaving
                   free cysteine available would delete the mechanism it describes.
                   Sulfate stays open: yeast extract carries sulfur, and closing both
                   makes cysteine unreachable rather than expensive.

  glucose          NOT changed.  50 mM is far past the point where the medium sets uptake,
                   so the bound is transport capacity, which Kornberg and M9 share.  Both
                   media therefore use the same GLC_UPTAKE and the comparison isolates the
                   yeast extract, which is the one thing that actually differs.
"""
from __future__ import annotations

import re

GLC_UPTAKE = 10.0
YE_CARBON = 20.0

AA_BIGG = ["ala__L", "arg__L", "asn__L", "asp__L", "gln__L", "glu__L", "gly",
           "his__L", "ile__L", "leu__L", "lys__L", "met__L", "phe__L", "pro__L",
           "ser__L", "thr__L", "trp__L", "tyr__L", "val__L"]
CYS_EXCLUDED = "cys__L"
BASE_BIGG = ["ade", "adn", "cytd", "csn", "gua", "gsn", "hxan", "ins", "thym",
             "thymd", "ura", "uri", "xan", "xtsn", "din", "dad_2", "dcyt"]
VITAMIN_BIGG = ["thm", "pnto__R", "nac", "ribflv", "4abz", "btn"]

_NC = re.compile(r"C(\d*)(?![a-z])")


def _carbons(formula: str) -> int:
    m = _NC.search(str(formula or ""))
    if not m:
        return 1
    return int(m.group(1)) if m.group(1) else 1


def yeast_extract_panel(model, formula_by_bigg: dict) -> tuple[dict, list]:
    """Exchange -> uptake bound for 0.6% yeast extract, on a shared carbon budget."""
    have, absent = [], []
    for b in AA_BIGG + BASE_BIGG + VITAMIN_BIGG:
        rid = f"EX_{b}_e"
        if rid in model.reactions:
            have.append((rid, max(1, _carbons(formula_by_bigg.get(b, "")))))
        else:
            absent.append(rid)
    if not have:
        return {}, absent
    per = YE_CARBON / len(have)
    return {rid: per / nc for rid, nc in have}, absent


def m9(model) -> dict:
    """The model's own shipped glucose-minimal medium -- what every prior run here used."""
    med = dict(model.medium)
    med["EX_glc__D_e"] = GLC_UPTAKE
    return med


def kornberg(model, formula_by_bigg: dict, nh4: float = 0.0) -> tuple[dict, list]:
    med = m9(model)
    med.pop("EX_nh4_e", None)
    if nh4:
        med["EX_nh4_e"] = nh4
    panel, absent = yeast_extract_panel(model, formula_by_bigg)
    med.update(panel)
    med.pop(f"EX_{CYS_EXCLUDED}_e", None)
    return med, absent


def formula_by_bigg(prop, bridge) -> dict:
    """bigg base id -> formula, via MetaNetX chem_prop through the biggM crosswalk."""
    f = prop.set_index("mnxm").formula.to_dict()
    out = {}
    for bigg, mnxm in zip(bridge.bigg, bridge.mnxm):
        v = f.get(mnxm)
        if isinstance(v, str) and v and v != "*":
            out.setdefault(bigg, v)
    return out
