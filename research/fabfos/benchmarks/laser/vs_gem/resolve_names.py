"""Name -> MNXM resolution for LASER targets and carbon sources.

The user's ask was that mapping must not be a source of error, so this is a
cascade of *exact* matches only -- override, then conservative-normalised, then
aggressive-normalised -- against a name index built from MetaNetX `chem_prop`
names plus every `chem_xref` description. There is deliberately no fuzzy tier:
`mnx_lookups.trigrams` justifies its generosity by an arbiter that does not exist
here, so a trigram hit would be an unaudited guess underneath every other result.

Ambiguity is disposed of mechanically rather than by hand:

* **Rule A** drops candidates with null formula *and* null InChIKey. Those are
  MetaNetX secondary/structureless entries; this is what separates real
  all-trans-lycopene from the `Lycopene` stub.
* **Rule B** groups survivors on the InChIKey connectivity block. When they
  collapse to one block they are the same compound differing in stereochemistry
  or protonation, and the answer is to MERGE them into one terminal. Picking one
  and dropping its twin silently loses current.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402

INDEX_CACHE = C.CACHE / "name_index.parquet"
PROP_CACHE = C.CACHE / "chem_prop_slim.parquet"


def build_name_index(force: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(index, prop). index is long-format name/mnxm; prop is the slim property
    table Rules A and B read. Both cached -- chem_prop is 810MB and chem_xref
    679MB, so this is streamed with vectorised string ops once."""
    if INDEX_CACHE.exists() and PROP_CACHE.exists() and not force:
        return pd.read_parquet(INDEX_CACHE), pd.read_parquet(PROP_CACHE)

    prop = pd.read_csv(
        C.CHEM_PROP, sep="\t", comment="#", header=None,
        names=["mnxm", "name", "reference", "formula", "charge", "mass",
               "inchi", "inchikey", "smiles"],
        dtype=str, low_memory=False)
    prop = prop[["mnxm", "name", "formula", "charge", "inchikey"]]
    prop.to_parquet(PROP_CACHE, index=False)

    xref = pd.read_csv(
        C.CHEM_XREF, sep="\t", comment="#", header=None,
        names=["source", "mnxm", "description"], dtype=str, low_memory=False)

    frames = [
        prop[["name", "mnxm"]].rename(columns={"name": "raw"}),
        xref[["description", "mnxm"]].rename(columns={"description": "raw"}),
    ]
    idx = pd.concat(frames, ignore_index=True).dropna(subset=["raw", "mnxm"])
    idx = idx[idx.raw.str.len() > 0]
    idx["cons"] = idx.raw.map(C.norm_conservative)
    idx["aggr"] = idx.cons.map(lambda s: C._NONALNUM.sub("", s))
    idx = idx[["cons", "aggr", "mnxm"]].drop_duplicates()
    idx.to_parquet(INDEX_CACHE, index=False)
    return idx, prop


class Resolver:
    def __init__(self, element: str = "C"):
        self.idx, self.prop = build_name_index()
        self.universe = C.atom_universe(element)
        self.by_cons = self.idx.groupby("cons").mnxm.apply(set).to_dict()
        self.by_aggr = self.idx.groupby("aggr").mnxm.apply(set).to_dict()
        p = self.prop.set_index("mnxm")
        self.formula = {k: ("" if pd.isna(v) else str(v))
                        for k, v in p.formula.to_dict().items()}
        self.inchikey = {k: ("" if pd.isna(v) else str(v))
                         for k, v in p.inchikey.to_dict().items()}

    # -- ambiguity -----------------------------------------------------
    def disambiguate(self, cands: set) -> tuple[list, str]:
        """Returns (mnxms, note). A list longer than 1 is a deliberate MERGE."""
        cands = sorted(cands)
        if len(cands) <= 1:
            return cands, "unique"
        keep = [m for m in cands
                if self.formula.get(m, "") or self.inchikey.get(m, "")]
        note = "ruleA" if len(keep) < len(cands) else "unique"
        if len(keep) <= 1:
            return keep, note
        blocks = {self.inchikey.get(m, "")[:14] for m in keep}
        blocks.discard("")
        if len(blocks) == 1:
            return keep, "ruleB_merge"
        if not blocks:
            return keep, "ambiguous_no_structure"
        return keep, "ambiguous"

    # -- the cascade ---------------------------------------------------
    def resolve(self, token: str) -> dict:
        cons = C.norm_conservative(token)
        aggr = C._NONALNUM.sub("", cons)
        for tier, cands in (("conservative", self.by_cons.get(cons, set())),
                            ("aggressive", self.by_aggr.get(aggr, set()))):
            if not cands:
                continue
            inu = {m for m in cands if m in self.universe}
            if inu:
                mnxms, note = self.disambiguate(inu)
                if mnxms:
                    return dict(status="resolved", tier=f"{tier}_in_universe",
                                mnxms=mnxms, note=note, in_universe=True)
            mnxms, note = self.disambiguate(cands)
            if mnxms:
                return dict(status="resolved", tier=f"{tier}_off_universe",
                            mnxms=mnxms, note=note, in_universe=False)
        return dict(status="unresolved", tier="none", mnxms=[], note="",
                    in_universe=False)
