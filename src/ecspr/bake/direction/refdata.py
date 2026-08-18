"""MetaNetX crosswalk loaders for the direction annotator.

Three reference files under references/metanetx/ (paths pinned in canon.py):
  reac_xref.tsv  -- source-reaction-id -> MNXR, plus the source's own equation in
                    the last '||'-field of col 3 (used only as an independent
                    orientation cross-check, never as the primary anchor).
  reac_prop.tsv  -- MNXR -> the MNXR equation in MNXM ids; this is the orientation
                    the whole annotator is expressed relative to.
  chem_xref.tsv  -- source-compound-id -> MNXM.

Loader shapes copied from main/annotations/lane_benchmarks/_bridges.py, with one
deliberate departure: many-to-one maps are kept as sets and asserted, never
silently collapsed with setdefault (that idiom would pick an arbitrary direction
from up to ~167 candidates).
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path


ARROWS = ("<=>", "-->", "<--", "=")   # order matters: match '<=>' before '='


def _split_terms(side: str) -> set[str]:
    """'1 MNXM1@MNXD1 + 1 WATER@MNXD1' -> {'MNXM1', 'WATER'}.

    Keeps the compound id verbatim (MNXM or a special like WATER/BIOMASS) and
    strips only the coefficient and the @compartment suffix. Namespace is NOT
    forced to MNXM: forcing it would silently drop water from both sides and
    corrupt the overlap score.
    """
    out = set()
    for term in side.split(" + "):
        term = term.strip()
        if not term:
            continue
        tok = term.split()[-1]          # drop the leading coefficient
        cid = tok.split("@", 1)[0]      # drop @MNXD1 / @cco:... compartment
        if cid:
            out.add(cid)
    return out


def load_mnxr_sides(reac_prop: Path) -> dict[str, tuple[frozenset, frozenset]]:
    """MNXR -> (substrate_set, product_set), from the ' = '-joined MNXR equation.

    reac_prop columns: #ID, mnx_equation, reference, classifs, is_balanced,
    is_transport. The arrow is always ' = '; sides are the MNXR's canonical
    left/right after MetaNetX re-canonicalisation.
    """
    out: dict[str, tuple[frozenset, frozenset]] = {}
    with open(reac_prop) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            mnxr, eqn = parts[0], parts[1]
            if not mnxr.startswith("MNXR") or " = " not in eqn:
                continue
            lhs, rhs = eqn.split(" = ", 1)
            out[mnxr] = (frozenset(_split_terms(lhs)), frozenset(_split_terms(rhs)))
    return out


def load_mnxr_stoich(reac_prop: Path):
    """MNXR -> (stoich, is_balanced, is_transport).

    stoich is {mnxm: signed_coeff} with substrates negative / products positive,
    compartment stripped (compound identity only). The thermo members need the
    coefficients the side-set loader discards. is_transport reactions carry a
    membrane term standard dGr'0 does not model -- calibration must skip them.
    """
    out = {}
    with open(reac_prop) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 6:
                continue
            mnxr, eqn = parts[0], parts[1]
            if not mnxr.startswith("MNXR") or " = " not in eqn:
                continue
            lhs, rhs = eqn.split(" = ", 1)
            st: dict[str, float] = {}
            for sign, side in ((-1.0, lhs), (1.0, rhs)):
                for term in side.split(" + "):
                    term = term.strip()
                    if not term:
                        continue
                    bits = term.split()
                    coeff = float(bits[0]) if len(bits) > 1 else 1.0
                    cid = bits[-1].split("@", 1)[0]
                    st[cid] = st.get(cid, 0.0) + sign * coeff
            out[mnxr] = (st, parts[4] == "B", parts[5] == "T")
    return out


def load_mnxm_props(chem_prop: Path):
    """MNXM -> {'inchikey','inchi','smiles'}, empties dropped.

    chem_prop columns: ID, name, reference, formula, charge, mass, InChI,
    InChIKey, SMILES. eQuilibrator is routed by InChIKey (its cache is frozen at
    an older MetaNetX, so MNXM accessions silently miss); dGbyG takes SMILES.
    """
    out = {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 9 or not p[0].startswith("MNXM"):
                continue
            rec = {}
            if p[6]:
                rec["inchi"] = p[6]
            if p[7]:
                rec["inchikey"] = p[7]
            if p[8]:
                rec["smiles"] = p[8]
            if rec:
                out[p[0]] = rec
    return out


def load_source_to_mnxr(reac_xref: Path, prefix: str) -> dict[str, str]:
    """'<prefix>:<id>' -> MNXR, asserted 1:1 (no silent first-wins collapse).

    e.g. prefix='metacyc.reaction'. reac_xref col 2 is an MNXR id or the literal
    'EMPTY'; EMPTY rows are skipped.
    """
    multi: dict[str, set[str]] = defaultdict(set)
    tag = prefix + ":"
    with open(reac_xref) as fh:
        for line in fh:
            if not line or line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            xref, mnxr = parts[0], parts[1]
            if mnxr.startswith("MNXR") and xref.startswith(tag):
                multi[xref[len(tag):]].add(mnxr)
    bad = {k: v for k, v in multi.items() if len(v) > 1}
    assert not bad, f"{prefix}: {len(bad)} ids map to >1 MNXR, e.g. {next(iter(bad.items()))}"
    return {k: next(iter(v)) for k, v in multi.items()}


def load_metacyc_compound_to_mnxm(chem_xref: Path) -> dict[str, str]:
    """Bare MetaCyc compound id -> MNXM.

    chem_xref carries the same compound under both 'metacycM:' and
    'metacyc.compound:' (verified identical); either resolves. Keyed by the bare
    id because the pgdb LEFT/RIGHT slots hold bare ids ('GLT', 'PROTON').
    """
    out: dict[str, str] = {}
    tag = "metacyc.compound:"
    with open(chem_xref) as fh:
        for line in fh:
            if not line or line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            src, mnxm = parts[0], parts[1]
            if not mnxm or mnxm == "EMPTY":
                continue
            if src.startswith(tag):
                out[src[len(tag):]] = mnxm
    return out


def parse_col3_sides(reac_xref: Path) -> dict[str, tuple[str, frozenset, frozenset]]:
    """metacyc.reaction id -> (arrow, left_ids, right_ids) from reac_xref col 3.

    The equation is ALWAYS the last '||'-field (the name is usually absent, so a
    fixed index is wrong). Compound ids are bare MetaCyc ids parsed from
    'metacycM:<id>@cco:...'. This is MetaCyc's own orientation, from a newer
    MetaCyc than the pgdb -- an independent cross-check anchor, not the primary.
    """
    out: dict[str, tuple[str, frozenset, frozenset]] = {}
    tag = "metacyc.reaction:"
    with open(reac_xref) as fh:
        for line in fh:
            if not line or line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3 or not parts[0].startswith(tag):
                continue
            rid = parts[0][len(tag):]
            eqn = parts[2].split("||")[-1]
            arrow = next((a for a in ARROWS if f" {a} " in eqn), None)
            if arrow is None:
                continue
            lhs, rhs = eqn.split(f" {arrow} ", 1)
            out[rid] = (arrow, frozenset(_metacyc_ids(lhs)), frozenset(_metacyc_ids(rhs)))
    return out


def _metacyc_ids(side: str) -> set[str]:
    """'1 metacycM:GLT@cco:CCO-IN + ...' -> {'GLT', ...}."""
    out = set()
    for term in side.split(" + "):
        term = term.strip()
        if not term:
            continue
        tok = term.split()[-1]
        cid = tok.split("@", 1)[0]
        if cid.startswith("metacycM:"):
            cid = cid[len("metacycM:"):]
        if cid:
            out.add(cid)
    return out
