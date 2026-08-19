from __future__ import annotations

from collections import defaultdict
from pathlib import Path


ARROWS = ("<=>", "-->", "<--", "=")


def _split_terms(side: str) -> set[str]:
    out = set()
    for term in side.split(" + "):
        term = term.strip()
        if not term:
            continue
        tok = term.split()[-1]
        cid = tok.split("@", 1)[0]
        if cid:
            out.add(cid)
    return out


def load_mnxr_sides(reac_prop: Path) -> dict[str, tuple[frozenset, frozenset]]:
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
