"""The five lookups every metabolism step reads instead of re-parsing MetaNetX.

WHAT THIS REPLACES, AND WHY IT IS NOT A CACHE
---------------------------------------------
Before this module, `chem_prop.tsv` (810 MB) was opened and re-parsed by the pair
extractor, the two neural members, the curated member, the direction lane's universe
builder and every curation proposer. Speed was never the problem. The problem is that
those six parses had six slightly different ideas of what a trustworthy formula is,
two crosswalk loaders with OPPOSITE collision policies (one `setdefault`, one
last-wins; only one validated the id shape), and three separate places computing a
canonical atom rank -- which is the identity the whole atom graph is keyed on.

Five near-copies of a parse drift apart while every table still looks well-formed.
That is the failure this retires. The lookups are the single answer, written once and
read by everything: if the rank is wrong it is wrong in one place, and if the join
policy changes it changes for every consumer at the same instant.

THE FIVE, AND WHAT EACH IS FOR
------------------------------
  reactions    one row per MNXR -- parsed equation, flags, and the reaction SMILES
               built ONCE. Every mapper must see the same string, or the ensemble's
               "disagreement" is partly a disagreement between two SMILES builders.
  metabolites  the whole compound table plus per-element counts and the InChIKey
               connectivity block. Whole table, because the curation lanes hunt for
               structured twins by name and formula, and a twin need not itself
               appear in any reaction.
  atom_ranks   THE NODE IDENTITY CONTRACT. (mnxm, element) -> canonical ranks, from
               `CanonicalRankAtoms(breakTies=True)` on a sanitised, map-number-stripped
               molecule. Three details are load-bearing and all three are easy to drop;
               see `canonical_ranks`.
  xrefs        both crossreference files as one long table, plus `n_mnx_for_source`
               so "is many-to-one legal here" is answered by data rather than by two
               loaders disagreeing. Measured on MNXref 4.5: it is 1 for every row of
               both files -- there are no many-to-one foreign ids at all.
  synonyms     normalised name keys over MetaNetX, ChEBI, ModelSEED and MetaCyc, with
               a conservative and an aggressive normalisation. The supplier index the
               curation sweep resolves structureless stubs through.

SCOPE OF atom_ranks. Reaction participants only (47,644 of 1,495,668). A metabolite
that appears in no reaction cannot be a graph node, so ranking it produces a row that
nothing can ever key on. `canonical_smiles` lives here rather than in `metabolites`
for the same reason: it is part of the identity, not a property of the compound.

Env: rdkit + pandas + pyarrow.
"""
from __future__ import annotations

import argparse
import gzip
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ELEMENTS = ("C", "N", "S", "P")

# MUST BE THE REGEX THAT BUILDS THE MAPPED SMILES, CHARACTER FOR CHARACTER -- it is
# copied from `ecspr_atom_pairs.EQ_TERM` and `aam_neural_members.EQ_TERM`, which must
# all agree. A token this matches and the SMILES builder does not (or vice versa) makes
# the template count disagree with the metabolite count for every reaction carrying it,
# and the reaction is refused as `stripped` -- not for chemistry, but because two
# regexes disagreed. That cost 19,930 reactions once already.
EQ_TERM = re.compile(r"(\d+(?:\.\d+)?)\s+(MNXM\w+)@\w+")

BATCH = 200_000


# =====================================================================
# shared derivations -- one definition each, which is the entire point
# =====================================================================

# ONE COUNTER IN THE TREE. This file's whole argument is that a derivation five consumers
# make for themselves drifts, so carrying a private copy of "atoms of X in a MetaNetX
# formula" -- byte-identical to the extractor's, untested against it -- was the argument
# failing on its own terms. The transform requires `buildlib::ecspr` for this import and
# for nothing else; staging is content-addressed, so the two library items land in
# SEPARATE directories and the caller must put both on PYTHONPATH.
from ecspr.bake.atom_pairs import count_element                        # noqa: E402


def inchikey_connectivity(ik):
    """The first block of an InChIKey -- the SKELETON, with protonation and
    stereochemistry deliberately discarded.

    This is the fix TIER4_FREEZE named and never applied. MetaCyc and MetaNetX
    routinely disagree about which protonation state and which tautomer a participant
    is drawn in, so identifying a mapped fragment by CANONICAL SMILES fails on a
    difference that is not a difference in identity. The 14-character connectivity
    layer is invariant to exactly that, and to nothing else that matters here.
    """
    if not ik or not isinstance(ik, str):
        return None
    ik = ik.strip()
    if len(ik) < 14 or "-" not in ik:
        return None
    return ik.split("-", 1)[0]


def canonical_ranks(mol):
    """Atom -> canonical rank, invariant to how a reaction happened to write it.

    THREE LOAD-BEARING DETAILS, and every one of them has already cost coverage:

      * ZERO THE MAP NUMBERS FIRST. They are per-reaction; leaving them in makes the
        rank a function of the mapping rather than of the molecule.
      * SANITIZE UNCONDITIONALLY, never as a fallback. Ranks depend on perceived
        aromaticity, so ranking some molecules sanitised and others raw gives ONE
        metabolite TWO rank systems -- the same defect `GetIdx()` has, arriving
        silently. It also normalises the kekulized-vs-aromatic spellings different
        mappers emit for the same molecule.
      * breakTies=True. Without it a (metabolite, rank) pair is a symmetry CLASS, not
        an atom, and two mappers assigning symmetric atoms differently read as
        disagreement. Measured at 41% of the apparent disagreement between two members.

    Returns None when sanitisation fails -- genuinely unrankable, and refused.
    """
    from rdkit import Chem
    m2 = Chem.Mol(mol)
    for a in m2.GetAtoms():
        a.SetAtomMapNum(0)
    try:
        Chem.SanitizeMol(m2)
        return list(Chem.CanonicalRankAtoms(m2, breakTies=True))
    except Exception:
        return None


def canon_smiles(smi: str):
    """Canonical SMILES with atom-map numbers stripped, sanitised the same way
    `canonical_ranks` sanitises -- both sides of every later comparison must be
    perceived identically or the lookup misses and the metabolite goes unnamed."""
    from rdkit import Chem
    if not smi:
        return None
    try:
        mol = Chem.MolFromSmiles(smi)
    except Exception:
        return None
    if mol is None:
        return None
    for a in mol.GetAtoms():
        a.SetAtomMapNum(0)
    try:
        return Chem.MolToSmiles(mol, canonical=True)
    except Exception:
        return None


def parse_equation(eq: str):
    """(substrates, products), expanded by stoichiometry, in equation order.

    Expanded rather than (coefficient, id) pairs because that is what the SMILES
    builder emits one fragment per, and the pair extractor aligns template i with
    metabolite i. A representation the two disagree about is the `stripped` bug.
    """
    if not eq or "=" not in eq:
        return None

    def expand(s):
        out = []
        for coef, m in EQ_TERM.findall(s):
            for _ in range(max(1, int(round(float(coef))))):
                out.append(m)
        return out

    lhs, rhs = eq.split("=", 1)
    L, R = expand(lhs), expand(rhs)
    return (L, R) if (L and R) else None


# =====================================================================
# name normalisation -- the supplier index's two keys
# =====================================================================

_GREEK = {
    "alpha": "a", "beta": "b", "gamma": "g", "delta": "d", "epsilon": "e",
    "α": "a", "β": "b", "γ": "g", "δ": "d", "ε": "e",
}
_STOP = {"a", "an", "the"}


def norm_conservative(s):
    """Lowercase, collapse punctuation to single spaces, strip. Reversible enough that
    a hit is a real name match and not an artefact of the normaliser."""
    if not s or not isinstance(s, str):
        return None
    n = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    return n or None


def norm_aggressive(s):
    """Conservative, then drop the things that differ between vocabularies without
    differing in chemistry: stereo/locant prefixes, greek letters spelled out either
    way, articles, and the acid/ate alternation.

    Deliberately generous. The arbiter -- map the completed reaction, then require an
    element to balance -- is what makes generosity safe, so tightening this to
    compensate for a loose fuzzy threshold trades recovered reactions for nothing.
    """
    n = norm_conservative(s)
    if not n:
        return None
    toks = []
    for t in n.split():
        t = _GREEK.get(t, t)
        if t in _STOP:
            continue
        # bare locants and stereo descriptors: 1, 2s, r, s, d, l, dl, cis, trans, n, o, p
        if re.fullmatch(r"\d+[a-z]?|[rsdlnopc]|dl|cis|trans|rac|sn|meso", t):
            continue
        toks.append(t)
    if not toks:
        return None
    j = " ".join(toks)
    # acid/ate: MetaNetX says "pyruvate", ChEBI says "pyruvic acid", ModelSEED says both
    j = re.sub(r"\bic acid\b", "ate", j)
    j = re.sub(r"\bacid\b", "", j).strip()
    j = re.sub(r"\s+", " ", j)
    return j or None


def trigrams(s):
    """3-gram set of a normalised name, for the fuzzy tier.

    Built in memory from `key_a` at read time rather than stored exploded: the
    synonyms table has ~1.4M rows and ~20 grams each, so materialising it would be a
    28M-row parquet answering a question a dict comprehension answers in seconds.
    """
    if not s:
        return set()
    p = f"  {s}  "
    return {p[i:i + 3] for i in range(len(p) - 2)}


# =====================================================================
# readers
# =====================================================================

def _iter_tsv(path: Path, ncol: int):
    """MetaNetX TSVs: `#` comments, tab separated, no quoting. Streamed, because two of
    them are 810 MB and 678 MB and a naive full read is 3-4x that in memory."""
    op = gzip.open if str(path).endswith(".gz") else open
    with op(path, "rt", errors="replace") as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < ncol:
                p = p + [""] * (ncol - len(p))
            yield p


def load_reac_prop(path: Path):
    """mnxr -> (equation, classifs, is_balanced, is_transport)."""
    out = {}
    for p in _iter_tsv(path, 6):
        if p[0] in ("", "EMPTY"):
            continue
        out[p[0]] = (p[1], p[3], p[4], p[5])
    return out


# =====================================================================
# 1. reactions
# =====================================================================

def build_reactions(reac_prop: Path, chem_prop: Path, out: Path):
    raw = load_reac_prop(reac_prop)
    parsed, universe = {}, set()
    unparseable = 0
    for r, (eq, _c, _b, _t) in raw.items():
        pe = parse_equation(eq)
        if pe is None:
            unparseable += 1
            continue
        parsed[r] = pe
        universe |= set(pe[0]) | set(pe[1])
    print(f"[lookups] reac_prop: {len(raw):,} reactions, {len(parsed):,} parseable "
          f"({unparseable:,} not), {len(universe):,} distinct participants", flush=True)

    smi = {}
    for p in _iter_tsv(chem_prop, 9):
        if p[0] in universe and p[8].strip():
            smi[p[0]] = p[8].strip()
    print(f"[lookups] {len(smi):,}/{len(universe):,} participants carry a SMILES",
          flush=True)

    rows = []
    n_built = 0
    for r, (eq, classifs, bal, tr) in raw.items():
        pe = parsed.get(r)
        if pe is None:
            rows.append(dict(mnxr=r, equation=eq, classifs=classifs,
                             is_balanced=bal, is_transport=tr,
                             substrates=[], products=[], rxn_smiles=None,
                             blockers=[], n_blockers=-1))
            continue
        subs, prods = pe
        blockers = sorted({m for m in set(subs) | set(prods) if m not in smi})
        rxn, sf, pf = None, [], []
        if not blockers:
            rxn = ".".join(smi[m] for m in subs) + ">>" + ".".join(smi[m] for m in prods)
            # HOW MANY `.`-SEPARATED FRAGMENTS EACH PARTICIPANT CONTRIBUTES. Almost
            # always 1, and the exception is the reason this column exists: 6,277
            # metabolites have a multi-fragment SMILES (salts, ion pairs, hydrates), so
            # a reaction touching one produces MORE reaction templates than it has
            # participants. `pairs_from_mapped` compares those two counts and refuses
            # the reaction as `stripped` when they differ -- a chemistry-blind refusal
            # over 485 reactions (0.84% of the buildable universe) that is really just
            # positional alignment losing track. With the per-participant fragment
            # count the alignment is exact and the refusal goes away.
            sf = [smi[m].count(".") + 1 for m in subs]
            pf = [smi[m].count(".") + 1 for m in prods]
            n_built += 1
        rows.append(dict(mnxr=r, equation=eq, classifs=classifs,
                         is_balanced=bal, is_transport=tr,
                         substrates=subs, products=prods, rxn_smiles=rxn,
                         sub_frags=sf, prod_frags=pf,
                         blockers=blockers, n_blockers=len(blockers)))

    schema = pa.schema([
        ("mnxr", pa.string()), ("equation", pa.string()), ("classifs", pa.string()),
        ("is_balanced", pa.string()), ("is_transport", pa.string()),
        ("substrates", pa.list_(pa.string())), ("products", pa.list_(pa.string())),
        ("rxn_smiles", pa.string()),
        ("sub_frags", pa.list_(pa.int32())), ("prod_frags", pa.list_(pa.int32())),
        ("blockers", pa.list_(pa.string())), ("n_blockers", pa.int32()),
    ])
    tbl = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(tbl, out, compression="zstd")
    print(f"[lookups] reactions -> {out}  ({len(rows):,} rows, {n_built:,} with a "
          f"buildable reaction SMILES)", flush=True)
    return universe


# =====================================================================
# 2. metabolites
# =====================================================================

def build_metabolites(chem_prop: Path, out: Path):
    schema = pa.schema([
        ("mnxm", pa.string()), ("name", pa.string()), ("reference", pa.string()),
        ("formula", pa.string()), ("charge", pa.string()), ("mass", pa.string()),
        ("inchi", pa.string()), ("inchikey", pa.string()),
        ("inchikey_cc", pa.string()), ("smiles", pa.string()),
        ("has_smiles", pa.bool_()),
        ("n_C", pa.int32()), ("n_N", pa.int32()), ("n_S", pa.int32()),
        ("n_P", pa.int32()),
    ])
    w = pq.ParquetWriter(out, schema, compression="zstd")
    batch, total, n_smi = [], 0, 0
    try:
        for p in _iter_tsv(chem_prop, 9):
            s = p[8].strip()
            f = p[3].strip()
            rec = dict(mnxm=p[0], name=p[1] or None, reference=p[2] or None,
                       formula=f or None, charge=p[4] or None, mass=p[5] or None,
                       inchi=p[6] or None, inchikey=p[7] or None,
                       inchikey_cc=inchikey_connectivity(p[7]),
                       smiles=s or None, has_smiles=bool(s))
            for X in ELEMENTS:
                rec[f"n_{X}"] = count_element(f, X)
            batch.append(rec)
            total += 1
            n_smi += bool(s)
            if len(batch) >= BATCH:
                w.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch = []
        if batch:
            w.write_table(pa.Table.from_pylist(batch, schema=schema))
    finally:
        w.close()
    print(f"[lookups] metabolites -> {out}  ({total:,} rows, {n_smi:,} with a SMILES)",
          flush=True)


# =====================================================================
# 3. atom_ranks -- the identity contract
# =====================================================================

def build_atom_ranks(chem_prop: Path, universe: set, out: Path, extra_smiles=None):
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")

    smi = {}
    for p in _iter_tsv(chem_prop, 9):
        if p[0] in universe and p[8].strip():
            smi[p[0]] = p[8].strip()
    # Curated structures, when a later layer supplies them, are ordinary metabolites and
    # must be ranked in the SAME system -- a resolved carrier's sulfur is a real node.
    if extra_smiles:
        smi.update({m: s for m, s in extra_smiles.items() if s})

    rows, n_bad = [], 0
    for m, s in smi.items():
        try:
            mol = Chem.MolFromSmiles(s)
        except Exception:
            mol = None
        if mol is None:
            n_bad += 1
            continue
        rk = canonical_ranks(mol)
        if rk is None:
            n_bad += 1
            continue
        cs = canon_smiles(s)
        by_el = defaultdict(list)
        for a in mol.GetAtoms():
            if a.GetSymbol() in ELEMENTS:
                by_el[a.GetSymbol()].append(rk[a.GetIdx()])
        for X in ELEMENTS:
            r = sorted(by_el.get(X, []))
            rows.append(dict(mnxm=m, canonical_smiles=cs, element=X,
                             n_atoms=len(r), ranks=r))

    schema = pa.schema([
        ("mnxm", pa.string()), ("canonical_smiles", pa.string()),
        ("element", pa.string()), ("n_atoms", pa.int32()),
        ("ranks", pa.list_(pa.int32())),
    ])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), out, compression="zstd")
    nz = sum(1 for r in rows if r["n_atoms"])
    print(f"[lookups] atom_ranks -> {out}  ({len(rows):,} rows over "
          f"{len(smi) - n_bad:,} metabolites, {nz:,} non-empty; {n_bad:,} unrankable)",
          flush=True)


# =====================================================================
# 4. xrefs
# =====================================================================

def _xref_rows(path: Path, kind: str):
    for p in _iter_tsv(path, 3):
        src, mnx, desc = p[0], p[1], p[2]
        if not src or not mnx:
            continue
        ns, _, fid = src.partition(":")
        if not fid:
            ns, fid = "", src
        eq = None
        if kind == "reac" and "||" in desc:
            # reac_xref packs `<name>||<the source's own equation>`; the equation is the
            # only place a foreign database's own stoichiometry is available, which is
            # what makes a MetaCyc join checkable rather than trusted.
            desc, _, eq = desc.partition("||")
        yield ns, fid, mnx, desc or None, eq or None


def build_xrefs(chem_xref: Path, reac_xref: Path, out: Path):
    schema = pa.schema([
        ("kind", pa.string()), ("namespace", pa.string()), ("foreign_id", pa.string()),
        ("mnx_id", pa.string()), ("description", pa.string()),
        ("source_equation", pa.string()), ("n_mnx_for_source", pa.int32()),
    ])
    w = pq.ParquetWriter(out, schema, compression="zstd")
    totals = {}
    try:
        for kind, path in (("chem", chem_xref), ("reac", reac_xref)):
            # Pass 1: how many DISTINCT MNX ids each foreign id reaches. Measured on
            # MNXref 4.5 this is 1 for every row of both files -- so the two loaders
            # that disagreed about whether many-to-one is legal were arguing about a
            # case the data does not contain. Carried anyway: the column is the fact,
            # and a future release that breaks it should break loudly.
            seen = defaultdict(set)
            for ns, fid, mnx, _d, _e in _xref_rows(path, kind):
                seen[(ns, fid)].add(mnx)
            nmap = {k: len(v) for k, v in seen.items()}
            multi = sum(1 for v in nmap.values() if v > 1)
            del seen

            batch, n = [], 0
            for ns, fid, mnx, desc, eq in _xref_rows(path, kind):
                batch.append(dict(kind=kind, namespace=ns, foreign_id=fid, mnx_id=mnx,
                                  description=desc, source_equation=eq,
                                  n_mnx_for_source=nmap[(ns, fid)]))
                n += 1
                if len(batch) >= BATCH:
                    w.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch = []
            if batch:
                w.write_table(pa.Table.from_pylist(batch, schema=schema))
            totals[kind] = (n, len(nmap), multi)
            print(f"[lookups]   {kind}_xref: {n:,} rows, {len(nmap):,} distinct foreign "
                  f"ids, {multi:,} many-to-one", flush=True)
    finally:
        w.close()
    print(f"[lookups] xrefs -> {out}  ({sum(v[0] for v in totals.values()):,} rows)",
          flush=True)


# =====================================================================
# 5. synonyms
# =====================================================================

def _syn_metanetx(chem_prop: Path, chem_xref: Path):
    for p in _iter_tsv(chem_prop, 9):
        if p[1].strip():
            yield "metanetx", p[0], p[1].strip()
    for p in _iter_tsv(chem_xref, 3):
        # A chem_xref description is the SOURCE database's name for the compound, which
        # is exactly the vocabulary a stub was named in. Free, and the largest single
        # supplier in the index.
        if p[1].strip() and p[2].strip() and p[2].strip() != p[1].strip():
            yield "mnx_xref", p[1].strip(), p[2].strip()


def _syn_chebi(chebi_dir: Path):
    """ChEBI names + compounds, joined on the internal compound_id.

    The accession is carried as `CHEBI:<n>` because that is how chem_xref spells it,
    so a name hit here is joinable to MNXM through `xrefs` with no re-spelling.
    """
    if chebi_dir is None:
        return
    rel = sorted(p for p in Path(chebi_dir).iterdir() if p.is_dir())
    if not rel:
        return
    d = rel[-1]
    acc = {}
    with gzip.open(d / "compounds.tsv.gz", "rt", errors="replace") as fh:
        hdr = fh.readline().rstrip("\n").split("\t")
        i_id, i_name = hdr.index("id"), hdr.index("name")
        i_acc = hdr.index("chebi_accession")
        for line in fh:
            p = line.rstrip("\n").split("\t")
            if len(p) <= max(i_id, i_name, i_acc):
                continue
            acc[p[i_id]] = p[i_acc]
            if p[i_name].strip():
                yield "chebi", p[i_acc], p[i_name].strip()
    with gzip.open(d / "names.tsv.gz", "rt", errors="replace") as fh:
        hdr = fh.readline().rstrip("\n").split("\t")
        i_cid, i_name = hdr.index("compound_id"), hdr.index("name")
        for line in fh:
            p = line.rstrip("\n").split("\t")
            if len(p) <= max(i_cid, i_name):
                continue
            a = acc.get(p[i_cid])
            if a and p[i_name].strip():
                yield "chebi", a, p[i_name].strip()


def _syn_modelseed(ms_dir: Path):
    """ModelSEED names plus its `aliases` column, which is the reason this source is
    here: `Name: x; y|BiGG: z|KEGG: C00001` reconciles a dozen model namespaces into
    one field, so a stub named the way a genome-scale model names it resolves."""
    if ms_dir is None:
        return
    rel = sorted(p for p in Path(ms_dir).iterdir() if p.is_dir())
    if not rel:
        return
    f = rel[-1] / "compounds.tsv"
    with open(f, errors="replace") as fh:
        hdr = fh.readline().rstrip("\n").split("\t")
        i_id, i_name = hdr.index("id"), hdr.index("name")
        i_al = hdr.index("aliases") if "aliases" in hdr else None
        for line in fh:
            p = line.rstrip("\n").split("\t")
            if len(p) <= i_name:
                continue
            if p[i_name].strip() and p[i_name] != "null":
                yield "modelseed", p[i_id], p[i_name].strip()
            if i_al is not None and len(p) > i_al and p[i_al] not in ("", "null"):
                for group in p[i_al].split("|"):
                    _, _, names = group.partition(":")
                    for nm in names.split(";"):
                        nm = nm.strip()
                        if nm:
                            yield "modelseed", p[i_id], nm


def _syn_metacyc(mc_data: Path):
    """MetaCyc COMMON-NAME and SYNONYMS out of compounds.dat.

    LICENSED. This reads a staged drop-in and never fetches; the names go into an
    index that stays inside the build. See the module's transform for the refusal.
    """
    if mc_data is None:
        return
    f = Path(mc_data) / "compounds.dat"
    if not f.exists():
        return
    uid, names = None, []
    with open(f, "r", encoding="latin-1", errors="replace") as fh:
        for line in fh:
            if line.startswith("//"):
                for nm in names:
                    if uid and nm:
                        yield "metacyc", uid, nm
                uid, names = None, []
                continue
            if line.startswith("#") or "-" not in line:
                continue
            k, _, v = line.rstrip("\n").partition(" - ")
            if k == "UNIQUE-ID":
                uid = v.strip()
            elif k in ("COMMON-NAME", "SYNONYMS"):
                # MetaCyc marks up names with HTML-ish tags (<i>, <sub>, <SUP>); they
                # are typography, not chemistry, and leaving them in makes every marked
                # up name a normalisation miss.
                names.append(re.sub(r"<[^>]+>", "", v).strip())
    for nm in names:
        if uid and nm:
            yield "metacyc", uid, nm


def build_synonyms(chem_prop: Path, chem_xref: Path, out: Path,
                   chebi_dir=None, modelseed_dir=None, metacyc_data=None):
    schema = pa.schema([
        ("source", pa.string()), ("source_id", pa.string()), ("raw_name", pa.string()),
        ("key_c", pa.string()), ("key_a", pa.string()),
    ])
    w = pq.ParquetWriter(out, schema, compression="zstd")
    batch, tally, seen = [], Counter(), set()
    try:
        for gen in (_syn_metanetx(chem_prop, chem_xref),
                    _syn_chebi(chebi_dir),
                    _syn_modelseed(modelseed_dir),
                    _syn_metacyc(metacyc_data)):
            for source, sid, name in gen:
                kc = norm_conservative(name)
                if not kc:
                    continue
                k = (source, sid, kc)
                if k in seen:
                    continue
                seen.add(k)
                batch.append(dict(source=source, source_id=sid, raw_name=name,
                                  key_c=kc, key_a=norm_aggressive(name)))
                tally[source] += 1
                if len(batch) >= BATCH:
                    w.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch = []
        if batch:
            w.write_table(pa.Table.from_pylist(batch, schema=schema))
    finally:
        w.close()
    for s, n in tally.most_common():
        print(f"[lookups]   {s:<10} {n:>10,} names", flush=True)
    print(f"[lookups] synonyms -> {out}  ({sum(tally.values()):,} rows)", flush=True)


# =====================================================================
# read-side helpers -- so every consumer joins these the SAME way
# =====================================================================

def read_reactions(path):
    return pd.read_parquet(path)


def read_metabolites(path, columns=None):
    return pd.read_parquet(path, columns=columns)


def ranks_of(atom_ranks_path):
    """(mnxm, element) -> [ranks]. The dict `forced_pairs` and every member index off."""
    d = pd.read_parquet(atom_ranks_path, columns=["mnxm", "element", "ranks"])
    return {(r.mnxm, r.element): list(r.ranks) for r in d.itertuples(index=False)}


def canon_map(atom_ranks_path):
    """mnxm -> canonical SMILES, from the one table that computed it."""
    d = pd.read_parquet(atom_ranks_path, columns=["mnxm", "canonical_smiles"])
    d = d.drop_duplicates("mnxm")
    return {r.mnxm: r.canonical_smiles for r in d.itertuples(index=False)
            if r.canonical_smiles}


def xref_map(xrefs_path, kind: str, namespace: str, strict=True):
    """foreign id -> MNX id for ONE namespace. THE only crosswalk loader.

    `strict` refuses a foreign id that reaches more than one MNX id rather than picking
    -- which is the policy the two previous loaders disagreed about, one silently
    keeping the first and one silently keeping the last. On MNXref 4.5 the strict
    branch never fires; when a future release makes it fire, it fires loudly.
    """
    d = pd.read_parquet(xrefs_path,
                        columns=["kind", "namespace", "foreign_id", "mnx_id",
                                 "n_mnx_for_source"])
    d = d[(d["kind"] == kind) & (d["namespace"] == namespace)]
    if strict:
        bad = d[d["n_mnx_for_source"] > 1]
        if len(bad):
            raise SystemExit(
                f"[xrefs] {namespace}: {bad.foreign_id.nunique():,} foreign ids reach "
                f"more than one MNX id. Picking one silently is what made the same "
                f"crosswalk produce two different joins in one build.")
    return dict(zip(d["foreign_id"], d["mnx_id"]))


def synonym_index(synonyms_path, sources=None):
    """(exact_c, exact_a, gram_index). Built in memory; see `trigrams`."""
    d = pd.read_parquet(synonyms_path)
    if sources:
        d = d[d["source"].isin(sources)]
    exact_c, exact_a = defaultdict(list), defaultdict(list)
    gram = defaultdict(set)
    for r in d.itertuples(index=False):
        exact_c[r.key_c].append((r.source, r.source_id))
        if r.key_a:
            exact_a[r.key_a].append((r.source, r.source_id))
            for g in trigrams(r.key_a):
                gram[g].add(r.key_a)
    return dict(exact_c), dict(exact_a), dict(gram)


# =====================================================================
# driver
# =====================================================================

def cmd_build(args):
    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)
    reac_prop = Path(args.reac_prop)
    chem_prop = Path(args.chem_prop)

    universe = build_reactions(reac_prop, chem_prop, out / "reactions.parquet")
    build_metabolites(chem_prop, out / "metabolites.parquet")
    build_atom_ranks(chem_prop, universe, out / "atom_ranks.parquet")
    build_xrefs(Path(args.chem_xref), Path(args.reac_xref), out / "xrefs.parquet")
    build_synonyms(chem_prop, Path(args.chem_xref), out / "synonyms.parquet",
                   chebi_dir=args.chebi, modelseed_dir=args.modelseed,
                   metacyc_data=args.metacyc_data)
    return 0


def cmd_check(args):
    """Do the five agree with each other, and with a direct rdkit computation?

    Cheap enough to run every build. It has caught the two failures that matter: an
    atom_ranks written without breakTies (ranks not distinct within a metabolite), and
    a reactions table whose SMILES fragment count did not equal its participant count.
    """
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    d = Path(args.outdir)
    fails = []

    rx = pd.read_parquet(d / "reactions.parquet")
    ar = pd.read_parquet(d / "atom_ranks.parquet")
    mt = pd.read_parquet(d / "metabolites.parquet", columns=["mnxm", "has_smiles"])
    print(f"[check] reactions {len(rx):,} · atom_ranks {len(ar):,} · "
          f"metabolites {len(mt):,}", flush=True)

    # 1. the fragment counts account for every fragment of the built SMILES. Not
    #    `len(split(".")) == len(participants)`: a participant whose own SMILES is
    #    multi-fragment contributes more than one, which is exactly what sub_frags
    #    records and what a positional alignment gets wrong.
    built = rx[rx.rxn_smiles.notna()]
    bad = 0
    for r in built.head(5000).itertuples(index=False):
        L, _, R = r.rxn_smiles.partition(">>")
        if (len(L.split(".")) != int(sum(r.sub_frags))
                or len(R.split(".")) != int(sum(r.prod_frags))):
            bad += 1
    if bad:
        fails.append(f"{bad} of 5000 sampled reactions: SMILES fragment count != "
                     f"sum(sub_frags)/sum(prod_frags)")

    # 2. ranks are DISTINCT within a metabolite -- this is what breakTies buys, and
    #    without it (mnxm, rank) is a symmetry class rather than an atom
    g = ar[ar.n_atoms > 0].groupby("mnxm")["ranks"].apply(
        lambda s: sum(len(x) for x in s))
    gu = ar[ar.n_atoms > 0].groupby("mnxm")["ranks"].apply(
        lambda s: len({int(v) for x in s for v in x}))
    nd = int((g != gu).sum())
    if nd:
        fails.append(f"{nd:,} metabolites have a repeated canonical rank across their "
                     f"C/N/S/P atoms -- breakTies is not in effect")

    # 3. spot-check ranks against a direct rdkit computation
    smp = ar[(ar.element == "C") & (ar.n_atoms > 2)].head(200)
    mism = 0
    for r in smp.itertuples(index=False):
        mol = Chem.MolFromSmiles(r.canonical_smiles)
        if mol is None:
            mism += 1
            continue
        rk = canonical_ranks(mol)
        want = sorted(rk[a.GetIdx()] for a in mol.GetAtoms() if a.GetSymbol() == "C")
        if want != sorted(int(v) for v in r.ranks):
            mism += 1
    if mism:
        fails.append(f"{mism}/200 sampled metabolites: stored ranks != a direct "
                     f"CanonicalRankAtoms over the stored canonical SMILES")

    # 4. every participant of a buildable reaction has a rank row
    ranked = set(ar.mnxm)
    missing = {m for r in built.head(5000).itertuples(index=False)
               for m in set(r.substrates) | set(r.products)} - ranked
    if missing:
        fails.append(f"{len(missing):,} participants of buildable reactions have no "
                     f"atom_ranks row")

    for f in fails:
        print(f"[check] FAIL {f}", flush=True)
    if not fails:
        print("[check] all lookup invariants hold", flush=True)
    return 1 if fails else 0


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--reac-prop", required=True)
    p.add_argument("--chem-prop", required=True)
    p.add_argument("--reac-xref", required=True)
    p.add_argument("--chem-xref", required=True)
    p.add_argument("--chebi", default=None, help="originals/chebi/ (holds one release)")
    p.add_argument("--modelseed", default=None,
                   help="originals/modelseed/ (holds one commit)")
    p.add_argument("--metacyc-data", default=None,
                   help="the resolved MetaCyc data directory holding compounds.dat")
    p.add_argument("--outdir", required=True)

    p = sub.add_parser("check"); p.set_defaults(fn=cmd_check)
    p.add_argument("--outdir", required=True)
    return ap.parse_args()


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
