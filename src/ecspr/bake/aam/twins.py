"""Recover a structure MNXref already holds, under another id.

TWO VERBS, ONE SEARCH, TWO EVIDENCE STANDARDS
---------------------------------------------
MetaNetX files the same chemistry twice more often than it looks. A structureless
record and a structured one can be the same role (`Acceptor` MNXM8975 and `A`
MNXM35), or literally the same compound entered twice under one name. Both cases are
answered by the same question -- *does MNXref itself already hold a structure for
this?* -- so they share a module and a scan of the 3.9 M-row xref table. What differs
is what counts as proof, and that is why they are two verbs and two transforms:

  * `blockers` accepts ONLY an ELEMENT-NEUTRAL twin: zero C, N, S and P, all four
    known. Such a twin can neither absorb nor emit a mapped atom, so admitting it
    unblocks the reaction for its concrete partners and asserts nothing about them.
    That single coded predicate is what admits the generic acceptors and refuses the
    acyl carriers -- MNXM1089660, the ACP twin, carries C14 N3 S1 P1 through exactly
    the thioester an acyl stand-in must never fabricate, and the predicate refuses it
    without anyone maintaining a list.
  * `nametwin` accepts a twin with REAL atoms, and therefore demands real evidence: a
    shared source accession, or a balance that only holds after the substitution.

WHY NOT JUST WIDEN `curation.lane_acceptor`
-------------------------------------------
That lane already draws six spellings of "acceptor" as a bare `*`, which for this
graph is the same claim an element-neutral twin makes -- zero tracked atoms, one
cancelling body. The difference is generality: a hand-written regex resolves the
spellings someone thought of, while the twin search finds MNXref's own record for any
structureless role. So the overlap between the two is expected to be large, it is
reported as a first-class number, and a large one is a finding about how good the
existing gate already is rather than a disappointment about this one.

THE PREDICATES ARE ORDERED, AND THE NAME GUARD RUNS FIRST
---------------------------------------------------------
`MNXM900` "hexadecenoate" against its `CO2*` twin BALANCES -- one carbon in, one
carbon out -- so a chemistry-first ordering would admit a sixteen-carbon fatty acid
drawn as a formate. The name asserts C16 by acyl nomenclature and the twin holds C1;
that contradiction is checked before any count is compared, so the refusal names the
reason a reader can act on.

DETERMINISM IS PART OF THE CONTRACT. Every tie breaks on the accession, never on set
iteration order: a set-iteration tie-break once made a reference lane's yield wander
over 887/891/893 on byte-identical inputs, and a yield that moves without an input
moving is not a measurement.

REFUSALS SHIP BESIDE ACCEPTS. `decisions.tsv` carries one row per candidate with a
named predicate, because the refused half is where the next lane's argument comes
from.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from . import curation as C

ELEMENTS = C.ELEMENTS

# An alias carried by more than this many structured records is vocabulary, not
# evidence. "water" and "an alcohol" reach hundreds of ids; `Acceptor||A||Hydrogen-
# acceptor||Oxidized donor` reaches two. The cut is deliberately tight because the
# lane's whole claim is that the twin is THE record for this role, and the refusal
# it produces (`no_structured_twin`) is cheap while a wrong twin is not.
ALIAS_FANOUT = 8

# A one-character key matches everything; `a` is itself a metabolite name here, which
# is precisely why it needs the fanout gate rather than a length gate alone.
MIN_KEY_LEN = 1

DECISION_COLS = ("mnxm", "mnx_name", "twin", "twin_name", "twin_smiles",
                 "decision", "n_candidates", "n_reactions", "evidence")

# The refusal predicates, IN THE ORDER THEY ARE TESTED. The order is the interface:
# a candidate that trips several is reported under the first, so the reason a reader
# gets is the informative one rather than the incidental one.
BLOCKER_PREDICATES = (
    "no_name",                  # nothing to search with, and nothing to key `admit` on
    "no_alias",                 # a name, but no alias survives the fanout gate
    "no_structured_twin",       # no structured record shares any alias
    "name_contradicts_twin",    # X3: the name asserts a size the twin does not have
    "uncountable_twin",         # the twin's own counts are unknown, so neutrality is not established
    "not_element_neutral",      # the twin carries tracked atoms -- this is A2, held by code
    "ambiguous_twins",          # several neutral twins, disagreeing structures
    "twin_smiles_unusable",     # the twin's SMILES does not survive sanitisation
)
NAMETWIN_PREDICATES = (
    "no_name",
    "no_same_name_twin",
    "twin_has_no_tracked_atoms",   # element-neutral: that is `blockers`' claim, not this one
    "name_contradicts_twin",
    "uncountable_twin",
    "formula_disagrees",           # the stub HAS a formula and the twin's structure contradicts it
    "inchikey_skeleton_differs",   # both carry a connectivity block and they are different compounds
    "no_evidence",                 # neither a shared accession nor a balance that needs the twin
    "ambiguous_twins",
    "twin_smiles_unusable",
)


def _acc_key(mnxm):
    """Sort key for a MetaNetX accession: numeric, so MNXM35 precedes MNXM1102421.

    String order would put MNXM1102421 first, which is not wrong so much as arbitrary
    -- and an arbitrary tie-break is the thing this exists to stop being arbitrary.
    """
    mo = re.match(r"^([A-Za-z]+)(\d+)$", str(mnxm))
    return (mo.group(1), int(mo.group(2))) if mo else (str(mnxm), 0)


def _clean(v):
    """A parquet string column, read as a value that is either a string or absent.

    NaN IS TRUTHY, and that is the whole reason this exists. A structureless record has
    no name, no formula and no InChIKey, and pandas hands all three back as `nan` -- so
    `if ikcc:` runs the skeleton comparison on a missing block, `nan != nan` is True,
    and every structureless blocker is refused for having a different skeleton from a
    skeleton it does not have. Measured: that refused both canaries at once.
    """
    if v is None or (isinstance(v, float) and v != v):
        return None
    s = str(v).strip()
    return s or None


def _norm_unique(series: pd.Series) -> pd.Series:
    """`curation.norm` over a column, paying for each DISTINCT string once.

    ONE NORMALISER IN THIS LANE. `lookup::synonyms` ships its own `key_c`/`key_a`
    columns, and joining a key made by one normaliser against a key made by another
    is a miss that looks like an absence -- so the raw names are re-normalised here
    with the same function the proposer lanes use, and the shipped keys are not read.
    """
    vals = pd.Index(series.dropna().unique())
    lut = {v: C.norm(v) for v in vals}
    return series.map(lut)


# =====================================================================
# the tables, indexed the one way both verbs need them
# =====================================================================

class Tables:
    """Metabolites, reactions, the recount, and the target set."""

    def __init__(self, lookups: Path, element_counts: Path, worklist=None):
        lookups = Path(lookups)
        self.mets = pd.read_parquet(
            lookups / "metabolites.parquet",
            columns=["mnxm", "name", "formula", "smiles", "has_smiles", "inchikey_cc"])
        self.name_of = {m: _clean(v)
                        for m, v in zip(self.mets["mnxm"], self.mets["name"])}
        self.formula_of = {m: _clean(v)
                           for m, v in zip(self.mets["mnxm"], self.mets["formula"])}
        struct = self.mets[self.mets["has_smiles"]]
        self.smiles_of = {m: v for m, v in zip(struct["mnxm"], struct["smiles"])
                          if _clean(v)}
        self.ikcc_of = {m: _clean(v)
                        for m, v in zip(self.mets["mnxm"], self.mets["inchikey_cc"])}

        self.counts, self.residue, self.source = read_element_counts(element_counts)

        rx = pd.read_parquet(lookups / "reactions.parquet",
                             columns=["mnxr", "substrates", "products",
                                      "blockers", "n_blockers"])
        if worklist is not None:
            wl = pd.read_parquet(worklist, columns=["mnxr", "verdict"])
            keep = set(wl.loc[wl["verdict"] == "blocked_no_structure", "mnxr"])
            rx = rx[rx["mnxr"].isin(keep)]
        else:
            rx = rx[rx["n_blockers"] > 0]
        self.targets = rx

        # blocker -> the target reactions it gates. Sorted, so every downstream
        # iteration over it is in one order.
        self.rxns_of = defaultdict(list)
        for r in rx.itertuples(index=False):
            for m in r.blockers:
                self.rxns_of[m].append(r)
        self.blockers = sorted((m for m in self.rxns_of if m not in self.smiles_of),
                               key=_acc_key)
        print(f"[twins] {len(self.targets):,} target reactions, "
              f"{len(self.blockers):,} structureless blockers, "
              f"{len(self.counts):,} metabolites with a complete recount", flush=True)

    def counts_of(self, m):
        """(C, N, S, P) or None. None is UNKNOWN and is never a zero."""
        return self.counts.get(m)


def read_element_counts(path):
    """`lookup::element_counts` -> (counts, residue, source), keyed on mnxm.

    A metabolite reaches `counts` only when all four elements are known. A partial
    recount cannot support either verb: neutrality is a claim about all four, and a
    balance needs every participant's count for the element being balanced.
    """
    d = pd.read_parquet(path, columns=["mnxm", "element", "n_atoms",
                                       "n_residue", "source"])
    counts, residue, source = {}, {}, {}
    per = defaultdict(dict)
    for r in d.itertuples(index=False):
        per[r.mnxm][r.element] = r.n_atoms
        residue[r.mnxm] = r.n_residue
        source[r.mnxm] = r.source
    for m, e in per.items():
        vals = [e.get(X) for X in ELEMENTS]
        if all(v is not None and not pd.isna(v) for v in vals):
            counts[m] = tuple(int(v) for v in vals)
    return counts, residue, source


# =====================================================================
# the alias index -- what MNXref calls a thing, in MNXref's own vocabulary
# =====================================================================

def _chem_xrefs(lookups: Path):
    x = pd.read_parquet(Path(lookups) / "xrefs.parquet",
                        columns=["kind", "namespace", "foreign_id", "mnx_id",
                                 "description"])
    return x[x["kind"] == "chem"]


def alias_index(lookups: Path, tab: Tables, use_synonyms=True):
    """(blocker -> alias keys, alias key -> structured mnxms carrying it).

    THREE VOCABULARIES, ONE NORMALISER. The metabolite's own name; the `||`-separated
    descriptions each source database attached to its cross-reference; and, when a
    synonym index is given, what ChEBI / ModelSEED / MetaCyc call the same accession.
    The twin is always an MNXM -- widening the vocabulary widens how the twin is
    FOUND, never what may be admitted as one.

    Built in two passes so the memory is bounded by the blocker set rather than by
    the 1.5 M-row compound table: collect the blockers' aliases first, then look for
    structured records carrying exactly those.
    """
    blk = set(tab.blockers)
    struct = set(tab.smiles_of)

    xr = _chem_xrefs(lookups)
    # A description is a `||`-joined list of the source's names for the compound.
    # MNXM8975 and MNXM35 share `Acceptor||A||Hydrogen-acceptor||Oxidized donor`
    # under DIFFERENT accessions across kegg, seed and sabiork -- which is why the
    # description is evidence here where the accession is not.
    desc = xr[["mnx_id", "description"]].dropna()
    # `regex=False` IS LOAD-BEARING. Pandas reads a multi-character `pat` as a regular
    # expression, and `||` as a regex is an alternation of two empty patterns -- which
    # matches between every pair of characters. Measured: `Acceptor` came back as the
    # alias set {a, c, e, o, p, r, t}, single letters that then matched half the
    # compound table. The separator is a literal.
    desc = desc.assign(tok=desc["description"].str.split("||", regex=False)) \
               .explode("tok")
    desc["key"] = _norm_unique(desc["tok"])
    desc = desc[desc["key"].str.len() >= MIN_KEY_LEN]

    names = tab.mets[["mnxm", "name"]].dropna().copy()
    names["key"] = _norm_unique(names["name"])
    names = names[names["key"].str.len() >= MIN_KEY_LEN]

    pairs = [names[["mnxm", "key"]],
             desc[["mnx_id", "key"]].rename(columns={"mnx_id": "mnxm"})]

    if use_synonyms:
        syn = pd.read_parquet(Path(lookups) / "synonyms.parquet",
                              columns=["source", "source_id", "raw_name"])
        # `n_mnx_for_source` is 1 on every row of MNXref 4.5, so a foreign id names
        # exactly one MNXM and this map is a function rather than a choice.
        fid2mnx = dict(zip(xr["foreign_id"], xr["mnx_id"]))
        direct = syn["source"] == "metanetx"
        syn["mnxm"] = syn["source_id"].where(direct,
                                             syn["source_id"].map(fid2mnx))
        syn = syn.dropna(subset=["mnxm"])
        syn["key"] = _norm_unique(syn["raw_name"])
        syn = syn[syn["key"].str.len() >= MIN_KEY_LEN]
        pairs.append(syn[["mnxm", "key"]])

    allp = pd.concat(pairs, ignore_index=True).drop_duplicates()

    of_blocker = defaultdict(set)
    for m, k in zip(allp["mnxm"], allp["key"]):
        if m in blk:
            of_blocker[m].add(k)

    wanted = set().union(*of_blocker.values()) if of_blocker else set()
    carriers = defaultdict(set)
    for m, k in zip(allp["mnxm"], allp["key"]):
        if k in wanted and m in struct:
            carriers[k].add(m)

    # THE FANOUT GATE. A key on many structured records is the vocabulary of a class,
    # not the identity of a record, and admitting a twin off one is how a lane starts
    # borrowing an arbitrary member of a family.
    dropped = {k for k, v in carriers.items() if len(v) > ALIAS_FANOUT}
    for k in dropped:
        del carriers[k]
    print(f"[twins] alias index: {len(wanted):,} blocker alias keys, "
          f"{len(carriers):,} resolve to <= {ALIAS_FANOUT} structured records "
          f"({len(dropped):,} dropped as too common)", flush=True)
    return of_blocker, carriers


def accessions_of(lookups: Path, mnxms: set):
    """mnxm -> {(namespace, foreign_id)} -- the hard identity evidence `nametwin` wants."""
    xr = _chem_xrefs(lookups)
    xr = xr[xr["mnx_id"].isin(mnxms)]
    out = defaultdict(set)
    for ns, fid, m in zip(xr["namespace"], xr["foreign_id"], xr["mnx_id"]):
        out[m].add((ns, fid))
    return out


# =====================================================================
# the guards
# =====================================================================

def name_budget(name):
    """The C/N/S/P a NAME asserts on its own, or None when it asserts nothing.

    Only the two routes that read a count straight out of nomenclature: an acyl chain
    length (`hexadecenoate` -> 16 carbons) and a spelled-out peptide. Anything vaguer
    is not a contradiction, and a guard that fires on a guess would refuse more than
    it protects.
    """
    n = C.norm(name)
    if not n:
        return None
    for fn in (C.acyl_budget, C.peptide_budget):
        try:
            b = fn(n)
        except Exception:
            b = None
        if b is not None:
            return tuple(int(x) for x in b)
    return None


def name_contradicts(name, twin_counts):
    """Does the name's own claim disagree with what the twin actually holds?

    Runs BEFORE any balance or neutrality test. `MNXM900` 'hexadecenoate' against a
    `CO2*` twin balances perfectly -- one carbon each side -- and only the name says
    the substitution turned a C16 fatty acid into a formate.
    """
    b = name_budget(name)
    if b is None or twin_counts is None:
        return None
    for X, want, got in zip(ELEMENTS, b, twin_counts):
        if want != got:
            return f"name asserts {want} {X}, twin holds {got}"
    return None


def formula_disagrees(formula, twin_counts):
    """The stub's own formula against the twin's structure, where the stub has one.

    Structureless does not always mean formula-less; when a formula IS readable it is
    an independent statement about the same compound, so a twin that contradicts it
    is a different compound whatever it is called.
    """
    if twin_counts is None:
        return None
    for X, got in zip(ELEMENTS, twin_counts):
        want = C.count_formula(formula, X)
        if want is not None and want != got:
            return f"formula says {want} {X}, twin holds {got}"
    return None


def substituted_balance(tab: Tables, blocker, twin):
    """The first (mnxr, element) that balances ONLY because the twin was substituted.

    Balance is not evidence of identity in general -- one unknown always back-fills
    the residual. It is evidence HERE because the substitution is not free: every
    other participant's count is fixed, so a wrong twin has to hit an exact number to
    balance, and the residue slots have to cancel on top of it.
    """
    from . import recount as RC

    tc = tab.counts_of(twin)
    if tc is None:
        return None
    for r in tab.rxns_of.get(blocker, []):
        for i, X in enumerate(ELEMENTS):
            sides, res, ok = {}, {}, True
            for side, ms in (("s", r.substrates), ("p", r.products)):
                n, rr = 0, []
                for m in ms:
                    c = tc[i] if m == blocker else None
                    if c is None:
                        cc = tab.counts_of(m)
                        if cc is None:
                            ok = False
                            break
                        c = cc[i]
                    n += c
                    rr.append(tab.residue.get(twin if m == blocker else m))
                if not ok:
                    break
                sides[side], res[side] = n, rr
            if not ok:
                continue
            if sides["s"] == 0 and sides["p"] == 0:
                continue
            if RC.residue_slots_cancel(res["s"], res["p"]) is not True:
                continue
            if sides["s"] == sides["p"]:
                return (r.mnxr, X)
    return None


def _crosswalk_rows(mnxm, name, smiles, counts, basis, lane):
    """The proposal, in `curation.CROSSWALK_COLS` shape, self-checked against rdkit.

    Returns None when the twin's SMILES does not survive the sanitising parse `admit`
    performs. `admit` ABORTS THE RUN on the first row it cannot verify, so a row that
    would fail there has to be refused here -- otherwise one unsanitisable structure
    in MNXref takes down a mapping pass.

    A twin with no tracked atoms still emits ONE row, element C with zero atoms. It
    contributes no atom pair and is not meant to: it exists so the metabolite is
    recorded as RESOLVED, which is what unblocks the reaction for its concrete
    partners.
    """
    for X, n in zip(ELEMENTS, counts):
        got = C.count_struct(smiles, X)
        if got is None or got != n:
            return None
    rows = C._row(mnxm, smiles, name, counts, basis, lane)
    if not rows:
        rows = [dict(mnxm=mnxm, smiles=smiles, mnx_name=name, element="C",
                     n_atoms=0, basis=basis, lane=lane)]
    return rows


# =====================================================================
# verb 1 -- blockers: an element-neutral twin, and nothing else
# =====================================================================

def resolve_blockers(tab: Tables, of_blocker, carriers):
    decisions, cross = [], []

    def decide(m, pred, twin=None, n_cand=0, evidence=""):
        decisions.append(dict(
            mnxm=m, mnx_name=tab.name_of.get(m), twin=twin,
            twin_name=tab.name_of.get(twin) if twin else None,
            twin_smiles=tab.smiles_of.get(twin) if twin else None,
            decision=pred, n_candidates=n_cand,
            n_reactions=len(tab.rxns_of.get(m, [])), evidence=evidence))

    for m in tab.blockers:
        name = tab.name_of.get(m)
        if not str(name or "").strip():
            decide(m, "refuse:no_name")
            continue
        keys = sorted(of_blocker.get(m, ()))
        cands = sorted({t for k in keys for t in carriers.get(k, ())} - {m},
                       key=_acc_key)
        if not keys:
            decide(m, "refuse:no_alias")
            continue
        if not cands:
            decide(m, "refuse:no_structured_twin", n_cand=0,
                   evidence=f"{len(keys)} alias keys, none on a structured record")
            continue

        # THE NAME GUARD, over every candidate, before any chemistry. A name that
        # contradicts one twin contradicts the claim being made, so it refuses the
        # blocker rather than merely dropping that candidate.
        contra = None
        for t in cands:
            why = name_contradicts(name, tab.counts_of(t))
            if why:
                contra = (t, why)
                break
        if contra:
            decide(m, "refuse:name_contradicts_twin", twin=contra[0],
                   n_cand=len(cands), evidence=contra[1])
            continue

        unknown = [t for t in cands if tab.counts_of(t) is None]
        neutral = [t for t in cands if tab.counts_of(t) == (0, 0, 0, 0)]
        if not neutral and unknown:
            decide(m, "refuse:uncountable_twin", twin=unknown[0], n_cand=len(cands),
                   evidence=f"{len(unknown)}/{len(cands)} candidates have no complete "
                            f"recount, so element-neutrality is not established")
            continue
        if not neutral:
            t = cands[0]
            decide(m, "refuse:not_element_neutral", twin=t, n_cand=len(cands),
                   evidence=f"{t} holds C/N/S/P {tab.counts_of(t)}; a twin carrying "
                            f"tracked atoms would route real atoms through a "
                            f"substituted structure")
            continue

        smis = {tab.smiles_of[t] for t in neutral}
        if len(smis) > 1:
            decide(m, "refuse:ambiguous_twins", twin=neutral[0], n_cand=len(cands),
                   evidence=f"{len(neutral)} element-neutral twins with "
                            f"{len(smis)} distinct structures")
            continue

        t = neutral[0]                      # sorted on the accession, never on a set
        smi = tab.smiles_of[t]
        basis = (f"element-neutral twin: MNXref's own record {t} "
                 f"'{tab.name_of.get(t)}' carries the same alias as {m} '{name}' "
                 f"(shared key '{sorted(k for k in keys if t in carriers.get(k, ()))[0]}') "
                 f"and holds zero C/N/S/P by recount ({tab.source.get(t)} route, "
                 f"{tab.residue.get(t)} residue slots), so it can neither absorb nor "
                 f"emit a mapped atom; body cancellation and per-element balance are "
                 f"tested by the arbiter")
        rows = _crosswalk_rows(m, name, smi, (0, 0, 0, 0), basis, "blockers")
        if rows is None:
            decide(m, "refuse:twin_smiles_unusable", twin=t, n_cand=len(cands),
                   evidence=f"{smi!r} does not survive the sanitising parse `admit` runs")
            continue
        cross += rows
        decide(m, "accept", twin=t, n_cand=len(cands), evidence=basis)
    return decisions, cross


# =====================================================================
# verb 2 -- nametwin: the same compound, entered twice
# =====================================================================

def resolve_nametwins(tab: Tables, accs):
    decisions, cross = [], []

    # name key -> structured records carrying it. SAME-NAME means the metabolite's own
    # name, not its cross-reference vocabulary: a duplicate record is a duplicate ENTRY,
    # and the description tokens that find a role twin would find a family here.
    by_name = defaultdict(set)
    struct = set(tab.smiles_of)
    nm = tab.mets[["mnxm", "name"]].dropna().copy()
    nm["key"] = _norm_unique(nm["name"])
    for m, k in zip(nm["mnxm"], nm["key"]):
        if m in struct and k:
            for a in C.base_aliases(k):
                by_name[a].add(m)

    def decide(m, pred, twin=None, n_cand=0, evidence=""):
        decisions.append(dict(
            mnxm=m, mnx_name=tab.name_of.get(m), twin=twin,
            twin_name=tab.name_of.get(twin) if twin else None,
            twin_smiles=tab.smiles_of.get(twin) if twin else None,
            decision=pred, n_candidates=n_cand,
            n_reactions=len(tab.rxns_of.get(m, [])), evidence=evidence))

    for m in tab.blockers:
        name = tab.name_of.get(m)
        if not str(name or "").strip():
            decide(m, "refuse:no_name")
            continue
        k = C.norm(name)
        cands = sorted({t for a in C.base_aliases(k) for t in by_name.get(a, ())} - {m},
                       key=_acc_key)
        if not cands:
            decide(m, "refuse:no_same_name_twin")
            continue

        contra = None
        for t in cands:
            why = name_contradicts(name, tab.counts_of(t))
            if why:
                contra = (t, why)
                break
        if contra:
            decide(m, "refuse:name_contradicts_twin", twin=contra[0],
                   n_cand=len(cands), evidence=contra[1])
            continue

        real = [t for t in cands
                if tab.counts_of(t) is not None and any(tab.counts_of(t))]
        if not real:
            unknown = [t for t in cands if tab.counts_of(t) is None]
            if unknown:
                decide(m, "refuse:uncountable_twin", twin=unknown[0], n_cand=len(cands),
                       evidence="no candidate has a complete recount")
            else:
                decide(m, "refuse:twin_has_no_tracked_atoms", twin=cands[0],
                       n_cand=len(cands),
                       evidence="every same-name twin is element-neutral; that is the "
                                "`blockers` claim, which is gated differently")
            continue

        bad = None
        for t in real:
            why = formula_disagrees(tab.formula_of.get(m), tab.counts_of(t))
            if why:
                bad = (t, why)
                break
        if bad:
            decide(m, "refuse:formula_disagrees", twin=bad[0], n_cand=len(cands),
                   evidence=bad[1])
            continue

        # THE CONNECTIVITY BLOCK, where both records carry one. Two compounds with the
        # same name and different skeletons are two compounds; `UDP` the glycan and
        # `UDP` the nucleotide is exactly that case.
        mine = tab.ikcc_of.get(m)
        if mine:
            skew = [t for t in real
                    if tab.ikcc_of.get(t) and tab.ikcc_of.get(t) != mine]
            real = [t for t in real if t not in skew]
            if not real:
                decide(m, "refuse:inchikey_skeleton_differs", twin=skew[0],
                       n_cand=len(cands),
                       evidence=f"connectivity block {mine} vs "
                                f"{tab.ikcc_of.get(skew[0])}")
                continue

        # EVIDENCE, and the order is the strength order. A shared accession is two
        # source databases saying these are one compound; a balance is chemistry that
        # only closes after the substitution.
        chosen, kind, ev = None, None, ""
        for t in real:
            shared = accs.get(m, set()) & accs.get(t, set())
            if shared:
                chosen, kind = t, "accession"
                ev = (f"{len(shared)} shared source accession(s), e.g. "
                      f"{sorted(shared)[0][0]}:{sorted(shared)[0][1]}")
                break
        if chosen is None:
            for t in real:
                hit = substituted_balance(tab, m, t)
                if hit:
                    chosen, kind = t, "balance"
                    ev = (f"substituting {t} closes the {hit[1]} balance of {hit[0]} "
                          f"with the residue slots cancelling")
                    break
        if chosen is None:
            decide(m, "refuse:no_evidence", twin=real[0], n_cand=len(cands),
                   evidence="no shared source accession, and no reaction balances "
                            "after the substitution")
            continue

        if kind == "accession":
            equals = [t for t in real if accs.get(m, set()) & accs.get(t, set())]
            if len({tab.smiles_of[t] for t in equals}) > 1:
                decide(m, "refuse:ambiguous_twins", twin=chosen, n_cand=len(cands),
                       evidence=f"{len(equals)} accession-sharing twins with "
                                f"different structures")
                continue

        cnt = tab.counts_of(chosen)
        basis = (f"same-name twin ({kind}): MNXref holds {chosen} "
                 f"'{tab.name_of.get(chosen)}' with a structure while {m} '{name}' "
                 f"has none; {ev}. C/N/S/P {cnt} taken from the twin's structure "
                 f"({tab.source.get(chosen)} route)")
        rows = _crosswalk_rows(m, name, tab.smiles_of[chosen], cnt, basis, "nametwin")
        if rows is None:
            decide(m, "refuse:twin_smiles_unusable", twin=chosen, n_cand=len(cands),
                   evidence=f"{tab.smiles_of[chosen]!r} does not survive the "
                            f"sanitising parse `admit` runs")
            continue
        cross += rows
        decide(m, "accept", twin=chosen, n_cand=len(cands), evidence=basis)
    return decisions, cross


# =====================================================================
# reporting
# =====================================================================

def summarise(tab: Tables, decisions, lane, overlap_with_acceptor=False):
    """The tally, plus -- for `blockers` -- the number this lane exists to report.

    `curation.lane_acceptor` already draws six spellings of a generic acceptor as a
    bare `*`, which is the same claim an element-neutral twin makes. The fraction of
    accepts that lane would also have made is the honest measure of what the twin
    search adds, and it belongs beside the yield rather than in a footnote.
    """
    rows, tally = [], Counter()
    acc = [d for d in decisions if d["decision"] == "accept"]
    for d in decisions:
        tally[d["decision"]] += 1
    covered = sorted({r.mnxr for d in acc for r in tab.rxns_of.get(d["mnxm"], [])})
    for k, n in sorted(tally.items(), key=lambda kv: (-kv[1], kv[0])):
        rows.append(dict(lane=lane, metric=k, value=n))
    rows.append(dict(lane=lane, metric="reactions_unblocked_by_accepts",
                     value=len(covered)))
    if overlap_with_acceptor:
        ov = [d for d in acc
              if C._GENACC.match((tab.name_of.get(d["mnxm"]) or "").strip())
              or C._PROTEIN.search(tab.name_of.get(d["mnxm"]) or "")]
        ov_rx = sorted({r.mnxr for d in ov for r in tab.rxns_of.get(d["mnxm"], [])})
        rows.append(dict(lane=lane, metric="accepts_also_matched_by_lane_acceptor",
                         value=len(ov)))
        rows.append(dict(lane=lane, metric="reactions_also_reachable_by_lane_acceptor",
                         value=len(ov_rx)))
        rows.append(dict(lane=lane, metric="reactions_only_this_lane_reaches",
                         value=len(set(covered) - set(ov_rx))))
    for r in rows:
        print(f"[twins]   {r['metric']:<44} {r['value']:>7,}", flush=True)
    return pd.DataFrame(rows, columns=["lane", "metric", "value"])


def _write(outdir: Path, lane, decisions, cross, summary):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(decisions, columns=list(DECISION_COLS)).to_csv(
        outdir / "decisions.tsv", sep="\t", index=False)
    pd.DataFrame(cross, columns=list(C.CROSSWALK_COLS)).to_csv(
        outdir / "crosswalk.tsv", sep="\t", index=False)
    summary.to_csv(outdir / "summary.tsv", sep="\t", index=False)
    print(f"[twins] {lane}: {len(cross):,} crosswalk rows over "
          f"{len({r['mnxm'] for r in cross}):,} metabolites -> {outdir}", flush=True)


def cmd_blockers(args):
    tab = Tables(args.lookups, args.element_counts, args.worklist)
    of_blocker, carriers = alias_index(Path(args.lookups), tab,
                                       use_synonyms=not args.no_synonyms)
    decisions, cross = resolve_blockers(tab, of_blocker, carriers)
    _write(args.out, "blockers", decisions, cross,
           summarise(tab, decisions, "blockers", overlap_with_acceptor=True))
    return 0


def cmd_nametwin(args):
    tab = Tables(args.lookups, args.element_counts, args.worklist)
    accs = accessions_of(Path(args.lookups), set(tab.blockers) | set(tab.smiles_of))
    decisions, cross = resolve_nametwins(tab, accs)
    _write(args.out, "nametwin", decisions, cross,
           summarise(tab, decisions, "nametwin"))
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    for verb, fn in (("blockers", cmd_blockers), ("nametwin", cmd_nametwin)):
        p = sub.add_parser(verb)
        p.set_defaults(fn=fn)
        p.add_argument("--lookups", required=True)
        p.add_argument("--element-counts", required=True,
                       help="lookup::element_counts -- the recount. Both verbs read "
                            "counts from here rather than from the formula columns, "
                            "which is the whole reason a `*` species can be judged")
        p.add_argument("--worklist", default=None,
                       help="interm::aam_worklist -- restricts the search to the "
                            "reactions adjudicated `blocked_no_structure`")
        p.add_argument("--out", required=True, help="the output DIRECTORY")
        if verb == "blockers":
            p.add_argument("--no-synonyms", action="store_true",
                           help="search MetaNetX's own names and xref descriptions "
                                "only, without the ChEBI/ModelSEED/MetaCyc vocabulary")
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
