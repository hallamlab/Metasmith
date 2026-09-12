"""Layer 3 -- the curation sweep: propose a structure, then let the chemistry judge it.

THE PROBLEM THIS SOLVES
-----------------------
Every mapper needs a SMILES for EVERY participant, so ONE structureless participant
sinks a whole reaction before any mapper runs. Measured here on MNXref 4.5: of 83,667
parseable reactions only 57,593 have a buildable reaction SMILES -- 26,074 are lost this
way, and the casualties are not exotic. Sulfate assimilation is severed at sulfite ->
H2S; the MEP pathway is severed at HMBPP -> IPP, which is the ONLY reaction joining IPP
to the rest of carbon metabolism.

The blockers have no formula, no InChI, no InChIKey and no SMILES -- that is what
"structureless" means. What they DO have, 100% of the time, is a NAME. So the name is
the entire handle, and using the name as both the claim and its only support is trap #1:
it has already produced false verdicts in this lane.

THE SHAPE: PROPOSER -> GATE -> EXTRACT, WITH ONE ARBITER
--------------------------------------------------------
Eleven ways of arguing from a name, ONE way of deciding. Every lane emits the same
six-column record --

    mnxm  smiles  mnx_name  element  n_atoms  basis

-- and one arbiter disposes of all of them: curated `*` bodies must pair across the
equation, then Indigo must map the completed reaction, then per-element mass balance
decides. A reaction BANKS if it maps AND at least one element balances, and it banks
only for the elements that balanced.

That last clause is what makes the whole thing safe rather than merely plausible. A
stand-in asserts its carrier is CONSERVED for an element; the assertion is not trusted,
it is TESTED, per reaction and per element. Ferredoxin is inert to carbon in IspH and is
the sulfur DONOR in biotin synthase -- so the same stand-in is admitted for carbon there
and refused for sulfur here, with no special-casing anywhere.

PLACEHOLDER vs RESOLVED -- THE DISTINCTION THE WHOLE DESIGN TURNS ON
--------------------------------------------------------------------
  * A PLACEHOLDER is scaffolding. It stands in for a generic carrier so the mapper sees
    a chemically sane reaction, and its atoms are SUPPRESSED downstream -- they are not
    that molecule's atoms, so they must never become graph nodes.
  * A RESOLVED metabolite IS the metabolite. Its structure is supplied where MetaNetX
    has none, and its atoms are REAL: they become nodes, and they must, because the
    entire payoff is that the carrier's sulfur reaches the sink. Suppressing it would
    kill the very edge the row exists to license.

WHAT MAKES A CURATED ROW HONEST, GIVEN THAT IT CANNOT BE VERIFIED
-----------------------------------------------------------------
A curated row is an ASSERTION and nothing can check it against MetaNetX -- structureless
means there is nothing there to check against. Its warrant is the `basis` citation. The
code's job is not to validate the biology but to refuse the ways a row could be silently
wrong, and `admit` refuses four of them: overriding a structure MetaNetX already has,
naming an id that no longer means what the curator thought, asserting an atom count its
own SMILES does not contain, and carrying no citation at all. It refuses by ABORTING
THE RUN on the first bad row, which is why `--dry-run` exists: find out before an
expensive pass, not during one.
"""
from __future__ import annotations

import argparse
import gzip
import re
import sys
import time
from collections import Counter, defaultdict
from io import StringIO
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .. import atom_pairs as AP
from . import worklist as aam_worklist

ELEMENTS = ("C", "N", "S", "P")

RESCUED_SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("verdict", pa.string()), ("rxn_smiles", pa.string()),
    ("atoms", pa.int32()), ("chars", pa.int32()),
])
CROSSWALK_COLS = ("mnxm", "smiles", "mnx_name", "element", "n_atoms", "basis", "lane")

# Lane priority for the merge. A metabolite is claimed by exactly ONE lane, so all of
# its element rows come from one argument -- mixing a twin's carbon count with a
# fragment-sum's nitrogen count would be a structure no lane actually proposed.
# CHANGING THIS ORDER CHANGES THE RESULT, which is why it is a named constant.
#
# The first five are the original order and are left exactly as they were, so the lanes
# that were measured keep the claims they were measured making. The three added lanes
# all propose `*`-BODY VEHICLES -- an asserted atom budget plus a body drawn as a dummy --
# which is a weaker claim than a structure borrowed from a named twin or fetched from
# ChEBI by accession, so they go after. `acceptor` is last but for `override` because it
# is the deliberate bypass of `REFUSE`, and a bypass should claim only what nothing else
# would.
# `lipid` sits below every lane that FINDS a structure someone asserted and above every
# lane that draws a conserved body as `*`: it is a construction, so a real record beats
# it, but it is a complete concrete structure, so it beats a placeholder.
# `nametwin` and `blockers` are produced OUTSIDE this module -- `aam.twins` writes them
# as crosswalks and they are read in like `override`. They bracket the existing order
# because they are the strongest and the weakest arguments in it. `nametwin` is first:
# it recovers MNXref's OWN structured record for the same compound, gated on a shared
# source accession or on a balance that closes only after the substitution, which is
# harder evidence than any name-stem inference below it. `blockers` sits immediately
# above `acceptor` because it makes the SAME claim -- a body with zero tracked atoms --
# from MNXref's own record rather than from six hand-written spellings, so where both
# fire the record wins and the regex stays as the fallback it was always meant to be.
# `llm` is the weakest argument here and sits last but for the bypass. Every other lane
# reasons from a record -- a twin's own structure, an xref, a name stem MNXref wrote --
# while this one reasons from a model's assertion, tested against a balance recount and
# nothing else. A tested assertion is real evidence, which is why it is admitted at all;
# it is still the thing to yield when any lane sourced from a record also fires.
LANE_PRIORITY = ("nametwin", "twin", "transform", "fragment", "carrier", "supplier",
                 "lipid", "conserved", "polymer", "blockers", "acceptor", "llm",
                 "override")


FE_OX, FE_RED = "[Fe+3]", "[Fe+2]"
DITHIOL, DISULFIDE = "SCCCS", "C1CCSS1"
QUINONE, QUINOL = "O=C1C=CC(=O)C=C1", "Oc1ccc(O)cc1"

# Order matters: first match wins. Each ox/red pair is atom-matched on purpose --
# identical heavy-atom skeletons differing only in oxidation state -- so the mapper pairs
# them with each other trivially and cannot confuse them with a concrete metabolite.
PLACEHOLDERS = [
    (r"^oxidized \[?2fe-2s\]?[- ]\[?ferredoxin", FE_OX, "fe_s_carrier"),
    (r"^reduced \[?2fe-2s\]?[- ]\[?ferredoxin", FE_RED, "fe_s_carrier"),
    (r"^oxidized \[?4fe-4s\]?[- ]\[?ferredoxin", FE_OX, "fe_s_carrier"),
    (r"^reduced \[?4fe-4s\]?[- ]\[?ferredoxin", FE_RED, "fe_s_carrier"),
    (r"^oxidized \[?ferredoxin", FE_OX, "fe_s_carrier"),
    (r"^reduced \[?ferredoxin", FE_RED, "fe_s_carrier"),
    (r"^oxidized \[?flavodoxin", FE_OX, "flavo_carrier"),
    (r"^reduced \[?flavodoxin", FE_RED, "flavo_carrier"),
    (r"^oxidized \[?electron-transfer flavoprotein", FE_OX, "etf_carrier"),
    (r"^reduced \[?electron-transfer flavoprotein", FE_RED, "etf_carrier"),
    (r"^fe\(iii\)-\[?cytochrome", FE_OX, "cytochrome"),
    (r"^fe\(ii\)-\[?cytochrome", FE_RED, "cytochrome"),
    (r"^oxidized \[?rubredoxin", FE_OX, "rubredoxin"),
    (r"^reduced \[?rubredoxin", FE_RED, "rubredoxin"),
    (r"^\[?oxidized \[?adrenodoxin", FE_OX, "adrenodoxin"),
    (r"^\[?reduced \[?adrenodoxin", FE_RED, "adrenodoxin"),
    (r"^\[?oxidized \[?nadph[- ]+hemoprotein reductase", FE_OX, "hemoprotein_reductase"),
    (r"^\[?reduced \[?nadph[- ]+hemoprotein reductase", FE_RED, "hemoprotein_reductase"),
    (r"^ferricytochrome", FE_OX, "cytochrome_ferri"),
    (r"^ferrocytochrome", FE_RED, "cytochrome_ferri"),
    (r"^\[?thioredoxin\]?-dithiol", DITHIOL, "thiol_carrier"),
    (r"^\[?thioredoxin\]?-disulfide", DISULFIDE, "thiol_carrier"),
    (r"^\[?glutaredoxin\]?-dithiol", DITHIOL, "thiol_carrier"),
    (r"^\[?glutaredoxin\]?-disulfide", DISULFIDE, "thiol_carrier"),
    (r"^an? ubiquinone$", QUINONE, "quinone"),
    (r"^an? ubiquinol$", QUINOL, "quinone"),
    (r"^an? menaquinone$", QUINONE, "quinone"),
    (r"^an? menaquinol$", QUINOL, "quinone"),
    (r"^an? demethylmenaquinone$", QUINONE, "quinone"),
    (r"^an? demethylmenaquinol$", QUINOL, "quinone"),
]
_COMPILED = [(re.compile(p, re.I), s, t) for p, s, t in PLACEHOLDERS]

REFUSE = re.compile(
    r"^(unknown|carbon|d|nad|nadh|idh\d*|enzyme-\w+ complex|acceptor|reduced acceptor"
    r"|.*\bacp\b.*|.*acyl-carrier.*|.*acyl carrier.*|starch|chitin|.*tRNA.*"
    r"|a|ah2|reduced acceptor"
    r"|e\(-\)|e-|hnu|hn|h\N{GREEK SMALL LETTER NU}|photon|light"
    r"|phosphoprotein|.*\[protein\]|protein .*)$", re.I)


def placeholder_for(name: str):
    if not name:
        return None
    n = str(name).strip()
    if REFUSE.match(n):
        return None
    for rx, smi, tag in _COMPILED:
        if rx.match(n):
            return smi, tag
    return None


def count_struct(smiles: str, X: str):
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    return sum(1 for a in mol.GetAtoms() if a.GetSymbol() == X)


def read_crosswalk(path):
    kept = [ln for ln in Path(path).read_text().splitlines()
            if not ln.lstrip().startswith("#")]
    return pd.read_csv(StringIO("\n".join(kept)), sep="\t")


def admit(rows: pd.DataFrame, mets: pd.DataFrame, strict=True):
    need = {"mnxm", "smiles", "mnx_name", "element", "n_atoms", "basis"}
    missing = need - set(rows.columns)
    if missing:
        raise SystemExit(f"[curation] crosswalk lacks columns: {sorted(missing)}")
    idx = mets.set_index("mnxm")
    out = {}
    for r in rows.itertuples(index=False):
        if r.mnxm not in idx.index:
            raise SystemExit(f"[curation] {r.mnxm}: not in the metabolite table")
        rec = idx.loc[r.mnxm]
        nm = rec["name"]
        smi = rec["smiles"]
        if isinstance(smi, str) and smi.strip():
            raise SystemExit(
                f"[curation] {r.mnxm} already HAS a structure ({smi!r}). A curated row "
                f"may only SUPPLY a structure MetaNetX lacks, never override one.")
        if str(nm or "").strip() != str(r.mnx_name).strip():
            raise SystemExit(
                f"[curation] {r.mnxm}: row says {str(r.mnx_name)!r}, the metabolite "
                f"table says {nm!r}. The id is the key -- a mismatch means the row is "
                f"about a different metabolite than the curator reasoned about.")
        got = count_struct(r.smiles, r.element)
        if got is None:
            raise SystemExit(f"[curation] {r.mnxm}: SMILES {r.smiles!r} does not parse")
        if got != int(r.n_atoms):
            raise SystemExit(
                f"[curation] {r.mnxm}: asserts {r.n_atoms} {r.element}, but its SMILES "
                f"{r.smiles!r} contains {got}")
        # 4. THE CITATION IS THE EVIDENCE. `pd.isna` FIRST: an empty cell arrives as NaN
        #    and `str(nan)` is "nan", which is truthy -- so a bare emptiness test accepts
        #    a row with no citation at all. Measured: that gate silently passed the very
        #    case it was written to catch.
        if pd.isna(r.basis) or not str(r.basis).strip():
            raise SystemExit(f"[curation] {r.mnxm}: no basis. A curated row cannot be "
                             f"verified against MetaNetX -- the citation IS its evidence.")
        out[r.mnxm] = r.smiles
    return out


def residue_slots(m, resolved, ph_mnxms, counts_of, residue_of):
    if m in ph_mnxms:
        return 0
    if m in resolved:
        return resolved[m].count("*")
    if m in counts_of:
        return residue_of.get(m)
    return 0


def gate_bodies_cancel(subs, prods, resolved: dict, ph_mnxms=(), counts_of=None,
                       residue_of=None):
    def slots(ms):
        return [residue_slots(m, resolved, ph_mnxms, counts_of or {}, residue_of or {})
                for m in ms]
    s, p = slots(subs), slots(prods)
    if any(v is None for v in s + p):
        return True
    return sum(s) == sum(p)


count_formula = AP.count_element


def concrete_balance(subs, prods, formula_of, ph_mnxms, X, resolved=None,
                     counts_of=None, residue_of=None):
    resolved = resolved or {}
    counts_of = counts_of or {}
    residue_of = residue_of or {}
    tot, res = {}, {}
    for side, ms in (("s", subs), ("p", prods)):
        n, slots = 0, []
        for m in ms:
            if m in ph_mnxms:
                continue
            if m in resolved:
                c = count_struct(resolved[m], X)
            elif m in counts_of:
                c = counts_of[m][ELEMENTS.index(X)]
            else:
                c = count_formula(formula_of.get(m), X)
            if c is None:
                return None
            n += c
            slots.append(residue_slots(m, resolved, ph_mnxms, counts_of, residue_of))
        tot[side], res[side] = n, slots
    if any(s is None for s in res["s"] + res["p"]):
        return None
    if sum(res["s"]) != sum(res["p"]):
        return None
    if tot["s"] == 0 and tot["p"] == 0:
        return None
    return tot["s"] == tot["p"]


_NORM = re.compile(r"[^a-z0-9]+")
_LOCANT = re.compile(r"^\d+$|^[nosprc]$|^alpha$|^beta$|^gamma$|^d$|^l$|^dl$|^cis$"
                     r"|^trans$|^\d+[a-z]$")

MODS = {
    "oxo": (0, 0, 0, 0), "keto": (0, 0, 0, 0), "hydroxy": (0, 0, 0, 0),
    "hydroxyl": (0, 0, 0, 0), "dehydro": (0, 0, 0, 0), "didehydro": (0, 0, 0, 0),
    "dehydrogenated": (0, 0, 0, 0), "dihydro": (0, 0, 0, 0), "tetrahydro": (0, 0, 0, 0),
    "hexahydro": (0, 0, 0, 0), "deoxy": (0, 0, 0, 0), "dideoxy": (0, 0, 0, 0),
    "dehydrated": (0, 0, 0, 0), "anhydro": (0, 0, 0, 0), "enol": (0, 0, 0, 0),
    "epi": (0, 0, 0, 0), "iso": (0, 0, 0, 0), "allo": (0, 0, 0, 0),
    "cyclo": (0, 0, 0, 0), "seco": (0, 0, 0, 0),
    "methyl": (1, 0, 0, 0), "dimethyl": (2, 0, 0, 0), "trimethyl": (3, 0, 0, 0),
    "methylene": (1, 0, 0, 0), "formyl": (1, 0, 0, 0), "carboxy": (1, 0, 0, 0),
    "hydroxymethyl": (1, 0, 0, 0), "acetyl": (2, 0, 0, 0), "ethyl": (2, 0, 0, 0),
    "propyl": (3, 0, 0, 0), "malonyl": (3, 0, 0, 0), "succinyl": (4, 0, 0, 0),
    "phospho": (0, 0, 0, 1), "phosphate": (0, 0, 0, 1), "diphospho": (0, 0, 0, 2),
    "bisphospho": (0, 0, 0, 2), "diphosphate": (0, 0, 0, 2),
    "triphosphate": (0, 0, 0, 3), "amino": (0, 1, 0, 0), "diamino": (0, 2, 0, 0),
    "imino": (0, 1, 0, 0), "nitro": (0, 1, 0, 0), "sulfo": (0, 0, 1, 0),
    "thio": (0, 0, 1, 0), "mercapto": (0, 0, 1, 0),
}

DELTA_FRAGMENT = {"C": "C", "N": "N", "S": "S", "P": "P"}


def norm(s):
    return _NORM.sub(" ", str(s or "").lower()).strip()


def base_aliases(n):
    yield n
    if n.endswith(" acid"):
        yield n[:-5]
    if n.endswith("ate"):
        yield n[:-3] + "ic acid"
        yield n[:-3] + "ic"
    if n.endswith("s") and len(n) > 4:
        yield n[:-1]


def residue_names(tok):
    if tok.endswith("yl"):
        stem = tok[:-2]
        yield stem + "ine"
        yield stem + "ate"
        yield stem + "ic acid"
        yield stem + "e"
        yield stem + "ose"
        yield stem
    yield tok


class Refs:
    def __init__(self, lookups: Path, element_counts=None):
        self.reactions = pd.read_parquet(lookups / "reactions.parquet")
        mets = pd.read_parquet(
            lookups / "metabolites.parquet",
            columns=["mnxm", "name", "formula", "smiles", "has_smiles",
                     "n_C", "n_N", "n_S", "n_P"])
        self.mets = mets
        self.name_of = dict(zip(mets["mnxm"], mets["name"]))
        self.formula_of = dict(zip(mets["mnxm"], mets["formula"]))
        struct = mets[mets["has_smiles"]]
        self.smiles_of = dict(zip(struct["mnxm"], struct["smiles"]))
        self.counts_of = {
            r.mnxm: (r.n_C, r.n_N, r.n_S, r.n_P)
            for r in mets.itertuples(index=False)
            if not (pd.isna(r.n_C) or pd.isna(r.n_N)
                    or pd.isna(r.n_S) or pd.isna(r.n_P))
        }
        self.residue_of = {m: 0 for m in self.counts_of}
        if element_counts is not None:
            from . import twins as _twins
            recounted, residue, _src = _twins.read_element_counts(element_counts)
            gained = len(set(recounted) - set(self.counts_of))
            self.counts_of.update(recounted)
            self.residue_of.update({m: residue.get(m) for m in recounted})
            print(f"[curation] recount: {len(recounted):,} metabolites counted from "
                  f"their structure, {gained:,} of them unknown to the formula columns",
                  flush=True)
        self.name2id = {}
        for m, s in self.smiles_of.items():
            c = self.counts_of.get(m)
            if not c or not any(c):
                continue
            n = norm(self.name_of.get(m, ""))
            if not n:
                continue
            for a in base_aliases(n):
                if a and (a not in self.name2id
                          or sum(c) < sum(self.counts_of.get(self.name2id[a],
                                                             (999,) * 4))):
                    self.name2id[a] = m
        print(f"[curation] refs: {len(self.mets):,} metabolites, "
              f"{len(self.smiles_of):,} structured, {len(self.name2id):,} name keys",
              flush=True)

    def blocked(self):
        d = self.reactions
        return d[(d["n_blockers"] > 0)]


def _row(mnxm, smiles, name, counts, basis, lane):
    out = []
    for X, n in zip(ELEMENTS, counts):
        if n and n > 0:
            out.append(dict(mnxm=mnxm, smiles=smiles, mnx_name=name, element=X,
                            n_atoms=int(n), basis=basis, lane=lane))
    return out


def lane_twin(refs: Refs, targets):
    from difflib import SequenceMatcher
    MIN_STEM = 5
    _strip = re.compile(r"\b(d|l|dl|alpha|beta|cis|trans|r|s|n|o|\d|acid|ion|anion|"
                        r"cation|the|of|a|an)\b")

    def stem_overlap(a, b):
        a = _strip.sub(" ", norm(a))
        b = _strip.sub(" ", norm(b))
        m = SequenceMatcher(None, a, b).find_longest_match(0, len(a), 0, len(b))
        return m.size

    rows, seen = [], set()
    for r in targets.itertuples(index=False):
        if len(r.blockers) != 1:
            continue
        P = r.blockers[0]
        if P in seen or P in refs.smiles_of:
            continue
        parts = set(r.substrates) | set(r.products)
        stoich = Counter(r.products)
        stoich.subtract(Counter(r.substrates))
        skel = [m for m in parts
                if m in refs.smiles_of and any(refs.counts_of.get(m, (0,) * 4))]
        if len(skel) != 1:
            continue
        D = skel[0]
        if abs(stoich[P]) != 1 or abs(stoich[D]) != 1:
            continue
        if stem_overlap(refs.name_of.get(P, ""), refs.name_of.get(D, "")) < MIN_STEM:
            continue
        cD = refs.counts_of.get(D)
        if not cD:
            continue
        # THIS LANE BORROWS A STRUCTURE AND SO CANNOT ADD A BODY TO IT, which makes it
        # the one lane that cannot be brought into line with the others on how many `*`
        # a carrier-bodied name declares. Declining those was measured and REFUSED: it
        # strands 50 metabolites that no lower lane picks up, and 52 reactions lose their
        # only structure -- a larger loss than the inconsistency costs. The stem-overlap
        # requirement above is what keeps the borrow honest instead.
        seen.add(P)
        basis = (f"single-blocker conservation: {P} '{refs.name_of.get(P)}' is the "
                 f"skeleton twin of structured {D} '{refs.name_of.get(D)}' in the 1:1 "
                 f"elementary reaction {r.mnxr} (isomer/epimer/lactone/hydration); "
                 f"C/N/S/P counts conserved, vehicle SMILES borrowed from {D}")
        rows += _row(P, refs.smiles_of[D], refs.name_of.get(P), cD, basis, "twin")
    return rows


def _vehicle(counts, caps=0):
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    nC, nN, nS, nP = (int(x) for x in counts)
    if nC + nN + nS + nP + caps == 0:
        return None
    for smi in ("C" * nC + "N" * nN + "S" * nS + "P" * nP + "*" * caps,
                ".".join(["C"] * nC + ["N"] * nN + ["S"] * nS + ["P"] * nP
                         + ["*"] * caps)):
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            continue
        got = tuple(sum(1 for a in mol.GetAtoms() if a.GetSymbol() == X)
                    for X in ELEMENTS)
        if got == (nC, nN, nS, nP) and smi.count("*") == caps:
            return smi
    return None


def lane_transform(refs: Refs, targets):
    rows, seen = [], set()
    for r in targets.itertuples(index=False):
        for P in r.blockers:
            if P in seen or P in refs.smiles_of:
                continue
            nm = refs.name_of.get(P, "") or ""
            # THE BODY BELONGS TO THE NAME, NOT TO THE LANE. `lane_conserved` drew
            # `4-methyl-trans-hex-2-enoyl-ACP` with one `*`; this lane drew its substrate
            # twin with none, and the dehydratase step between them was refused for an
            # imbalance neither lane's chemistry claims -- the carrier cannot leave, and
            # both sides say so once they agree on how many bodies the name declares.
            #
            # ONLY THE CAP COUNT IS TAKEN FROM THE SPLIT, not the cargo. Resolving the
            # base against `strip_carriers`' core instead of the whole name reads better
            # and measured worse: it costs 338 reactions and 808 balanced keys, because
            # a name whose carrier tokens are part of how its budget resolves stops
            # resolving at all and the reaction loses its only structure.
            caps = len(BODY_RE.findall(nm))
            toks = norm(nm).split()
            if not toks:
                continue
            base_toks, delta, used = [], [0, 0, 0, 0], 0
            for t in toks:
                if t in MODS:
                    for i in range(4):
                        delta[i] += MODS[t][i]
                    used += 1
                elif _LOCANT.match(t):
                    continue
                else:
                    base_toks.append(t)
            if not used or not base_toks:
                continue
            bid = None
            for a in base_aliases(" ".join(base_toks)):
                bid = refs.name2id.get(a)
                if bid:
                    break
            if not bid:
                continue
            cB = refs.counts_of.get(bid)
            if not cB:
                continue
            counts = tuple(cB[i] + delta[i] for i in range(4))
            if any(c < 0 for c in counts):
                continue
            veh = _vehicle(counts, caps=caps)
            if not veh:
                continue
            seen.add(P)
            basis = (f"named transform: '{nm}' parses as base '{' '.join(base_toks)}' "
                     f"({bid}, C/N/S/P {cB}) plus modifier(s) contributing {tuple(delta)}; "
                     f"vehicle carries the summed budget"
                     + (f" plus {caps} `*` for the carrier body the name declares"
                        if caps else ""))
            rows += _row(P, veh, refs.name_of.get(P), counts, basis, "transform")
    return rows


def lane_fragment(refs: Refs, targets):
    # FRAGMENT SUM. A name that splits into >=2 residue tokens each resolving to a
    # metabolite (dipeptides, acyl-amino-acids, glycosides) -> the summed budget.
    #
    # THE CURATED PAIR OUTRANKS AN INFERRED SUM, and this lane is the only one that has to
    # say so. Every other lane derives its budget from a structure or from a conservation
    # argument; this one infers it by reading the NAME as chemistry, which is exactly the
    # reading a redox carrier's name defeats. `oxidized [NADPH--hemoprotein reductase]` is
    # an enzyme in an oxidation state, but its tokens resolve -- `nadph` to real NADPH and
    # `oxidized` to MNXM588580, a ModelSEED fragment stub whose name is the bare word
    # `Oxidized-`, the trailing hyphen erased by `norm`. The sum handed the reductase
    # C33/N11/P3. Its reduced twin has no such stub to collide with, so it fell through to
    # the placeholder library's `[Fe+2]`, and the couple stopped balancing: 1,122 reactions
    # completed, failed `concrete_balance` on carbon, and were never mapped.
    #
    # Deferring costs nothing measurable and is the weaker claim of the two -- the library's
    # entries are atom-matched ox/red pairs, so where it covers a metabolite it also covers
    # its twin, which is the property the balance gate is testing.
    rows, seen = [], set()
    for r in targets.itertuples(index=False):
        for P in r.blockers:
            if P in seen or P in refs.smiles_of:
                continue
            nm = refs.name_of.get(P, "") or ""
            if placeholder_for(nm) is not None:
                continue
            caps = len(BODY_RE.findall(nm))
            toks = [t for t in norm(nm).split() if not _LOCANT.match(t)]
            tot, frags, parts = [0, 0, 0, 0], 0, []
            for t in toks:
                bid = None
                for rn in residue_names(t):
                    bid = refs.name2id.get(rn)
                    if bid:
                        break
                c = refs.counts_of.get(bid) if bid else None
                if bid and c:
                    frags += 1
                    parts.append(f"{t}->{bid}")
                    for i in range(4):
                        tot[i] += c[i]
            if frags < 2:
                continue
            veh = _vehicle(tuple(tot), caps=caps)
            if not veh:
                continue
            seen.add(P)
            basis = (f"fragment sum: '{nm}' resolves into {frags} residues "
                     f"({', '.join(parts)}) summing to C/N/S/P {tuple(tot)}"
                     + (f", plus {caps} `*` for the carrier body the name declares"
                        if caps else ""))
            rows += _row(P, veh, refs.name_of.get(P), tuple(tot), basis, "fragment")
    return rows


_ACP = re.compile(r"\bacp\b|acyl.?carrier", re.I)
_COA = re.compile(r"\bcoa\b|coenzyme a", re.I)
_CARGO = re.compile(r"^\s*(?:an?\s+)?([a-z0-9,\-\s\[\]]+?)[- ]+(?:\[?acp\]?|"
                    r"\[?acyl-carrier[- ]protein\]?|coa)\s*$", re.I)


def lane_carrier(refs: Refs, targets):
    # CARRIER / ACYL VEHICLES -- the lane the previous generation measured a real
    # payoff from, and the one that needs the `*` body.
    #
    # An acyl carrier is NOT an electron carrier: its thioester genuinely carries the cargo
    # atoms through. So the vehicle is `cargo + *` -- the cargo's atom budget drawn
    # explicitly, the ACP body drawn as one dummy that CANCELS across the equation. The
    # cargo transits; the body says nothing and is required to say nothing on both sides.
    #
    # The cargo budget comes from the cargo's own name resolving to a structured
    # metabolite. Where it does not resolve, the lane declines -- inventing a chain length
    # is exactly the invention the REFUSE list exists to prevent.
    rows, seen = [], set()
    for r in targets.itertuples(index=False):
        for P in r.blockers:
            if P in seen or P in refs.smiles_of:
                continue
            nm = refs.name_of.get(P, "") or ""
            if not (_ACP.search(nm) or _COA.search(nm)):
                continue
            m = _CARGO.match(nm)
            if not m:
                if re.fullmatch(r"\s*(?:an?\s+)?(?:holo-)?\[?acp\]?\s*", nm, re.I):
                    seen.add(P)
                    rows.append(dict(mnxm=P, smiles="*", mnx_name=refs.name_of.get(P),
                                     element="C", n_atoms=0,
                                     basis=f"bare acyl-carrier '{nm}': no cargo, the "
                                           f"whole molecule is the cancelling body",
                                     lane="carrier"))
                continue
            cargo = m.group(1).strip()
            bid = None
            for a in base_aliases(norm(cargo)):
                bid = refs.name2id.get(a)
                if bid:
                    break
            c = refs.counts_of.get(bid) if bid else None
            if not c or not any(c):
                continue
            veh = _vehicle(c, caps=1)
            if not veh:
                continue
            seen.add(P)
            basis = (f"acyl-carrier vehicle: '{nm}' carries cargo '{cargo}' resolving to "
                     f"{bid} (C/N/S/P {c}); the carrier body is drawn as one `*` which "
                     f"cancels across the equation, so only the cargo transits")
            rows += _row(P, veh, refs.name_of.get(P), c, basis, "carrier")
    return rows


def _chebi_release(chebi_dir: Path):
    if chebi_dir is None:
        return None
    rel = sorted(p for p in Path(chebi_dir).iterdir() if p.is_dir())
    return rel[-1] if rel else None


def _chebi_structures(chebi_dir: Path):
    # ChEBI SMILES, keyed BOTH ways: `(by_accession, by_compound_id)`.
    #
    # One pass, two keyings, because the lane needs both and the structures table is 89 MB
    # of gzip. The accession route arrives holding `CHEBI:15377` out of MetaNetX's xref
    # table; the name route arrives holding a bare `compound_id` out of ChEBI's own names
    # table, and translating between them costs a second scan for nothing.
    #
    # `default_structure` wins where a compound has several. ChEBI carries tautomers and
    # protonation states as separate rows, so taking the first is taking an arbitrary one.
    d = _chebi_release(chebi_dir)
    if d is None:
        return {}, {}
    acc = {}
    with gzip.open(d / "compounds.tsv.gz", "rt", errors="replace") as fh:
        hdr = fh.readline().rstrip("\n").split("\t")
        i_id, i_acc = hdr.index("id"), hdr.index("chebi_accession")
        for line in fh:
            p = line.rstrip("\n").split("\t")
            if len(p) > max(i_id, i_acc):
                acc[p[i_id]] = p[i_acc]
    out, by_cid, is_default = {}, {}, set()
    import csv
    csv.field_size_limit(1 << 24)
    with gzip.open(d / "structures.tsv.gz", "rt", errors="replace", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        for rec in rd:
            smi = (rec.get("smiles") or "").strip()
            if not smi:
                continue
            cid = rec.get("compound_id")
            a = acc.get(cid)
            if a and a not in out:
                out[a] = smi
            if cid:
                dflt = str(rec.get("default_structure", "")).lower() in ("true", "t", "1", "y")
                if cid not in by_cid or (dflt and cid not in is_default):
                    by_cid[cid] = smi
                    if dflt:
                        is_default.add(cid)
    print(f"[curation] chebi: {len(out):,} accessions, {len(by_cid):,} compound ids "
          f"with a SMILES", flush=True)
    return out, by_cid


_GREEK = {"alpha": "a", "beta": "b", "gamma": "g", "delta": "d", "epsilon": "e",
          "α": "a", "β": "b", "γ": "g", "δ": "d", "ε": "e",
          "ω": "w", "omega": "w"}
_CHEBI_PUNCT = re.compile(r"[^a-z0-9]+")


def norm_chebi(s) -> str:
    s = str(s or "").lower().strip()
    if not s:
        return ""
    for k, v in _GREEK.items():
        s = s.replace(k, v)
    return _CHEBI_PUNCT.sub("", s)


def _chebi_name_index(chebi_dir: Path, want: set) -> dict:
    # `normalised name -> compound_id`, restricted to names some blocker asked for.
    #
    # ChEBI's name table is the reach this lane exists for. MetaNetX's synonym index --
    # which route 2 already searches -- holds only the names MetaNetX itself recorded, so a
    # well-named small molecule that MetaNetX never cross-referenced is invisible to it
    # while ChEBI has both the name and the structure. Filtering to `want` while scanning
    # keeps a 8.7 MB table from becoming a dict of every name ChEBI knows.
    d = _chebi_release(chebi_dir)
    if d is None or not want:
        return {}
    out = {}
    import csv
    with gzip.open(d / "names.tsv.gz", "rt", errors="replace", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        for rec in rd:
            for key in (norm_chebi(rec.get("name")), norm_chebi(rec.get("ascii_name"))):
                if key and key in want and key not in out:
                    out[key] = rec.get("compound_id")
    print(f"[curation] chebi: {len(out):,} of {len(want):,} wanted name keys matched",
          flush=True)
    return out


def _modelseed_structures(ms_dir: Path):
    if ms_dir is None:
        return {}
    rel = sorted(p for p in Path(ms_dir).iterdir() if p.is_dir())
    if not rel:
        return {}
    out = {}
    with open(rel[-1] / "compounds.tsv", errors="replace") as fh:
        hdr = fh.readline().rstrip("\n").split("\t")
        i_id = hdr.index("id")
        i_smi = hdr.index("smiles") if "smiles" in hdr else None
        if i_smi is None:
            return {}
        for line in fh:
            p = line.rstrip("\n").split("\t")
            if len(p) > max(i_id, i_smi) and p[i_smi] not in ("", "null"):
                out[p[i_id]] = p[i_smi]
    print(f"[curation] modelseed: {len(out):,} ids with a SMILES", flush=True)
    return out


def lane_supplier(refs: Refs, targets, lookups: Path, chebi=None, modelseed=None):
    chebi_smi, chebi_by_cid = _chebi_structures(chebi)
    ms_smi = _modelseed_structures(modelseed)
    if not chebi_smi and not ms_smi:
        return []

    blockers = sorted({m for r in targets.itertuples(index=False) for m in r.blockers}
                      - set(refs.smiles_of))
    if not blockers:
        return []
    want = set(blockers)

    xr = pd.read_parquet(lookups / "xrefs.parquet",
                         columns=["kind", "namespace", "foreign_id", "mnx_id"])
    xr = xr[(xr["kind"] == "chem") & xr["mnx_id"].isin(want)
            & xr["namespace"].isin(["chebi", "seedM", "seed.compound"])]
    by_acc = defaultdict(list)
    for r in xr.itertuples(index=False):
        key = (f"CHEBI:{r.foreign_id}" if r.namespace == "chebi"
               and not str(r.foreign_id).upper().startswith("CHEBI") else r.foreign_id)
        smi = chebi_smi.get(key) or chebi_smi.get(str(r.foreign_id)) or ms_smi.get(r.foreign_id)
        if smi:
            by_acc[r.mnx_id].append((r.namespace, r.foreign_id, smi))

    still = [m for m in blockers if m not in by_acc]
    by_name = {}
    if still:
        syn = pd.read_parquet(lookups / "synonyms.parquet",
                              columns=["source", "source_id", "key_c"])
        syn = syn[syn["source"].isin(["chebi", "modelseed"])]
        idx = defaultdict(list)
        for r in syn.itertuples(index=False):
            idx[r.key_c].append((r.source, r.source_id))
        for m in still:
            k = norm(refs.name_of.get(m, ""))
            if not k:
                continue
            for a in base_aliases(k):
                for src, sid in idx.get(a, []):
                    smi = chebi_smi.get(sid) if src == "chebi" else ms_smi.get(sid)
                    if smi:
                        by_name[m] = (src, sid, smi)
                        break
                if m in by_name:
                    break

    still2 = [m for m in blockers if m not in by_acc and m not in by_name]
    by_cname = {}
    if still2 and chebi_by_cid:
        syn_of = defaultdict(list)
        sy = pd.read_parquet(lookups / "synonyms.parquet",
                             columns=["source", "source_id", "raw_name"])
        sy = sy[(sy["source"] == "metanetx") & sy["source_id"].isin(set(still2))]
        for r in sy.itertuples(index=False):
            syn_of[r.source_id].append(r.raw_name)
        keys_of = {}
        want_names = set()
        for m in still2:
            ks = [norm_chebi(refs.name_of.get(m, ""))]
            ks += [norm_chebi(s) for s in syn_of.get(m, [])]
            ks = [k for k in dict.fromkeys(ks) if k]
            keys_of[m] = ks
            want_names.update(ks)
        name2cid = _chebi_name_index(chebi, want_names)
        for m in still2:
            for k in keys_of[m]:
                cid = name2cid.get(k)
                smi = chebi_by_cid.get(cid) if cid else None
                if smi:
                    by_cname[m] = (cid, smi)
                    break

    rows = []
    for m, hits in by_acc.items():
        ns, fid, smi = hits[0]
        c = _counts_from_smiles(smi)
        if not c or not any(c):
            continue
        rows += _row(m, smi, refs.name_of.get(m), c,
                     f"external supplier (accession): {m} is {ns}:{fid} per MetaNetX's "
                     f"own crossreference table, and that entry carries the structure "
                     f"{smi}", "supplier")
    for m, (cid, smi) in by_cname.items():
        c = _counts_from_smiles(smi)
        if not c or not any(c):
            continue
        rows += _row(m, smi, refs.name_of.get(m), c,
                     f"external supplier (ChEBI name): '{refs.name_of.get(m)}' matches "
                     f"ChEBI compound {cid} by name or synonym under ChEBI's own name "
                     f"table; that entry carries the structure {smi}", "supplier")
    for m, (src, sid, smi) in by_name.items():
        c = _counts_from_smiles(smi)
        if not c or not any(c):
            continue
        rows += _row(m, smi, refs.name_of.get(m), c,
                     f"external supplier (name): '{refs.name_of.get(m)}' matches {src} "
                     f"{sid} exactly under conservative normalisation; that entry "
                     f"carries the structure {smi}", "supplier")
    return rows


def _counts_from_smiles(smi):
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    mol = Chem.MolFromSmiles(smi)
    if mol is None:
        return None
    return tuple(sum(1 for a in mol.GetAtoms() if a.GetSymbol() == X) for X in ELEMENTS)


_LIPID_HEAD = {
    "OH":    ("O", 0, 0, 0),
    "PC":    ("OP(=O)([O-])OCC[N+](C)(C)C", 5, 1, 1),
    "PE":    ("OP(=O)(O)OCCN", 2, 1, 1),
    "PI":    ("OP(=O)(O)OC1C(O)C(O)C(O)C(O)C1O", 6, 0, 1),
    "PIP":   ("OP(=O)(O)OC1C(O)C(O)C(OP(=O)(O)O)C(O)C1O", 6, 0, 2),
    "PIP2":  ("OP(=O)(O)OC1C(O)C(O)C(OP(=O)(O)O)C(OP(=O)(O)O)C1O", 6, 0, 3),
    "PIP3":  ("OP(=O)(O)OC1C(O)C(OP(=O)(O)O)C(OP(=O)(O)O)C(OP(=O)(O)O)C1O", 6, 0, 4),
    "GAL":   ("OC1OC(CO)C(O)C(O)C1O", 6, 0, 0),
    "DIGAL": ("OC1OC(COC2OC(CO)C(O)C(O)C2O)C(O)C(O)C1O", 12, 0, 0),
    "CDP":   ("OP(=O)(O)OP(=O)(O)OCC1OC(n2ccc(N)nc2=O)C(O)C1O", 9, 3, 2),
    "DGTS":  ("OCCC(C(=O)[O-])[N+](C)(C)C", 7, 1, 0),
    "PA":    ("OP(=O)(O)O", 0, 0, 1),
}

_LIPID_FAMILY = {
    "DAG": (2, "OH"), "TAG": (3, None), "PC": (2, "PC"), "PE": (2, "PE"),
    "LPC": (1, "PC"), "LPE": (1, "PE"), "PI": (2, "PI"), "PIP": (2, "PIP"),
    "PIP2": (2, "PIP2"), "PIP3": (2, "PIP3"), "MGDG": (2, "GAL"), "DGDG": (2, "DIGAL"),
    "CDPDAG": (2, "CDP"), "MAG": (1, "OH"), "DGTS": (2, "DGTS"), "PA": (2, "PA"),
    "LPA": (1, "PA"), "FA": (1, None),
}

_LIPID_EXTRA = (
    "glucosaminyl", "glucosamin", "mannosyl", "mannosid", "mannose", "glucosyl",
    "glucuron", "sulfoquinovosyl", "sulphoquinovosyl", "acetyl", "glucosphingo",
    " gpi", "(gpi", "signal sequence", "peptide", "diglucosyl", "monoglucosyl",
    "ceramide", "sphingo", "arabino", "rhamn", "fucos", "methyl branch", "methyl-branch")

_CD = re.compile(r"(\d{1,2}):(\d+)")


def _lipid_family_raw(n: str):
    if "ferredoxin" in n or "flavodoxin" in n or "thioredoxin" in n:
        return None
    if "trisphosphate" in n and "inositol" in n:
        return "PIP3"
    if "bisphosphate" in n and "inositol" in n:
        return "PIP2"
    if "inositol" in n and "phosphate" in n and "phosphatidyl" in n:
        return "PIP"
    if "inositol" in n and "phosphatidyl" in n:
        return "PI"
    if "digalactosyl" in n:
        return "DGDG"
    if ("galactosyl" in n or "monogalactosyl" in n) and ("glycerol" in n or "diacyl" in n):
        return "MGDG"
    if "cdp-" in n and "glycerol" in n:
        return "CDPDAG"
    if "trimethylhomoserine" in n:
        return "DGTS"
    if "lysophosphatidylcholine" in n or ("glycero" in n and "phosphocholine" in n):
        return "LPC"
    if "lysophosphatidylethanolamine" in n or ("glycero" in n and "phosphoethanolamine" in n):
        return "LPE"
    if "phosphatidylcholine" in n:
        return "PC"
    if "phosphatidylethanolamine" in n:
        return "PE"
    if "triacylglycerol" in n:
        return "TAG"
    if "diacyl" in n and "glycerol" in n:
        return "DAG"
    if "monoacylglycerol" in n or "acylglycerol" in n:
        return "MAG"
    if "lysophosphatidic acid" in n:
        return "LPA"
    if "phosphatidic acid" in n:
        return "PA"
    if (" acid (" in n and _CD.search(n) and "phosphatid" not in n
            and "methyl" not in n and "acp" not in n):
        return "FA"
    return None


def lipid_family(name):
    n = str(name or "").lower()
    fam = _lipid_family_raw(n)
    if fam is None:
        return None
    if any(t in n for t in _LIPID_EXTRA):
        return None
    if "galactosyl" in n and fam not in ("MGDG", "DGDG"):
        return None
    if len(_CD.findall(n)) != _LIPID_FAMILY[fam][0]:
        return None
    return fam


def _acyl(n: int, d: int):
    k = n - 1
    if k < 0:
        return None, 0
    placed, b, at = 0, 1, set()
    while placed < d and b <= k - 2:
        at.add(b)
        placed += 1
        b += 3
    s = "C(=O)" + "".join("=C" if (i - 1) in at else "C" for i in range(k))
    return s, n


def _build_lipid(family: str, specs):
    n_acyl, head_key = _LIPID_FAMILY[family]
    if len(specs) != n_acyl:
        return None
    esters = []
    for c, d in specs:
        frag, nc = _acyl(c, d)
        if frag is None:
            return None
        esters.append(("O" + frag, nc))
    chain_c = sum(nc for _, nc in esters)

    if family == "FA":
        return esters[0][0], chain_c, 0, 0
    if family == "TAG":
        a1, a2, a3 = (e[0] for e in esters)
        return f"{a1}CC({a2})C{a3}", 3 + chain_c, 0, 0
    head, hc, hn, hp = _LIPID_HEAD[head_key]
    if n_acyl == 2:
        a1, a2 = esters[0][0], esters[1][0]
        return f"{a1}CC({a2})C{head}", 3 + hc + chain_c, hn, hp
    a1 = esters[0][0]
    return f"{a1}CC(O)C{head}", 3 + hc + chain_c, hn, hp


def lane_lipid(refs: Refs, targets):
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    blockers = sorted({m for r in targets.itertuples(index=False) for m in r.blockers}
                      - set(refs.smiles_of))
    rows, fams, refused = [], Counter(), Counter()
    for m in blockers:
        name = refs.name_of.get(m, "")
        fam = lipid_family(name)
        if fam is None:
            continue
        specs = [(int(c), int(d)) for c, d in _CD.findall(str(name).lower())]
        built = _build_lipid(fam, specs)
        if built is None:
            refused[f"{fam}:acyl_count"] += 1
            continue
        smi, hc, hn, hp = built
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            refused[f"{fam}:unparseable"] += 1
            continue
        c = tuple(sum(1 for a in mol.GetAtoms() if a.GetSymbol() == X) for X in ELEMENTS)
        if (c[0], c[1], c[3]) != (hc, hn, hp):
            refused[f"{fam}:count_mismatch"] += 1
            continue
        if not any(c):
            continue
        acyls = "/".join(f"{a}:{b}" for a, b in specs)
        rows += _row(m, Chem.MolToSmiles(mol), name, c,
                     f"LIPID-MAPS shorthand templated: family {fam}, acyls {acyls}. The "
                     f"shorthand fixes C/N/S/P exactly and the double-bond count changes "
                     f"only hydrogen, which is untracked; C/N/P cross-checked hand "
                     f"{hc}/{hn}/{hp} against rdkit {c[0]}/{c[1]}/{c[3]}", "lipid")
        fams[fam] += 1
    if fams:
        print(f"[curation]   lipid templater: {sum(fams.values()):,} metabolites over "
              f"{len(fams)} families -- "
              + ", ".join(f"{f} {n:,}" for f, n in fams.most_common(6)), flush=True)
    if refused:
        print("[curation]     refused: "
              + ", ".join(f"{k} {n:,}" for k, n in refused.most_common()), flush=True)
    return rows


_FTOK = re.compile(r"([A-Z][a-z]?)(\d*)")


def counts_formula_caps(f):
    if not isinstance(f, str) or not f.strip():
        return None
    caps = f.count("*")
    core = f.replace("*", "")
    if re.search(r"[RXZ().]", core):
        return None
    d = {X: 0 for X in ELEMENTS}
    for sym, num in _FTOK.findall(core):
        if sym in d:
            d[sym] += int(num) if num else 1
    return (d["C"], d["N"], d["S"], d["P"], caps)


def heavy_from_smiles(smi):
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    mol = Chem.MolFromSmiles(smi)
    if mol is None:
        return None
    d = {X: 0 for X in ELEMENTS}
    caps = 0
    for a in mol.GetAtoms():
        if a.GetAtomicNum() == 0:
            caps += 1
        elif a.GetSymbol() in d:
            d[a.GetSymbol()] += 1
    return (d["C"], d["N"], d["S"], d["P"], caps)


CARRIER_PATS = [r"\[acyl-carrier protein\]", r"acyl-?carrier ?protein-?", r"\[acp\]",
                r"\bacp\b", r"\[protein\]", r"protein\]-", r"-\[protein", r"\btrna\b",
                r"\bholo\b", r"\bapo\b"]
CARRIER_RE = re.compile("|".join(CARRIER_PATS), re.I)

# THE SUBSET A VEHICLE-BUILDING LANE MAY DRAW A BODY FOR, derived from the list above so
# the two cannot drift. Two different reasons for the two groups:
#
#   * `holo`, `apo` and `trna` are STATE PREFIXES rather than body markers. The bare form
#     of the same protein carries no word at all, so drawing a cap for `apo-[X ligase]`
#     and none for `[X ligase]` manufactures on one side exactly the asymmetry this rule
#     exists to remove on the other. Measured: 49 reactions and 112 balanced keys lost
#     against r6, gaining nothing.
#   * `[protein]` IS a body and is excluded anyway, for one reaction. MNXR171321 joins two
#     protein bodies into one, so counting them refuses it -- correctly, the two do not
#     cancel -- and the r6 bake holds its sulfur key. Counting them is worth +7 reactions
#     and +10 keys and costs that one, and the coverage stop-line does not permit the
#     trade. Revisit if the stop-line is ever relaxed to net rather than per-key.
#
# `lane_conserved` still reads the FULL list: it resolves a budget rather than adding a
# cap to one, and there a prefix is evidence of a body rather than a claim about count.
STATE_PREFIX_PATS = (r"\btrna\b", r"\bholo\b", r"\bapo\b",
                     r"\[protein\]", r"protein\]-", r"-\[protein")
BODY_RE = re.compile("|".join(p for p in CARRIER_PATS if p not in STATE_PREFIX_PATS),
                     re.I)


def strip_carriers(name):
    caps = len(CARRIER_RE.findall(name or ""))
    core = CARRIER_RE.sub(" ", name or "")
    core = re.sub(r"^\s*(an?|the)\s+", " ", norm(re.sub(r"[\[\]]", " ", core)))
    return caps, core.strip()


REDOX_KW = re.compile(r"flavoprotein|flavodoxin|ferredoxin|thioredoxin|glutaredoxin"
                      r"|rubredoxin|adrenodoxin|plastocyanin|azurin|cytochrome", re.I)
PHOTON = re.compile(r"^h\s*nu$|^hnu$|^photon$|^light$|^e-?$|^electron$", re.I)


def redox_carrier(nm):
    s = re.sub(r"[\[\]]", " ", nm or "").strip()
    if PHOTON.match(s) or REDOX_KW.search(s):
        return True
    if re.match(r"^(an? )?(reduced|oxidi[sz]ed)[- ]?(acceptor|donor|ah2|a|form)?\s*$",
                s, re.I):
        return True
    if re.match(r"^(an? )?(reduced |oxidi[sz]ed )?([\w,]+[- ])?(acceptor|donor)s?\s*$",
                s, re.I):
        return True
    return bool(re.match(r"^ah2$|^a$", s, re.I))


NUM_ROOTS = {"meth": 1, "eth": 2, "prop": 3, "but": 4, "pent": 5, "hex": 6, "hept": 7,
             "oct": 8, "non": 9, "dec": 10, "undec": 11, "dodec": 12, "tridec": 13,
             "tetradec": 14, "pentadec": 15, "hexadec": 16, "heptadec": 17,
             "octadec": 18, "nonadec": 19, "icos": 20, "eicos": 20, "henicos": 21,
             "heneicos": 21, "docos": 22, "tricos": 23, "tetracos": 24, "pentacos": 25,
             "hexacos": 26, "heptacos": 27, "octacos": 28, "triacont": 30}
TRIVIAL_ACYL = {"acetyl": 2, "acetoacetyl": 4, "propionyl": 3, "propanoyl": 3,
                "butyryl": 4, "butanoyl": 4, "isobutyryl": 4, "valeryl": 5,
                "crotonyl": 4, "caproyl": 6, "capryloyl": 8, "caprylyl": 8,
                "pelargonyl": 9, "capryl": 10, "caprinoyl": 10, "lauroyl": 12,
                "myristoyl": 14, "myristoleyl": 14, "palmitoyl": 16, "palmitoleyl": 16,
                "margaroyl": 17, "stearoyl": 18, "oleoyl": 18, "elaidoyl": 18,
                "linoleoyl": 18, "linolenoyl": 18, "ricinoleoyl": 18, "arachidoyl": 20,
                "arachidonoyl": 20, "gadoleoyl": 20, "behenoyl": 22, "erucoyl": 22,
                "lignoceroyl": 24, "nervonoyl": 24, "cerotoyl": 26, "malonyl": 3,
                "succinyl": 4, "glutaryl": 5, "adipoyl": 6, "pimeloyl": 7, "suberoyl": 8,
                "azelaoyl": 9, "sebacoyl": 10, "tiglyl": 5, "cinnamoyl": 9, "benzoyl": 7}
_ACYL_SUFFIX = re.compile(r"(enoyl|anoyl|ynoyl|oyl|enoate|anoate|enoic|anoic)")

AA3 = {"ala": (3, 1, 0, 0), "arg": (6, 4, 0, 0), "asn": (4, 2, 0, 0), "asp": (4, 1, 0, 0),
       "cys": (3, 1, 1, 0), "gln": (5, 2, 0, 0), "glu": (5, 1, 0, 0), "gly": (2, 1, 0, 0),
       "his": (6, 3, 0, 0), "ile": (6, 1, 0, 0), "leu": (6, 1, 0, 0), "lys": (6, 2, 0, 0),
       "met": (5, 1, 1, 0), "phe": (9, 1, 0, 0), "pro": (5, 1, 0, 0), "ser": (3, 1, 0, 0),
       "thr": (4, 1, 0, 0), "trp": (11, 2, 0, 0), "tyr": (9, 1, 0, 0), "val": (5, 1, 0, 0),
       "sec": (3, 1, 1, 0), "pyl": (12, 3, 0, 0)}


def acyl_budget(core):
    x = core.replace("-", "").replace(" ", "")
    for name, c in TRIVIAL_ACYL.items():
        if name in x:
            return (c, 0, 0, 0)
    if not _ACYL_SUFFIX.search(core):
        return None
    best = None
    for root, c in NUM_ROOTS.items():
        if root in x and (best is None or c > best):
            best = c
    return (best, 0, 0, 0) if best else None


def peptide_budget(name):
    toks = [t for t in re.split(r"[- ]", (name or "").strip()) if t]
    if len(toks) < 2:
        return None
    tot = [0, 0, 0, 0]
    for t in toks:
        c = AA3.get(t.lower())
        if c is None:
            return None
        for i in range(4):
            tot[i] += c[i]
    return tuple(tot)


def _budget_from_name(refs: Refs, core: str):
    if not core:
        return None
    for a in base_aliases(core):
        bid = refs.name2id.get(a)
        if bid and refs.counts_of.get(bid):
            return tuple(refs.counts_of[bid])
    toks = core.split()
    base_toks, delta, used = [], [0, 0, 0, 0], 0
    for t in toks:
        if t in MODS:
            for i in range(4):
                delta[i] += MODS[t][i]
            used += 1
        elif _LOCANT.match(t):
            continue
        else:
            base_toks.append(t)
    if used and base_toks:
        for a in base_aliases(" ".join(base_toks)):
            bid = refs.name2id.get(a)
            c = refs.counts_of.get(bid) if bid else None
            if c:
                out = tuple(c[i] + delta[i] for i in range(4))
                if all(v >= 0 for v in out):
                    return out
    tot, frags = [0, 0, 0, 0], 0
    for t in [t for t in toks if not _LOCANT.match(t)]:
        for rn in residue_names(t):
            bid = refs.name2id.get(rn)
            c = refs.counts_of.get(bid) if bid else None
            if c:
                frags += 1
                for i in range(4):
                    tot[i] += c[i]
                break
    if frags >= 2:
        return tuple(tot)
    b = acyl_budget(core) or peptide_budget(core)
    return b


def resolve_budget(refs: Refs, m: str):
    nm = refs.name_of.get(m, "") or ""
    if redox_carrier(nm):
        return ((0, 0, 0, 0), 1, "redox_carrier")
    cf = counts_formula_caps(refs.formula_of.get(m))
    if cf is not None and (any(cf[:4]) or cf[4]):
        return (cf[:4], cf[4], "formula*" if cf[4] else "formula")
    s = refs.smiles_of.get(m)
    if s:
        hs = heavy_from_smiles(s)
        if hs:
            return (hs[:4], hs[4], "smiles*" if hs[4] else "smiles")
    caps, core = strip_carriers(nm)
    if caps and not core:
        return ((0, 0, 0, 0), caps, "carrier_body")
    h = _budget_from_name(refs, core if caps else norm(nm))
    if h:
        return (h, caps, "name" + ("+carrier" if caps else ""))
    return None


def _vehicle_row(mnxm, name, heavy, caps, basis, lane):
    smi = _vehicle(heavy, caps=caps)
    if smi is None:
        return None
    rows = _row(mnxm, smi, name, heavy, basis, lane)
    if not rows:
        rows = [dict(mnxm=mnxm, smiles=smi, mnx_name=name, element="C", n_atoms=0,
                     basis=basis, lane=lane)]
    return rows


_GENACC = re.compile(r"^(a|ah2|acceptor|reduced acceptor|oxidized acceptor|oxidised "
                     r"acceptor|an? electron acceptor|an? acceptor|an? donor)$", re.I)
_PROTEIN = re.compile(r"\[protein\]|phosphoprotein|-\[.*protein.*\]|\bprotein\b", re.I)


def lane_acceptor(refs: Refs, targets):
    # THE DELIBERATE `REFUSE` BYPASS: generic acceptors and `[protein]` bodies.
    #
    # `REFUSE` declines these for a good reason -- `A + 2[H] <-> AH2` is a template rather
    # than an instance, and a stand-in for a template is an invention. This lane does not
    # dispute that; it declines to draw the template's ATOMS and draws only its BODY:
    #
    #   * a generic acceptor becomes a bare `*`. Zero C/N/S/P, one cancelling dummy. It
    #     contributes NO atom pair. What it does is unblock the reaction so the CONCRETE
    #     partners' real transit can be mapped, and the balance gate then tests, per
    #     element, that nothing crossed into the acceptor.
    #   * a `[protein]`-bodied metabolite becomes `cargo + *` -- the same decomposition as
    #     an acyl carrier, because it is the same chemistry: `L-seryl-[protein]` carries a
    #     serine that genuinely transits, on a body that does not.
    #
    # IT IS A SEPARATE LANE SO ITS DELTA STAYS A SEPARATE NUMBER. In the deployed chain
    # this lever banked +426 reactions and cost 1,129 refusals at the body-cancel gate,
    # and that trade has to remain visible rather than dissolve into a total. `--drop-lane
    # acceptor` removes it and nothing else.
    rows, seen = [], set()
    for r in targets.itertuples(index=False):
        for P in r.blockers:
            if P in seen or P in refs.smiles_of:
                continue
            nm = (refs.name_of.get(P) or "").strip()
            if _GENACC.match(nm):
                seen.add(P)
                rows.append(dict(
                    mnxm=P, smiles="*", mnx_name=refs.name_of.get(P), element="C",
                    n_atoms=0, lane="acceptor",
                    basis=f"REFUSE override (generic acceptor): {P} '{nm}' is drawn as a "
                          f"bare `*` -- zero C/N/S/P, one cancelling body. It adds no "
                          f"atom pair; it unblocks the reaction so the concrete "
                          f"partners' transit maps, and the per-element balance gate "
                          f"then tests that nothing crossed into the acceptor."))
            elif _PROTEIN.search(nm):
                b = resolve_budget(refs, P)
                if b is None:
                    continue
                heavy, caps, meth = b
                caps = max(caps, 1)
                got = _vehicle_row(
                    P, refs.name_of.get(P), heavy, caps,
                    f"REFUSE override (protein cargo): {P} '{nm}' is drawn as its "
                    f"concrete cargo (C/N/S/P {tuple(heavy)}, resolved by {meth}) plus "
                    f"{caps} `*` for the protein body, which cancels across the equation "
                    f"so only the cargo transits; balance-gated per element.",
                    "acceptor")
                if got:
                    seen.add(P)
                    rows += got
    return rows


def lane_conserved(refs: Refs, targets):
    from collections import defaultdict as _dd
    from difflib import SequenceMatcher

    def stem(a, b):
        a, b = norm(a), norm(b)
        m = SequenceMatcher(None, a, b).find_longest_match(0, len(a), 0, len(b))
        return m.size

    coa_idx = _dd(list)
    for m, s in refs.smiles_of.items():
        nm = (refs.name_of.get(m) or "").lower()
        if "coa" in nm.replace(" ", "") or "coenzyme a" in nm:
            c = refs.counts_of.get(m)
            if c:
                coa_idx[tuple(c)].append((m, refs.name_of.get(m), s))

    def coa_twin(heavy, nm):
        best, bs = None, 0
        for tm, tn, ts in coa_idx.get(tuple(heavy), []):
            sc = stem(nm, tn or "")
            if sc > bs:
                best, bs = (tm, tn, ts), sc
        return best if (best and bs >= 6) else None

    rows, seen = [], set()
    for r in targets.itertuples(index=False):
        for P in r.blockers:
            if P in seen or P in refs.smiles_of:
                continue
            nm = refs.name_of.get(P) or ""
            b = resolve_budget(refs, P)
            if b is None:
                continue
            heavy, caps, meth = b
            if caps == 0 and not CARRIER_RE.search(nm) and meth != "redox_carrier":
                continue
            if "coa" in norm(nm).replace(" ", "") and any(heavy):
                tw = coa_twin(heavy, nm)
                if tw is not None:
                    tm, tn, ts = tw
                    seen.add(P)
                    rows += _row(P, ts, refs.name_of.get(P), heavy,
                                 f"acyl-CoA twin: {P} '{nm}' takes the structure of CoA "
                                 f"metabolite {tm} '{tn}', which has identical C/N/S/P "
                                 f"{tuple(heavy)} and a shared acyl stem. Concrete, no "
                                 f"`*`, so free-CoA reactions balance against the same "
                                 f"atoms.", "conserved")
                    continue
            got = _vehicle_row(
                P, refs.name_of.get(P), heavy, max(caps, 1),
                f"conserved-body vehicle: {P} '{nm}' is drawn as its concrete cargo "
                f"(C/N/S/P {tuple(heavy)}, resolved by {meth}) plus "
                f"{max(caps, 1)} `*` for the conserved body. The `*` cancels across the "
                f"equation, so only the cargo transits; balance-gated per element.",
                "conserved")
            if got:
                seen.add(P)
                rows += got
    return rows


_MONO_DEFS = [
    ("sialic",    (11, 1, 0, 0), "CC(=O)NC1C(O)CC(O)(C(O)C(O)CO)OC1C(=O)O"),
    ("HexNAc",    (8, 1, 0, 0),  "CC(=O)NC1C(O)OC(CO)C(O)C1O"),
    ("hexose",    (6, 0, 0, 0),  "OCC1OC(O)C(O)C(O)C1O"),
    ("pentose",   (5, 0, 0, 0),  "OCC1OC(O)C(O)C1O"),
    ("glycerolP", (3, 0, 0, 1),  "OCC(O)COP(O)(O)=O"),
    ("glycerol",  (3, 0, 0, 0),  "OCC(O)CO"),
    ("acetyl",    (2, 0, 0, 0),  "CC=O"),
    ("C1",        (1, 0, 0, 0),  "CO"),
    ("sulfate",   (0, 0, 1, 0),  "OS(O)(=O)=O"),
    ("phosphate", (0, 0, 0, 1),  "OP(O)(O)=O"),
]


def _monomers():
    out = []
    for name, sig, smi in _MONO_DEFS:
        got = _counts_from_smiles(smi)
        if got != sig:
            raise SystemExit(f"[curation] monomer {name}: SMILES {smi} is {got}, "
                             f"the table says {sig}")
        out.append((name, sig, smi))
    return out


def _decompose(vec):
    if all(v == 0 for v in vec):
        return {}
    for name, sig, _ in _monomers():
        nz = [i for i in range(4) if sig[i]]
        if not nz:
            continue
        k0 = vec[nz[0]] / sig[nz[0]]
        if k0 != int(k0) or k0 == 0:
            continue
        k = int(k0)
        if all(vec[i] == k * sig[i] for i in range(4)):
            return {name: k}
    return None


def _backbone(nm):
    b = re.sub(r"\([^)]*\)|\bn\s*=?\s*[+-]?\d*\b|[0-9]|->|,|\bx\b", "", (nm or "").lower())
    b = re.sub(r"\b(alpha|beta|d|l|linked|unlinked|repeat|units?|substituted|"
               r"unsubstituted|branch|precursor|degradation|product|nonreducing|"
               r"reducing|end)\b", " ", b)
    return re.sub(r"[^a-z]", "", b)


def lane_polymer(refs: Refs, targets):
    from collections import defaultdict as _dd, deque

    def structured(m):
        return m in refs.smiles_of or bool(
            counts_formula_caps(refs.formula_of.get(m)) or ())

    edges, adj, diag = [], _dd(list), Counter()
    for r in targets.itertuples(index=False):
        subs, prods = list(r.substrates), list(r.products)
        sb, pb = _dd(list), _dd(list)
        for m in subs:
            if m not in refs.smiles_of:
                k = _backbone(refs.name_of.get(m, ""))
                if len(k) > 3:
                    sb[k].append(m)
        for m in prods:
            if m not in refs.smiles_of:
                k = _backbone(refs.name_of.get(m, ""))
                if len(k) > 3:
                    pb[k].append(m)
        shared = set(sb) & set(pb)
        pairs = [(sb[k][0], pb[k][0]) for k in shared
                 if len(sb[k]) == 1 and len(pb[k]) == 1]
        if not pairs or len(pairs) != len(shared):
            diag["multi_or_unbalanced_pairing"] += 1
            continue
        if len(pairs) != 1:
            diag["multiple_polymer_pairs"] += 1
            continue
        cancelled = {m for a, b in pairs for m in (a, b)}
        rem_sub = [m for m in subs if m not in cancelled]
        rem_prod = [m for m in prods if m not in cancelled]
        res, ok = {}, True
        for m in set(rem_sub) | set(rem_prod):
            b = resolve_budget(refs, m) if m not in refs.smiles_of else None
            if b is None and m in refs.smiles_of:
                c = refs.counts_of.get(m)
                b = (tuple(c), 0, "structured") if c else None
            if b is None:
                ok = False
                break
            res[m] = b
        if not ok:
            diag["remaining_unresolved"] += 1
            continue
        caps, heavy = 0, [0, 0, 0, 0]
        for coll, sign in ((rem_sub, +1), (rem_prod, -1)):
            for m in coll:
                h, cp_, _ = res[m]
                for i in range(4):
                    heavy[i] += sign * h[i]
                caps += sign * cp_
        if caps != 0:
            diag["remaining_caps_dont_cancel"] += 1
            continue
        d = _decompose(tuple(-heavy[i] for i in range(4)))
        if d is None:
            diag["residual_not_single_monomer"] += 1
            continue
        a, b = pairs[0]
        edges.append((r.mnxr, a, b, d))
        adj[a].append((b, Counter(d)))
        adj[b].append((a, Counter({k: -v for k, v in d.items()})))

    level, dropped = {}, set()
    n_fam = 0
    for seed in list(adj):
        if seed in level or seed in dropped:
            continue
        comp, consistent = [], True
        level[seed] = Counter()
        q = deque([seed])
        while q:
            u = q.popleft()
            comp.append(u)
            for v, dlt in adj[u]:
                want = Counter({k: c for k, c in (level[u] + dlt).items() if c != 0})
                if v in level:
                    cur = Counter({k: c for k, c in level[v].items() if c != 0})
                    if cur != want:
                        consistent = False
                else:
                    level[v] = want
                    q.append(v)
        if not consistent:
            for u in comp:
                dropped.add(u)
                level.pop(u, None)
            diag["family_inconsistent_dropped"] += 1
            continue
        types = set()
        for u in comp:
            types |= set(level[u])
        for t in types:
            mn = min(level[u].get(t, 0) for u in comp)
            if mn < 0:
                for u in comp:
                    level[u][t] = level[u].get(t, 0) - mn
        n_fam += 1

    smi_of = {name: smi for name, _, smi in _monomers()}
    rows = []
    for chain, cnt in level.items():
        if chain in dropped:
            continue
        frags = []
        for name, n in cnt.items():
            frags += [smi_of[name]] * n
        smiles = (".".join(frags) + ".*") if frags else "*"
        counts = _counts_from_smiles(smiles)
        if counts is None:
            continue
        cargo = "+".join(f"{n}x{name}" for name, n in sorted(cnt.items())) \
            or "bare backbone"
        basis = (f"polymer ladder cap-cancel vehicle: {chain} "
                 f"'{refs.name_of.get(chain)}' is drawn as its levelled monomer cargo "
                 f"({cargo}) plus a `*` backbone. Within its backbone family the "
                 f"per-chain monomer count is solved so every ladder reaction's residual "
                 f"is the balance-forced monomer; the `*` cancels across the equation. "
                 f"Mass balance, not the name, pins which monomer transits.")
        got = _row(chain, smiles, refs.name_of.get(chain), counts, basis, "polymer")
        rows += got or [dict(mnxm=chain, smiles=smiles,
                             mnx_name=refs.name_of.get(chain), element="C", n_atoms=0,
                             basis=basis, lane="polymer")]
    print(f"[curation]   polymer ladder: {len(edges):,} clean edges, "
          f"{len(level):,} chains levelled over {n_fam:,} families, "
          f"{len(dropped):,} chains dropped", flush=True)
    for k, n in diag.most_common():
        print(f"[curation]     {k:<34} {n:>7,}", flush=True)
    return rows


def merge(lanes: dict):
    # One metabolite, one lane. Priority is LANE_PRIORITY and it changes the result.
    rows = []
    claimed = set()
    for lane in LANE_PRIORITY:
        for r in lanes.get(lane, []):
            if r["mnxm"] in claimed:
                continue
            rows.append(r)
        claimed |= {r["mnxm"] for r in lanes.get(lane, [])}
    return pd.DataFrame(rows, columns=list(CROSSWALK_COLS))


def complete(refs: Refs, resolved: dict, targets, smiles_limit=8000, atom_limit=None,
             collapsed_atom_limit=None):
    # Build the completed reaction SMILES for every rescuable reaction. NO MAPPER RUNS.
    #
    # THIS USED TO BE `arbitrate`, AND IT USED TO MAP. Indigo ran here, inside the curation
    # sweep, and whatever it produced became layer 3 -- which is why in the deployed table
    # every one of the 9,089 rescue-derived reactions is `mcs_only`: Indigo alone, at half
    # weight, because the crosswalk did not exist when the neural members ran and nothing
    # ever showed them the completed reactions. Splitting the mapping out turns those into
    # a second pass that all three members see, and what they agree on becomes full-weight
    # consensus. The completion itself -- which structures, which bodies cancel, which
    # elements balance -- is unchanged, and it never needed a mapper:
    #
    #   * `gate_bodies_cancel` is arithmetic over the `*` counts, curated and MetaNetX's.
    #   * `concrete_balance` is arithmetic over formulas and curated structures.
    #
    # Both are computed here so a rescued reaction arrives at the mappers already carrying
    # its verdict, and a reaction where no element could balance is never mapped at all.
    #
    # Returns (rescued_rows, balance_rows, placeholder smiles, placeholder tags, tally).
    smi = dict(refs.smiles_of)
    smi.update(resolved)
    ph_smi, ph_tag = {}, {}
    todo, tally = [], Counter()

    for r in targets.itertuples(index=False):
        parts = set(r.substrates) | set(r.products)
        gens = [m for m in parts if m not in smi]
        has_resolved = any(m in resolved for m in parts)
        if not gens and not has_resolved:
            tally["already buildable"] += 1
            continue
        res = {m: placeholder_for(refs.name_of.get(m, "")) for m in gens}
        if any(v is None for v in res.values()):
            tally["a generic with no admissible placeholder"] += 1
            continue
        if not gate_bodies_cancel(r.substrates, r.products, resolved,
                                  ph_mnxms=set(res), counts_of=refs.counts_of,
                                  residue_of=refs.residue_of):
            tally["REFUSED: curated bodies do not cancel"] += 1
            continue
        for m, (s, t) in res.items():
            ph_smi[m], ph_tag[m] = s, t
        todo.append(r)
        tally["completable"] += 1

    merged = dict(smi)
    merged.update(ph_smi)
    rescued, bal_rows = [], []
    t0 = time.time()
    for i, r in enumerate(todo):
        if i and i % 2_000 == 0:
            el = time.time() - t0
            print(f"[curation] completing {i:,}/{len(todo):,} "
                  f"({el / 60:.1f} min, {i / max(el, 1e-9):.1f} rxn/s, "
                  f"{len(rescued):,} rescued so far)", flush=True)
        try:
            rxn = ".".join(merged[m] for m in r.substrates) + ">>" + \
                  ".".join(merged[m] for m in r.products)
        except KeyError:
            tally["still missing a structure"] += 1
            continue
        # THE SAME TWO CUTS THE WORKLIST APPLIES TO PASS 1, and the same second chance.
        # A completed reaction is a NEW string, so it has to be measured again rather
        # than inherited -- and both sides call the same functions against the same
        # constants, or the two disagree about what oversize means.
        #
        # THE CHAR GATE COMES FIRST AND IT GATES THE PARSE. `count_atoms` calls RDKit on
        # the whole string, and a stoichiometric expansion can reach 80.7 MB
        # (MNXR144749) -- a size RDKit does not return from. Counting unconditionally put
        # that unbounded work AHEAD of the cheap bound that excludes it, which is how a
        # 33-second loop became a two-hour one killed at its walltime. `worklist.adjudicate`
        # has always ordered it this way; the two now agree.
        chars = len(rxn)
        atoms = (aam_worklist.count_atoms(rxn)
                 if atom_limit and chars <= smiles_limit else None)
        collapsed = False
        if chars > smiles_limit or (atom_limit and (atoms is None or atoms > atom_limit)):
            rxn_c = aam_worklist.collapse(rxn)
            ok = False
            if rxn_c is not None and len(rxn_c) <= smiles_limit:
                atoms_c = aam_worklist.count_atoms(rxn_c) if collapsed_atom_limit else None
                ok = (not collapsed_atom_limit
                      or (atoms_c is not None and atoms_c <= collapsed_atom_limit))
            if ok:
                rxn, atoms, collapsed = rxn_c, atoms_c, True
                tally["recovered by collapse"] += 1
            elif len(rxn) > smiles_limit:
                tally["SMILES over the length limit"] += 1
                continue
            else:
                tally["over the atom limit"] += 1
                continue
        banked = False
        for X in ELEMENTS:
            b = concrete_balance(r.substrates, r.products, refs.formula_of,
                                 set(ph_smi), X, resolved,
                                 counts_of=refs.counts_of,
                                 residue_of=refs.residue_of)
            if b is not None:
                bal_rows.append(dict(mnxr=r.mnxr, element=X, balanced=bool(b)))
                banked = banked or bool(b)
        if not banked:
            tally["no element balances"] += 1
            continue
        rescued.append(dict(mnxr=r.mnxr, verdict="mappable", rxn_smiles=rxn,
                            atoms=atoms, chars=len(rxn), collapsed=collapsed))
    tally["RESCUED"] = len(rescued)
    return rescued, bal_rows, ph_smi, ph_tag, tally


def _targets(refs: Refs, worklist=None):
    if worklist is None:
        d = refs.blocked()
        print(f"[curation] {len(d):,} reactions have at least one structureless "
              f"participant", flush=True)
        return d
    wl = pd.read_parquet(worklist, columns=["mnxr", "verdict"])
    keep = set(wl.loc[wl["verdict"] == "blocked_no_structure", "mnxr"])
    d = refs.reactions[refs.reactions["mnxr"].isin(keep)]
    print(f"[curation] worklist: {len(keep):,} reactions adjudicated "
          f"`blocked_no_structure`, {len(d):,} matched in the reaction table",
          flush=True)
    return d


def cmd_propose(args):
    lookups = Path(args.lookups)
    refs = Refs(lookups, args.element_counts)
    targets = _targets(refs, args.worklist)

    lanes = {
        "twin": lane_twin(refs, targets),
        "transform": lane_transform(refs, targets),
        "fragment": lane_fragment(refs, targets),
        "carrier": lane_carrier(refs, targets),
        "supplier": lane_supplier(refs, targets, lookups, args.chebi, args.modelseed),
        "lipid": lane_lipid(refs, targets),
        "conserved": lane_conserved(refs, targets),
        "polymer": lane_polymer(refs, targets),
        "acceptor": lane_acceptor(refs, targets),
    }
    # THE TWO TWIN LANES ARE READ IN, NOT RUN HERE. They are separate transforms so
    # each delta stays a separate number and a 3.9 M-row xref scan is not repeated
    # inside every rescue; what arrives is a crosswalk in exactly the shape the merge
    # takes, and it goes through `admit` with every other row rather than around it.
    for key, path in (("nametwin", args.nametwin), ("blockers", args.blockers),
                      ("llm", args.llm)):
        if not path:
            continue
        t = read_crosswalk(path)
        t["lane"] = key
        lanes[key] = t.to_dict("records")
    if args.override:
        ov = read_crosswalk(args.override)
        ov["lane"] = "override"
        lanes["override"] = ov.to_dict("records")
    for k in (args.drop_lane or []):
        if k not in lanes:
            raise SystemExit(f"[curation] --drop-lane {k}: no such lane. "
                             f"Known: {sorted(lanes)}")
        # DROPPED, NOT SKIPPED. The lane still runs and still prints its count, so the
        # crosswalk records what excluding it cost rather than merely that it was
        # excluded. That is the difference between a measured delta and an assumption.
        print(f"[curation]   lane {k} DROPPED by request "
              f"({len({r['mnxm'] for r in lanes[k]}):,} metabolites withheld)",
              flush=True)
        lanes[k] = []

    for k, v in lanes.items():
        print(f"[curation]   lane {k:<10} {len({r['mnxm'] for r in v}):>6,} metabolites, "
              f"{len(v):>6,} element rows", flush=True)
    df = merge(lanes)
    df.to_csv(args.out, sep="\t", index=False)
    print(f"[curation] merged crosswalk: {df.mnxm.nunique():,} metabolites, "
          f"{len(df):,} rows -> {args.out}", flush=True)

    # DRY-RUN THE ADMISSION CHECK BEFORE ANYTHING EXPENSIVE. `admit` aborts the whole
    # run on the first bad row, and the previous generation learned this by losing a
    # mapping pass to a stale id in row 4,000.
    admit(df, refs.mets)
    print(f"[curation] admission check passed on all {len(df):,} rows", flush=True)
    return 0


def cmd_complete(args):
    lookups = Path(args.lookups)
    refs = Refs(lookups, args.element_counts)
    rows = read_crosswalk(args.crosswalk)
    resolved = admit(rows, refs.mets)
    print(f"[curation] {len(resolved):,} curated structures admitted", flush=True)

    targets = _targets(refs, args.worklist)
    rescued, bal, ph_smi, ph_tag, tally = complete(
        refs, resolved, targets, smiles_limit=args.char_limit,
        atom_limit=args.atom_limit, collapsed_atom_limit=args.collapsed_atom_limit)

    pq.write_table(
        pa.Table.from_pandas(
            pd.DataFrame(rescued, columns=["mnxr", "verdict", "rxn_smiles",
                                           "atoms", "chars"]),
            schema=RESCUED_SCHEMA, preserve_index=False),
        args.out, compression="zstd")
    pd.DataFrame(bal, columns=["mnxr", "element", "balanced"]).to_csv(
        args.out_balance, sep="\t", index=False)
    pd.DataFrame([dict(mnxm=m, smiles=s, tag=ph_tag[m], name=refs.name_of.get(m))
                  for m, s in ph_smi.items()],
                 columns=["mnxm", "smiles", "tag", "name"]).to_csv(
        args.out_placeholders, sep="\t", index=False)

    print("\n" + "=" * 64)
    print("CURATION SWEEP -- completion verdicts (no mapper ran)")
    print("=" * 64)
    for k, n in tally.most_common():
        print(f"  {k:<44} {n:>7,}")
    print(f"\nwrote {args.out}, {args.out_balance}, {args.out_placeholders}")
    if not rescued:
        raise SystemExit(
            "[curation] the rescue completed ZERO reactions. Pass 2 would then map an "
            "empty universe and the ensemble would silently lose layer 3 -- which is "
            "14.5% of the reactions in the table this build reproduces. Something "
            "upstream is wrong; refusing rather than producing an empty product.")
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("propose"); p.set_defaults(fn=cmd_propose)
    p.add_argument("--lookups", required=True)
    p.add_argument("--element-counts", default=None,
                   help="lookup::element_counts. Supplies the C/N/S/P of every species "
                        "whose formula declines to state one, which is what most of "
                        "these lanes abstain on")
    p.add_argument("--blockers", default=None,
                   help="interm::aam_blockers/crosswalk.tsv -- element-neutral twins")
    p.add_argument("--llm", default=None,
                   help="a crosswalk harvested from the LLM curation lane. Pre-filtered "
                        "to rows that pass `admit`: this reads it like any other, and "
                        "`admit` aborts rather than skips, so one bad row kills the bake")
    p.add_argument("--nametwin", default=None,
                   help="interm::aam_nametwin/crosswalk.tsv -- same-name duplicates")
    p.add_argument("--worklist", default=None,
                   help="interm::aam_worklist -- restricts the lanes to the reactions "
                        "adjudicated `blocked_no_structure`")
    p.add_argument("--chebi", default=None)
    p.add_argument("--modelseed", default=None)
    p.add_argument("--override", default=None,
                   help="a hand-authored crosswalk, applied last")
    p.add_argument("--drop-lane", nargs="*", default=None,
                   help="lanes to withhold from the merge. They still run and still "
                        "report, so the cost of excluding one is a measured number")
    p.add_argument("--out", required=True)

    p = sub.add_parser("complete"); p.set_defaults(fn=cmd_complete)
    p.add_argument("--lookups", required=True)
    p.add_argument("--element-counts", default=None,
                   help="lookup::element_counts. The balance gate consults it before "
                        "the formula, and requires the residue slots to cancel")
    p.add_argument("--worklist", default=None)
    p.add_argument("--crosswalk", required=True)
    p.add_argument("--char-limit", type=int, default=aam_worklist.SMILES_LEN_LIMIT)
    p.add_argument("--atom-limit", type=int, default=aam_worklist.ATOM_LIMIT)
    p.add_argument("--collapsed-atom-limit", type=int,
                   default=aam_worklist.COLLAPSED_ATOM_LIMIT,
                   help="the cap on the DISTINCT-molecule count, applied only to "
                        "completed reactions the expanded measure would refuse")
    p.add_argument("--out", required=True, help="the rescued universe, parquet")
    p.add_argument("--out-balance", required=True)
    p.add_argument("--out-placeholders", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
