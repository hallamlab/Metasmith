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

# The rescued universe is read by the SAME loaders the pass-1 members use, so it carries
# the same three columns those loaders require. Everything else about it differs: the
# SMILES is a completed reaction, not MetaNetX's, so the same MNXR has a different string
# here than in the worklist -- which is exactly why the two passes are separate products.
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
LANE_PRIORITY = ("nametwin", "twin", "transform", "fragment", "carrier", "supplier",
                 "lipid", "conserved", "polymer", "blockers", "acceptor", "override")


# =====================================================================
# the placeholder library -- deterministic stand-ins for generic carriers
# =====================================================================
FE_OX, FE_RED = "[Fe+3]", "[Fe+2]"
# thioredoxin-family: two cysteine thiols <-> one disulfide. Both C3S2; they differ by
# the 2 H that oxidation removes, which is the real chemistry.
DITHIOL, DISULFIDE = "SCCCS", "C1CCSS1"
# quinone pool: redox conserves every carbon of the ring. Both C6O2.
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
    # diflavin NADPH--hemoprotein (cytochrome P450) reductase: a 2e- carrier via FAD/FMN,
    # no transferable C/N/S/P. Both bracket orderings occur and MetaNetX writes the dash
    # run as -- or ---.
    (r"^\[?oxidized \[?nadph[- ]+hemoprotein reductase", FE_OX, "hemoprotein_reductase"),
    (r"^\[?reduced \[?nadph[- ]+hemoprotein reductase", FE_RED, "hemoprotein_reductase"),
    # ferri = oxidized (Fe3+), ferro = reduced (Fe2+); one-electron protein carriers.
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

# NEVER stand in for these, whatever else matches. Three classes, refused because a
# stand-in for them would be an INVENTION rather than a substitution:
#   * non-molecules -- `Unknown`, `Carbon`, an electron, a photon. There is nothing to
#     stand in FOR.
#   * acyl carriers -- unlike an electron carrier, an ACP's thioester DOES carry the
#     atoms through, so a stand-in would have to invent where they attach, and the
#     balance gate cannot catch that: the atoms would balance while being routed through
#     fabricated bonds.
#   * generic donor/acceptor templates -- `A + 2[H] <-> AH2` is a template, not an
#     instance.
REFUSE = re.compile(
    r"^(unknown|carbon|d|nad|nadh|idh\d*|enzyme-\w+ complex|acceptor|reduced acceptor"
    r"|.*\bacp\b.*|.*acyl-carrier.*|.*acyl carrier.*|starch|chitin|.*tRNA.*"
    r"|a|ah2|reduced acceptor"
    r"|e\(-\)|e-|hnu|hn|h\N{GREEK SMALL LETTER NU}|photon|light"
    r"|phosphoprotein|.*\[protein\]|protein .*)$", re.I)


def placeholder_for(name: str):
    """(smiles, tag) for a structureless generic, or None to leave it refused."""
    if not name:
        return None
    n = str(name).strip()
    if REFUSE.match(n):
        return None
    for rx, smi, tag in _COMPILED:
        if rx.match(n):
            return smi, tag
    return None


# =====================================================================
# the admission check -- the four ways a curated row can be silently wrong
# =====================================================================

def count_struct(smiles: str, X: str):
    """Atoms of element X in a SMILES, counted from the STRUCTURE.

    The `*` dummy contributes to NO element -- the correct reading of a curated carrier,
    not a convenient one: the row asserts the drawn atoms and explicitly declines to
    claim the body. `gate_bodies_cancel` is what makes that silence safe for balance.
    """
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    return sum(1 for a in mol.GetAtoms() if a.GetSymbol() == X)


def read_crosswalk(path):
    """Parse a curated crosswalk TSV.

    `#` is a comment ONLY at line start. An inline `comment="#"` truncates any curated
    SMILES containing a `#` triple bond (nitrile C#N, alkyne C#C) -- silently dropping
    exactly the rows a curator most needs to supply.
    """
    kept = [ln for ln in Path(path).read_text().splitlines()
            if not ln.lstrip().startswith("#")]
    return pd.read_csv(StringIO("\n".join(kept)), sep="\t")


def admit(rows: pd.DataFrame, mets: pd.DataFrame, strict=True):
    """mnxm -> curated SMILES, or abort. Every row is an ASSERTION; see the module doc.

    ABORTS on the first bad row rather than skipping it. A curated crosswalk is a small
    hand-authored artifact; a bad row in it is an authoring error to fix, not noise to
    filter, and filtering would let a stale id sit in the file forever while its
    contribution silently read as zero.
    """
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
        # 1. ADDITIVE ONLY. A row may only SUPPLY a structure MetaNetX lacks. Overriding
        #    one is a different and far larger claim, and it would let the crosswalk
        #    perturb reactions that already map today.
        if isinstance(smi, str) and smi.strip():
            raise SystemExit(
                f"[curation] {r.mnxm} already HAS a structure ({smi!r}). A curated row "
                f"may only SUPPLY a structure MetaNetX lacks, never override one.")
        # 2. STALE-ID TRIPWIRE. Not name-as-proof -- the check that the id the curator
        #    reasoned about is the id they wrote down.
        if str(nm or "").strip() != str(r.mnx_name).strip():
            raise SystemExit(
                f"[curation] {r.mnxm}: row says {str(r.mnx_name)!r}, the metabolite "
                f"table says {nm!r}. The id is the key -- a mismatch means the row is "
                f"about a different metabolite than the curator reasoned about.")
        # 3. THE ROW CHECKS ITSELF.
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
    """Unspecified `*` slots this participant carries, whichever source drew it.

    ONE LEDGER FOR THE BODY, and that is the whole point of the function existing. A
    body reaches a reaction by two routes -- a curated row draws it as `*` in the SMILES
    this run supplies, and MetaNetX draws it as an R-group in a structure it already
    had, recounted into `residue_of`. Those two used to be tallied in two different
    places: `gate_bodies_cancel` saw only the curated half and `concrete_balance` only
    the recounted half. A curated body on one side and a MetaNetX body on the other
    therefore failed BOTH checks, each for the half it could not see, when between them
    the two bodies cancel exactly.

    `None` is an UNKNOWN slot count and is not a zero -- the caller must refuse it. A
    placeholder is scaffolding whose atoms are suppressed downstream, so it carries no
    body here either. A plain-formula participant carries none by construction:
    `count_element` refuses a `*` outright, so anything it counted had no remainder.
    """
    if m in ph_mnxms:
        return 0
    if m in resolved:
        return resolved[m].count("*")
    if m in counts_of:
        return residue_of.get(m)
    return 0


def gate_bodies_cancel(subs, prods, resolved: dict, ph_mnxms=(), counts_of=None,
                       residue_of=None):
    """Do the unspecified `*` bodies pair across the reaction?

    A carrier draws its body as `*` and counts it as zero for every element. That is only
    safe for balance when the SAME body stands on both sides and cancels. A carrier
    appearing on ONE side would have its unknown body silently counted as nothing, and
    the balance verdict would be about a molecule that does not exist.

    This is also the answer to the standing objection that "the body cancels, so a wrong
    carrier balances as well as the right one". That objection kills IDENTITY assertion
    and not this one: identity was never tested by balance, whereas the DIFFERENCE the
    row asserts -- one sulfur, say -- is both what is claimed and what is tested.

    AN UNKNOWN SLOT COUNT PASSES HERE and is refused by `concrete_balance` instead. Both
    are refusals and the reaction dies either way; refusing it there keeps "the bodies do
    not cancel" and "a count is unknown" as separate verdicts in the tally, which is the
    difference between a diagnosable bucket and an undiagnosable one.
    """
    def slots(ms):
        return [residue_slots(m, resolved, ph_mnxms, counts_of or {}, residue_of or {})
                for m in ms]
    s, p = slots(subs), slots(prods)
    if any(v is None for v in s + p):
        return True
    return sum(s) == sum(p)


# ONE COUNTER IN THE TREE, and this is the alias rather than a second copy of it. Three
# byte-identical implementations of "atoms of X in a MetaNetX formula" used to exist here,
# in `atom_pairs`, and in `mnx_lookups`, all of which had to agree and none of which was
# tested against the others. The one that is tested is the extractor's, so it is the one
# that survives; the name stays because it is what this module's balance gates read.
count_formula = AP.count_element


def concrete_balance(subs, prods, formula_of, ph_mnxms, X, resolved=None,
                     counts_of=None, residue_of=None):
    """Do the CONCRETE (non-placeholder) atoms of element X balance?

    THIS is what tests the conservation claim. If a carrier actually donated or absorbed
    an X atom, the concrete side that gained or lost it no longer balances and this
    returns False -- so the reaction is refused for X while staying usable for the
    elements the carrier really is inert to.

    Returns None when a concrete participant's formula is untrustworthy: an unknown
    count cannot be balanced, and abstaining is a refusal too.

    A RESOLVED participant is counted from its CURATED STRUCTURE, not skipped. It is not
    scaffolding -- it is a metabolite whose structure this run supplies, so its atoms are
    part of the chemistry being balanced, and its asserted difference is precisely what
    the balance then tests.

    `counts_of` IS THE RECOUNT, AND IT COMES FIRST. A `C70H131N3O9PS*2` species has no
    countable formula and an exactly countable structure, so consulting the formula
    first would abstain on a reaction whose atoms are known. The count it supplies is
    exact for the EXPLICIT atoms only, which is why the residue travels with it: the
    unspecified slots have to cancel across the equation before the count means
    anything about conservation, and an unknown slot count is a refusal like any other.

    THE SLOTS ARE COUNTED BY `residue_slots`, NOT HERE, and the reason is in that
    function's docstring: a curated `*` and a MetaNetX R-group are the same claim from
    two sources, and this check used to see only the second of them.
    """
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
        return None          # an unknown residue count is not a zero
    if sum(res["s"]) != sum(res["p"]):
        return None          # the unspecified remainders do not cancel
    if tot["s"] == 0 and tot["p"] == 0:
        return None          # element absent; nothing to say
    return tot["s"] == tot["p"]


# =====================================================================
# name normalisation shared by every name-anchored lane
# =====================================================================
_NORM = re.compile(r"[^a-z0-9]+")
_LOCANT = re.compile(r"^\d+$|^[nosprc]$|^alpha$|^beta$|^gamma$|^d$|^l$|^dl$|^cis$"
                     r"|^trans$|^\d+[a-z]$")

# ONE table, not four. In the previous generation this dictionary was duplicated
# verbatim across four scripts, which is three chances for them to drift while every
# script still ran. Each entry is the C/N/S/P DELTA a named modifier adds to its base.
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

# One fragment per added atom, so a delta can be built as a real structure rather than
# asserted as a number. Disconnected on purpose: the vehicle carries a COUNT, and
# pretending to know where the methyl attaches would be a claim the name does not make.
DELTA_FRAGMENT = {"C": "C", "N": "N", "S": "S", "P": "P"}


def norm(s):
    return _NORM.sub(" ", str(s or "").lower()).strip()


def base_aliases(n):
    """The acid/ate/plural alternations that are spelling, not chemistry."""
    yield n
    if n.endswith(" acid"):
        yield n[:-5]
    if n.endswith("ate"):
        yield n[:-3] + "ic acid"
        yield n[:-3] + "ic"
    if n.endswith("s") and len(n) > 4:
        yield n[:-1]


def residue_names(tok):
    """alanyl->alanine, acetyl->acetate, glucosyl->glucose -- the acyl/glycosyl
    residue naming a fragment-sum argument reads."""
    if tok.endswith("yl"):
        stem = tok[:-2]
        yield stem + "ine"
        yield stem + "ate"
        yield stem + "ic acid"
        yield stem + "e"
        yield stem + "ose"
        yield stem
    yield tok


# =====================================================================
# the reference index every lane reads
# =====================================================================

class Refs:
    """The lookups, indexed the one way every lane needs them."""

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
        # THE FORMULA COLUMNS ARE THE FALLBACK, NOT THE SOURCE. `n_C..n_P` are
        # `count_element` over the MetaNetX formula, so every `*` species is NULL there
        # -- and a NULL disqualifies the metabolite from every budget, every twin
        # comparison and every balance below. `lookup::element_counts` reads the
        # structure instead, which is where those counts actually are. Both are carried
        # so this module still runs standalone against the five lookups alone.
        self.counts_of = {
            r.mnxm: (r.n_C, r.n_N, r.n_S, r.n_P)
            for r in mets.itertuples(index=False)
            if not (pd.isna(r.n_C) or pd.isna(r.n_N)
                    or pd.isna(r.n_S) or pd.isna(r.n_P))
        }
        # Residues are 0 for a formula-derived count by construction: `count_element`
        # refuses a `*` outright, so anything it counted had no unspecified remainder.
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
        # name -> the SMALLEST structured metabolite carrying that name. Smallest,
        # because a name that matches both a monomer and a polymer of it should resolve
        # to the monomer: over-claiming atoms is the failure mode balance cannot catch.
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
        """Reactions that parse but have at least one structureless participant --
        exactly the set no mapper has ever seen."""
        d = self.reactions
        return d[(d["n_blockers"] > 0)]


# =====================================================================
# the proposer lanes
# =====================================================================

def _row(mnxm, smiles, name, counts, basis, lane):
    """One proposal, expanded to the per-element rows the crosswalk carries."""
    out = []
    for X, n in zip(ELEMENTS, counts):
        if n and n > 0:
            out.append(dict(mnxm=mnxm, smiles=smiles, mnx_name=name, element=X,
                            n_atoms=int(n), basis=basis, lane=lane))
    return out


def lane_twin(refs: Refs, targets):
    """SINGLE-BLOCKER CONSERVATION. A skeleton-preserving transform leaves a stub's
    C/N/S/P counts equal to its structured partner's.

    The independent evidence is the NAME, not the balance. Balance alone CANNOT validate
    a single-blocker inference -- one unknown always back-fills the residual, so balance
    is tautological here and the argument has to come from somewhere else. It comes from
    the two names sharing a skeleton stem, which is exactly what "isomerisation /
    epimerisation / lactonisation / hydration" means.

    Restricted to ELEMENTARY 1:1 reactions with exactly one structured skeleton partner;
    lumped multi-substrate pseudo-equations are excluded, and would fail automap anyway.
    """
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
    """A SMILES carrying exactly `counts` heavy atoms plus `caps` dummies.

    Connected if rdkit will take it, disconnected atoms otherwise. Either way the
    vehicle asserts a COUNT and declines to assert connectivity -- which is the honest
    reading of a name-derived budget, and is why the atom graph (which counts atoms, not
    bonds) can use it at all.
    """
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
    """NAMED TRANSFORM. name = base + recognised modifier(s) -> base counts + delta."""
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
    """FRAGMENT SUM. A name that splits into >=2 residue tokens each resolving to a
    metabolite (dipeptides, acyl-amino-acids, glycosides) -> the summed budget.

    THE CURATED PAIR OUTRANKS AN INFERRED SUM, and this lane is the only one that has to
    say so. Every other lane derives its budget from a structure or from a conservation
    argument; this one infers it by reading the NAME as chemistry, which is exactly the
    reading a redox carrier's name defeats. `oxidized [NADPH--hemoprotein reductase]` is
    an enzyme in an oxidation state, but its tokens resolve -- `nadph` to real NADPH and
    `oxidized` to MNXM588580, a ModelSEED fragment stub whose name is the bare word
    `Oxidized-`, the trailing hyphen erased by `norm`. The sum handed the reductase
    C33/N11/P3. Its reduced twin has no such stub to collide with, so it fell through to
    the placeholder library's `[Fe+2]`, and the couple stopped balancing: 1,122 reactions
    completed, failed `concrete_balance` on carbon, and were never mapped.

    Deferring costs nothing measurable and is the weaker claim of the two -- the library's
    entries are atom-matched ox/red pairs, so where it covers a metabolite it also covers
    its twin, which is the property the balance gate is testing.
    """
    rows, seen = [], set()
    for r in targets.itertuples(index=False):
        for P in r.blockers:
            if P in seen or P in refs.smiles_of:
                continue
            nm = refs.name_of.get(P, "") or ""
            if placeholder_for(nm) is not None:
                continue
            # The cap count only, and for the reason `lane_transform` records.
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
    """CARRIER / ACYL VEHICLES -- the lane the previous generation measured a real
    payoff from, and the one that needs the `*` body.

    An acyl carrier is NOT an electron carrier: its thioester genuinely carries the cargo
    atoms through. So the vehicle is `cargo + *` -- the cargo's atom budget drawn
    explicitly, the ACP body drawn as one dummy that CANCELS across the equation. The
    cargo transits; the body says nothing and is required to say nothing on both sides.

    The cargo budget comes from the cargo's own name resolving to a structured
    metabolite. Where it does not resolve, the lane declines -- inventing a chain length
    is exactly the invention the REFUSE list exists to prevent.
    """
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
                # bare `ACP` / `holo-[ACP]`: no cargo at all, so the whole molecule is
                # body. It carries nothing, which a lone `*` states exactly.
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
    """ChEBI SMILES, keyed BOTH ways: `(by_accession, by_compound_id)`.

    One pass, two keyings, because the lane needs both and the structures table is 89 MB
    of gzip. The accession route arrives holding `CHEBI:15377` out of MetaNetX's xref
    table; the name route arrives holding a bare `compound_id` out of ChEBI's own names
    table, and translating between them costs a second scan for nothing.

    `default_structure` wins where a compound has several. ChEBI carries tautomers and
    protonation states as separate rows, so taking the first is taking an arbitrary one.
    """
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
    # The molfile column is a multi-line quoted CSV field, so this must go through a
    # real csv reader -- splitting on tabs loses the row alignment for every structure
    # that carries one, which is most of them.
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


# ChEBI's own name table normalisation, kept SEPARATE from `norm` on purpose. `norm`
# collapses to space-delimited words because the lanes that use it go on to read those
# words; this one strips to bare alphanumerics because it only ever tests equality, and
# the near-homographs it merges (stereo, locant and charge prefixes) share a formula --
# which is all this lane takes from the hit. A genuinely wrong merge does not survive:
# the element budget it proposes has to balance the reaction, and `complete` drops it if
# it does not.
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
    """`normalised name -> compound_id`, restricted to names some blocker asked for.

    ChEBI's name table is the reach this lane exists for. MetaNetX's synonym index --
    which route 2 already searches -- holds only the names MetaNetX itself recorded, so a
    well-named small molecule that MetaNetX never cross-referenced is invisible to it
    while ChEBI has both the name and the structure. Filtering to `want` while scanning
    keeps a 8.7 MB table from becoming a dict of every name ChEBI knows.
    """
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
    """EXTERNAL SUPPLIER. Resolve a blocker through an accession first, then a name.

    TWO ROUTES, AND THE ORDER IS THE POINT. An ACCESSION route (`xrefs` says this MNXM
    is chebi:15377, and ChEBI has a structure for chebi:15377) is an IDENTIFIER match --
    the thing "resolving by name is what this codebase has already been burned by" was
    asking for. Only where no accession resolves does the lane fall back to a NAME match
    through the synonym index, and that fallback is deliberately generous because the
    arbiter, not the proposer, is what makes generosity safe.
    """
    chebi_smi, chebi_by_cid = _chebi_structures(chebi)
    ms_smi = _modelseed_structures(modelseed)
    if not chebi_smi and not ms_smi:
        return []

    blockers = sorted({m for r in targets.itertuples(index=False) for m in r.blockers}
                      - set(refs.smiles_of))
    if not blockers:
        return []
    want = set(blockers)

    # route 1: accession
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

    # route 2: name, through the synonym index, for what route 1 missed
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

    # route 3: ChEBI's OWN name table, for what neither of the first two reached.
    # Route 2 searches the names MetaNetX recorded; this searches the names ChEBI
    # recorded, which is a far larger set and is the only route that reaches a
    # well-named molecule MetaNetX cross-referenced to BiGG and nothing else.
    still2 = [m for m in blockers if m not in by_acc and m not in by_name]
    by_cname = {}
    if still2 and chebi_by_cid:
        # The blocker's own name plus every synonym MetaNetX filed against it: the
        # deployed lever queried on both, and the synonym is often the one ChEBI knows.
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


# =====================================================================
# the glycerolipid templater
# =====================================================================
# WHY A STRUCTURE MAY BE BUILT HERE AND NOWHERE ELSE. Every other lane finds a structure
# that someone else asserted -- a twin's, a supplier's, a curated cap. This one CONSTRUCTS
# one from the metabolite's name, which would be indefensible for a general molecule and is
# defensible for exactly this family: a glycerolipid written in LIPID-MAPS shorthand states
# its own composition. `1,2-Diacyl-sn-glycerol(16:1(9Z)/20:5(5Z,8Z,11Z,14Z,17Z))` fixes the
# backbone, the head group, and every acyl chain's carbon count. MetaNetX simply never
# recorded a structure for it, so every reaction touching one is refused over chemistry that
# is fully determined -- and these are the largest single family left in the refused set.
#
# WHAT MAKES IT HONEST is that the build only has to be right about what is TRACKED. The
# atom-pair table follows C/N/S/P; the `(C:D)` shorthand fixes those counts exactly, and D
# -- the number of double bonds -- changes only hydrogen, which nothing here counts. So the
# POSITION and GEOMETRY of the double bonds are free, and this lane places them canonically
# rather than pretending to know them. That is the whole safety argument, and it is why the
# same construction would be wrong for a lane that scored stereochemistry.
#
# THREE INDEPENDENT NETS, because a templater that is subtly wrong about carbon corrupts
# atom identity silently:
#   1. the family must be recognised AND the name must carry exactly as many `C:D` specs as
#      the family has acyl positions -- a partial shorthand is refused, not guessed at;
#   2. a name carrying a substituent no template represents is refused outright, because
#      under-counting is the dangerous failure: the same wrong stub can sit on both sides of
#      a transfer and cancel, walking a bad edge straight past the balance gate;
#   3. the assembled structure is re-counted with RDKit and must equal the counts derived
#      from the shorthand by hand. A mismatch is a template bug and drops the row.
# The per-element balance gate in `complete` is the fourth, and it is not this lane's.

_LIPID_HEAD = {                     # (SMILES from the sn-3 oxygen, head C, head N, head P)
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

# family -> (acyl positions, head key). Glycerol contributes 3 C to every glycerolipid;
# TAG has no head because a third acyl takes the sn-3 position, and FA is not a
# glycerolipid at all -- it is the bare acid the same shorthand also describes.
_LIPID_FAMILY = {
    "DAG": (2, "OH"), "TAG": (3, None), "PC": (2, "PC"), "PE": (2, "PE"),
    "LPC": (1, "PC"), "LPE": (1, "PE"), "PI": (2, "PI"), "PIP": (2, "PIP"),
    "PIP2": (2, "PIP2"), "PIP3": (2, "PIP3"), "MGDG": (2, "GAL"), "DGDG": (2, "DIGAL"),
    "CDPDAG": (2, "CDP"), "MAG": (1, "OH"), "DGTS": (2, "DGTS"), "PA": (2, "PA"),
    "LPA": (1, "PA"), "FA": (1, None),
}

# Substituents that add carbon the `C:D` specs do not account for. Net 2 above.
_LIPID_EXTRA = (
    "glucosaminyl", "glucosamin", "mannosyl", "mannosid", "mannose", "glucosyl",
    "glucuron", "sulfoquinovosyl", "sulphoquinovosyl", "acetyl", "glucosphingo",
    " gpi", "(gpi", "signal sequence", "peptide", "diglucosyl", "monoglucosyl",
    "ceramide", "sphingo", "arabino", "rhamn", "fucos", "methyl branch", "methyl-branch")

_CD = re.compile(r"(\d{1,2}):(\d+)")


def _lipid_family_raw(n: str):
    """Most specific first: `phosphatidylinositol bisphosphate` is PIP2, not PI."""
    if "ferredoxin" in n or "flavodoxin" in n or "thioredoxin" in n:
        return None                                   # `4:2` here is an Fe-S cluster
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
    # A free fatty acid ONLY where the shorthand fully determines it. Phospholipid
    # "acids", methyl-branched chains and acyl-ACP carriers all carry carbon the spec
    # does not state.
    if (" acid (" in n and _CD.search(n) and "phosphatid" not in n
            and "methyl" not in n and "acp" not in n):
        return "FA"
    return None


def lipid_family(name):
    """The family, or None where any of the first two nets refuses."""
    n = str(name or "").lower()
    fam = _lipid_family_raw(n)
    if fam is None:
        return None
    if any(t in n for t in _LIPID_EXTRA):
        return None
    # Galactosyl is carbon the template accounts for only in the galactolipids.
    if "galactosyl" in n and fam not in ("MGDG", "DGDG"):
        return None
    # The shorthand must name every acyl position. A DAG written with one `C:D` is a
    # name we have not understood, not a DAG with one chain.
    if len(_CD.findall(n)) != _LIPID_FAMILY[fam][0]:
        return None
    return fam


def _acyl(n: int, d: int):
    """The ester fragment from the carbonyl out: `C(=O)` plus n-1 chain carbons.

    Double bonds are spaced three carbons apart from the beta position, which keeps them
    non-cumulated and always valid. Their placement is arbitrary and that is admissible
    here: only C/N/S/P counts are read downstream, and d changes neither.
    """
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
    """`(smiles, hand_C, hand_N, hand_P)` derived from the shorthand alone."""
    n_acyl, head_key = _LIPID_FAMILY[family]
    if len(specs) != n_acyl:
        return None
    esters = []
    for c, d in specs:
        frag, nc = _acyl(c, d)
        if frag is None:
            return None
        esters.append(("O" + frag, nc))          # the glycerol oxygen plus the acyl
    chain_c = sum(nc for _, nc in esters)

    if family == "FA":
        return esters[0][0], chain_c, 0, 0       # `OC(=O)...` is the free acid
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
        # Net 3: the assembled structure must carry exactly what the shorthand said.
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


# =====================================================================
# the budget resolver -- (heavy C/N/S/P, caps) for a structureless metabolite
# =====================================================================
# WHAT THE THREE ADDED LANES ALL NEED, and the reason they are three lanes and not
# thirty rules. A conserved-body vehicle is `cargo + *`: an asserted atom budget for the
# part that transits, and one dummy for the part that does not. The lanes differ in WHICH
# metabolites they will draw that way and on what warrant; the arithmetic is shared.
#
# THE FORMULA IS THE FIRST AND BEST SOURCE, and it is the one the original five lanes do
# not use. MetaNetX gives many structureless metabolites a formula that already says both
# halves -- `C25H38N7O17P3S*` is a budget AND a declaration that one substituent is
# unbounded. `count_formula` above returns None for exactly those, correctly, because an
# unknown count cannot BALANCE. Here the question is different: what may be DRAWN. A `*`
# in the formula is MetaNetX's own statement that the rest is a conserved body, which is
# precisely the vehicle's claim.

_FTOK = re.compile(r"([A-Z][a-z]?)(\d*)")


def counts_formula_caps(f):
    """(C, N, S, P, caps) from a formula, or None when it cannot be read.

    `R`, `X`, `Z`, brackets and dots defeat it: those denote a variable group whose atom
    count is not merely unknown but unbounded, and a vehicle drawn from a guess at one is
    the invention this module exists to refuse. `*` does NOT defeat it -- it is counted.
    """
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
    """(C, N, S, P, caps) from a SMILES; `caps` counts the `*` dummies."""
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


# Carrier motifs. Each occurrence is one substitutable cap: the ACP, the CoA, the
# protein, the holo/apo body. Longest first so `[acyl-carrier protein]` is one cap and
# not two.
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
    """(caps, the name with its carrier motifs removed) -- the cargo half of the split."""
    caps = len(CARRIER_RE.findall(name or ""))
    core = CARRIER_RE.sub(" ", name or "")
    # The leading article goes with the carrier. `an [acyl-carrier protein]` must leave
    # NO core -- "an" is not a cargo, and a core of "an" is the difference between
    # recognising a bare carrier body and failing to resolve one.
    core = re.sub(r"^\s*(an?|the)\s+", " ", norm(re.sub(r"[\[\]]", " ", core)))
    return caps, core.strip()


# Abstract redox / acceptor placeholders: no C/N/S/P transits, one cancelable body.
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


# Fatty-acyl nomenclature -> carbon count. An acyl name states its chain length, which is
# the whole budget an acyl vehicle needs; nothing else in the name transits.
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

# Free amino-acid C/N/S/P. Peptide-bond formation loses only H2O, so the C/N/S/P sum over
# residues is exact -- which is what makes an aminoacyl cargo resolvable from its name.
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
    for root, c in NUM_ROOTS.items():        # longest matching numeric root wins
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
            return None      # one non-standard residue and the whole peptide is unresolved
        for i in range(4):
            tot[i] += c[i]
    return tuple(tot)


def _budget_from_name(refs: Refs, core: str):
    """The cargo's C/N/S/P from its NAME, by every route the original lanes already use
    plus the two acyl/peptide vocabularies they do not."""
    if not core:
        return None
    for a in base_aliases(core):
        bid = refs.name2id.get(a)
        if bid and refs.counts_of.get(bid):
            return tuple(refs.counts_of[bid])
    # named transform: base + modifier deltas
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
    # fragment sum over >=2 resolvable residues
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
    """`(heavy(C,N,S,P), caps, method)` for a structureless metabolite, or None.

    The order is a trust order. A formula MetaNetX wrote beats a name we parsed; a name
    that names a carrier and a cargo beats one we can only read as a whole. None means
    the lanes decline -- inventing a chain length is exactly what `REFUSE` exists for.
    """
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


# =====================================================================
# the three lanes the deployed chain had and this one did not
# =====================================================================

def _vehicle_row(mnxm, name, heavy, caps, basis, lane):
    """A `cargo + *` proposal, or None when the budget will not draw.

    A vehicle with NO cargo still emits one row -- element C, zero atoms. That row
    contributes no atom pair and is not meant to: it exists so `admit` records the
    metabolite as resolved, which is what unblocks the reaction for its CONCRETE
    partners. Dropping it because it looks empty is how a lane silently rescues nothing.
    """
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
    """THE DELIBERATE `REFUSE` BYPASS: generic acceptors and `[protein]` bodies.

    `REFUSE` declines these for a good reason -- `A + 2[H] <-> AH2` is a template rather
    than an instance, and a stand-in for a template is an invention. This lane does not
    dispute that; it declines to draw the template's ATOMS and draws only its BODY:

      * a generic acceptor becomes a bare `*`. Zero C/N/S/P, one cancelling dummy. It
        contributes NO atom pair. What it does is unblock the reaction so the CONCRETE
        partners' real transit can be mapped, and the balance gate then tests, per
        element, that nothing crossed into the acceptor.
      * a `[protein]`-bodied metabolite becomes `cargo + *` -- the same decomposition as
        an acyl carrier, because it is the same chemistry: `L-seryl-[protein]` carries a
        serine that genuinely transits, on a body that does not.

    IT IS A SEPARATE LANE SO ITS DELTA STAYS A SEPARATE NUMBER. In the deployed chain
    this lever banked +426 reactions and cost 1,129 refusals at the body-cancel gate,
    and that trade has to remain visible rather than dissolve into a total. `--drop-lane
    acceptor` removes it and nothing else.
    """
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
                caps = max(caps, 1)               # the protein body is always one `*`
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
    """CONSERVED-BODY VEHICLES for every carrier family, not just ACP and CoA.

    `lane_carrier` splits a name on the literal words ACP / acyl-carrier / CoA and
    resolves the cargo through the synonym index. That is precise and it is narrow: the
    same `cargo + *` decomposition is correct for tRNA-charged species, holo- and
    apo-forms, protein bodies, and any metabolite whose MetaNetX FORMULA already carries
    a `*` -- which is MetaNetX's own statement that the rest is an unbounded body.

    This lane runs after `lane_carrier` and picks up what it declined. It prefers a
    CONCRETE CoA TWIN where one exists: a structured CoA metabolite with the same
    C/N/S/P and a shared acyl stem gives a real structure rather than a vehicle, so both
    the thioester's reactions and free-CoA's reactions balance against the same atoms.
    """
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
            # Only draw a `*` body where there is a body to draw. A metabolite whose
            # formula is clean and whose name names no carrier is not this lane's
            # business -- the twin/transform/fragment/supplier lanes had first refusal
            # and declining is their answer, not an invitation to invent one here.
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


# Canonical monomer structures for the ladder, tried LARGEST first so a C6 residual
# decomposes to one hexose rather than six one-carbon units. Each is verified against its
# own SMILES at import, so an emitted n_atoms always equals the structure it cites.
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
    """A signed C/N/S/P residual -> `{monomer: signed k}`, or None.

    ACCEPTS ONLY A SINGLE-TYPE MULTIPLE. A residual that needs two different monomers to
    explain it is a residual we have not understood, and the ladder's whole warrant is
    that mass balance -- not the name -- pins which monomer transits.
    """
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
    """The polymer family key: a name with its length variance stripped out."""
    b = re.sub(r"\([^)]*\)|\bn\s*=?\s*[+-]?\d*\b|[0-9]|->|,|\bx\b", "", (nm or "").lower())
    b = re.sub(r"\b(alpha|beta|d|l|linked|unlinked|repeat|units?|substituted|"
               r"unsubstituted|branch|precursor|degradation|product|nonreducing|"
               r"reducing|end)\b", " ", b)
    return re.sub(r"[^a-z]", "", b)


def lane_polymer(refs: Refs, targets):
    """POLYMER / GLYCOCONJUGATE LADDERS, levelled globally rather than per reaction.

    The forsaken polymer tail is elongation and degradation ladders: a stub appears on
    BOTH sides as two length variants, and a structured participant donates or releases
    the monomer that distinguishes them. Neither variant has a structure, so MetaNetX
    refuses the reaction -- yet its chemistry is determined. The shared backbone is a
    carrier that cancels, and the balance-forced residual is exactly k whole monomers.

    CONSISTENCY IS GLOBAL, NOT PER REACTION, and that is the part worth the code. Within
    a backbone family the per-chain monomer count is solved by BFS over the ladder edges,
    so one chain metabolite is never drawn two different ways in two reactions. A family
    whose edges cannot be levelled -- a cycle disagrees, or a residual is not a clean
    single-type multiple -- is DROPPED WHOLE. That refusal is the lever's honesty: 85
    families were dropped in the deployed run, and a per-reaction version of this lane
    would have banked them all with contradictory structures.
    """
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
        # Offset each monomer type so the family's minimum is >= 0. Every pairwise
        # difference -- which is what the ladder actually determines -- is preserved.
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


# =====================================================================
# merge + the arbiter
# =====================================================================

def merge(lanes: dict):
    """One metabolite, one lane. Priority is LANE_PRIORITY and it changes the result."""
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
    """Build the completed reaction SMILES for every rescuable reaction. NO MAPPER RUNS.

    THIS USED TO BE `arbitrate`, AND IT USED TO MAP. Indigo ran here, inside the curation
    sweep, and whatever it produced became layer 3 -- which is why in the deployed table
    every one of the 9,089 rescue-derived reactions is `mcs_only`: Indigo alone, at half
    weight, because the crosswalk did not exist when the neural members ran and nothing
    ever showed them the completed reactions. Splitting the mapping out turns those into
    a second pass that all three members see, and what they agree on becomes full-weight
    consensus. The completion itself -- which structures, which bodies cancel, which
    elements balance -- is unchanged, and it never needed a mapper:

      * `gate_bodies_cancel` is arithmetic over the `*` counts, curated and MetaNetX's.
      * `concrete_balance` is arithmetic over formulas and curated structures.

    Both are computed here so a rescued reaction arrives at the mappers already carrying
    its verdict, and a reaction where no element could balance is never mapped at all.

    Returns (rescued_rows, balance_rows, placeholder smiles, placeholder tags, tally).
    """
    smi = dict(refs.smiles_of)
    smi.update(resolved)
    ph_smi, ph_tag = {}, {}
    todo, tally = [], Counter()

    for r in targets.itertuples(index=False):
        parts = set(r.substrates) | set(r.products)
        gens = [m for m in parts if m not in smi]
        # `smi` now carries the curated structures, so a reaction blocked ONLY by
        # resolved carriers has no generics left -- and would fall through as "already
        # buildable" and be dropped, rescuing exactly nothing. It is not already
        # buildable: it was skipped precisely because those participants had no structure
        # at the time. Supplying one is what makes it a rescue case.
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
    # A PROGRESS LINE, because this loop was silent for two hours and a lane with no
    # output is a lane nobody can tell from a hung one. It parses a completed reaction
    # SMILES per reaction and can collapse it, and a rescued reaction is the LARGEST
    # string this build makes -- placeholder-completed polymers and lipids -- so the cost
    # per reaction is bounded by nothing the caller can see. What it prints is what a
    # reader needs to decide whether to wait: how far in, and how fast.
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
            # Nothing to gain by mapping it: the extractor drops every element whose
            # concrete atoms did not balance, so this reaction would produce no pair
            # whatever the mappers said.
            tally["no element balances"] += 1
            continue
        rescued.append(dict(mnxr=r.mnxr, verdict="mappable", rxn_smiles=rxn,
                            atoms=atoms, chars=len(rxn), collapsed=collapsed))
    tally["RESCUED"] = len(rescued)
    return rescued, bal_rows, ph_smi, ph_tag, tally


# =====================================================================
# driver
# =====================================================================

def _targets(refs: Refs, worklist=None):
    """The reactions the rescue lanes aim at.

    THE WORKLIST DECIDES, when one is given. `blocked_no_structure` is the rescuable
    class; `non_molecule` is not (nothing can stand in for an electron), `no_transfer` is
    not (the two sides are the same multiset, so there is nothing to map), and `oversize`
    is not (it was refused for cost before any of this). Without a worklist the old
    behaviour stands -- every reaction with a structureless participant -- so the module
    is still usable on its own.
    """
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
    for key, path in (("nametwin", args.nametwin), ("blockers", args.blockers)):
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
