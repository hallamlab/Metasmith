"""Give the thermo members a readable equation where MetaNetX gave them an unreadable one.

Both members walk a reaction's participants in equation order and return at the FIRST one
they cannot use, so one underspecified participant silences the whole reaction. MetaNetX
underspecifies two ways that matter here: a generic redox carrier with no SMILES or a `*`
residue, and a polymer carried as a fixed-formula molecule rather than a chain increment.
This lane substitutes a declared model compound for the first and rewrites the equation for
the second, and does neither unless a table says to.

WHAT THIS IS NOT. `bake/aam/curation.py` supplies placeholder structures for the atom
mapper, and its `PLACEHOLDERS` table must NOT be reused, imported or paralleled here --
the next reader will want to and it is the wrong instinct. A placeholder is safe there
because the mapper only needs the atoms to be countable and `atom_pairs` suppresses them
downstream. A thermodynamic member COMPUTES WITH whatever structure it is handed:
`[Fe+3]`/`[Fe+2]` makes ferredoxin balance and gives a confident, wrong dG'0. The
requirement here is not a stand-in that balances, it is a model compound whose POTENTIAL
is right, which is why every row below cites an anchor reaction and a congener set and no
row is admitted on balance alone.

THE OPERATION IS ONE OPERATION. A row replaces one participant's contribution to a
reaction with a list of terms drawn from declared model compounds:

    carrier   `Reduced flavin` -> 1 x MODEL:FMNH2                 (a rename)
    polymer   `Glycogen` -> 1 x MODEL:maltotetraose
                            - 1 x MODEL:maltotriose               (rename + insert)

A props-level override alone cannot do the second: for `G1P = Glycogen + Pi` the acceptor
is ABSENT from the equation, not underspecified in it, and no assignment of structures to
existing keys balances it. A stoichiometry rewrite alone cannot do it either: it names a
compound with nothing to look up. So both, together, once.

NO METANETX KEY IS EVER OVERWRITTEN. Model compounds live under synthetic `MODEL:` ids and
the props extension is asserted disjoint from the base. A reaction the tables do not cover
therefore walks the identical code path it walks today -- which is the property that makes
this safe to switch on without re-validating either member. Neither member changes: they
take `(stoich, props)`, they get a different stoich and a wider props, and they compute.
The wildcard guard, the sigma ceiling and the balance abstention all stay verbatim.

WHAT REPLACES `concrete_balance`. The AAM lane admits a curated structure per element,
which does not transfer: a dG'0 is one number over the whole equation, not a per-element
ledger. What replaces it is weaker and sufficient -- if a carrier really did transfer atoms
in some reaction, the fixed model structure fails to balance there and the member abstains
exactly as it does today. The failure mode is silence, not a wrong number.

A CARRIER ROW IS COMPOUND-KEYED; A POLYMER ROW IS NOT. A carrier substitution is a rename
and is right wherever the carrier appears, so it applies unconditionally. A polymer row
asserts something about the REACTION -- that MetaNetX wrote a chain increment as a fixed
molecule and the equation is short an acceptor -- so it applies only where that is
observably true, by three conditions computed per reaction:

    (i)   exactly ONE flattened-polymer participant is present,
    (ii)  the equation does not balance on heavy atoms today, and
    (iii) it does balance once the acceptor is inserted.

Condition (i) is not bookkeeping. MetaNetX ALIASES one physical polymer under several
flattened accessions -- `Glycogen`, `Branching glycogen`, `1,4-alpha-D-glucan`, `Amylose`
-- and writes reactions between two of them: `MNXR157776` is `1,4-alpha-D-glucan ->
Glycogen`, a hexamer becoming a tetramer. Their residual is an artifact of the aliasing,
not a chemical deficit, and substituting BOTH sides balances it perfectly and returns a
confident dG'0 near zero: a tier-2 claim of reversibility on a reaction that does not
exist. Balance is exactly as worthless as evidence here as it is for a carrier.

Conditions (ii) and (iii) are computed from the chem_prop `formula` column and NOT from
RDKit, both because heavy-atom counting needs no molecular graph and because this predicate
has to run wherever the pipeline runs. Hydrogen is excluded: protonation state is the one
thing the ledger cannot settle, and demanding it would refuse the ADP-glucose rows over
MetaNetX's own charge conventions rather than over any chemistry.
"""
from __future__ import annotations

import math
import re
from io import StringIO
from pathlib import Path

import pandas as pd

from . import canon
from .thermo_dgbyg import _has_wildcard

MODEL_PREFIX = "MODEL:"

MODEL_COLUMNS = ("model_key", "name", "smiles", "inchi", "inchikey", "basis")
# Optional. A model row naming another model key declares itself an ALTERNATIVE to it --
# a different compound a curator could defensibly have chosen for the same role. `anchor`
# scores the anchor reaction under each and writes the spread back as the row's
# `congeners`, which is how an asserted structure acquires a width instead of implying
# none. Optional rather than required so a table can be authored before it is priced.
# `source_mnxm` is the MetaNetX accession the structure was copied from, verbatim and by
# machine. It is not used for anything -- the members read the declared structure, never
# the accession -- and it is here so a reader can re-derive the row rather than trust a
# hand-transcribed 300-character InChI, which is the transcription error the stale-id
# tripwire exists to catch one level up.
# `formula` is required of a model compound a POLYMER row names and ignored otherwise: the
# polymer scope decides whether an equation balances, and it decides it over the model
# compounds it inserted. Copied by machine from chem_prop like every other structure here,
# and cross-checked against `source_mnxm` when both are declared.
MODEL_OPTIONAL = ("congener_of", "source_mnxm", "formula")

# THE WIDTH AND THE GATE ARE PER MEMBER, AND THE COLUMN NAMES SAY WHICH.
#
# A stand-in structure is an assertion, and how wrong it is depends on WHO IS READING IT.
# eQuilibrator places the FMN model pair 0.45 kJ/mol from the potentials this table cites;
# dGbyG places it 9.60 away -- the same offset to five decimals across four different
# anchors with four different siblings, so it is a systematic property of the model pair
# and not anchor noise. One number cannot describe both members, and the one that used to
# be written described eQuilibrator while being read as though it described dGbyG too.
#
# So there is no member-agnostic `congeners` column any more. Each member gets its own,
# and `load` REQUIRES a member -- a table scored by one member and read by another is a
# refusal rather than a silent reuse, because that silent reuse is exactly the defect
# these columns replace.
MEMBERS = ("eq", "dgbyg")
MEMBER_COLUMNS = tuple(f"{c}_{m}" for m in MEMBERS for c in ("congeners", "gap"))
ROW_COLUMNS = ("kind", "mnxm", "mnx_name", "terms", "congener_terms", "couple_id", "state",
               "e0_V", "e0_model_V", "n_e", "n_h", "anchor_mnxr", "sibling_mnxr",
               "sibling_e0_V") + MEMBER_COLUMNS + ("basis",)

# `carrier`  a redox couple: both states substituted, no heavy atoms transferred, and the
#            model's potential within a decade of the real carrier's.
# `polymer`  a chain increment: the acceptor is inserted on the opposite side.
# `thioester` an acyl carrier. NOT a redox couple -- the acyl group genuinely transits, so
#            the model must carry it too, and the whole family must share one backbone or
#            the anchor will not balance. There is no potential to declare, so the couple
#            and potential gates do not apply and the ANCHOR is the entire safety argument.
#            In scope by the principal's decision, against the recommendation to exclude.
KINDS = ("carrier", "polymer", "thioester")

# Kinds whose anchor predicts a ZERO offset instead of one computed from two potentials.
# A polymer row's anchor and sibling are one glucosyl moving between the same two partners
# at two chain lengths. A thioester row's are the same acyl transfer written on two
# carriers that share the 4'-phosphopantetheine thiol -- the only part of either carrier
# the acyl group is bonded to, and the part the model compound reproduces exactly. Neither
# has a potential to declare, so ZERO IS THE PREDICTION rather than the absence of one, and
# a row is refused unless the restaged equation lands on the number the deployed bake
# already holds for the transformation, computed from different accessions.
ZERO_OFFSET_KINDS = ("polymer", "thioester")

# Faraday constant, kJ/(mol*V). Converts a declared couple potential difference into the
# same units the members and `canon.DIR_DECADE` are in.
FARADAY = 96.485


class Refused(SystemExit):
    pass


def read_table(path: Path) -> pd.DataFrame:
    kept = [ln for ln in Path(path).read_text().splitlines()
            if not ln.lstrip().startswith("#")]
    return pd.read_csv(StringIO("\n".join(kept)), sep="\t")


def _cited(value) -> bool:
    return not pd.isna(value) and bool(str(value).strip())


def _parse_terms(spec: str, row_id: str) -> list[tuple[str, float]]:
    # `'1*MODEL:x;-1*MODEL:y'` -> [('MODEL:x', 1.0), ('MODEL:y', -1.0)].
    out = []
    for term in str(spec).split(";"):
        term = term.strip()
        if not term:
            continue
        if "*" not in term:
            raise Refused(f"[substitute] {row_id}: term {term!r} is not '<coeff>*<key>'")
        coeff, key = term.split("*", 1)
        try:
            out.append((key.strip(), float(coeff)))
        except ValueError:
            raise Refused(f"[substitute] {row_id}: term {term!r} has a non-numeric coeff")
    if not out:
        raise Refused(f"[substitute] {row_id}: no terms")
    return out


# The elements MetaNetX writes in `formula`. An unknown symbol is the point: a formula
# carrying `R` or `X` is a residue placeholder, `R` would otherwise parse as an element,
# and the reaction it appears in must be left alone rather than balanced against a
# fiction. Same instinct as the wildcard guard one layer up.
ELEMENTS = frozenset("""
H He Li Be B C N O F Ne Na Mg Al Si P S Cl Ar K Ca Sc Ti V Cr Mn Fe Co Ni Cu Zn Ga Ge As
Se Br Kr Rb Sr Y Zr Nb Mo Tc Ru Rh Pd Ag Cd In Sn Sb Te I Xe Cs Ba La Ce Pr Nd Pm Sm Eu
Gd Tb Dy Ho Er Tm Yb Lu Hf Ta W Re Os Ir Pt Au Hg Tl Pb Bi Po At Rn Fr Ra Ac Th Pa U
""".split())

_FORMULA_TOKEN = re.compile(r"([A-Z][a-z]?)(\d*)")


def heavy_formula(formula) -> dict[str, int] | None:
    f = str(formula or "").strip()
    if not f:
        return None
    f = re.sub(r"\(\d*[-+]\)$", "", f)              # a trailing charge, e.g. `HO4P(2-)`
    out: dict[str, int] = {}
    pos = 0
    for m in _FORMULA_TOKEN.finditer(f):
        if m.start() != pos:                        # a gap means an unparseable character
            return None
        pos = m.end()
        el, n = m.group(1), m.group(2)
        if el not in ELEMENTS:
            return None
        out[el] = out.get(el, 0) + (int(n) if n else 1)
    if pos != len(f) or not out:
        return None
    out.pop("H", None)
    return out


def _apply(stoich: dict[str, float], rows: dict) -> dict[str, float]:
    out: dict[str, float] = {}
    for mnxm, coeff in stoich.items():
        row = rows.get(mnxm)
        if row is None:
            out[mnxm] = out.get(mnxm, 0.0) + coeff
            continue
        for key, mult in row["terms"]:
            out[key] = out.get(key, 0.0) + coeff * mult
    return {k: v for k, v in out.items() if abs(v) > 1e-12}


class Substitutions:
    # Loaded tables, or nothing at all. `Substitutions()` covers no reaction.
    #
    # The empty instance is the configuration every baseline was taken under and the one
    # that must leave the pipeline bit-identical, so it is the default rather than a mode.

    def __init__(self, models: dict | None = None, rows: pd.DataFrame | None = None,
                 by_mnxm: dict | None = None, formulas: dict | None = None):
        self.models = models or {}
        self.rows = rows if rows is not None else pd.DataFrame(columns=list(ROW_COLUMNS))
        self._by_mnxm = by_mnxm or {}
        self.formulas = formulas or {}
        # WHICH MEMBER THIS SET WAS ADMITTED FOR. `None` on the empty instance, which
        # covers nothing and so cannot disagree with anyone; set by `load`. Everything
        # downstream -- the rewrite, the width, the props extension -- is this member's,
        # which is why the member is fixed once here rather than threaded through each
        # call and forgotten at one of them.
        self.member = None
        # Refusals ship beside accepts, so a table's silence about a compound is a
        # recorded decision rather than an absence somebody has to reconstruct.
        self.decisions = pd.DataFrame(columns=list(DECISION_COLS))

    def __len__(self) -> int:
        return len(self._by_mnxm)

    # -- the two things the pipeline asks for -----------------------------

    def props(self, base: dict) -> dict:
        clash = set(base) & set(self.models)
        if clash:
            raise Refused(f"[substitute] model ids collide with MetaNetX accessions: "
                          f"{sorted(clash)[:5]}")
        out = dict(base)
        for key, m in self.models.items():
            rec = {}
            for field in ("inchi", "inchikey", "smiles"):
                if _cited(m.get(field)):
                    rec[field] = str(m[field])
            out[key] = rec
        return out

    def _heavy(self, key: str) -> dict[str, int] | None:
        if key.startswith(MODEL_PREFIX):
            return heavy_formula((self.models.get(key) or {}).get("formula"))
        return heavy_formula(self.formulas.get(key))

    def _residual(self, stoich: dict[str, float]) -> dict[str, float] | None:
        res: dict[str, float] = {}
        for key, coeff in stoich.items():
            counts = self._heavy(key)
            if counts is None:
                return None
            for el, n in counts.items():
                res[el] = res.get(el, 0.0) + coeff * n
        return {el: v for el, v in res.items() if abs(v) > 1e-9}

    def _applied(self, stoich: dict[str, float]) -> dict:
        hit = {m: self._by_mnxm[m] for m in stoich if m in self._by_mnxm}
        poly = {m: r for m, r in hit.items() if str(r["kind"]) == "polymer"}
        if not poly:
            return hit
        plain = {m: r for m, r in hit.items() if m not in poly}
        if len(poly) != 1:                      # (i) two aliases of one polymer
            return plain
        base = _apply(stoich, plain) if plain else stoich
        before = self._residual(base)
        if not before:                          # (ii) unknowable, or already balanced
            return plain
        if self._residual(_apply(base, poly)) != {}:      # (iii) the insert must close it
            return plain
        return hit

    def rewrite(self, stoich: dict[str, float]) -> dict[str, float]:
        if not self._by_mnxm or not (set(stoich) & set(self._by_mnxm)):
            return stoich
        use = self._applied(stoich)
        if not use:
            return stoich
        return _apply(stoich, use)

    def covers(self, stoich: dict[str, float]) -> bool:
        if not self._by_mnxm or not (set(stoich) & set(self._by_mnxm)):
            return False
        return bool(self._applied(stoich))

    def balances(self, stoich: dict[str, float]) -> bool | None:
        # Does this equation close on heavy atoms? None when it cannot be decided, which
        # is not the same as False -- a participant with no formula leaves the question
        # open, and reporting that as "unbalanced" would blame the equation for a gap in
        # the reference.
        #
        # PUBLIC BECAUSE THE QUOTIENT LANE NEEDS THE VERDICT ON THE RESTAGED EQUATION.
        # `reac_prop`'s own is_balanced describes the form MetaNetX wrote, and a polymer
        # reaction that only closes once the substitution has put the acceptor on both
        # sides is exactly the case where the two disagree.
        res = self._residual(stoich)
        return None if res is None else (res == {})

    def sigma_sub(self, stoich: dict[str, float]) -> float:
        # Congener spread over the participants this reaction substituted, in quadrature.
        #
        # CARRIED SEPARATELY AND FOLDED IN LATER, never added to the member's own sigma.
        # `combine.eq_vote` detects an eQuilibrator group cancellation by testing sigma
        # against the floor; inflating the member's sigma lifts a cancelling zero over that
        # floor and re-promotes it to tier 1 -- precisely the defect r8 was baked to remove.
        #
        # Over the rows that ACTUALLY acted, so a polymer row the scope declined contributes
        # no width to a reaction it did not touch.
        if not self._by_mnxm or not (set(stoich) & set(self._by_mnxm)):
            return 0.0
        var = 0.0
        for row in self._applied(stoich).values():
            var += float(row["sigma_sub"]) ** 2
        return math.sqrt(var)


# =====================================================================
# admission
# =====================================================================

def _admit_models(df: pd.DataFrame, formulas: dict | None = None) -> dict:
    missing = set(MODEL_COLUMNS) - set(df.columns)
    if missing:
        raise Refused(f"[substitute] models table lacks columns: {sorted(missing)}")
    out = {}
    for r in df.itertuples(index=False):
        key = str(r.model_key)
        if not key.startswith(MODEL_PREFIX):
            raise Refused(f"[substitute] model {key!r} must start with {MODEL_PREFIX!r} "
                          f"-- the synthetic namespace is what keeps MetaNetX untouched")
        if key in out:
            raise Refused(f"[substitute] model {key!r} declared twice")
        if not _cited(r.smiles):
            raise Refused(f"[substitute] model {key}: no SMILES")
        # THE MEMBER'S OWN PREDICATE, not a copy of it, so the two cannot drift: a model
        # compound carrying a wildcard would be admitted here and then abstained on by
        # dGbyG, which is the silence this lane exists to remove.
        wc = _has_wildcard(str(r.smiles))
        if wc is None:
            raise Refused(f"[substitute] model {key}: SMILES {r.smiles!r} does not parse")
        if wc:
            raise Refused(f"[substitute] model {key}: SMILES {r.smiles!r} carries a "
                          f"wildcard. A model compound must be a concrete molecule -- "
                          f"this lane admits no `*` at all")
        if not _cited(r.basis):
            raise Refused(f"[substitute] model {key}: no basis. A curated row cannot be "
                          f"verified against anything -- the citation IS its evidence")
        rec = {c: getattr(r, c) for c in MODEL_COLUMNS}
        for c in MODEL_OPTIONAL:
            rec[c] = getattr(r, c, "")
        out[key] = rec
    for key, rec in out.items():
        parent = rec.get("congener_of")
        if _cited(parent) and str(parent) not in out:
            raise Refused(f"[substitute] model {key}: congener_of {parent!r} is not a "
                          f"declared model")
        # THE TRANSCRIPTION TRIPWIRE, and the reason `source_mnxm` is worth carrying: a
        # declared formula that disagrees with the accession it was copied from means the
        # row was hand-edited after the copy, which is exactly the error the stale-id gate
        # catches one level up for names.
        src = str(rec.get("source_mnxm") or "").strip()
        if _cited(rec.get("formula")) and formulas and src in formulas:
            want, have = heavy_formula(formulas[src]), heavy_formula(rec["formula"])
            if want != have:
                raise Refused(
                    f"[substitute] model {key}: formula {rec['formula']!r} disagrees with "
                    f"{src}'s {formulas[src]!r} in chem_prop. Copy it by machine")
    return out


def _gate_replaceable(mnxm: str, props: dict, row_id: str) -> None:
    # A CARRIER row may only displace a participant the member cannot use TODAY.
    #
    # Tested with `thermo_dgbyg._has_wildcard` itself rather than a reimplementation, so the
    # admission predicate cannot drift from the abstention it is meant to be undoing. A row
    # that displaces a usable participant is not a substitution, it is an override of
    # MetaNetX chemistry, and nothing here reviewed that.
    #
    # CARRIER AND THIOESTER ONLY, and the scoping is load-bearing rather than tidy. A
    # flattened polymer carries a perfectly readable wildcard-free SMILES -- glycogen's is
    # maltotetraose's -- so this gate refuses every polymer row by construction, and a
    # polymer row is not making this claim in the first place. What blocks the member there
    # is not an unreadable participant but a MISSING one, and `_gate_polymer` plus the
    # per-reaction scope in `Substitutions._applied` is where a polymer row earns its place.
    p = props.get(mnxm) or {}
    smi = p.get("smiles")
    if not smi:
        return                                    # no structure: the member abstains today
    wc = _has_wildcard(str(smi))
    if wc is None or wc:
        return                                    # unparseable or `*`: abstains today
    raise Refused(f"[substitute] {row_id}: {mnxm} already carries a usable structure "
                  f"({smi!r}). A row may only displace a participant the member cannot "
                  f"read; overriding one is a different and far larger claim")


def _gate_polymer(mnxm: str, terms: list, models: dict, formulas: dict | None,
                  row_id: str) -> None:
    if not formulas:
        raise Refused(
            f"[substitute] {row_id}: a polymer row needs chem_prop formulas and none were "
            f"supplied. Pass `formulas=load_mnxm_formulas(chem_prop)` to `load`")
    have = heavy_formula(formulas.get(mnxm))
    if have is None:
        raise Refused(f"[substitute] {row_id}: chem_prop gives {mnxm} formula "
                      f"{formulas.get(mnxm)!r}, which is not a heavy-atom composition")
    net: dict[str, float] = {}
    for key, mult in terms:
        counts = heavy_formula((models.get(key) or {}).get("formula"))
        if counts is None:
            raise Refused(
                f"[substitute] {row_id}: model {key} declares no usable `formula`. A "
                f"polymer row is admitted on whether its insert BALANCES a reaction, and "
                f"that question cannot be asked of a compound with no composition")
        for el, n in counts.items():
            net[el] = net.get(el, 0.0) + mult * n
    delta = {el: net.get(el, 0.0) - have.get(el, 0)
             for el in set(net) | set(have)}
    if not any(abs(v) > 1e-9 for v in delta.values()):
        raise Refused(
            f"[substitute] {row_id}: the terms sum to {mnxm}'s own composition, so this "
            f"row inserts nothing. A polymer row exists to supply the acceptor MetaNetX "
            f"left out; one that changes no count can never balance anything")


def _gate_couple(rows: pd.DataFrame, models: dict) -> None:
    carriers = rows[rows["kind"] == "carrier"]
    for couple_id, g in carriers.groupby("couple_id"):
        states = sorted(str(s) for s in g["state"])
        if states != ["ox", "red"]:
            raise Refused(f"[substitute] couple {couple_id!r}: states {states}, expected "
                          f"exactly ['ox', 'red']")
        heavy = {}
        for r in g.itertuples(index=False):
            keys = [k for k, _ in r.terms]
            if len(keys) != 1:
                raise Refused(f"[substitute] couple {couple_id!r}: a carrier row "
                              f"substitutes one compound, got {len(keys)} terms")
            heavy[str(r.state)] = _heavy_counts(models[keys[0]]["smiles"])
        n_h = int(g["n_h"].iloc[0])
        delta = {el: heavy["red"].get(el, 0) - heavy["ox"].get(el, 0)
                 for el in set(heavy["red"]) | set(heavy["ox"])}
        # Hydrogen is not a heavy atom, so a pure electron carrier's two states differ in
        # NO heavy element. Anything else means the couple carries atoms through, and a
        # fixed model pair cannot represent that.
        nonzero = {el: d for el, d in delta.items() if d}
        if nonzero:
            raise Refused(
                f"[substitute] couple {couple_id!r}: oxidised and reduced model compounds "
                f"differ by heavy atoms {nonzero}, declaring n_h={n_h}. A redox couple "
                f"that transfers heavy atoms is not a lookup -- bring it back rather than "
                f"widening this gate")


def _gate_potential(rows: pd.DataFrame) -> dict:
    out = {}
    for r in rows[rows["kind"] == "carrier"].itertuples(index=False):
        for col, val in (("e0_V", r.e0_V), ("e0_model_V", r.e0_model_V)):
            if not _cited(val):
                raise Refused(
                    f"[substitute] couple {r.couple_id!r}: {r.mnxm} declares no {col}. A "
                    f"generic with no tabulated potential cannot be substituted -- there "
                    f"is no number to be right about")
        n_e = int(r.n_e)
        gap = abs(FARADAY * n_e * (float(r.e0_V) - float(r.e0_model_V)))
        if gap > canon.DIR_DECADE:
            raise Refused(
                f"[substitute] couple {r.couple_id!r}: {r.mnxm} sits at {float(r.e0_V)} V "
                f"and its model at {float(r.e0_model_V)} V, a {gap:.2f} kJ/mol gap past "
                f"DIR_DECADE ({canon.DIR_DECADE:.2f}). One decade of conductance is the "
                f"whole quantity being estimated")
        out[str(r.mnxm)] = gap
    return out


def _heavy_counts(smiles: str) -> dict[str, int]:
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    mol = Chem.MolFromSmiles(str(smiles))
    if mol is None:
        raise Refused(f"[substitute] SMILES {smiles!r} does not parse")
    out: dict[str, int] = {}
    for a in mol.GetAtoms():
        if a.GetSymbol() != "H":
            out[a.GetSymbol()] = out.get(a.GetSymbol(), 0) + 1
    return out


DECISION_COLS = ("kind", "mnxm", "mnx_name", "couple_id", "verdict", "predicate", "detail")

# The order a reader gets their reason in. First failure wins, so the ladder runs from
# "this row is not about what you think" to "this row's chemistry is wrong" -- a stale id
# reported as a potential-gate failure would send a curator to the wrong question.
ROW_PREDICATES = ("kind_known", "consistent_repeat", "stale_id", "cited", "anchored",
                  "replaceable_only", "terms_parse", "models_declared", "polymer_increment",
                  "member_unscored", "member_drift", "congener_spread")
COUPLE_PREDICATES = ("couple_complete", "no_heavy_transfer", "potential_declared",
                     "potential_within_decade")

# NON-FATAL, AND THE DISTINCTION IS THE WHOLE POINT OF THIS SET.
#
# Every other predicate is a defect in the table: a stale id, an unparseable term, a row
# with no basis. Those abort, because a table that half-loads is worse than one that
# refuses and the curator has to fix them before anything runs.
#
# `member_drift` is not a defect. It is the mechanism WORKING: this member cannot place
# this stand-in, so it does not get the substitution, and the other member still does. The
# load continues with the reduced set and the ensemble loses one vote rather than the
# build. Aborting here would mean one member's disagreement could stop the other member's
# bake, which is the opposite of what an independently-abstaining ensemble is for.
#
# `member_unscored` stays FATAL: an arm nobody has run the anchor for is an absence of
# evidence, not evidence of absence, and admitting it silently is exactly the "asserted
# structure nothing checked" failure the anchor gate exists to prevent.
NONFATAL_PREDICATES = ("member_drift",)


def _check_row(r, props: dict, names: dict, seen: dict, models: dict,
               formulas: dict | None = None, member: str = "eq"):
    row_id = f"{r.kind}/{r.mnxm}"
    if str(r.kind) not in KINDS:
        return "kind_known", f"[substitute] {row_id}: kind must be one of {KINDS}"
    prior = seen.get(str(r.mnxm))
    if prior is not None and str(prior).strip() != str(r.terms).strip():
        return "consistent_repeat", (
            f"[substitute] {r.mnxm} substituted twice with different terms: {prior!r} then "
            f"{str(r.terms)!r}. One compound rewrites one way or the rewrite is ambiguous")
    # STALE-ID TRIPWIRE. Not name-as-proof -- the check that the id the curator reasoned
    # about is the id they wrote down.
    have = str(names.get(str(r.mnxm), "") or "").strip()
    if have != str(r.mnx_name).strip():
        return "stale_id", (
            f"[substitute] {r.mnxm}: row says {str(r.mnx_name)!r}, chem_prop says "
            f"{have!r}. The id is the key -- a mismatch means the row is about a "
            f"different compound than the curator reasoned about")
    if not _cited(r.basis):
        return "cited", f"[substitute] {row_id}: no basis"
    if not _cited(r.anchor_mnxr):
        return "anchored", (
            f"[substitute] {row_id}: no anchor_mnxr. Balance is necessary and worthless "
            f"as evidence here -- a row is admitted because a reaction a member already "
            f"scored still scores the same under the model compound, not because the "
            f"atoms add up")
    if str(r.kind) != "polymer":
        try:
            _gate_replaceable(str(r.mnxm), props, row_id)
        except Refused as e:
            return "replaceable_only", str(e)
    try:
        terms = _parse_terms(r.terms, row_id)
    except Refused as e:
        return "terms_parse", str(e)
    for key, _ in terms:
        if key not in models:
            return "models_declared", (f"[substitute] {row_id}: term names {key!r}, "
                                       f"which models.tsv does not declare")
    if str(r.kind) == "polymer":
        try:
            _gate_polymer(str(r.mnxm), terms, models, formulas, row_id)
        except Refused as e:
            return "polymer_increment", str(e)
    drift = _gate_member_drift(r, row_id, member)
    if drift is not None:
        return drift
    try:
        sigma_sub = _congener_spread(r, models, row_id, member)
    except Refused as e:
        return "congener_spread", str(e)
    rec = {c: getattr(r, c) for c in ROW_COLUMNS}
    rec["terms"] = terms
    rec["sigma_sub"] = sigma_sub
    return None, rec


def _check_couples(frame: pd.DataFrame, models: dict):
    for default, run in (("couple_complete", lambda: _gate_couple(frame, models)),
                         ("potential_declared", lambda: _gate_potential(frame))):
        try:
            run()
        except Refused as e:
            msg = str(e)
            if "differ by heavy atoms" in msg:
                return "no_heavy_transfer", msg
            if "past DIR_DECADE" in msg:
                return "potential_within_decade", msg
            return default, msg
    return None, None


def load(directory: Path | None, props: dict, names: dict,
         *, formulas: dict | None = None, collect: bool = False,
         member: str | None = None) -> Substitutions:
    if directory is None:
        return Substitutions()
    if member is None:
        raise Refused(
            "[substitute] load() needs a member: the admitted set is per member, and a "
            "table scored by one member and read by another is the defect these columns "
            f"replace. Pass member= one of {MEMBERS + ('any',)}")
    if member not in MEMBERS and member != "any":
        raise Refused(f"[substitute] unknown member {member!r}; "
                      f"expected one of {MEMBERS + ('any',)}")
    directory = Path(directory)
    models = _admit_models(read_table(directory / "models.tsv"), formulas)
    df = read_table(directory / "substitutions.tsv")

    missing = set(ROW_COLUMNS) - set(df.columns)
    if missing:
        raise Refused(f"[substitute] substitutions table lacks columns: {sorted(missing)}")

    parsed, seen, decisions = [], {}, []
    for r in df.itertuples(index=False):
        pred, payload = _check_row(r, props, names, seen, models, formulas, member)
        base = dict(kind=r.kind, mnxm=r.mnxm, mnx_name=r.mnx_name, couple_id=r.couple_id)
        if pred is not None:
            if not collect and pred not in NONFATAL_PREDICATES:
                raise Refused(payload)
            decisions.append(dict(base, verdict="refused", predicate=pred, detail=payload))
            continue
        decisions.append(dict(base, verdict="admitted", predicate="", detail=""))
        parsed.append(payload)
        seen[str(r.mnxm)] = str(r.terms)

    # PER COUPLE, not over the whole table. A couple-level failure condemns its own two
    # rows; condemning every admitted row would let one bad couple empty a table of twelve,
    # and a curator would then be fixing the couple the ledger happened to name first.
    frame = pd.DataFrame(parsed) if parsed else pd.DataFrame(columns=list(ROW_COLUMNS))
    for couple_id in sorted({str(p["couple_id"]) for p in parsed}):
        g = frame[frame["couple_id"].astype(str) == couple_id]
        pred, detail = _check_couples(g, models)
        if pred is None:
            continue
        if not collect:
            raise Refused(detail)
        for d in decisions:
            if d["verdict"] == "admitted" and str(d["couple_id"]) == couple_id:
                d.update(verdict="refused", predicate=pred, detail=detail)
        parsed = [p for p in parsed if str(p["couple_id"]) != couple_id]
        frame = frame[frame["couple_id"].astype(str) != couple_id]

    by_mnxm = {str(p["mnxm"]): p for p in parsed}
    out = Substitutions(models, frame, by_mnxm, formulas)
    out.member = member
    out.decisions = pd.DataFrame(decisions, columns=list(DECISION_COLS))
    # SAID OUT LOUD, because a non-fatal refusal narrows what this member is given and a
    # silent narrowing is indistinguishable from a table that was never written. The count
    # belongs in the run log beside the member's own tally, so a bake can be read back and
    # asked why an arm covered what it covered.
    dropped = [d for d in decisions if d["predicate"] in NONFATAL_PREDICATES]
    if dropped and not collect:
        couples = sorted({str(d["couple_id"]) for d in dropped})
        print(f"[substitute:{member}] {len(dropped)} row(s) across {len(couples)} couple(s) "
              f"refused for this member and kept for the other: {', '.join(couples)}",
              flush=True)
    return out


def _congener_spread(r, models: dict, row_id: str, member: str) -> float:
    # How much THIS MEMBER's answer moves across the declared alternatives, as a sigma.
    #
    # Carried as `sigma_sub` rather than discarded, because the choice of model compound is
    # an assertion with a width and reporting it as zero would make an asserted structure
    # look like a measurement. Filled by `substitute anchor --member`, which scores each and
    # writes the spread back; an unscored row declares 0.0 and the anchor verb says so.
    #
    # THE REFUSAL LIVES IN `_gate_member_drift`, NOT HERE. This column mixes two things --
    # how far apart the alternative stand-ins are (a property of the curation) and how far
    # this member sits from the potentials the row cites (a property of the member). Refusing
    # on the mixture is what let eQuilibrator's 0.45 speak for dGbyG's 9.60: the conflated
    # number passed a gate the member-specific one fails. Width is reported here; admission
    # is decided there.
    if member == "any":
        return 0.0
    raw = getattr(r, f"congeners_{member}", None)
    if not _cited(raw):
        return 0.0
    values = []
    for part in str(raw).split(";"):
        part = part.strip()
        if not part:
            continue
        try:
            values.append(float(part))
        except ValueError:
            raise Refused(f"[substitute] {row_id}: congeners_{member} entry {part!r} is "
                          f"not a kJ/mol number. Run `substitute anchor --member {member}`")
    if not values:
        return 0.0
    return (max(values) - min(values)) / 2.0


def _gate_member_drift(r, row_id: str, member: str):
    # `any` is the union: which compounds exist at all, for a caller that is not asking
    # anyone to score them. `resolve` uses it -- eQuilibrator's cache does not care which
    # member will eventually read the structure it looks up.
    if member == "any":
        return None
    raw = getattr(r, f"gap_{member}", None)
    if not _cited(raw):
        return ("member_unscored",
                f"[substitute] {row_id}: gap_{member} is empty. The {member} arm of this "
                f"row has never been scored, and admitting it would assert a structure "
                f"this member has not been checked against. Run "
                f"`substitute anchor --member {member}` and write the gap back")
    try:
        gap = abs(float(raw))
    except ValueError:
        return ("member_unscored",
                f"[substitute] {row_id}: gap_{member} {raw!r} is not a kJ/mol number")
    if gap > canon.DIR_DECADE:
        return ("member_drift",
                f"[substitute] {row_id}: {member} places this model couple {gap:.2f} "
                f"kJ/mol from the potentials the row cites, past DIR_DECADE "
                f"({canon.DIR_DECADE:.2f}). REFUSED FOR {member} ONLY -- the other member "
                f"keeps the row. Widening this gate would ship a confident direction "
                f"displaced by {gap / canon.DIR_DECADE:.2f} decades")
    return None


# =====================================================================
# the command line
# =====================================================================

def _tables(args):
    from .refdata import load_mnxm_formulas, load_mnxm_names, load_mnxm_props
    props = load_mnxm_props(args.chem_prop)
    names = load_mnxm_names(args.chem_prop)
    formulas = load_mnxm_formulas(args.chem_prop)
    return props, names, formulas


def cmd_check(args):
    # Every predicate on every row, and what the admitted rows would reach.
    #
    # Reports two numbers a curator cannot get from the table itself: how many reactions the
    # admitted rows actually unblock, and -- the answer to "this is just the ones somebody
    # happened to look at" -- which accessions carry a name already admitted under some other
    # id and are NOT in the table. Under-coverage becomes a printed number instead of an
    # absence, the way `twins.alias_index` answers the same objection.
    from .refdata import load_mnxr_stoich
    props, names, formulas = _tables(args)
    # ONE PASS PER MEMBER. The admitted set differs between them, so a single verdict
    # column would have to pick a member to be about and would then read as though it were
    # about both -- which is the defect the per-member columns exist to close.
    per_member = {m: load(args.tables, props, names, formulas=formulas, collect=True,
                          member=m) for m in MEMBERS}
    for m in MEMBERS:
        dm = per_member[m].decisions
        bad_m = int((dm["verdict"] == "refused").sum())
        print(f"[substitute:{m}] {len(dm):,} rows · {len(dm) - bad_m:,} admitted · "
              f"{bad_m:,} refused")
        for pred in ROW_PREDICATES + COUPLE_PREDICATES:
            n = int((dm["predicate"] == pred).sum())
            if n:
                print(f"    {pred:<26} {n:>5,}")
                for detail in dm.loc[dm["predicate"] == pred, "detail"].head(3):
                    print(f"        {detail}")

    d = pd.concat([per_member[m].decisions.assign(member=m) for m in MEMBERS],
                  ignore_index=True)
    n_bad = int((d["verdict"] == "refused").sum())

    reached = 0
    if args.reac_prop:
        stoich = load_mnxr_stoich(args.reac_prop)
        for m in MEMBERS:
            n = sum(1 for _, (st, _b, _t) in stoich.items() if per_member[m].covers(st))
            print(f"[substitute:{m}] admitted rows touch {n:,} of {len(stoich):,} "
                  f"reactions")
            reached = max(reached, n)

    # The under-coverage report. An accession whose name normalises to one already admitted
    # is a compound the curator's own reasoning covers and their table does not.
    # RESTRICTED TO COMPOUNDS THE MEMBER CANNOT READ TODAY. `_norm` strips parentheses and
    # charges, so `NAD(P)` and `NADP(+)` collide -- without this the report tells a curator
    # to substitute real NADP+, which `_gate_replaceable` would then refuse. Only an
    # accession the member is actually blocked on is a miss.
    def blocked(m):
        smi = (props.get(m) or {}).get("smiles")
        if not smi:
            return True
        wc = _has_wildcard(str(smi))
        return wc is None or bool(wc)

    admitted = {str(m) for m in d.loc[d["verdict"] == "admitted", "mnxm"]}
    want = {_norm(names.get(m, "")) for m in admitted} - {""}
    missed = sorted(m for m, n in names.items()
                    if m not in admitted and _norm(n) in want and blocked(m))
    print(f"[substitute] {len(missed):,} accessions share an admitted name and are NOT in "
          f"the table" + (f": {missed[:10]}" if missed else ""))

    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        d.to_csv(out / "decisions.tsv", sep="\t", index=False)
        pd.DataFrame([dict(rows=len(d), admitted=len(d) - n_bad, refused=n_bad,
                           reactions_reached=reached, name_missed=len(missed))]) \
            .to_csv(out / "summary.tsv", sep="\t", index=False)
        pd.DataFrame([dict(mnxm=m, name=names.get(m, "")) for m in missed]) \
            .to_csv(out / "name_missed.tsv", sep="\t", index=False)
        print(f"[substitute] -> {out}")
    return 1 if n_bad else 0


def _norm(name) -> str:
    import re as _re
    return _re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def predicted_offset(n_e: float, c_red: float, e0_model_V: float,
                     e0_sibling_V: float) -> float:
    return -float(c_red) * float(n_e) * FARADAY * (float(e0_model_V) - float(e0_sibling_V))


def cmd_anchor(args):
    # Score each row's anchor under the model couple and check it against a sibling.
    #
    # THE ANSWER TO "how do you refuse a substitution that balances but is thermodynamically
    # unjustified". Balance is necessary and worthless as evidence here: `[Fe+3]`/`[Fe+2]`
    # balances ferredoxin perfectly and returns a confident wrong number.
    #
    # THE TEST IS A DIFFERENCE AGAINST A PREDICTED NUMBER, not a drift toward zero, and it
    # has to be: MetaNetX writes almost no generic-carrier reaction a second time with the
    # same family of carrier concretely. It writes it with a DIFFERENT one -- the flavin
    # monooxygenases appear again as their overall NADPH reactions -- so an anchor and its
    # sibling differ by a real quantity rather than by nothing. `predicted_offset` computes
    # that quantity from the two declared potentials, and the row survives only if the member,
    # running on the substituted equation, lands within one decade of it.
    #
    # That makes `e0_V` falsifiable instead of decorative: a carrier row asserting the wrong
    # potential predicts the wrong offset and is refused here rather than shipping a
    # confident number nothing checked.
    #
    # BOTH ARMS ARE RUNNABLE HERE. The dGbyG arm needs the IMAGE, not a conda env -- build it
    # with `TAGS=dgbyg docker/ecspr_bake/dev.sh --build` and run this verb inside it. The
    # long-standing note that it could not be run here named the wrong obstacle and outlived
    # its own justification.
    from .refdata import load_mnxr_stoich
    props, names, formulas = _tables(args)
    # `any`, NOT `args.member`. This verb EXISTS to produce `gap_<member>`, so loading for
    # that member would refuse every unscored row as `member_unscored` and the table could
    # never be scored a first time. Scoring is not admitting.
    subs = load(args.tables, props, names, formulas=formulas, member="any")
    stoich = load_mnxr_stoich(args.reac_prop)
    wide = subs.props(props)

    if args.member == "eq":
        from .thermo_eq import EquilibratorMember as M
    else:
        from .thermo_dgbyg import DgbygMember as M
    member = M()

    baseline = {}
    if args.member_table:
        mt = pd.read_parquet(args.member_table)
        baseline = dict(zip(mt["mnxr"].astype(str), mt["dg"]))

    def score(st):
        return member.dgr(subs.rewrite(st), wide)

    rows, bad = [], 0
    for rec in subs.rows.to_dict("records"):
        mnxm, mnxr = str(rec["mnxm"]), str(rec["anchor_mnxr"])
        out = dict(mnxm=mnxm, couple_id=rec["couple_id"], anchor=mnxr, member=args.member)
        s = stoich.get(mnxr)
        if s is None:
            rows.append(dict(out, verdict="no_stoich")); bad += 1; continue
        st = s[0]
        # A row whose anchor it does not cover tests nothing: `rewrite` is the identity
        # there and the comparison passes however wrong the model compound is. Asked of
        # the SCOPE and not of the equation's participant list, because a polymer row that
        # is present in a reaction the three conditions decline is exactly that case.
        if mnxm not in subs._applied(st):
            rows.append(dict(out, verdict="ANCHOR_UNCOVERED")); bad += 1; continue

        dg, sig, _flag, reason = score(st)
        out.update(model_dg=dg, sigma=sig, reason=reason)
        if dg is None:
            # The substitution bought nothing: the member is still silent on the very
            # reaction the row exists to unblock.
            rows.append(dict(out, verdict="member_silent")); bad += 1; continue

        # A ZERO-OFFSET KIND PREDICTS 0.0, and zero is a real prediction here rather than
        # the absence of one -- see ZERO_OFFSET_KINDS for why each kind earns it. The
        # polymer case rests on MetaNetX's own maltodextrin ladder, which demonstrates dG'0
        # chain-length invariant to six decimals across n = 5, 6, 7. No potential is
        # declared or wanted: neither a sugar nor a thioester has one.
        zero_offset = str(rec["kind"]) in ZERO_OFFSET_KINDS
        sib = str(rec.get("sibling_mnxr") or "")
        was = baseline.get(sib) if _cited(rec.get("sibling_mnxr")) else None
        if was is None or pd.isna(was) or not (zero_offset
                                              or _cited(rec.get("sibling_e0_V"))):
            rows.append(dict(out, verdict="no_sibling"))
        else:
            want = 0.0 if zero_offset else predicted_offset(
                rec["n_e"], st[mnxm] if str(rec["state"]) == "red" else -st[mnxm],
                rec["e0_model_V"], rec["sibling_e0_V"])
            gap = abs((dg - float(was)) - want)
            out.update(sibling=sib, sibling_dg=float(was), predicted=want, gap=gap)
            verdict = "ok" if gap <= canon.DIR_DECADE else "DRIFT"
            bad += verdict == "DRIFT"
            rows.append(dict(out, verdict=verdict))

        # The congener spread: the same anchor under each declared alternative couple. This
        # is what fills `congeners`, and it prices the curator's CHOICE rather than the
        # chemistry -- a spread past a decade means the model compound is not pinning the
        # answer and no single one of them should be asserted.
        #
        # SWAPPED A COUPLE AT A TIME, never a row at a time. Replacing NAD+ with NADP+ and
        # leaving the reduced half as NADH writes a reaction with a phosphate on one side
        # only; eQuilibrator scores it happily and the spread comes back as ~900 kJ/mol,
        # which reads as "this model compound pins nothing" when it means "the variant was
        # nonsense". `_cited` first, because `str(nan)` is 'nan' and would parse as a term.
        family = [r2 for r2 in subs.rows.to_dict("records")
                  if str(r2["couple_id"]) == str(rec["couple_id"])]
        depth = max((len(str(r2["congener_terms"]).split("|"))
                     if _cited(r2.get("congener_terms")) else 0) for r2 in family)
        vals = [dg]
        for i in range(depth):
            swapped = dict(subs._by_mnxm)
            for r2 in family:
                if not _cited(r2.get("congener_terms")):
                    continue
                alt = str(r2["congener_terms"]).split("|")
                if i >= len(alt):
                    continue
                key = str(r2["mnxm"])
                swapped[key] = dict(r2, terms=_parse_terms(alt[i], f"{key}/congener{i}"))
            v, _s, _f, _r = member.dgr(
                Substitutions(subs.models, subs.rows, swapped,
                              subs.formulas).rewrite(st), wide)
            vals.append(v)
        # THE TABULATED PREDICTION IS ONE OF THE CANDIDATES. A row's width is how far the
        # answer moves across everything it could defensibly have been, and a second model
        # compound is only one source of that -- the other is the member disagreeing with
        # the potentials the row cites. A couple with no second defensible model would
        # otherwise report a width of zero, which is the "asserted structure looks like a
        # measurement" failure `sigma_sub` exists to prevent.
        if rows[-1].get("predicted") is not None:
            vals.append(rows[-1]["sibling_dg"] + rows[-1]["predicted"])
        got = [v for v in vals if v is not None]
        if len(got) > 1:
            rows[-1]["congeners"] = ";".join(f"{v:.4f}" for v in got)
            rows[-1]["spread"] = max(got) - min(got)
        print(f"  {mnxm:<14} {mnxr:<12} {rows[-1]['verdict']:<17} "
              f"model {dg} vs sibling {rows[-1].get('sibling_dg')} "
              f"(predicted {rows[-1].get('predicted')}, gap {rows[-1].get('gap')})")

    df = pd.DataFrame(rows)
    # PASTE-READY AND NAMED FOR THE MEMBER THAT PRODUCED THEM. The defect this verb's
    # output feeds was exactly a number computed per member and then transcribed into a
    # column that did not say which member it came from. Emitting the destination column
    # names makes that transcription mechanical instead of a judgement call.
    if not df.empty:
        df[f"congeners_{args.member}"] = df["congeners"] if "congeners" in df else ""
        df[f"gap_{args.member}"] = df["gap"] if "gap" in df else ""
    if args.out:
        df.to_csv(args.out, sep="\t", index=False)
        print(f"[substitute] -> {args.out}")
    print(f"[substitute] {len(df):,} anchors · {bad:,} not ok "
          f"(DIR_DECADE = {canon.DIR_DECADE:.2f} kJ/mol)")
    print(f"[substitute] write these back as congeners_{args.member} and "
          f"gap_{args.member}; a row whose gap_{args.member} exceeds DIR_DECADE is "
          f"refused for {args.member} alone and kept for the other member")
    return 1 if bad else 0


def parse_args(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("check"); p.set_defaults(fn=cmd_check)
    p.add_argument("--tables", required=True)
    p.add_argument("--chem-prop", required=True)
    p.add_argument("--reac-prop", default=None)
    p.add_argument("--out", default=None)

    p = sub.add_parser("anchor"); p.set_defaults(fn=cmd_anchor)
    p.add_argument("--tables", required=True)
    p.add_argument("--chem-prop", required=True)
    p.add_argument("--reac-prop", required=True)
    p.add_argument("--member", required=True, choices=["eq", "dgbyg"])
    p.add_argument("--member-table", default=None,
                   help="the deployed member table the anchor's baseline dG is read from")
    p.add_argument("--out", default=None)
    return ap.parse_args(argv)


if __name__ == "__main__":
    import sys
    a = parse_args()
    sys.exit(a.fn(a))
