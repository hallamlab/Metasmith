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
"""
from __future__ import annotations

import math
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
MODEL_OPTIONAL = ("congener_of", "source_mnxm")
ROW_COLUMNS = ("kind", "mnxm", "mnx_name", "terms", "congener_terms", "couple_id", "state",
               "e0_V", "e0_model_V", "n_e", "n_h", "anchor_mnxr", "sibling_mnxr",
               "sibling_e0_V", "congeners", "basis")

# `carrier`  a redox couple: both states substituted, no heavy atoms transferred, and the
#            model's potential within a decade of the real carrier's.
# `polymer`  a chain increment: the acceptor is inserted on the opposite side.
# `thioester` an acyl carrier. NOT a redox couple -- the acyl group genuinely transits, so
#            the model must carry it too, and the whole family must share one backbone or
#            the anchor will not balance. There is no potential to declare, so the couple
#            and potential gates do not apply and the ANCHOR is the entire safety argument.
#            In scope by the principal's decision, against the recommendation to exclude.
KINDS = ("carrier", "polymer", "thioester")

# Faraday constant, kJ/(mol*V). Converts a declared couple potential difference into the
# same units the members and `canon.DIR_DECADE` are in.
FARADAY = 96.485


class Refused(SystemExit):
    """A table row that cannot be admitted. Aborts, and names the row and the predicate.

    ABORTS ON THE FIRST BAD ROW rather than skipping it, for the reason `curation.admit`
    aborts: these are small hand-authored artifacts, so a bad row is an authoring error to
    fix rather than noise to filter -- and filtering would let a stale id sit in the file
    while its contribution silently read as zero.
    """


def read_table(path: Path) -> pd.DataFrame:
    """Parse a curated TSV. `#` is a comment ONLY at line start.

    An inline `comment='#'` truncates any SMILES carrying a `#` triple bond -- nitrile
    C#N, alkyne C#C -- silently dropping exactly the rows a curator most needs to supply.
    """
    kept = [ln for ln in Path(path).read_text().splitlines()
            if not ln.lstrip().startswith("#")]
    return pd.read_csv(StringIO("\n".join(kept)), sep="\t")


def _cited(value) -> bool:
    """`pd.isna` FIRST: an empty cell arrives as NaN and `str(nan)` is 'nan', which is
    truthy, so a bare emptiness test accepts a row with no citation at all."""
    return not pd.isna(value) and bool(str(value).strip())


def _parse_terms(spec: str, row_id: str) -> list[tuple[str, float]]:
    """`'1*MODEL:x;-1*MODEL:y'` -> [('MODEL:x', 1.0), ('MODEL:y', -1.0)]."""
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


class Substitutions:
    """Loaded tables, or nothing at all. `Substitutions()` covers no reaction.

    The empty instance is the configuration every baseline was taken under and the one
    that must leave the pipeline bit-identical, so it is the default rather than a mode.
    """

    def __init__(self, models: dict | None = None, rows: pd.DataFrame | None = None,
                 by_mnxm: dict | None = None):
        self.models = models or {}
        self.rows = rows if rows is not None else pd.DataFrame(columns=list(ROW_COLUMNS))
        self._by_mnxm = by_mnxm or {}
        # Refusals ship beside accepts, so a table's silence about a compound is a
        # recorded decision rather than an absence somebody has to reconstruct.
        self.decisions = pd.DataFrame(columns=list(DECISION_COLS))

    def __len__(self) -> int:
        return len(self._by_mnxm)

    # -- the two things the pipeline asks for -----------------------------

    def props(self, base: dict) -> dict:
        """`base` widened with the model compounds. Never narrowed, never overwritten."""
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

    def rewrite(self, stoich: dict[str, float]) -> dict[str, float]:
        """The equation as the members should see it. Identity when nothing is covered."""
        if not self._by_mnxm or not (set(stoich) & set(self._by_mnxm)):
            return stoich
        out: dict[str, float] = {}
        for mnxm, coeff in stoich.items():
            row = self._by_mnxm.get(mnxm)
            if row is None:
                out[mnxm] = out.get(mnxm, 0.0) + coeff
                continue
            for key, mult in row["terms"]:
                out[key] = out.get(key, 0.0) + coeff * mult
        return {k: v for k, v in out.items() if abs(v) > 1e-12}

    def covers(self, stoich: dict[str, float]) -> bool:
        return bool(set(stoich) & set(self._by_mnxm))

    def sigma_sub(self, stoich: dict[str, float]) -> float:
        """Congener spread over the participants this reaction substituted, in quadrature.

        CARRIED SEPARATELY AND FOLDED IN LATER, never added to the member's own sigma.
        `combine.eq_vote` detects an eQuilibrator group cancellation by testing sigma
        against the floor; inflating the member's sigma lifts a cancelling zero over that
        floor and re-promotes it to tier 1 -- precisely the defect r8 was baked to remove.
        """
        var = 0.0
        for mnxm in stoich:
            row = self._by_mnxm.get(mnxm)
            if row is not None:
                var += float(row["sigma_sub"]) ** 2
        return math.sqrt(var)


# =====================================================================
# admission
# =====================================================================

def _admit_models(df: pd.DataFrame) -> dict:
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
    return out


def _gate_replaceable(mnxm: str, props: dict, row_id: str) -> None:
    """A row may only displace a participant the member cannot use TODAY.

    Tested with `thermo_dgbyg._has_wildcard` itself rather than a reimplementation, so the
    admission predicate cannot drift from the abstention it is meant to be undoing. A row
    that displaces a usable participant is not a substitution, it is an override of
    MetaNetX chemistry, and nothing here reviewed that.
    """
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


def _gate_couple(rows: pd.DataFrame, models: dict) -> None:
    """Both states of a carrier couple present, and differing by what they declare.

    A single-sided carrier row is the failure this catches: substituting the oxidised
    partner and leaving the reduced one structureless leaves the member abstaining anyway,
    while the table reads as covering the reaction.
    """
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
    """The MODEL compound's potential, against the REAL carrier's, within one decade.

    THE TWO NUMBERS ARE THE REAL ONE AND THE STAND-IN'S, not the two states of one couple.
    A couple has ONE E0', carried by both its rows, so comparing the ox row's declaration
    to the red row's compares a value to itself and can never refuse anything. What has to
    be small is the gap between the carrier MetaNetX would not let the member read and the
    compound substituted for it -- that is the entire claim a carrier row makes.

    A generic with no defined potential fails by having no number to declare, which refuses
    `Acceptor`, `A` and `AH2` by the same rule that admits NAD for NAD(P) -- no
    special-casing, and no name list to keep in step with anything.
    """
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
                  "replaceable_only", "terms_parse", "models_declared", "congener_spread")
COUPLE_PREDICATES = ("couple_complete", "no_heavy_transfer", "potential_declared",
                     "potential_within_decade")


def _check_row(r, props: dict, names: dict, seen: dict, models: dict):
    """`(predicate, detail)` for the first predicate this row fails, or `(None, rec)`.

    `seen` maps an already-admitted mnxm to its terms string. A REPEAT IS LEGAL AND
    NECESSARY: MetaNetX carries one reduced thioredoxin (`MNXM741334`) as the partner of
    three separately-accessioned disulfides, so the couple rows must share it. What has to
    hold is that `_by_mnxm` stays single-valued -- one compound, one rewrite -- so a repeat
    is admitted when it asks for the same terms and refused when it asks for different
    ones, which is a conflict no later stage could detect.
    """
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
    try:
        sigma_sub = _congener_spread(r, models, row_id)
    except Refused as e:
        return "congener_spread", str(e)
    rec = {c: getattr(r, c) for c in ROW_COLUMNS}
    rec["terms"] = terms
    rec["sigma_sub"] = sigma_sub
    return None, rec


def _check_couples(frame: pd.DataFrame, models: dict):
    """`(predicate, detail)` for the first couple-level failure, or `(None, None)`.

    Two gates, four named outcomes: each gate can fail for the structural reason or the
    chemical one, and a curator needs those apart -- "you forgot the reduced row" and
    "this couple carries atoms through" are different work.
    """
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
         *, collect: bool = False) -> Substitutions:
    """Read and admit the tables, or return the empty configuration.

    `props` and `names` are MetaNetX's own, and both are needed: props for the
    replaceable-only gate, names for the stale-id tripwire.

    `collect=True` is the `check` verb's mode: run every predicate on every row and record
    the verdicts instead of aborting on the first. The pipeline never uses it -- a table
    that half-loads is worse than one that refuses -- but a curator authoring twenty rows
    needs all twenty verdicts, not the first.
    """
    if directory is None:
        return Substitutions()
    directory = Path(directory)
    models = _admit_models(read_table(directory / "models.tsv"))
    df = read_table(directory / "substitutions.tsv")

    missing = set(ROW_COLUMNS) - set(df.columns)
    if missing:
        raise Refused(f"[substitute] substitutions table lacks columns: {sorted(missing)}")

    parsed, seen, decisions = [], {}, []
    for r in df.itertuples(index=False):
        pred, payload = _check_row(r, props, names, seen, models)
        base = dict(kind=r.kind, mnxm=r.mnxm, mnx_name=r.mnx_name, couple_id=r.couple_id)
        if pred is not None:
            if not collect:
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
    out = Substitutions(models, frame, by_mnxm)
    out.decisions = pd.DataFrame(decisions, columns=list(DECISION_COLS))
    return out


def _congener_spread(r, models: dict, row_id: str) -> float:
    """How much the answer moves across the declared alternatives to this model compound.

    Carried as `sigma_sub` rather than discarded, because the choice of model compound is
    an assertion with a width and reporting it as zero would make an asserted structure
    look like a measurement. Filled by `substitute congeners`, which scores each and
    writes the spread back; an unscored row declares 0.0 and the anchor verb says so.
    """
    if not _cited(r.congeners):
        return 0.0
    values = []
    for part in str(r.congeners).split(";"):
        part = part.strip()
        if not part:
            continue
        try:
            values.append(float(part))
        except ValueError:
            raise Refused(f"[substitute] {row_id}: congener entry {part!r} is not a "
                          f"kJ/mol number. Run `substitute congeners` to fill this")
    if not values:
        return 0.0
    spread = max(values) - min(values)
    if spread > canon.DIR_DECADE:
        raise Refused(
            f"[substitute] {row_id}: congeners span {spread:.2f} kJ/mol, past DIR_DECADE "
            f"({canon.DIR_DECADE:.2f}). The model compound is not pinning the answer -- "
            f"bring it back rather than widening this gate")
    return spread / 2.0


# =====================================================================
# the command line
# =====================================================================

def _tables(args):
    from .refdata import load_mnxm_names, load_mnxm_props
    props = load_mnxm_props(args.chem_prop)
    names = load_mnxm_names(args.chem_prop)
    return props, names


def cmd_check(args):
    """Every predicate on every row, and what the admitted rows would reach.

    Reports two numbers a curator cannot get from the table itself: how many reactions the
    admitted rows actually unblock, and -- the answer to "this is just the ones somebody
    happened to look at" -- which accessions carry a name already admitted under some other
    id and are NOT in the table. Under-coverage becomes a printed number instead of an
    absence, the way `twins.alias_index` answers the same objection.
    """
    from .refdata import load_mnxr_stoich
    props, names = _tables(args)
    subs = load(args.tables, props, names, collect=True)
    d = subs.decisions
    n_bad = int((d["verdict"] == "refused").sum())
    print(f"[substitute] {len(d):,} rows · {len(d) - n_bad:,} admitted · {n_bad:,} refused")
    for pred in ROW_PREDICATES + COUPLE_PREDICATES:
        n = int((d["predicate"] == pred).sum())
        if n:
            print(f"    {pred:<26} {n:>5,}")
            for detail in d.loc[d["predicate"] == pred, "detail"].head(3):
                print(f"        {detail}")

    reached = 0
    if args.reac_prop:
        stoich = load_mnxr_stoich(args.reac_prop)
        reached = sum(1 for _, (st, _b, _t) in stoich.items() if subs.covers(st))
        print(f"[substitute] admitted rows touch {reached:,} of {len(stoich):,} reactions")

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
    """kJ/mol the substituted anchor should sit from its sibling, from the two potentials.

    Anchor and sibling are the SAME transformation written with two different carriers, so
    their standard free energies differ only by the carriers' half-cells. For a reaction
    written `A_ox + carrier_red = A_red + carrier_ox`, dG'0 = -nF(E_A - E_carrier), and the
    acceptor term cancels in the difference:

        dG(anchor) - dG(sibling) = -c_red * n_e * F * (E_model - E_sibling)

    `c_red` is the reduced carrier's signed coefficient in the MNXR equation, which is what
    carries the orientation -- MetaNetX's left/right is arbitrary, and reading the sign off
    the equation is the only way the prediction tracks it.
    """
    return -float(c_red) * float(n_e) * FARADAY * (float(e0_model_V) - float(e0_sibling_V))


def cmd_anchor(args):
    """Score each row's anchor under the model couple and check it against a sibling.

    THE ANSWER TO "how do you refuse a substitution that balances but is thermodynamically
    unjustified". Balance is necessary and worthless as evidence here: `[Fe+3]`/`[Fe+2]`
    balances ferredoxin perfectly and returns a confident wrong number.

    THE TEST IS A DIFFERENCE AGAINST A PREDICTED NUMBER, not a drift toward zero, and it
    has to be: MetaNetX writes almost no generic-carrier reaction a second time with the
    same family of carrier concretely. It writes it with a DIFFERENT one -- the flavin
    monooxygenases appear again as their overall NADPH reactions -- so an anchor and its
    sibling differ by a real quantity rather than by nothing. `predicted_offset` computes
    that quantity from the two declared potentials, and the row survives only if the member,
    running on the substituted equation, lands within one decade of it.

    That makes `e0_V` falsifiable instead of decorative: a carrier row asserting the wrong
    potential predicts the wrong offset and is refused here rather than shipping a
    confident number nothing checked.

    Only `eq` is runnable on this workstation; `build-refs-dgbyg` does not exist here, so
    the dGbyG arm has to be run in its own image or reported unverified.
    """
    from .refdata import load_mnxr_stoich
    props, names = _tables(args)
    subs = load(args.tables, props, names)
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
        # there and the comparison passes however wrong the model compound is.
        if mnxm not in st:
            rows.append(dict(out, verdict="ANCHOR_UNCOVERED")); bad += 1; continue

        dg, sig, _flag, reason = score(st)
        out.update(model_dg=dg, sigma=sig, reason=reason)
        if dg is None:
            # The substitution bought nothing: the member is still silent on the very
            # reaction the row exists to unblock.
            rows.append(dict(out, verdict="member_silent")); bad += 1; continue

        sib = str(rec.get("sibling_mnxr") or "")
        was = baseline.get(sib) if _cited(rec.get("sibling_mnxr")) else None
        if was is None or pd.isna(was) or not _cited(rec.get("sibling_e0_V")):
            rows.append(dict(out, verdict="no_sibling"))
        else:
            want = predicted_offset(rec["n_e"], st[mnxm] if str(rec["state"]) == "red"
                                    else -st[mnxm], rec["e0_model_V"], rec["sibling_e0_V"])
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
                Substitutions(subs.models, subs.rows, swapped).rewrite(st), wide)
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
    if args.out:
        df.to_csv(args.out, sep="\t", index=False)
        print(f"[substitute] -> {args.out}")
    print(f"[substitute] {len(df):,} anchors · {bad:,} not ok "
          f"(DIR_DECADE = {canon.DIR_DECADE:.2f} kJ/mol)")
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
