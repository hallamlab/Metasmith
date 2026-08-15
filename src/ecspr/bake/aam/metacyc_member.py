"""Curated member: MetaCyc ``atom-mappings-smiles.dat``, joined to MNXR.

The third AAM ensemble member, and the first NON-NEURAL one. RXNMapper and
LocalMapper are both transformers trained on reaction SMILES -- when they agree
part of the agreement is shared architectural bias, not independent confirmation
(the ``NEURAL_SHARED_FLOOR`` discount in ``combine_aam.py``). MetaCyc is a curated
database, not a model, so it is a genuinely INDEPENDENT vote: it corroborates the
neural pair at full weight when it agrees and is a finding, not noise, when it does
not -- the exact role BioCyc plays against the correlated thermo pair in the
directionality ensemble (``direction/curated.py``).

WHY THE SMILES FILE, NOT THE INDEX FILE. A prior round treated the curated member as
blocked -- "a MetaCyc ATOM-MAPPINGS decoder is absent from committed code and would
have to be re-derived", plus a ``classes.dat`` blocker on carrier formulae. Both apply
to the OTHER decode route (``atom-mapping.dat`` INDICES resolved against per-compound
formulae). ``atom-mappings-smiles.dat`` sidesteps both: it is already mapped reaction
SMILES in the exact ``[C:n]...>>...`` form the engine's ``pairs_from_mapped`` consumes,
and R-groups are embedded in the SMILES so no missing formulae are needed. No decoder;
the file IS the decode.

DROP-IN. ``load_member`` returns ``{mnxr -> (mapped_rxn_smiles, confidence)}`` -- the
same shape ``combine_aam.load_mapper`` returns for a cached neural member -- with
``confidence`` = ``CURATED_CONFIDENCE`` for every reaction (a curated map is not scored;
it is asserted). The mapped SMILES then flow through the engine's own
``pairs_from_mapped`` unchanged, exactly like a neural member's cached SMILES, so the
only thing that varies between members is the mapping, never the identity bookkeeping.

THE CROSS-NAMESPACE CAVEAT (measured downstream, not here). MetaCyc's per-metabolite
molecules must anchor to the MNXR's MetaNetX participants inside ``pairs_from_mapped``'s
``match_mols`` (canonical-SMILES match) and the template-count check. Where MetaCyc and
MetaNetX disagree on a reaction's participant set (protons/water/generic frames written
differently) the reaction comes back ``stripped`` or ``no_pairs`` -- SAFE (never a wrong
pair) but it lowers coverage. That split is measured in ``combine_aam``'s status report,
not silently folded into agreement.

Standalone: ``python metacyc_member.py --out metacyc_aam.tsv`` materialises the member as
a cached TSV (``mnxr, mapped_rxn_smiles, confidence``) so it is inspectable and matches the
"member = cached TSV" abstraction the neural members already use. Runs under any env with
pandas (no rdkit needed for the join; rdkit is only used by the combiner).
"""
from __future__ import annotations

import argparse
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

# A curated atom map is asserted, not scored -- it carries full weight, the analogue of
# the neural members' cached mapper confidence. Kept a named constant so the combiner and
# the materialised TSV agree on one value.
CURATED_CONFIDENCE = 1.0

# reac_xref column-0 prefix for MetaCyc reactions; the id after it is the atom-mapping
# file's own reaction id, verbatim (the rare (+)-/(-)- stereo-frame prefixes are PART of
# the id, not something to strip -- they appear prefixed in reac_xref too).
XREF_PREFIX = "metacyc.reaction:"

# Loud floor on the crosswalk. If the prefix is wrong, reac_xref is stale, or the file
# is empty, almost nothing joins and the "curated member" is silently a handful of rows.
# The real join is ~99.6%; this floor only fires on a broken crosswalk, not on data.
JOIN_FLOOR = 0.9

# ---------------------------------------------------------------------------
# MetaCyc writes bracket atoms rdkit cannot read, and the two kinds must be
# treated OPPOSITELY. Measured over the 16,526 joined records: without this,
# 4,452 (26.9%) fail to parse at all and are lost as `unparseable`.
#
#   [R:19] [R1:3] [X:7]   an UNSPECIFIED substituent. `*` is exactly what that
#                         means in SMILES, so dummying is a translation, not a
#                         deletion -- nothing is claimed about atoms because
#                         MetaCyc claims nothing about them either.
#   [L-cysteine:8]        a NAMED molecule. Dummying it deletes the atoms it
#   [a protein:9]         actually has -- and for L-cysteine those atoms include
#   [a tRNA:4]            the SULFUR that ligates the Fe-S cluster the reaction
#                         is about. The record is REFUSED instead: a coverage gap
#                         is honest, a silently desulfurised reaction is not.
#
# 2,579 records carry a named residue and are refused; the remaining 13,947 all
# parse, with zero residual parse failures.
# ---------------------------------------------------------------------------
_BRACKET = re.compile(r"\[([^\]]*)\]")
# `<iso><symbol><chirality><H count><charge><:map>` -- the symbol is whatever sits
# before the first of `:`, `@`, `+`, `-`, which is why a name like `L-cysteine` does
# not match and lands in the "unrecognised body" branch rather than being mangled.
_BODY = re.compile(r"^(\d*)([A-Za-z][^:@+\-]*?)((?:@{1,2})?(?:H\d*)?(?:[+-]\d*)?(?::\d+)?)$")
_RGROUP = re.compile(r"^(R\d*|X)$")
_MAPNUM = re.compile(r":(\d+)$")


def _is_element(sym: str) -> bool:
    from rdkit import Chem
    try:
        return Chem.GetPeriodicTable().GetAtomicNumber(sym) > 0
    except Exception:
        return False


def _classify_body(body: str):
    """('element' | 'rgroup' | 'named', symbol) for one bracket-atom body."""
    m = _BODY.match(body)
    if not m:
        return "named", body
    sym = m.group(2).strip()
    if _is_element(sym):
        return "element", sym
    # `[NaH]`-shaped bodies where the H count fused into the symbol capture
    m2 = re.match(r"^([A-Za-z][a-z]?)H\d*$", sym)
    if m2 and _is_element(m2.group(1)):
        return "element", m2.group(1)
    if _RGROUP.match(sym):
        return "rgroup", sym
    return "named", sym


def normalize_mapped_smiles(smi: str):
    """(smiles, None) with R-groups dummied, or (None, residue_name) to refuse.

    Refuse-not-coerce on a named residue. The rule is not "which names look
    dangerous" -- it is that a NAME is a claim about atoms and `*` is a claim that
    there are none, so turning one into the other is a silent deletion whatever the
    name happens to be. Returning the name rather than a bare failure is what lets
    the report say which residues cost how many reactions.
    """
    named = None
    for body in _BRACKET.findall(smi):
        kind, sym = _classify_body(body)
        if kind == "named":
            named = named or sym
    if named is not None:
        return None, named

    def rep(m):
        body = m.group(1)
        kind, _sym = _classify_body(body)
        if kind != "rgroup":
            return m.group(0)
        n = _MAPNUM.search(body)
        return f"[*:{n.group(1)}]" if n else "[*]"

    return _BRACKET.sub(rep, smi), None


def load_id2mnxr(reac_xref: Path) -> dict:
    """MetaCyc reaction id -> MNXR, from the ``metacyc.reaction:`` rows of reac_xref.

    Only real ``MNXR<digits>`` targets are kept; MetaNetX's ``EMPTY`` sentinel (a
    reaction with no reconciled content) is not a reaction and is dropped.
    """
    out = {}
    with open(reac_xref) as fh:
        for line in fh:
            if not line.startswith(XREF_PREFIX):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 2:
                continue
            mnxr = p[1]
            if mnxr.startswith("MNXR") and mnxr[len("MNXR"):].isdigit():
                out[p[0][len(XREF_PREFIX):]] = mnxr
    return out


def load_rows(smiles_dat: Path, id2mnxr: dict) -> pd.DataFrame:
    """Every ``metacyc_id <TAB> mapped_reaction_SMILES`` line that joins to an MNXR.

    Returns a per-reaction frame ``[metacyc_id, mnxr, mapped_rxn_smiles]`` BEFORE the
    per-MNXR collapse, plus is the basis for the join report. Non-joining ids are simply
    absent (logged as a count by the caller, never special-cased).
    """
    rows = []
    n_lines = 0
    with open(smiles_dat) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or "\t" not in line:
                continue
            n_lines += 1
            mc_id, smi = line.split("\t", 1)
            mnxr = id2mnxr.get(mc_id)
            if mnxr is None or not smi:
                continue
            rows.append((mc_id, mnxr, smi))
    df = pd.DataFrame(rows, columns=["metacyc_id", "mnxr", "mapped_rxn_smiles"])
    df.attrs["n_lines"] = n_lines
    return df


def collapse_to_mnxr(df: pd.DataFrame) -> pd.DataFrame:
    """One row per MNXR. Several MetaCyc reactions can share one MNXR (stereo/direction
    frame variants reconciled to a single MetaNetX reaction); dedupe DETERMINISTICALLY
    -- sort by (mnxr, metacyc_id), keep first -- mirroring ``load_mapper``'s
    ``drop_duplicates(keep="first")``, so two MetaCyc SMILES never fight over one row.
    """
    return (df.sort_values(["mnxr", "metacyc_id"])
              .drop_duplicates(subset="mnxr", keep="first")
              .reset_index(drop=True))


def build(smiles_dat: Path, reac_xref: Path):
    """Return (per_mnxr_frame, report). The frame has columns
    ``mnxr, mapped_rxn_smiles, confidence`` -- the cached-member TSV schema.
    """
    id2mnxr = load_id2mnxr(reac_xref)
    per_rxn = load_rows(smiles_dat, id2mnxr)
    n_lines = per_rxn.attrs.get("n_lines", len(per_rxn))

    # Normalise BEFORE the per-MNXR collapse, so that when two MetaCyc frames map to
    # one MNXR and only one of them names a residue, the usable frame is the one kept
    # rather than whichever sorted first.
    residues = Counter()
    keep, kept_smi = [], []
    for r in per_rxn.itertuples(index=False):
        s, bad = normalize_mapped_smiles(r.mapped_rxn_smiles)
        if bad is not None:
            residues[bad] += 1
            continue
        keep.append(r.Index if hasattr(r, "Index") else None)
        kept_smi.append((r.metacyc_id, r.mnxr, s))
    norm_rxn = pd.DataFrame(kept_smi,
                            columns=["metacyc_id", "mnxr", "mapped_rxn_smiles"])
    n_refused = len(per_rxn) - len(norm_rxn)

    per_mnxr = collapse_to_mnxr(norm_rxn)
    per_mnxr = per_mnxr[["mnxr", "mapped_rxn_smiles"]].copy()
    per_mnxr["confidence"] = CURATED_CONFIDENCE

    join_frac = (len(per_rxn) / n_lines) if n_lines else 0.0
    # Loud floor: a curated member that joins almost nothing is a broken crosswalk, not
    # a data property -- the direction lane's ``curated.py`` guards the same way.
    assert join_frac >= JOIN_FLOOR, (
        f"metacyc: only {join_frac:.1%} of atom-mapping lines joined to an MNXR "
        f"(floor {JOIN_FLOOR:.0%}) -- the reac_xref crosswalk is likely broken, not "
        f"the data")
    report = dict(
        n_lines=n_lines,
        n_joined=len(per_rxn),
        join_frac=join_frac,
        n_xref=len(id2mnxr),
        n_mnxr=len(per_mnxr),
        n_refused_named=n_refused,
        residues=residues,
        n_collapsed=len(norm_rxn) - len(per_mnxr),
    )
    return per_mnxr, report


def load_member(smiles_dat: Path, reac_xref: Path) -> dict:
    """``{mnxr -> (mapped_rxn_smiles, confidence)}`` -- drop-in for ``load_mapper``'s
    output shape, so the combiner treats MetaCyc as just another member."""
    per_mnxr, _ = build(Path(smiles_dat), Path(reac_xref))
    return {r.mnxr: (r.mapped_rxn_smiles, float(r.confidence))
            for r in per_mnxr.itertuples(index=False)}


def main(argv=None):
    # No default paths. The originals pointed into the sibling project's data tree; here
    # the inputs are staged by the planner and named on the command line.
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--smiles-dat", type=Path, required=True)
    ap.add_argument("--reac-xref", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--out-report", type=Path, default=None,
                    help="the named residues this member REFUSED, and how many records "
                         "each cost. Printed either way, but printing is not keeping: "
                         "'MetaCyc reaches 13,947 of 16,818' is only interpretable "
                         "next to WHICH 2,804 were declined and why")
    a = ap.parse_args(argv)

    per_mnxr, rep = build(a.smiles_dat, a.reac_xref)
    per_mnxr.to_csv(a.out, sep="\t", index=False)
    if a.out_report:
        pd.DataFrame(
            [dict(residue=nm, n_records=n) for nm, n in rep["residues"].most_common()],
            columns=["residue", "n_records"],
        ).to_csv(a.out_report, sep="\t", index=False)
    print("=" * 64)
    print("MetaCyc curated AAM member -- join report")
    print("=" * 64)
    print(f"reac_xref metacyc->MNXR entries : {rep['n_xref']:,}")
    print(f"atom-mapping lines              : {rep['n_lines']:,}")
    print(f"joined to an MNXR              : {rep['n_joined']:,} ({rep['join_frac']:.2%})")
    print(f"refused -- named residue        : {rep['n_refused_named']:,}")
    print(f"distinct MNXR (member size)     : {rep['n_mnxr']:,}")
    print(f"collapsed (frame variants)      : {rep['n_collapsed']:,}")
    if rep["residues"]:
        print("\nnamed residues refused (dummying them would delete real atoms):")
        for nm, n in rep["residues"].most_common(12):
            print(f"    {nm[:56]:<56} {n:>5,}")
    print(f"\nwrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
