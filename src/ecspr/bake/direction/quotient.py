# The reaction quotient: what turns a standard-state dG'o into a physiological dG'.
#
# WHAT THIS FIXES. Every ratio the bake ships is exp(dG'o/RT) -- the standard state, every
# participant at one molar. No lane forms a Q term, so no row carries any information about
# intracellular concentration, and glycogen phosphorylase therefore ships as a glycogen
# SYNTHASE at dir_confidence 0.94. The thermodynamics were never wrong; the question they
# answered was not the one a conductance model asks.
#
# TWO COMMANDS.
#   `table`    aggregate the cited sources into one concentration and one width per MNXM.
#   `annotate` emit the per-MNXR correction, per member.
#
# IT LIVES AT THE COMBINER, NOT IN A MEMBER, and that placement is the whole design. Q is a
# function of (stoichiometry, concentrations) alone -- it does not know or care whether
# eQuilibrator or dGbyG produced the standard term. Putting it inside one member would make
# the two members incommensurable: one would be answering about a cell and the other about
# a beaker, and `thermo_vote` fuses them as though they answered the same question.
#
# BUT IT IS COMPUTED PER MEMBER ANYWAY, because the STOICHIOMETRY is per member. The
# substitution lane restages polymer equations, a row can be refused for one member and
# kept for the other, and applying a correction to MetaNetX's unsubstituted form of a
# polymer reaction is meaningless -- the two glucans cancel only once the substitution has
# put both of them there. So: one Q definition, evaluated on each member's own equation.
#
# THE DEFAULT IS 1 mM AND IT IS NEVER SKIPPED. A participant with no measurement left at
# the 1 M standard state while its partners move to millimolar makes the correction
# ONE-SIDED. Measured on MNXR145036 that returns -17.3 kJ/mol where the balanced answer is
# +5.4: [Pi] got its 5 mM, [G1P] got nothing, and a fifty-fold pool skew was invented out
# of an absence. A one-sided reaction quotient is worse than none.
#
# Standalone: `python -m ecspr.bake.direction.quotient annotate ...` (any env with pandas).
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import pandas as pd

from .canon import (DIR_CONC_COLUMNS, DIR_CONC_DEFAULT_mM, DIR_CONC_GAS_PHASE,
                    DIR_CONC_MAX_EXPANSION,
                    DIR_CONC_IMPLICIT, DIR_CONC_SPREAD_DEFAULT, DIR_CONC_SPREAD_FLOOR,
                    DIR_CONC_UNIT_ACTIVITY, DIR_DECADE, DIR_RT)
from .refdata import (load_mnxm_formulas, load_mnxm_names, load_mnxm_props,
                      load_mnxr_stoich)
from .substitute import load as load_substitutions

MEMBERS = ("eq", "dgbyg")


# =====================================================================
# the MetaNetX join
# =====================================================================

OBSOLETE = "secondary/obsolete/fantasy identifier"


def _norm(name: str) -> str:
    # Case, spacing and punctuation are display choices, not chemistry. 'D-Glucose
    # 1-phosphate' and 'D-glucose-1-phosphate' are the same string once they are gone.
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def read_xref(chem_xref: Path):
    # (accession -> {MNXM}, normalised name -> {MNXM}) from chem_xref.
    #
    # COLUMN 3 IS A LIST OF NAMES, not of accessions -- 'O2||Disauerstoff||dioxygen||...'.
    # That distinction is the whole join. Reading it as accessions silently produces a
    # table that is 20% short and misses the one metabolite this lane was built for.
    by_accession: dict[str, set[str]] = {}
    by_name: dict[str, set[str]] = {}
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
            names = parts[2] if len(parts) > 2 else ""
            if names.strip() == OBSOLETE:
                # A superseded id points at history, not at the current compound.
                continue
            by_accession.setdefault(src, set()).add(mnxm)
            for name in names.split("||"):
                key = _norm(name)
                if key:
                    by_name.setdefault(key, set()).add(mnxm)
    return by_accession, by_name


def read_props(chem_prop: Path):
    formulas, charges, names = {}, {}, {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 5 or not p[0]:
                continue
            names[p[0]], formulas[p[0]], charges[p[0]] = p[1], p[3], p[4]
    return formulas, charges, names


def joiner(chem_xref: Path, chem_prop: Path, max_expansion: int = DIR_CONC_MAX_EXPANSION):
    # Returns `mnxms_of(namespace, accession) -> [mnxm]`, expanded by name and guarded on
    # formula and charge.
    #
    # IT RETURNS A SET, NOT ONE ID, and that is the point. MetaNetX splits glucose
    # 1-phosphate into `MNXM1364212` (D-glucopyranose 1-phosphate) and `MNXM1364214`
    # (the alpha anomer). KEGG's C00103 resolves to the anomer; the reaction universe
    # writes glycogen phosphorylase with the other one. A measurement of the POOL belongs
    # to both, so resolving to a single id loses the measurement exactly where it is most
    # load-bearing.
    #
    # THE EXPANSION IS BY NAME, NOT BY InChIKey SKELETON, and the skeleton was tried first.
    # The connectivity block `HXXFSFRBOHSIMQ` holds seventeen accessions: glucose,
    # galactose, mannose, allose and gulose 1-phosphate, every anomer and both enantiomeric
    # series. It is a hexose-phosphate bucket, not a compound. Two accessions sharing the
    # NAME `D-Glucose 1-phosphate` are two accessions MetaNetX says are that compound,
    # which separates the anomers of glucose 1-phosphate from galactose 1-phosphate where
    # a skeleton cannot.
    #
    # FORMULA AND CHARGE MUST STILL AGREE. Name lists span protonation states and R-group
    # generics, so an unguarded expansion hands one measurement to several distinct
    # compounds.
    #
    # AND THE EXPANSION IS CAPPED, because formula and charge cannot separate stereoisomers.
    # Unbounded, `kegg.compound:C08353` (beta-D-ribopyranose) reaches seven accessions
    # including `lyxose` and `aldehydo-L-ribose` -- a C2 epimer and an enantiomer, same
    # formula, same charge, genuinely different compounds. The link runs through a GENERIC
    # entry (`pentofuranose`) that lists many pentoses as synonyms, so a name shared by many
    # accessions is a category label rather than an identity. An accession that expands past
    # the cap falls back to its primary id alone rather than spraying one measurement across
    # a sugar family. The cap is SWEPT against held-out curated directions, not chosen.
    by_accession, by_name = read_xref(chem_xref)
    formulas, charges, names = read_props(chem_prop)

    # Reversed once, so expanding a metabolite is a lookup rather than a scan of a name
    # index with ~1.1 M keys.
    names_of: dict[str, set[str]] = {}
    for name, holders in by_name.items():
        for m in holders:
            names_of.setdefault(m, set()).add(name)

    def shape(m):
        return (formulas.get(m), charges.get(m))

    def mnxms_of(namespace, accession):
        # A MetaNetX accession is its own answer. `chem_xref` carries no
        # `metanetx.chemical:` self-references, so a source that already speaks MNXM --
        # BioNumbers does, because its rows have no chemical identifier of their own --
        # would otherwise never join. Checked against `chem_prop`, not trusted.
        if namespace == "metanetx.chemical":
            return [accession] if accession in formulas else []
        primary = by_accession.get(f"{namespace}:{accession}") or set()
        if not primary:
            return []
        want = {shape(m) for m in primary}
        if len(want) != 1 or want == {(None, None)}:
            # The accession itself names more than one compound. Choosing between them is
            # not this function's call.
            return []
        target = next(iter(want))
        out = set(primary)
        for m in primary:
            for name in names_of.get(m, ()):
                for other in by_name.get(name, ()):
                    if shape(other) == target:
                        out.add(other)
        if len(out) > max_expansion:
            return sorted(primary)
        return sorted(out)

    return mnxms_of, names


# =====================================================================
# the correction
# =====================================================================

def correction(stoich: dict, conc: dict) -> dict:
    # One reaction's quotient, over ONE member's (already restaged) stoichiometry.
    #
    #   molecularity = RT * dn * ln(1 mM)
    #   skew         = RT * sum(nu_i * ln(c_i / 1 mM))
    #   sigma_conc   = DECADE * sqrt(sum (nu_i * spread_i)^2)
    #
    # Every participant that is a free solute contributes to all three. One that is not --
    # water, the proton, a dissolved gas, a polymer -- contributes to none and is COUNTED,
    # so a reaction whose correction is small because it was mostly excluded is
    # distinguishable from one whose correction is small because it is balanced.
    delta_n = skew = var_log = 0.0
    n_measured = n_defaulted = n_excluded = n_gas = 0
    for mnxm, coeff in stoich.items():
        if mnxm in DIR_CONC_IMPLICIT:
            # Already inside eQuilibrator's prime potentials; adding it double-counts.
            continue
        if mnxm in DIR_CONC_UNIT_ACTIVITY:
            # No free-solute concentration exists for a polymer or a generic acceptor,
            # so there is nothing to price. This is the ONLY exclusion.
            n_excluded += 1
            continue
        n_gas += mnxm in DIR_CONC_GAS_PHASE
        delta_n += coeff
        hit = conc.get(mnxm)
        if hit is None:
            n_defaulted += 1
            var_log += (coeff * DIR_CONC_SPREAD_DEFAULT) ** 2
            continue
        n_measured += 1
        value, spread = hit
        skew += coeff * math.log10(value / DIR_CONC_DEFAULT_mM)
        var_log += (coeff * max(spread, DIR_CONC_SPREAD_FLOOR)) ** 2
    molecularity = DIR_RT * delta_n * math.log(DIR_CONC_DEFAULT_mM * 1e-3)
    skew_kj = DIR_RT * skew * math.log(10.0)
    return dict(molecularity=molecularity, skew=skew_kj,
                dG_correction=molecularity + skew_kj,
                sigma_conc=DIR_DECADE * math.sqrt(var_log),
                n_conc_measured=n_measured, n_conc_defaulted=n_defaulted,
                n_conc_excluded=n_excluded, n_conc_gas_phase=n_gas, delta_n=delta_n)


def read_table(path: Path) -> dict:
    frame = pd.read_parquet(path) if Path(path).suffix == ".parquet" \
        else pd.read_csv(path, sep="\t")
    return {r.mnxm: (float(r.conc_mM), float(r.log10_spread))
            for r in frame.itertuples(index=False)}


# =====================================================================
# commands
# =====================================================================

def cmd_table(args):
    from . import sources as src_pkg
    import importlib

    for name in args.source:
        importlib.import_module(f"{__package__}.sources.{name}")
    registry = src_pkg.registry()
    unknown = set(args.source) - set(registry)
    if unknown:
        raise SystemExit(f"[quotient] no adapter registered for {sorted(unknown)}")

    frames = []
    for name in args.source:
        chunk = Path(dict(p.split("=", 1) for p in args.chunk)[name])
        frame = src_pkg.validate(registry[name].read(chunk), name)
        print(f"[quotient] {name}: {len(frame)} measured rows from {chunk}")
        frames.append(frame)
    rows = pd.concat(frames, ignore_index=True)

    mnxms_of, names = joiner(Path(args.chem_xref), Path(args.chem_prop),
                            max_expansion=args.max_expansion)
    table = src_pkg.aggregate(rows, mnxms_of, names)
    placed = len(table)
    print(f"[quotient] {len(rows)} rows -> {placed} MNXM "
          f"({int((table.n_conditions > 1).sum())} with more than one condition, "
          f"{int((table.n_sources > 1).sum())} with more than one source)")
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    if str(args.out).endswith(".parquet"):
        table.to_parquet(args.out, index=False)
    else:
        table.to_csv(args.out, sep="\t", index=False)
    print(f"[quotient] -> {args.out}")


def cmd_annotate(args):
    conc = read_table(Path(args.table))
    stoich = load_mnxr_stoich(Path(args.reac_prop))
    props = load_mnxm_props(Path(args.chem_prop))
    names = load_mnxm_names(Path(args.chem_prop))
    formulas = load_mnxm_formulas(Path(args.chem_prop))
    print(f"[quotient] {len(conc)} measured metabolites, {len(stoich)} reactions")

    rows = []
    for member in args.member:
        # PER MEMBER, and `load` refuses without one: the admitted substitution set is a
        # member's own, so one restaged equation is not two.
        subs = load_substitutions(Path(args.substitutions) if args.substitutions else None,
                                  props, names, formulas=formulas, member=member)
        covered = 0
        for mnxr, (st, _balanced, _transport) in stoich.items():
            covered += subs.covers(st)
            rows.append(dict(mnxr=mnxr, member=member,
                             **correction(subs.rewrite(st), conc)))
        print(f"[quotient:{member}] {len(stoich)} reactions, {covered} restaged by "
              f"substitution before the quotient")

    frame = pd.DataFrame(rows, columns=list(DIR_CONC_COLUMNS))
    frame.to_parquet(args.out, index=False)
    for member, g in frame.groupby("member"):
        # Bracketed, not attribute access: `skew` is also a DataFrame METHOD, so `g.skew`
        # silently returns the bound method rather than the column.
        big = int((g["dG_correction"].abs() > DIR_DECADE).sum())
        skewed = int((g["skew"].abs() > DIR_DECADE).sum())
        print(f"[quotient:{member}] median |molecularity| "
              f"{g['molecularity'].abs().median():.2f}, median |skew| "
              f"{g['skew'].abs().median():.2f} kJ/mol; {big} reactions move more than a "
              f"decade, {skewed} of them by skew")
    print(f"[quotient] -> {args.out}")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="the reaction quotient that turns a standard-state dG'o into a "
                    "physiological dG'")
    sub = ap.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("table", help="aggregate the cited sources into one table")
    t.add_argument("--source", nargs="+", required=True,
                   help="adapter names under ecspr.bake.direction.sources")
    t.add_argument("--chunk", nargs="+", required=True, metavar="NAME=PATH",
                   help="pinned originals directory per source")
    t.add_argument("--chem-xref", required=True)
    t.add_argument("--chem-prop", required=True)
    t.add_argument("--max-expansion", type=int, default=DIR_CONC_MAX_EXPANSION,
                   help="refuse a name expansion wider than this and keep the primary id "
                        "alone; swept against held-out curated directions")
    t.add_argument("--out", required=True)
    t.set_defaults(fn=cmd_table)

    a = sub.add_parser("annotate", help="per-MNXR correction, per member")
    a.add_argument("--table", required=True)
    a.add_argument("--reac-prop", required=True)
    a.add_argument("--chem-prop", required=True)
    a.add_argument("--substitutions", default=None,
                   help="substitution table directory; omit to leave every equation as "
                        "MetaNetX states it")
    a.add_argument("--member", nargs="+", default=list(MEMBERS), choices=list(MEMBERS))
    a.add_argument("--out", required=True)
    a.set_defaults(fn=cmd_annotate)
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    return args.fn(args) or 0


if __name__ == "__main__":
    sys.exit(main())
