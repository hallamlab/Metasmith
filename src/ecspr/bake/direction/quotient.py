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

from .canon import (DIR_CONC_COLUMNS, DIR_CONC_DEFAULT_mM, DIR_CONC_GASES,
                    DIR_CONC_IMPLICIT, DIR_CONC_SPREAD_DEFAULT, DIR_CONC_SPREAD_FLOOR,
                    DIR_CONC_UNIT_ACTIVITY, DIR_DECADE, DIR_RT)
from .refdata import (load_mnxm_formulas, load_mnxm_names, load_mnxm_props,
                      load_mnxr_stoich)
from .substitute import load as load_substitutions

MEMBERS = ("eq", "dgbyg")


# =====================================================================
# the MetaNetX join
# =====================================================================

def alias_index(chem_xref: Path) -> dict[str, set[str]]:
    # '<namespace>:<accession>' -> {MNXM}, from chem_xref column 2 AND the '||'-separated
    # alias list in column 3.
    #
    # COLUMN 3 IS THE POINT, and joining on the accession alone is the trap. KEGG's C00103
    # (glucose 1-phosphate) resolves through column 2 to MNXM1364214, the ALPHA anomer,
    # while the reaction universe uses MNXM1364212 -- so the single most load-bearing
    # metabolite in the phosphorylase case is missed by an exact-accession join.
    #
    # The other tempting key is worse. `HXXFSFRBOHSIMQ`, the InChIKey connectivity block
    # that reaches MNXM1364212, holds seventeen accessions: glucose, galactose, mannose,
    # allose and gulose 1-phosphate, every anomer and both enantiomeric series. It is a
    # hexose-phosphate bucket, not a compound.
    index: dict[str, set[str]] = {}
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
            index.setdefault(src, set()).add(mnxm)
            if len(parts) < 3:
                continue
            for alias in parts[2].split("||"):
                alias = alias.strip()
                # MetaNetX marks superseded entries in the same list. A secondary id is a
                # pointer to history, not a synonym for the current compound.
                if not alias or ":" not in alias:
                    continue
                low = alias.lower()
                if "secondary" in low or "obsolete" in low:
                    continue
                index.setdefault(alias, set()).add(mnxm)
    return index


def joiner(chem_xref: Path, chem_prop: Path):
    # Returns `mnxm_of(namespace, accession) -> mnxm | None`, guarded on formula+charge.
    #
    # THE GUARD IS WHAT MAKES THE ALIAS LIST SAFE. Alias lists span protonation states and
    # R-group generics, so an unguarded expansion hands one measurement to several distinct
    # compounds. Requiring the candidates to agree on formula AND charge collapses that to
    # the one MetaNetX itself treats as the reference protonation.
    index = alias_index(chem_xref)
    formulas, charges, names = {}, {}, {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 5 or not p[0]:
                continue
            names[p[0]], formulas[p[0]], charges[p[0]] = p[1], p[3], p[4]

    def mnxm_of(namespace, accession):
        key = f"{namespace}:{accession}"
        hits = index.get(key)
        if not hits:
            return None
        if len(hits) == 1:
            return next(iter(hits))
        by_shape: dict[tuple, list[str]] = {}
        for m in hits:
            by_shape.setdefault((formulas.get(m), charges.get(m)), []).append(m)
        # One (formula, charge) shape across every candidate means the alias list is
        # naming one compound under several ids. More than one means it is naming several,
        # and picking between them is not this function's call.
        if len(by_shape) != 1:
            return None
        return sorted(next(iter(by_shape.values())))[0]

    return mnxm_of, names


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
    n_measured = n_defaulted = n_excluded = 0
    for mnxm, coeff in stoich.items():
        if mnxm in DIR_CONC_IMPLICIT:
            # Already inside eQuilibrator's prime potentials; adding it double-counts.
            continue
        if mnxm in DIR_CONC_GASES or mnxm in DIR_CONC_UNIT_ACTIVITY:
            n_excluded += 1
            continue
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
                n_conc_excluded=n_excluded, delta_n=delta_n)


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

    mnxm_of, names = joiner(Path(args.chem_xref), Path(args.chem_prop))
    table = src_pkg.aggregate(rows, mnxm_of, names)
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
