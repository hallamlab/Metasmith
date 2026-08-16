"""What conservation settles for a reaction no mapper will ever be given.

THREE ARMS, TWO GRAINS, AND THE SPLIT BETWEEN THEM IS THE DELIVERABLE
---------------------------------------------------------------------
`atom_pairs.forced_pairs` already emits the pairing conservation leaves no choice about,
and it fires only where EVERY participant has a countable formula. Two things widen that,
and one of them cannot be widened honestly at all:

  * THE CONJUGATE ARM, atom-grain and exact. A participant appearing on both sides with
    EQUAL MULTIPLICITY contributes the same unknown amount to each side, so it cancels --
    on IDENTITY, with no formula consulted. What is left is a smaller reaction over which
    conservation may leave exactly one possibility, and that possibility is banked as
    atom-grain pairs. Nothing is inferred: an unknown that cancels was never counted.
  * THE SINGLE-UNKNOWN ARM, species-grain and a CLAIM. Where exactly one participant's
    count for X is unknown, conservation determines what it must be. That settles a
    number about a SPECIES; it says nothing about which of its atoms became which, and
    the species has no structure to hang a canonical rank on -- so there is no atom-grain
    row to write, and writing one would be inventing the atom identity this graph exists
    to respect. It is computed, reported, and NOT banked.
  * THE CARRIER CLASS, species-grain and weaker still. Two different ids playing the
    oxidised and reduced roles of one generic carrier also cancel, but their equality is
    asserted by their NAMES rather than by their identity. That is the rescue's argument
    and the rescue's arbiter already tests it per element, so this lane records the
    cancellation as a claim and leaves the admission where it is.

Saying that plainly is the point. The measured agreement between this kind of algebra
and a mapper is 99.7%, which is an excellent argument for the exact arm and no argument
at all for banking a species-grain number as if it were an atom correspondence.

THE RECOUNT IS WHAT MAKES ANY OF IT REACH
------------------------------------------
`count_element` refuses a `*` formula, and refusing is right for a formula -- but the
SMILES beside it states the explicit atoms exactly. Counts come from
`lookup::element_counts` first and the formula second, which is what lets a reaction
carrying a residue species be settled at all. The price is carried openly: an exact count
of the EXPLICIT atoms is not a conservation claim until the unspecified residue slots
cancel across the reaction, so they are summed per side and a difference -- or a single
unknown slot count -- refuses the element.

WHY MULTIPLICITY 1, WHERE `forced_pairs` DOES NOT ASK
-----------------------------------------------------
`atom_ranks` holds one rank list per (metabolite, element), so a participant written
twice offers two indistinguishable copies of the same rank list and a bijection over them
is a choice rather than a consequence. The arm therefore requires the X-carrying
participant to appear exactly once on its side. It is stricter than the extractor's
version deliberately: this arm exists to emit what is FORCED, and the extractor's
dilute-not-gap trade belongs where a mapper was actually consulted.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .. import atom_pairs as AP
from . import recount as RC
from . import worklist as W

ELEMENTS = AP.ELEMENTS

# The atom-grain product, in the EXTRACTOR's compact shape so `layers.explode` reads it
# with no special case -- the same reasoning `partial.FORCED_COLS` records.
FORCED_COLS = AP.PAIR_COLS
FORCED_SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("element", pa.string()),
    ("substrate", pa.string()), ("product", pa.string()), ("n_atoms", pa.int32()),
    ("sub_idx", pa.string()), ("prod_idx", pa.string()), ("pair_w", pa.string()),
])

# The species-grain product. A DIFFERENT TABLE, not a column, because the difference is
# not a degree of confidence: one is an atom correspondence and the other is a number
# about a molecule, and a reader that could join them would eventually join them.
CLAIM_COLS = ("mnxr", "element", "mnxm", "arm", "n_atoms", "basis")

# Provenance for the atom-grain rows. Distinguishable from `partial_forced` because the
# two arms answer different questions: that one reduces a reaction to one element, this
# one cancels an unknown and settles what is left.
FORCED_METHOD = "algebra_conjugate"
ALGEBRA_SOURCE = "algebra"

# The generic redox roles that pair as one class. NAME-BASED and rated medium by the
# measurement that proposed it, which is exactly why nothing here is banked from it:
# `curation.lane_acceptor` and `aam_blockers` already resolve these species and the
# rescue's arbiter tests the cancellation per element. This list exists to COUNT what
# that arm would reach, not to reach it.
CARRIER_ROLES = {
    "oxidised": ("acceptor", "a", "oxidized acceptor", "oxidised acceptor",
                 "an acceptor", "an electron acceptor", "oxidized donor",
                 "electron acceptor"),
    "reduced": ("ah2", "reduced acceptor", "a donor", "an electron donor",
                "hydrogen donor", "donor", "reduced donor"),
}


def _norm(s):
    return " ".join(str(s or "").lower().replace("-", " ").split())


def carrier_role(name):
    n = _norm(name)
    for role, names in CARRIER_ROLES.items():
        if n in names:
            return role
    return None


# =====================================================================
# cancellation
# =====================================================================

def cancel_conjugates(subs, prods):
    """Drop every participant standing on BOTH sides with equal multiplicity.

    EQUAL MULTIPLICITY, NOT MERE PRESENCE. `2 A + B >> A + C` does not cancel A: one copy
    of an unknown amount is left over, and treating the species as absent would silently
    subtract it from one side only. That is the same trap that makes a naive
    stoichiometric collapse unfaithful, and the rule here is the strict reading of it --
    a species whose coefficients differ is left in place, where the count gates below can
    refuse it honestly.

    ON IDENTITY, so no formula is consulted and the arm is exact rather than inferred.
    Returns `(kept_subs, kept_prods, cancelled)`, or None when the cancellation would
    empty a side -- which is not a settled reaction but an empty one.
    """
    cs, cp = Counter(subs), Counter(prods)
    cancelled = sorted(m for m in cs if cs[m] == cp.get(m, 0))
    if not cancelled:
        return list(subs), list(prods), []
    drop = set(cancelled)
    ks = [m for m in subs if m not in drop]
    kp = [m for m in prods if m not in drop]
    if not ks or not kp:
        return None
    return ks, kp, cancelled


def counts_for(m, X, counts_of, formulas):
    """Atoms of X in one molecule of `m`: the recount first, the formula second.

    None is UNKNOWN and never a zero. The order matters and is not a preference: a `*`
    formula states no count while the structure beside it states one exactly, and
    `atom_ranks` -- which every pair row indexes into -- is keyed on that structure.
    """
    c = counts_of.get(m)
    if c is not None:
        return c[ELEMENTS.index(X)]
    return AP.count_element(formulas.get(m), X)


def _residues_cancel(ks, kp, counts_of, residue_of):
    """Do the unspecified residue slots cancel across the kept participants?

    An exact count of the EXPLICIT atoms is not a statement about the whole molecule, so
    a reaction whose two sides carry different amounts of unspecified remainder has not
    been shown to conserve anything. Only recount-sourced participants have slots; a
    formula-sourced count came from a formula `count_element` was willing to read, which
    by construction had no residue in it.
    """
    def slots(ms):
        return [residue_of.get(m) for m in ms if m in counts_of]
    return RC.residue_slots_cancel(slots(ks), slots(kp))


# =====================================================================
# the three arms
# =====================================================================

def settle(mnxr, subs, prods, counts_of, residue_of, formulas, ranks_of, names):
    """`(forced_rows, claim_rows, tally)` for one reaction.

    Per element, in strength order: the exact arm if the cancellation leaves conservation
    no choice, otherwise the single-unknown arm as a claim. The carrier class is counted
    once per reaction, independently, because it is a statement about the reaction's
    shape rather than about one element.
    """
    forced, claims, tally = [], [], Counter()

    got = cancel_conjugates(subs, prods)
    if got is None:
        tally["cancellation would empty a side"] += 1
        return forced, claims, tally
    ks, kp, cancelled = got
    if cancelled:
        tally["reactions with a cancelling conjugate"] += 1

    # THE CARRIER CLASS, counted and not banked. An oxidised role on one side and a
    # reduced role on the other, in equal number, is the same cancellation argued from
    # names instead of from identity -- so it is recorded as a claim and left to the
    # arbiter that already tests it.
    def roled(ms, role):
        return [m for m in ms if carrier_role(names.get(m)) == role]

    s_ox, s_red = roled(ks, "oxidised"), roled(ks, "reduced")
    p_ox, p_red = roled(kp, "oxidised"), roled(kp, "reduced")
    # MIRRORED, not merely present. The class cancels when what is oxidised on one side
    # is reduced on the other in the same number; a carrier appearing in the same role on
    # both sides is not a conjugate pair and has no cancelling partner.
    if (len(s_ox) == len(p_red) and len(s_red) == len(p_ox)
            and (s_ox or s_red) and (p_ox or p_red)):
        tally["carrier-class conjugate (claim only)"] += 1
        for m in sorted(set(s_ox) | set(s_red) | set(p_ox) | set(p_red)):
            # `any` rather than an element: the claim is about the reaction's SHAPE --
            # a role standing on both sides -- and pinning it to one element would read
            # as a conservation statement it does not make.
            claims.append((mnxr, "any", m, "carrier_class", None,
                           f"generic redox role '{names.get(m)}' pairs across the "
                           f"equation as one class; the cancellation is asserted by the "
                           f"NAME, so it is recorded here and admitted, if at all, by "
                           f"the rescue's per-element balance"))

    for X in ELEMENTS:
        # THE RESIDUE GUARD FIRST, and it gates BOTH arms. A count taken off a `*`
        # structure is exact for the explicit atoms and silent about the remainder, so
        # neither a forced pairing nor an inferred species count means anything until
        # the unspecified slots cancel across the equation.
        if _residues_cancel(ks, kp, counts_of, residue_of) is not True:
            tally[f"residue slots do not cancel: {X}"] += 1
            continue

        unknown = sorted({m for m in ks + kp
                          if counts_for(m, X, counts_of, formulas) is None})
        if len(unknown) == 1:
            # SPECIES-GRAIN. Conservation fixes the number; nothing fixes the atoms, and
            # a structureless species has no ranks to hang them on in any case.
            u = unknown[0]
            ns = sum(c for m in ks if m != u
                     for c in [counts_for(m, X, counts_of, formulas)])
            np_ = sum(c for m in kp if m != u
                      for c in [counts_for(m, X, counts_of, formulas)])
            k_u = Counter(ks)[u] - Counter(kp)[u]
            if k_u:
                need = (np_ - ns) / k_u
                if need >= 0 and float(need).is_integer():
                    claims.append((mnxr, X, u, "single_unknown", int(need),
                                   f"conservation of {X} over the {len(ks) + len(kp)} "
                                   f"participants left after cancellation fixes {u} at "
                                   f"{int(need)} {X}; a species-grain number, and no "
                                   f"atom correspondence follows from it"))
                    tally[f"single-unknown claim: {X}"] += 1
            continue
        if unknown:
            tally[f"more than one unknown: {X}"] += 1
            continue

        # `sorted`, not the set itself: a set-iteration order that changes between runs
        # is how a lane's yield wanders on byte-identical inputs.
        sx = [(m, counts_for(m, X, counts_of, formulas)) for m in sorted(set(ks))
              if counts_for(m, X, counts_of, formulas)]
        px = [(m, counts_for(m, X, counts_of, formulas)) for m in sorted(set(kp))
              if counts_for(m, X, counts_of, formulas)]
        if len(sx) != 1 or len(px) != 1:
            tally[f"not a single carrier per side: {X}"] += 1
            continue
        (sm, sn), (pm, pn) = sx[0], px[0]
        if sm == pm:
            continue
        if Counter(ks)[sm] != 1 or Counter(kp)[pm] != 1:
            # See the module header: two copies of one rank list are indistinguishable,
            # so a bijection over them would be a choice.
            tally[f"carrier written more than once: {X}"] += 1
            continue
        if sn != pn:
            tally[f"carriers disagree about the count: {X}"] += 1
            continue
        sr, pr = ranks_of.get((sm, X)), ranks_of.get((pm, X))
        if not sr or not pr or len(sr) != sn or len(pr) != pn:
            # The ranks ARE the node identity. A count that does not agree with the
            # number of ranks would index a pair row into a list of the wrong length.
            tally[f"count disagrees with atom_ranks: {X}"] += 1
            continue
        w = 1.0 / sn
        idxs = [(a, b, w) for a in sr for b in pr]
        forced.append((mnxr, X, sm, pm, len(idxs),
                       ",".join(str(int(i)) for i, _, _ in idxs),
                       ",".join(str(int(j)) for _, j, _ in idxs),
                       ",".join(repr(float(w)) for _, _, w in idxs)))
        tally[f"FORCED (reaction, element): {X}"] += 1
    return forced, claims, tally


# =====================================================================
# inputs
# =====================================================================

def load_context(lookups: Path, element_counts: Path):
    mets = pd.read_parquet(lookups / "metabolites.parquet",
                           columns=["mnxm", "name", "formula"])
    formulas = dict(zip(mets["mnxm"], mets["formula"]))
    names = dict(zip(mets["mnxm"], mets["name"]))
    from . import twins as T
    counts_of, residue_of, _src = T.read_element_counts(element_counts)
    ar = pd.read_parquet(lookups / "atom_ranks.parquet",
                         columns=["mnxm", "element", "ranks"])
    ranks_of = {(r.mnxm, r.element): list(r.ranks) for r in ar.itertuples(index=False)
                if len(r.ranks)}
    return formulas, names, counts_of, residue_of, ranks_of


def targets_from(worklist: Path, rescued: Path | None, everything=False):
    """The reactions no member will be given -- which is what this lane is FOR.

    `INDIGO_ADMITS` is the widest admission any member makes, so its complement is the
    set the ensemble cannot reach however it runs; the rescue's completions are removed
    because those become mappable and DO get a member. Stated by subtracting from the
    admit sets rather than by listing verdicts, so a new verdict does not have to be
    added here to be handled.

    `--targets all` widens it to the whole universe. The layer stack is additive and
    refuses rather than warns, so an algebra row for a reaction a mapper also reached is
    never used -- it is a cross-check, and it costs arithmetic rather than mapper time.
    """
    wl = pd.read_parquet(worklist, columns=["mnxr", "verdict"])
    if everything:
        return list(wl["mnxr"].astype(str))
    unreachable = set(wl.loc[~wl["verdict"].isin(W.INDIGO_ADMITS), "mnxr"].astype(str))
    if rescued is not None:
        unreachable -= set(pd.read_parquet(rescued, columns=["mnxr"])["mnxr"].astype(str))
    return sorted(unreachable)


def cmd_build(args):
    rx = pd.read_parquet(args.lookups / "reactions.parquet",
                         columns=["mnxr", "equation"])
    equations = dict(zip(rx["mnxr"], rx["equation"]))
    formulas, names, counts_of, residue_of, ranks_of = load_context(
        args.lookups, args.element_counts)
    targets = targets_from(args.worklist, args.rescued, args.targets == "all")
    print(f"[algebra] {len(targets):,} target reactions, {len(counts_of):,} metabolites "
          f"with a complete recount, {len(ranks_of):,} (metabolite, element) rank lists",
          flush=True)

    forced, claims, tally = [], [], Counter()
    for mnxr in targets:
        eq = equations.get(mnxr)
        if not isinstance(eq, str):
            tally["no equation"] += 1
            continue
        pe = AP.parse_equation(eq)
        if not pe:
            tally["unparseable equation"] += 1
            continue
        f, c, t = settle(mnxr, pe[0], pe[1], counts_of, residue_of, formulas,
                         ranks_of, names)
        forced += f
        claims += c
        tally.update(t)

    fdf = pd.DataFrame(forced, columns=list(FORCED_COLS))
    pq.write_table(pa.Table.from_pandas(fdf, schema=FORCED_SCHEMA, preserve_index=False),
                   args.out_forced, compression="zstd")
    cdf = pd.DataFrame(claims, columns=list(CLAIM_COLS))
    cdf.to_csv(args.out_claims, sep="\t", index=False)

    lines = ["kind\tkey\tn"]
    for k, v in sorted(tally.items()):
        lines.append(f"tally\t{k}\t{v}")
    lines.append(f"product\tforced_pair_rows\t{len(fdf)}")
    lines.append(f"product\tforced_reaction_elements\t"
                 f"{len(fdf.groupby(['mnxr', 'element'])) if len(fdf) else 0}")
    lines.append(f"product\tforced_reactions\t{fdf['mnxr'].nunique() if len(fdf) else 0}")
    # BANKED vs CLAIMED, side by side and in the summary rather than in prose. The
    # species-grain half is the larger number and the smaller result; a report that gave
    # one figure would be reporting the wrong one.
    lines.append(f"product\tspecies_grain_claims_NOT_banked\t{len(cdf)}")
    lines.append(f"product\ttarget_reactions\t{len(targets)}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 64)
    print("ALGEBRA -- what conservation settles, and at which grain")
    print("=" * 64)
    for k, v in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<46} {v:>8,}")
    print(f"\n  {'BANKED atom-grain pair rows':<46} {len(fdf):>8,}")
    print(f"  {'CLAIMED species-grain rows (not banked)':<46} {len(cdf):>8,}")
    print(f"\nwrote {args.out_forced}, {args.out_claims} and {args.out_summary}",
          flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--lookups", required=True, type=Path)
    p.add_argument("--element-counts", required=True, type=Path)
    p.add_argument("--worklist", required=True, type=Path)
    p.add_argument("--rescued", type=Path, default=None,
                   help="the rescued universe; its reactions DO reach a member, so they "
                        "come out of the target set")
    p.add_argument("--targets", choices=("unreachable", "all"), default="unreachable",
                   help="`unreachable` (default) is the reactions no member is given; "
                        "`all` runs the arithmetic over the whole universe as a "
                        "cross-check, which the additive stack makes free")
    p.add_argument("--out-forced", required=True,
                   help="the atom-grain pairs, already in layer shape")
    p.add_argument("--out-claims", required=True,
                   help="the species-grain claims. REPORTED, NEVER BANKED")
    p.add_argument("--out-summary", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
