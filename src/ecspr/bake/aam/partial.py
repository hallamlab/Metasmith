"""Map what can be mapped of a reaction no full map reaches -- per element, labelled.

WHAT THIS IS FOR. After the stoichiometric collapse, two populations still bank no
pairs and they fail for different reasons:

  * a small residual the size cut still refuses -- genuinely large distinct chemistry,
    78 reactions of the 57,593 buildable;
  * a much larger set the ledger already names, where a member returned NOTHING. That
    is not a chemistry verdict at all: RXNMapper's transformer accepts at most 512
    tokens and these are the long reactions -- median SMILES length 983 against the
    universe's 268. A context-window limit, eating a BIASED sample: the reactions with
    the most and largest cofactors.

That bias is what makes the fix sound rather than a loosening. FOR A SINGLE ELEMENT
most of a long reaction is irrelevant -- `3 NADPH + 3 NADP+` blows the limit while
contributing no sulfur at all -- so the submission is an ELEMENT REDUCTION: keep only
the participants whose formula carries X, require X to balance across the kept set, and
map the much smaller reaction that results. Only X's pairs are read out. See
`atom_pairs.reduce_for_element` for why dropping the X-free participants cannot
re-route an X atom the way a strip does.

THE EXACT ARM GOES FIRST. `atom_pairs.forced_pairs` emits the pairing conservation
leaves no choice about -- one substrate and one product carrying X in equal counts is a
unique bijection at full weight; n > 1 is the doubly-stochastic completion. That needs
no mapper at all, so it is tried before a submission is built, and what it produces is
not partial in any sense that matters: nothing was chosen and nothing was dropped.
It is still LABELLED partial, because it reaches only one element of the reaction.

WHY A LANE AND NOT A FLAG. This follows the shape `aam_rescue` already established: a
transform produces a universe parquet, the same three members map it, and the result is
laid down as a layer. The partial layer is L4, laid down LAST, so it can only claim
(reaction, element) combinations no layer above it claimed -- and `aam_layers`'
additive gates REFUSE rather than warn, which is what makes "a partial map never
overwrites a real one" a fact about machinery rather than a promise in a docstring.

AND IT IS LABELLED EVERYWHERE IT IS READ. That is the half the goal names explicitly: a
partially-mapped reaction must be distinguishable from a banked one and from a dropped
one, or the ledger is telling three different stories with one word. So it carries its
own verdict (`partial`), its own ledger outcomes, and its own method/source on every
pair row -- the ledger is not the only thing downstream reads, and a partial pair that
reads as banked is the stop line.
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
from . import worklist as W

# The composite key. A member reads `{mnxr -> rxn_smiles}` and cannot hold two rows for
# one reaction, so a per-element submission needs a per-element id. `#` is not in any
# MNXR id, and `split_key` is the only place the convention is spelled out.
KEY_SEP = "#"

UNIVERSE_COLS = ("mnxr", "verdict", "rxn_smiles", "base_mnxr", "element",
                 "sub_mnxms", "prod_mnxms", "atoms", "chars", "collapsed")

# The forced arm's pairs, in the EXTRACTOR's compact shape -- `atom_pairs.PAIR_COLS` --
# so `aam_layers.explode` reads them with no special case and stamps their method and
# source the same way it stamps every other layer's. Emitting the already-exploded shape
# would need a second reader, which is a second place for the atom identity contract to
# be implemented slightly differently.
#
# Keyed on the REAL MNXR, not the composite key: that exists only because a member's
# universe is one row per reaction, and the layer stack is already keyed on (reaction,
# element). Carrying it further would make every partial pair look like a reaction the
# bake's vocabulary has never heard of.
FORCED_COLS = AP.PAIR_COLS

# Provenance, and it must not be mistakable for a full map's. `aam_layers.explode`
# stamps method/source per layer; these are the strings that layer carries.
FORCED_METHOD = "partial_forced"
REDUCED_METHOD = "partial_reduced"
PARTIAL_SOURCE = "partial"

# A partial pair is a real correspondence over a real element, so it is not discounted
# for being partial -- the discount would be a second, hidden claim about reliability.
# What it IS is narrower, and the method column is where that is said.
PARTIAL_CONFIDENCE = 1.0


def make_key(mnxr: str, element: str) -> str:
    return f"{mnxr}{KEY_SEP}{element}"


def split_key(key: str):
    """`MNXR123#N` -> `("MNXR123", "N")`. Refuses anything else.

    A key that does not split is a partial product that reached a reader expecting a
    plain MNXR, which would silently attribute one element's map to a whole reaction.
    """
    base, sep, el = str(key).rpartition(KEY_SEP)
    if not sep or el not in AP.ELEMENTS:
        raise ValueError(f"{key!r} is not a partial-lane key (expected MNXR{KEY_SEP}<element>)")
    return base, el


def load_lookups(lookups: Path):
    """`(formulas, smiles_of, ranks_of)` -- everything the reduction needs.

    `ranks_of` is the canonical rank of each (metabolite, element)'s atoms, taken from
    the metabolite's OWN molecule rather than from a reaction template. That is the same
    identity the mapped path emits, which is what lets a forced pair and a mapped pair
    name the same node -- see `atom_pairs.canonical_ranks` for why a template index
    would not.
    """
    mets = pd.read_parquet(lookups / "metabolites.parquet",
                           columns=["mnxm", "formula", "smiles"])
    formulas = dict(zip(mets["mnxm"], mets["formula"]))
    smiles_of = {m: s for m, s in zip(mets["mnxm"], mets["smiles"])
                 if isinstance(s, str) and s}
    ar = pd.read_parquet(lookups / "atom_ranks.parquet",
                         columns=["mnxm", "element", "ranks"])
    ranks_of = {(r.mnxm, r.element): list(r.ranks) for r in ar.itertuples(index=False)
                if len(r.ranks)}
    return formulas, smiles_of, ranks_of


def _write(sub_mnxms: list, prod_mnxms: list, smiles_of: dict) -> str:
    """One SMILES component per kept participant, in order. The submission string."""
    return (".".join(smiles_of[m] for m in sub_mnxms) + ">>"
            + ".".join(smiles_of[m] for m in prod_mnxms))


def _uniq(mnxms: list) -> list:
    """Each distinct metabolite once, in order -- the reduction's own collapse.

    Keyed on the MNXM rather than on the SMILES text, which is where this differs from
    `worklist.collapse`. Two distinct metabolites that happen to share a structure stay
    two components here, so `match_mols` keeps both name candidates and dilutes between
    them rather than having one of them silently disappear from the submission.
    """
    seen, out = set(), []
    for m in mnxms:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out


def _still_balances(sub_mnxms: list, prod_mnxms: list, formulas: dict, X: str) -> bool:
    """Does X still balance once each participant is written once per side?

    THE GUARD THAT MAKES COLLAPSING A REDUCTION SAFE, and the reason it is not simply
    `worklist.collapse` applied again. Collapsing is faithful exactly when the reaction
    is a whole multiple of a per-copy reaction: `16 ATP >> 16 ADP + 16 Pi` collapses to
    `ATP >> ADP + Pi` and every atom's destination is the one it had. When the
    coefficients are not a common multiple -- `A + 3 B >> C`, one X in each B and five
    in C -- the collapsed string offers three X sources for five X destinations, and the
    mapper must then CHOOSE which three of C's atoms were filled. That choice is
    fabricated, and it is fabricated at rank level where nothing downstream can see it.

    So the balance is re-checked against the collapsed lists, and a collapse that breaks
    it is not taken: the submission stays expanded and is refused for size as before.
    A refusal is a gap, which is honest; a rank-level invention is not.
    """
    ns = np_ = 0
    for ms, sink in ((sub_mnxms, "s"), (prod_mnxms, "p")):
        for m in ms:
            c = AP.count_element(formulas.get(m), X)
            if c is None:
                return False
            if sink == "s":
                ns += c
            else:
                np_ += c
    return ns == np_


def build(targets: list, equations: dict, formulas: dict, smiles_of: dict,
          ranks_of: dict, char_limit: int, atom_limit: int):
    """`(universe_rows, forced_rows, tally)` for the reactions in `targets`.

    Per reaction, per element, in this order: the forced arm if conservation settles it,
    otherwise a reduced submission if the element balances across the kept participants,
    otherwise nothing -- and the tally says which, so the lane's yield is a number rather
    than a hope.

    A reduction that is still over a size cut gets the same second reading pass 1 gives a
    whole reaction: written once per distinct participant, and taken only if X still
    balances that way. See `_still_balances` for why that guard is not optional.
    """
    uni, forced, tally = [], [], Counter()
    for mnxr in targets:
        eq = equations.get(mnxr)
        if not isinstance(eq, str):
            tally["no equation"] += 1
            continue
        pe = AP.parse_equation(eq)
        if not pe:
            tally["unparseable equation"] += 1
            continue
        subs, prods = pe

        # The exact arm, once for the whole reaction: it decides per element internally
        # and needs no mapper, so anything it settles never becomes a submission.
        fp = AP.forced_pairs(subs, prods, formulas, ranks_of)
        for (X, sm, pm), idxs in fp.items():
            forced.append((mnxr, X, sm, pm, len(idxs),
                           ",".join(str(int(i)) for i, _, _ in idxs),
                           ",".join(str(int(j)) for _, j, _ in idxs),
                           ",".join(repr(float(w)) for _, _, w in idxs)))
        settled = {X for (X, _s, _p) in fp}
        tally["forced (reaction, element)"] += len(settled)

        for X in AP.ELEMENTS:
            if X in settled:
                continue
            red = AP.reduce_for_element(subs, prods, formulas, X)
            if red is None:
                tally[f"no balanced reduction: {X}"] += 1
                continue
            ks, kp = red
            missing = [m for m in ks + kp if m not in smiles_of]
            if missing:
                # A participant with no structure cannot be written into a submission,
                # and leaving it out is the strip this lane exists not to do.
                tally["reduction has a structureless participant"] += 1
                continue
            smi = _write(ks, kp, smiles_of)
            # The SAME two size cuts pass 1 applies, on the SAME constants. A reduced
            # submission is a new string; the whole point is that it is small, so one
            # that is not is not a submission worth making.
            n = W.count_atoms(smi) if len(smi) <= char_limit else None

            # THE SECOND READING, and the reason it belongs here too. `reduce_for_element`
            # preserves stoichiometry, so its submission is a stoichiometric EXPANSION --
            # the exact measure the whole scope exists to stop applying. A phosphorus
            # reduction of nitrogenase is `16 ATP >> 16 ADP + 16 Pi`, over a thousand
            # atoms of one molecule written sixteen times.
            #
            # Attempted only where the expanded submission would be refused, so nothing
            # that fits today moves -- the same monotonicity the worklist's collapse has.
            collapsed = False
            if len(smi) > char_limit or n is None or n > atom_limit:
                cks, ckp = _uniq(ks), _uniq(kp)
                if ((len(cks), len(ckp)) != (len(ks), len(kp))
                        and _still_balances(cks, ckp, formulas, X)):
                    smi_c = _write(cks, ckp, smiles_of)
                    n_c = W.count_atoms(smi_c) if len(smi_c) <= char_limit else None
                    if n_c is not None and n_c <= atom_limit:
                        ks, kp, smi, n, collapsed = cks, ckp, smi_c, n_c, True

            if len(smi) > char_limit or n is None:
                # The character cap is universal: it is the bound that stops anything
                # parsing MNXR144749's 80.7 MB string, and no member is exempt from it.
                tally["reduction over the character cap"] += 1
                continue

            # THE ATOM CAP ROUTES, IT DOES NOT REFUSE -- the same correction pass 1 got,
            # carried through here with the same word. `oversize` means the neural
            # members will not see this submission and Indigo will; both read the verdict
            # through `worklist.NEURAL_ADMITS` / `INDIGO_ADMITS`, so the lane needs no
            # column of its own and no member needs a special case.
            verdict = "mappable" if n <= atom_limit else "oversize"
            uni.append((make_key(mnxr, X), verdict, smi, mnxr, X,
                        list(ks), list(kp), int(n), int(len(smi)), collapsed))
            tally["reduced submission"] += 1
            if verdict == "oversize":
                tally["reduced submission over the atom cap (Indigo only)"] += 1
            if collapsed:
                tally["reduced submission (collapsed)"] += 1
    return uni, forced, tally


UNIVERSE_SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("verdict", pa.string()), ("rxn_smiles", pa.string()),
    ("base_mnxr", pa.string()), ("element", pa.string()),
    ("sub_mnxms", pa.list_(pa.string())), ("prod_mnxms", pa.list_(pa.string())),
    ("atoms", pa.int32()), ("chars", pa.int32()),
    # WHICH string this submission holds. The extractor does not need telling -- it
    # recomputes the distinct-structure count from the same participant lists and the
    # strict guard accepts either -- so this exists for the reader, and for the summary
    # to be able to say how much of the lane's reach the collapse is responsible for.
    ("collapsed", pa.bool_()),
])

FORCED_SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("element", pa.string()),
    ("substrate", pa.string()), ("product", pa.string()), ("n_atoms", pa.int32()),
    ("sub_idx", pa.string()), ("prod_idx", pa.string()), ("pair_w", pa.string()),
])


def targets_from(worklist: Path, rescued: Path | None, covered: list) -> list:
    """The reactions this lane is for: refused by size, or reached by nobody.

    TWO POPULATIONS, and neither is derivable from a reaction's text.

    The first is the worklist's own size refusals -- `oversize` and `too_long` after the
    collapse has had its second reading, 78 reactions of the 57,593 buildable.

    The second is every reaction that was ADMITTED and still ended with nothing. That is
    a fact about a RUN, not about a reaction: it covers a member returning an empty
    mapping (RXNMapper's 512-token limit, the population this lane is really aimed at),
    a mapping the extractor refused as `stripped`, and one that named no pair. All three
    end in the same place and want the same treatment, so the lane asks the question the
    products can answer -- "did any member produce a pair for this reaction" -- rather
    than reading three status codes and having to keep the list of them current.

    `covered` is every member's pairs parquet, from both passes. An ABSENT one is
    refused rather than treated as covering nothing: the gap is defined by subtraction,
    so a missing member would silently make the gap the whole universe.
    """
    wl = pd.read_parquet(worklist, columns=["mnxr", "verdict"])
    refused = list(wl.loc[wl["verdict"].isin(("oversize", "too_long")), "mnxr"])

    admitted = set(wl.loc[wl["verdict"] == "mappable", "mnxr"].astype(str))
    if rescued is not None:
        admitted |= set(pd.read_parquet(rescued, columns=["mnxr"])["mnxr"].astype(str))

    done = set()
    for c in covered:
        c = Path(c)
        if not c.exists() or c.stat().st_size == 0:
            raise SystemExit(
                f"[partial] {c} is missing or empty. This lane's target set is what the "
                f"members did NOT reach, so an absent member would make the target set "
                f"the whole universe -- a pass 3 over everything, at full mapper cost, "
                f"reported as a gap.")
        done |= set(pd.read_parquet(c, columns=["mnxr"])["mnxr"].astype(str))

    unreached = sorted(admitted - done)
    seen = set(refused)
    return refused + [m for m in unreached if m not in seen]


def cmd_build(args):
    rx = pd.read_parquet(args.lookups / "reactions.parquet",
                         columns=["mnxr", "equation"])
    equations = dict(zip(rx["mnxr"], rx["equation"]))
    formulas, smiles_of, ranks_of = load_lookups(args.lookups)
    targets = targets_from(args.worklist, args.rescued, args.covered or [])
    print(f"[partial] {len(targets):,} target reactions "
          f"({len(equations):,} equations, {len(smiles_of):,} structures)", flush=True)

    uni, forced, tally = build(targets, equations, formulas, smiles_of, ranks_of,
                               args.char_limit, args.atom_limit)

    udf = pd.DataFrame(uni, columns=list(UNIVERSE_COLS))
    pq.write_table(pa.Table.from_pandas(udf, schema=UNIVERSE_SCHEMA,
                                        preserve_index=False),
                   args.out, compression="zstd")
    fdf = pd.DataFrame(forced, columns=list(FORCED_COLS))
    pq.write_table(pa.Table.from_pandas(fdf, schema=FORCED_SCHEMA,
                                        preserve_index=False),
                   args.out_forced, compression="zstd")

    lines = ["kind\tkey\tn"]
    for k, v in sorted(tally.items()):
        lines.append(f"tally\t{k}\t{v}")
    lines.append(f"product\treduced_submissions\t{len(udf)}")
    lines.append(f"product\tcollapsed_submissions\t"
                 f"{int(udf['collapsed'].sum()) if len(udf) else 0}")
    lines.append(f"product\tforced_pair_rows\t{len(fdf)}")
    n_fre = len(fdf.groupby(["mnxr", "element"])) if len(fdf) else 0
    lines.append(f"product\tforced_reaction_elements\t{n_fre}")
    lines.append(f"product\ttarget_reactions\t{len(targets)}")
    lines.append(f"limit\tatom_limit\t{args.atom_limit}")
    lines.append(f"limit\tchar_limit\t{args.char_limit}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("PARTIAL LANE -- what the reduction reaches")
    print("=" * 60)
    for k, v in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<44} {v:>8,}")
    print(f"\n  {'reduced submissions':<44} {len(udf):>8,}")
    print(f"  {'forced pair rows':<44} {len(fdf):>8,}")
    print(f"\nwrote {args.out}, {args.out_forced} and {args.out_summary}", flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--lookups", required=True, type=Path)
    p.add_argument("--worklist", required=True, type=Path,
                   help="the adjudicated universe; its refused verdicts are one target set")
    p.add_argument("--rescued", type=Path, default=None,
                   help="the rescued universe; its reactions were admitted too, so one "
                        "that still banked nothing belongs in the target set")
    p.add_argument("--covered", nargs="*", type=Path,
                   help="every member's pairs parquet, from both passes. The target set "
                        "is defined by SUBTRACTION from these, so a missing one is "
                        "refused rather than treated as covering nothing.")
    p.add_argument("--atom-limit", type=int, default=W.ATOM_LIMIT)
    p.add_argument("--char-limit", type=int, default=W.SMILES_LEN_LIMIT)
    p.add_argument("--out", required=True, help="the partial universe, parquet")
    p.add_argument("--out-forced", required=True,
                   help="the conservation-forced pairs, already in layer shape")
    p.add_argument("--out-summary", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
