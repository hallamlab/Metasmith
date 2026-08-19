"""Map what can be mapped of a reaction no full map reaches -- per element, labelled.

WHAT THIS IS FOR. After the stoichiometric collapse, two populations still bank no
pairs and they fail for different reasons:

  * a small residual the size cut still refuses -- genuinely large distinct chemistry,
    78 reactions of the 57,593 buildable;
  * a much larger set where a member returns NOTHING. That is not a chemistry verdict at
    all: RXNMapper's transformer accepts at most 512 tokens and these are the long
    reactions -- median SMILES length 1,040 against the successful population's 251. A
    context-window limit, eating a BIASED sample: the reactions with the most and largest
    cofactors.

That bias is what makes the fix sound rather than a loosening. FOR A SINGLE ELEMENT
most of a long reaction is irrelevant -- `3 NADPH + 3 NADP+` blows the limit while
contributing no sulfur at all -- so the submission is an ELEMENT REDUCTION: keep only
the participants whose formula carries X, require X to balance across the kept set, and
map the much smaller reaction that results. Only X's pairs are read out. See
`atom_pairs.reduce_for_element` for why dropping the X-free participants cannot
re-route an X atom the way a strip does.

THE TARGETS COME FROM THE FORECAST NOW, AND THIS DOCSTRING USED TO ARGUE THE OPPOSITE.
It said there was no way to know which reactions would end with nothing without having
run the members, and that a lane guessing from reaction length would be aiming at a proxy
for the thing it can simply be told. The first half is still true and the second half was
the wrong conclusion, for a reason that is about the layer stack rather than about
prediction:

  * BEING TOLD COST THREE PASSES. The members had to run over the worklist, then over the
    rescue's completions, then a third time over these reductions, because this lane's
    target set was defined by subtracting finished products. Nine mapper lanes, three
    sequential waits, for a lane whose submissions are small.
  * BEING WRONG COSTS NOTHING IN ONE DIRECTION. Layers are additive and ordered and their
    gates refuse rather than warn, so a reduced submission built for a reaction that maps
    fine is never claimed by anything. Over-offer and you pay mapper time; under-offer and
    you lose exactly the coverage this lane exists to add.
  * AND THE PREDICTION IS NOT A PROXY. `aam_forecast` fires on NAMED MECHANISMS -- our own
    two size caps, RXNMapper's context window as a property of the string, and the prior
    run's own records of what hung, timed out and came back empty. Its offer rule catches
    97.2% of the previous run's recorded RXNMapper silences. A proxy for length would have
    been the wrong shape; a mechanism is not.

So the trade taken here is: one pass, an over-offered target set, and a recall number
that is measured and reported rather than assumed. `--forecast` is the whole change.

IT READS THE RESCUE'S STRUCTURES, and until it did it could not reach a single
rescue-completed reaction: the reduction is built from the RAW equation and a reduction
holding a structureless participant is refused, so the reactions the rescue exists to
unblock were invisible here. The crosswalk supplies the SMILES and the formula is derived
from it, because MetaNetX has neither. Placeholders are excluded -- their atoms are
invented, and a formula taken off one would let the balance test certify a balance made of
them. `load_lookups` reports how many structures this added, as its own number, because it
is a coverage lever independent of the forecast.

THE EXACT ARM STILL GOES FIRST. `atom_pairs.forced_pairs` emits the pairing conservation
leaves no choice about -- one substrate and one product carrying X in equal counts is a
unique bijection at full weight; n > 1 is the doubly-stochastic completion. That needs
no mapper at all, so it is tried before a submission is built, and what it produces is
not partial in any sense that matters: nothing was chosen and nothing was dropped.
It is still LABELLED partial, because it reaches only one element of the reaction.

WHY A LANE AND NOT A FLAG. A transform produces a submission table, `aam_universe`
concatenates it with the other two classes, the three members map the one table, and the
result is laid down as its own layer -- LAST, so it can only claim (reaction, element)
combinations no layer above it claimed. `aam_layers`' additive gates REFUSE rather than
warn, which is what makes "a partial map never overwrites a real one" a fact about
machinery rather than a promise in a docstring.

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

KEY_SEP = "#"

UNIVERSE_COLS = ("mnxr", "verdict", "rxn_smiles", "base_mnxr", "element",
                 "sub_mnxms", "prod_mnxms", "atoms", "chars", "collapsed")

FORCED_COLS = AP.PAIR_COLS

FORCED_METHOD = "partial_forced"
REDUCED_METHOD = "partial_reduced"
PARTIAL_SOURCE = "partial"

PARTIAL_CONFIDENCE = 1.0


def make_key(mnxr: str, element: str) -> str:
    return f"{mnxr}{KEY_SEP}{element}"


def split_key(key: str):
    base, sep, el = str(key).rpartition(KEY_SEP)
    if not sep or el not in AP.ELEMENTS:
        raise ValueError(f"{key!r} is not a partial-lane key (expected MNXR{KEY_SEP}<element>)")
    return base, el


def _formula_of(smi: str):
    from rdkit import Chem
    from rdkit.Chem import rdMolDescriptors
    m = Chem.MolFromSmiles(smi)
    return rdMolDescriptors.CalcMolFormula(m) if m is not None else None


def load_lookups(lookups: Path, rescue: Path | None = None):
    mets = pd.read_parquet(lookups / "metabolites.parquet",
                           columns=["mnxm", "formula", "smiles"])
    formulas = dict(zip(mets["mnxm"], mets["formula"]))
    smiles_of = {m: s for m, s in zip(mets["mnxm"], mets["smiles"])
                 if isinstance(s, str) and s}
    n_rescued = 0
    if rescue is not None:
        resolved = AP.load_resolved(Path(rescue) / "crosswalk.tsv")
        placeholders = set(AP.load_placeholders(Path(rescue) / "placeholders.tsv"))
        for m, smi in resolved.items():
            if m in placeholders or m in smiles_of:
                continue
            if not isinstance(smi, str) or not smi:
                continue
            f = _formula_of(smi)
            if f is None:
                continue
            smiles_of[m], formulas[m] = smi, f
            n_rescued += 1
    ar = pd.read_parquet(lookups / "atom_ranks.parquet",
                         columns=["mnxm", "element", "ranks"])
    ranks_of = {(r.mnxm, r.element): list(r.ranks) for r in ar.itertuples(index=False)
                if len(r.ranks)}
    return formulas, smiles_of, ranks_of, n_rescued


def _write(sub_mnxms: list, prod_mnxms: list, smiles_of: dict) -> str:
    return (".".join(smiles_of[m] for m in sub_mnxms) + ">>"
            + ".".join(smiles_of[m] for m in prod_mnxms))


def _uniq(mnxms: list) -> list:
    seen, out = set(), []
    for m in mnxms:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out


def _still_balances(sub_mnxms: list, prod_mnxms: list, formulas: dict, X: str) -> bool:
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


def build(targets: dict, equations: dict, formulas: dict, smiles_of: dict,
          ranks_of: dict, char_limit: int, atom_limit: int):
    uni, forced, tally = [], [], Counter()
    for mnxr in sorted(targets):
        offered = targets[mnxr]
        eq = equations.get(mnxr)
        if not isinstance(eq, str):
            tally["no equation"] += 1
            continue
        pe = AP.parse_equation(eq)
        if not pe:
            tally["unparseable equation"] += 1
            continue
        subs, prods = pe

        fp = AP.forced_pairs(subs, prods, formulas, ranks_of)
        for (X, sm, pm), idxs in fp.items():
            forced.append((mnxr, X, sm, pm, len(idxs),
                           ",".join(str(int(i)) for i, _, _ in idxs),
                           ",".join(str(int(j)) for _, j, _ in idxs),
                           ",".join(repr(float(w)) for _, _, w in idxs)))
        settled = {X for (X, _s, _p) in fp}
        tally["forced (reaction, element)"] += len(settled)

        for X in offered:
            if X in settled:
                tally[f"settled by the forced arm: {X}"] += 1
                continue
            red = AP.reduce_for_element(subs, prods, formulas, X)
            if red is None:
                tally[f"no balanced reduction: {X}"] += 1
                continue
            ks, kp = red
            missing = [m for m in ks + kp if m not in smiles_of]
            if missing:
                tally["reduction has a structureless participant"] += 1
                continue
            smi = _write(ks, kp, smiles_of)
            n = W.count_atoms(smi) if len(smi) <= char_limit else None

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
                tally["reduction over the character cap"] += 1
                continue

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
    ("collapsed", pa.bool_()),
])

FORCED_SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("element", pa.string()),
    ("substrate", pa.string()), ("product", pa.string()), ("n_atoms", pa.int32()),
    ("sub_idx", pa.string()), ("prod_idx", pa.string()), ("pair_w", pa.string()),
])


def targets_from(forecast: Path) -> dict:
    f = Path(forecast)
    if not f.exists():
        raise SystemExit(
            f"[partial] {f} is not here. This lane's targets are the forecast's offers; "
            f"without it the lane would produce an empty universe and report it as "
            f"'nothing needed a reduction'.")
    d = pd.read_parquet(f, columns=["base_mnxr", "element", "offer"])
    d = d[d["offer"].astype(bool)]
    out = {}
    for mnxr, X in zip(d["base_mnxr"].astype(str), d["element"].astype(str)):
        out.setdefault(mnxr, [])
        if X not in out[mnxr]:
            out[mnxr].append(X)
    return {m: [X for X in AP.ELEMENTS if X in els] for m, els in out.items()}


def cmd_build(args):
    rx = pd.read_parquet(args.lookups / "reactions.parquet",
                         columns=["mnxr", "equation"])
    equations = dict(zip(rx["mnxr"], rx["equation"]))
    formulas, smiles_of, ranks_of, n_rescued = load_lookups(args.lookups, args.rescue)
    targets = targets_from(args.forecast)
    n_offers = sum(len(v) for v in targets.values())
    print(f"[partial] {len(targets):,} target reactions, {n_offers:,} offered "
          f"(reaction, element) pairs "
          f"({len(equations):,} equations, {len(smiles_of):,} structures, "
          f"{n_rescued:,} of them the rescue's)", flush=True)

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
    lines.append(f"input\trescued_structures_merged\t{n_rescued}")
    lines.append(f"product\toffered_pairs\t{n_offers}")
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
    p.add_argument("--forecast", required=True, type=Path,
                   help="interm::aam_forecast -- the (reaction, element) pairs a member "
                        "is expected to return nothing for, with the mechanism named. "
                        "This lane's whole target set, read rather than re-derived")
    p.add_argument("--rescue", type=Path, default=None,
                   help="interm::aam_rescue. Its crosswalk supplies the structures this "
                        "lane needs to reduce a rescue-completed reaction at all; its "
                        "placeholder list is what keeps the invented ones out")
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
