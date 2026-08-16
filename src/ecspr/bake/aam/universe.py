"""One submission table. Three classes of row. The only thing a mapper member reads.

WHAT THIS REPLACES. The graph used to run the three members THREE TIMES over three
universes -- the worklist, then the rescue's completions, then the partial lane's element
reductions -- because each universe could only be built after the pass before it had
finished. The rescue genuinely needs the worklist's verdicts; the partial lane did not
need a finished run, it needed to know which reactions would end with nothing, and
`aam_forecast` answers that from the string and from the last run's records. With both
built up front the three passes are one, and nine mapper lanes are three.

THE KEYS DO NOT COLLIDE, AND THAT IS A FACT ABOUT THE CLASSES RATHER THAN A CONVENTION.

  * `whole`     -- key is the bare MNXR. Verdict in `INDIGO_ADMITS`.
  * `completed` -- key is the bare MNXR. The rescue completes reactions the worklist
                   called `blocked_no_structure`, which is NOT in `INDIGO_ADMITS`, so the
                   two classes are disjoint on mnxr and neither can shadow the other.
  * `reduced`   -- key is `MNXR#X` (`partial.KEY_SEP`), which is not a valid MNXR, so it
                   collides with neither of the above even for a reaction that is in both.

Uniqueness is ASSERTED here rather than trusted, because the failure it prevents is
silent: a member reads `{key -> smiles}` into a dict, so a duplicate key does not raise,
it drops one of two different molecules submitted under one name.

WHAT THE CLASS IS FOR DOWNSTREAM. Layer identity used to be "which pass produced this",
which made layer membership depend on scheduling. It is now "which class of submission
this row answers", which the row carries -- a strictly better key, and the reason the
class travels in the table rather than being re-derived from a key's shape.

THE MEMBERS NEED NO SPECIAL CASE. Each row carries its own `verdict`, so
`NEURAL_ADMITS` / `INDIGO_ADMITS` route the oversized tail to Indigo exactly as they did
when the verdict came from the worklist alone.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import partial as P
from . import worklist as W

SUBMISSION_CLASSES = ("whole", "completed", "reduced")

UNIVERSE_COLS = ("mnxr", "verdict", "rxn_smiles", "base_mnxr", "element",
                 "sub_mnxms", "prod_mnxms", "atoms", "chars", "collapsed",
                 "submission_class")

# `mnxr` HOLDS THE SUBMISSION KEY AND `base_mnxr` HOLDS THE REACTION, which is the
# convention `aam.partial` already established and is kept rather than improved on: every
# member reads the column called `mnxr` as the id it maps under, and renaming it here
# would mean touching three members to gain a better word.
UNIVERSE_SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("verdict", pa.string()), ("rxn_smiles", pa.string()),
    ("base_mnxr", pa.string()), ("element", pa.string()),
    ("sub_mnxms", pa.list_(pa.string())), ("prod_mnxms", pa.list_(pa.string())),
    ("atoms", pa.int32()), ("chars", pa.int32()), ("collapsed", pa.bool_()),
    ("submission_class", pa.string()),
])


def _int(v):
    return None if v is None or pd.isna(v) else int(v)


def from_worklist(worklist: Path) -> list:
    """The whole-reaction class: every row the adjudication admits to a member."""
    wl = pd.read_parquet(worklist, columns=["mnxr", "verdict", "rxn_smiles",
                                            "atoms", "chars", "atoms_collapsed",
                                            "chars_collapsed", "collapsed"])
    wl = wl[wl["verdict"].isin(W.INDIGO_ADMITS) & wl["rxn_smiles"].notna()]
    out = []
    for r in wl.itertuples(index=False):
        # WHICHEVER STRING THE WORKLIST WROTE is the one the member is handed, so the
        # size columns must describe THAT string. The adjudication keeps the expanded
        # measure in `atoms`/`chars` so the pre-collapse yield curve stays recomputable;
        # here the collapsed pair wins where it exists, because a cache keyed on the
        # submission string is checked against what was actually sent.
        atoms = _int(r.atoms_collapsed) if r.collapsed else _int(r.atoms)
        chars = _int(r.chars_collapsed) if r.collapsed else _int(r.chars)
        out.append((str(r.mnxr), str(r.verdict), str(r.rxn_smiles), str(r.mnxr), None,
                    [], [], atoms, chars, bool(r.collapsed), "whole"))
    return out


def from_rescue(rescued: Path | None) -> list:
    """The completed class: reactions a curated structure made mappable at all."""
    if rescued is None or not Path(rescued).exists():
        return []
    rs = pd.read_parquet(rescued)
    out = []
    for r in rs.itertuples(index=False):
        out.append((str(r.mnxr), str(r.verdict), str(r.rxn_smiles), str(r.mnxr), None,
                    [], [], _int(r.atoms), _int(r.chars), False, "completed"))
    return out


def from_partial(partial: Path | None) -> list:
    """The reduced class: one submission per (reaction, element) the forecast offered.

    ITS PARTICIPANT LISTS TRAVEL WITH IT and the two whole classes' do not. That is not an
    inconsistency: the extractor re-derives a whole reaction's participants from the
    equation and gets the right answer, while a reduction's participants are deliberately
    fewer than the equation's, so re-deriving them would compare a reduced map against a
    full template and refuse every one as `stripped`.
    """
    if partial is None or not Path(partial).exists():
        return []
    pu = pd.read_parquet(partial)
    out = []
    for r in pu.itertuples(index=False):
        out.append((str(r.mnxr), str(r.verdict), str(r.rxn_smiles), str(r.base_mnxr),
                    str(r.element), list(r.sub_mnxms), list(r.prod_mnxms),
                    _int(r.atoms), _int(r.chars), bool(r.collapsed), "reduced"))
    return out


def assemble(worklist: Path, rescued: Path | None, partial: Path | None):
    """`(rows, tally)` -- the three classes concatenated, keys asserted unique."""
    rows = from_worklist(worklist) + from_rescue(rescued) + from_partial(partial)
    tally = Counter(r[-1] for r in rows)

    seen = {}
    dupes = []
    for r in rows:
        key = r[0]
        if key in seen:
            dupes.append((key, seen[key], r[-1]))
        seen[key] = r[-1]
    if dupes:
        raise SystemExit(
            f"[universe] {len(dupes)} duplicate submission key(s), e.g. {dupes[:3]}.\n"
            f"    A member reads this table into `{{key -> smiles}}`, so a duplicate does "
            f"not raise -- it silently drops one of two DIFFERENT strings submitted under "
            f"one id, and the map that survives is a map of the other molecule. The "
            f"classes are supposed to be disjoint by construction: `whole` takes "
            f"{list(W.INDIGO_ADMITS)}, `completed` takes what the worklist called "
            f"blocked_no_structure, and `reduced` keys on MNXR{P.KEY_SEP}<element>.")

    over = [r[0] for r in rows if r[8] is not None and r[8] > W.SMILES_LEN_LIMIT]
    if over:
        raise SystemExit(
            f"[universe] {len(over):,} submissions exceed the {W.SMILES_LEN_LIMIT}-char "
            f"cap, e.g. {over[0]}. That cap is universal -- it is what stops anything "
            f"parsing MNXR144749's 80.7 MB string -- so this is two limits that have "
            f"drifted apart, not a long reaction slipping through.")
    return rows, tally


def cmd_build(args):
    rows, tally = assemble(args.worklist, args.rescued, args.partial)
    df = pd.DataFrame(rows, columns=list(UNIVERSE_COLS))
    pq.write_table(pa.Table.from_pandas(df, schema=UNIVERSE_SCHEMA, preserve_index=False),
                   args.out, compression="zstd")

    neural = int(df["verdict"].isin(W.NEURAL_ADMITS).sum()) if len(df) else 0
    indigo = int(df["verdict"].isin(W.INDIGO_ADMITS).sum()) if len(df) else 0

    lines = ["kind\tkey\tn"]
    for c in SUBMISSION_CLASSES:
        lines.append(f"submission_class\t{c}\t{int(tally.get(c, 0))}")
    lines.append(f"admits\tneural\t{neural}")
    lines.append(f"admits\tindigo\t{indigo}")
    lines.append(f"admits\tindigo_only\t{indigo - neural}")
    lines.append(f"product\tsubmissions\t{len(df)}")
    lines.append(f"product\treactions\t{df['base_mnxr'].nunique() if len(df) else 0}")
    lines.append(f"product\tcollapsed\t{int(df['collapsed'].sum()) if len(df) else 0}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("UNIVERSE -- one table, three submission classes, one pass")
    print("=" * 60)
    for c in SUBMISSION_CLASSES:
        print(f"  {c:<24} {int(tally.get(c, 0)):>8,}")
    print(f"  {'TOTAL':<24} {len(df):>8,} submissions over "
          f"{df['base_mnxr'].nunique() if len(df) else 0:,} reactions")
    print(f"\n  {'to the neural members':<24} {neural:>8,}")
    print(f"  {'to Indigo':<24} {indigo:>8,} "
          f"({indigo - neural:,} of them Indigo alone -- over the atom cap)")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--worklist", required=True, type=Path,
                   help="interm::aam_worklist -- the whole class")
    p.add_argument("--rescued", type=Path, default=None,
                   help="the rescue's rescued.parquet -- the completed class")
    p.add_argument("--partial", type=Path, default=None,
                   help="the partial lane's universe -- the reduced class")
    p.add_argument("--out", required=True, help="the one submission table, parquet")
    p.add_argument("--out-summary", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
