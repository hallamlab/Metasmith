"""Predict which (reaction, element) a member will return nothing for, BEFORE it runs.

WHY THIS IS ALLOWED TO EXIST. The partial lane used to argue the opposite case in its own
docstring -- "there is no way to know which reactions ended with nothing without having run
the members" -- and that argument is correct about KNOWING. It is not the question. The
question is whether a prediction that is wrong in the cheap direction costs anything, and
the layer stack answers it: layers are additive and ordered, a lower layer may only claim
`(reaction, element)` combinations no layer above it claimed, and `aam_layers`' gates
REFUSE rather than warn. So a reduced submission built for a reaction that turns out to map
fine is never used. It cost mapper time and nothing else.

THAT ASYMMETRY IS THE WHOLE WARRANT. Over-predict and you pay compute; under-predict and
you lose exactly the coverage the partial lane exists to add. So the rule this module is
built around is stated once and enforced by the ledger it writes:

    THE FORECAST MAY ONLY EVER ADD SUBMISSIONS, NEVER REMOVE THEM.

Nothing here refuses a submission the old three-pass sequencing would have made. `settled`
is the one predicted outcome that suppresses an offer, and it does so only where the
conservation algebra has ALREADY BANKED that (reaction, element) -- an offer there would be
a submission whose result the stack discards unread.

THE MECHANISMS ARE NAMED, AND THREE OF THE SIX ARE EXACT. A prediction with a score is a
number nobody can audit; a prediction with a mechanism is a claim about a specific piece of
machinery that a reader can go and check.

  * `char_cap` / `atom_cap` -- our own constants, applied by our own adjudicator. Exact.
  * `context_window` -- RXNMapper's transformer accepts at most 512 tokens and returns an
    empty map above it. A property of the STRING, computable before the run. See
    CONTEXT_WINDOW_CHARS for the measurement that sets the threshold.
  * `prior_timeout` / `prior_hang` / `prior_empty` -- read from the previous run's records
    rather than predicted. Which reaction Indigo HANGS on is not a function of the string
    and does not need to be: the last run wrote it down.

THE EMPIRICAL HALF COVERS ROUGHLY HALF THE UNIVERSE and that is stated rather than hidden.
The prior run was killed -- Indigo reached 20,152 of ~44.6k attempts, the worklist stopped
at 20,000 of 83,795 -- so a reaction with no prior record is not evidence of success, it is
absence of evidence, and only the deterministic rules speak for it. `summary.tsv` reports
that split as a first-class number, because a recall figure computed over the half with
records would otherwise read as a recall figure over the universe.
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
from . import partial as P
from . import worklist as W

# RXNMapper's transformer takes at most 512 tokens. Its tokenizer is a SMILES regex, so
# tokens and characters are the same order and the limit lands somewhere in the high
# hundreds of characters -- which makes the exact threshold a measurement rather than an
# arithmetic conversion.
#
# MEASURED against the prior run's 44,547 recorded RXNMapper outcomes, joined to each
# reaction's string length:
#
#   returned a map   n=42,459   median  251 chars   p90   497
#   returned nothing n= 2,088   median 1040 chars   p10   702
#
#   threshold   recall of the silences   fraction of the universe offered
#       384              98.1%                        25.1%
#       512              97.2%                        13.2%
#       600              95.7%                         8.5%
#       700              90.1%                         5.5%
#       800              79.0%                         4.1%
#
# 512 is where the token limit says the wall is AND where the curve says it is: it catches
# 97.2% of the recorded silences for 13.2% of the universe, and the successful population's
# p90 is 497, so almost nothing that maps today is offered a reduction it will not use.
# Below 512 the offer grows twice as fast as the recall; above 700 the recall falls off a
# cliff. `research/fabfos/benchmarks/aam_forecast/` reproduces the table.
#
# The residual 2.8% is the argument for keeping the empirical half rather than tuning this
# number: those reactions returned nothing at UNDER 512 characters, so whatever stopped
# them was not the context window and no threshold on length will find them.
CONTEXT_WINDOW_CHARS = 512

# Named mechanisms, in the order they are tested. DETERMINISTIC FIRST, which is the plan's
# rule and also the informative one: a reaction that is over the character cap AND timed
# out last time is better described by the cap, because the cap is why no member will see
# it this time either.
MECHANISMS = ("char_cap", "atom_cap", "context_window",
              "prior_hang", "prior_timeout", "prior_empty")

# The closed set of predicted outcomes. `offer` is derived from this and from nothing else.
PREDICTIONS = (
    "silent",        # no member is expected to return a whole-reaction map at all
    "at_risk",       # at least one member is expected to return nothing; others may not
    "settled",       # the conservation algebra already banked this (reaction, element)
    "expected_ok",   # nothing fired; the whole-reaction map is expected to answer
)

OFFERED = ("silent", "at_risk")

FORECAST_COLS = ("key", "base_mnxr", "element", "submission_class", "predicted",
                 "mechanism", "mechanisms", "chars", "atoms", "offer")

FORECAST_SCHEMA = pa.schema([
    ("key", pa.string()), ("base_mnxr", pa.string()), ("element", pa.string()),
    ("submission_class", pa.string()), ("predicted", pa.string()),
    ("mechanism", pa.string()), ("mechanisms", pa.string()),
    ("chars", pa.int32()), ("atoms", pa.int32()), ("offer", pa.bool_()),
])


# =====================================================================
# the empirical half -- what the prior run wrote down
# =====================================================================

def read_prior(logs: Path | None):
    """`{mechanism -> set(mnxr)}` from a previous bake's per-reaction records.

    THREE FILES, THREE DIFFERENT KINDS OF SILENCE, and they are kept apart because they
    imply different repairs. A `timeout` is a statement about a budget. A HANG is a
    statement about the compiled search, and the only record of one is the difference
    between what a shard said it was about to attempt and what it wrote back. An `empty`
    is the mapper declining, which for RXNMapper is derived from a missing confidence
    because its output carries no status column at all.

    An absent directory yields three empty sets. The deterministic rules still fire, the
    summary says the empirical half was not consulted, and nothing silently reads as
    "the prior run recorded no failures".
    """
    out = {m: set() for m in ("prior_timeout", "prior_hang", "prior_empty")}
    if not logs:
        return out, False
    logs = Path(logs)
    if not logs.is_dir():
        return out, False

    ind = logs / "indigo_status.tsv"
    if ind.exists():
        d = pd.read_csv(ind, sep="\t")
        out["prior_timeout"] |= set(d.loc[d["status"] == "timeout", "mnxr"].astype(str))
        out["prior_empty"] |= set(
            d.loc[d["status"].isin(("error", "empty")), "mnxr"].astype(str))
        returned = set(d["mnxr"].astype(str))
    else:
        returned = set()

    unret = logs / "indigo_unreturned.tsv"
    if unret.exists():
        out["prior_hang"] |= set(pd.read_csv(unret, sep="\t")["mnxr"].astype(str))

    # ATTEMPTED MINUS RETURNED is the definition of a hang, and it is derived here rather
    # than trusted from `indigo_unreturned.tsv` alone -- that file was written by one
    # generation of the lane and the sidecars are what every generation leaves behind.
    att = logs / "attempted"
    if att.is_dir() and returned:
        attempted = set()
        for p in sorted(att.glob("*.attempted")):
            lines = [l.strip() for l in p.read_text().splitlines() if l.strip()]
            attempted |= {l for l in lines if not l.startswith("#")}
        out["prior_hang"] |= (attempted - returned)

    for name in ("rxnmapper", "localmapper"):
        f = logs / f"{name}_derived_status.tsv"
        if not f.exists():
            continue
        d = pd.read_csv(f, sep="\t")
        out["prior_empty"] |= set(
            d.loc[d["derived_status"] != "ok", "mnxr"].astype(str))

    n = {k: len(v) for k, v in out.items()}
    print(f"[forecast] prior run: {n}", flush=True)
    return out, True


def prior_coverage(logs: Path | None) -> set[str]:
    """Every reaction the prior run has ANY record for.

    The denominator that keeps a recall figure honest. The prior run did not converge, so
    a reaction absent from every one of its tables was never asked -- and treating that as
    "recorded as fine" is the one way the empirical half could quietly narrow the offer.
    """
    seen = set()
    if not logs or not Path(logs).is_dir():
        return seen
    logs = Path(logs)
    for f, col in ((logs / "indigo_status.tsv", "mnxr"),
                   (logs / "rxnmapper_derived_status.tsv", "mnxr"),
                   (logs / "localmapper_derived_status.tsv", "mnxr")):
        if f.exists():
            seen |= set(pd.read_csv(f, sep="\t")[col].astype(str))
    att = logs / "attempted"
    if att.is_dir():
        for p in sorted(att.glob("*.attempted")):
            seen |= {l.strip() for l in p.read_text().splitlines()
                     if l.strip() and not l.startswith("#")}
    return seen


# =====================================================================
# which elements a reaction is even about
# =====================================================================

def elements_of(subs, prods, counts_of) -> list:
    """The elements this reaction could possibly transfer.

    UNKNOWN COUNTS AS PRESENT, which is the `None`-is-not-a-zero rule applied to an offer
    rather than to a balance. A participant with no structure states nothing about its
    sulfur, and refusing to offer a sulfur reduction on that basis would withhold the
    submission from exactly the reactions the rescue exists to complete.
    """
    out = []
    for X in AP.ELEMENTS:
        i = AP.ELEMENTS.index(X)
        carries = False
        for m in list(subs) + list(prods):
            c = counts_of.get(m)
            if c is None or c[i] is None or c[i] > 0:
                carries = True
                break
        if carries:
            out.append(X)
    return out


def load_counts(element_counts: Path | None):
    """`{mnxm -> (C, N, S, P)}` with None where the count is genuinely unknown."""
    if not element_counts:
        return {}
    d = pd.read_parquet(element_counts, columns=["mnxm", "element", "n_atoms"])
    out = {}
    for r in d.itertuples(index=False):
        if r.element not in AP.ELEMENTS:
            continue
        out.setdefault(r.mnxm, [None] * len(AP.ELEMENTS))
        n = r.n_atoms
        out[r.mnxm][AP.ELEMENTS.index(r.element)] = None if pd.isna(n) else int(n)
    return {m: tuple(v) for m, v in out.items()}


# =====================================================================
# the forecast
# =====================================================================

def classify(chars, atoms, verdict, mnxr, prior) -> list:
    """Which mechanisms fire for this reaction, in MECHANISMS order."""
    fired = []
    if verdict == "too_long" or (chars is not None and chars > W.SMILES_LEN_LIMIT):
        fired.append("char_cap")
    if verdict == "oversize" or (atoms is not None and atoms > W.ATOM_LIMIT):
        fired.append("atom_cap")
    if chars is not None and chars > CONTEXT_WINDOW_CHARS:
        fired.append("context_window")
    for m in ("prior_hang", "prior_timeout", "prior_empty"):
        if mnxr in prior[m]:
            fired.append(m)
    return [m for m in MECHANISMS if m in fired]


def predict(fired, verdict) -> str:
    """`silent` when NO member can answer, `at_risk` when one cannot.

    The distinction is the atom cap: an `oversize` reaction is refused by the two neural
    members and taken by Indigo, so it is at risk rather than silent, and the reduced
    submission is what gives the other two something they can read.
    """
    if not fired:
        return "expected_ok"
    if "char_cap" in fired:
        return "silent"
    # A hang and a timeout are Indigo's; an empty is whichever member recorded it. Neither
    # says the other two members will be silent, so both are `at_risk` unless the string
    # itself is past a bound every member honours.
    return "at_risk"


def build(rows, counts_of, settled, prior):
    """`(forecast_rows, tally)` -- one row per (reaction, element) worth a prediction.

    `rows` is `(mnxr, verdict, chars, atoms, submission_class, subs, prods)`. The two
    submission classes that reach a mapper as a WHOLE reaction are forecast here; the
    reduced class is what this table produces and so cannot also be an input to it.
    """
    out, tally = [], Counter()
    for mnxr, verdict, chars, atoms, cls, subs, prods in rows:
        fired = classify(chars, atoms, verdict, mnxr, prior)
        pred = predict(fired, verdict)
        els = elements_of(subs, prods, counts_of)
        if not els:
            tally["reaction carries no tracked element"] += 1
            continue
        for X in els:
            if (mnxr, X) in settled:
                p, mech = "settled", ""
            else:
                p, mech = pred, (fired[0] if fired else "")
            offer = p in OFFERED
            out.append((P.make_key(mnxr, X), mnxr, X, cls, p, mech,
                        ",".join(fired), chars, atoms, offer))
            tally[f"predicted {p}"] += 1
            if mech:
                tally[f"mechanism {mech}"] += 1
    return out, tally


def load_targets(worklist: Path, rescued: Path | None):
    """Every submission a member will be given as a WHOLE reaction, with its class.

    `whole` and `completed` are disjoint on `mnxr` by construction and that is what lets
    one universe hold both under one key: `aam_rescue` completes reactions the worklist
    called `blocked_no_structure`, which is not in `INDIGO_ADMITS`. The check is asserted
    rather than assumed, because the day it stops being true two different molecules share
    one submission id.
    """
    wl = pd.read_parquet(worklist, columns=["mnxr", "verdict", "chars", "atoms"])
    whole = wl[wl["verdict"].isin(W.INDIGO_ADMITS)]
    rows = [(str(r.mnxr), str(r.verdict),
             None if pd.isna(r.chars) else int(r.chars),
             None if pd.isna(r.atoms) else int(r.atoms), "whole")
            for r in whole.itertuples(index=False)]

    # `too_long` never reaches a mapper at all, so it is not in the universe -- but it is
    # exactly what the partial lane is for, and leaving it out of the forecast would drop
    # the one population whose silence is certain.
    refused = wl[wl["verdict"] == "too_long"]
    rows += [(str(r.mnxr), str(r.verdict),
              None if pd.isna(r.chars) else int(r.chars),
              None if pd.isna(r.atoms) else int(r.atoms), "whole")
             for r in refused.itertuples(index=False)]

    if rescued is not None and Path(rescued).exists():
        rs = pd.read_parquet(rescued, columns=["mnxr", "verdict", "chars", "atoms"])
        clash = set(rs["mnxr"].astype(str)) & {r[0] for r in rows}
        if clash:
            raise SystemExit(
                f"[forecast] {len(clash)} reaction(s) are BOTH whole-mappable and "
                f"rescue-completed, e.g. {sorted(clash)[:3]}. The one-universe key "
                f"assumes those two classes are disjoint on mnxr; they are not, so two "
                f"different strings would be submitted under one id.")
        rows += [(str(r.mnxr), str(r.verdict),
                  None if pd.isna(r.chars) else int(r.chars),
                  None if pd.isna(r.atoms) else int(r.atoms), "completed")
                 for r in rs.itertuples(index=False)]
    return rows


def load_settled(forced: Path | None) -> set:
    """`(mnxr, element)` the conservation algebra already banked, so no offer is useful."""
    if not forced or not Path(forced).exists() or Path(forced).stat().st_size == 0:
        return set()
    d = pd.read_parquet(forced, columns=["mnxr", "element"])
    return set(zip(d["mnxr"].astype(str), d["element"].astype(str)))


def participants(reactions_parquet) -> dict:
    """`{mnxr: (substrates, products)}` from `lookup::reactions`.

    A LIST COLUMN COMES BACK FROM PARQUET AS A NUMPY ARRAY, and `array or []` raises
    `ValueError: the truth value of an array with more than one element is ambiguous`.
    It reads as an ordinary null-guard and is a crash on every reaction with two
    substrates -- which is to say on the first row. `worklist.adjudicate` reads the same
    three columns and has always written the explicit `is not None`; this is that.
    """
    rx = pd.read_parquet(Path(reactions_parquet),
                         columns=["mnxr", "substrates", "products"])

    def seq(v):
        if v is None:
            return []
        try:
            return list(v)
        except TypeError:
            # A null in a list column arrives as a float NaN, which is not iterable.
            return []

    return {r.mnxr: (seq(r.substrates), seq(r.products))
            for r in rx.itertuples(index=False)}


def cmd_build(args):
    parts = participants(Path(args.lookups) / "reactions.parquet")
    counts_of = load_counts(args.element_counts)
    settled = load_settled(args.forced)
    prior, had_logs = read_prior(args.prior_logs)
    covered = prior_coverage(args.prior_logs)

    targets = load_targets(args.worklist, args.rescued)
    rows = [(m, v, c, a, cls) + (parts.get(m, ([], []))) for m, v, c, a, cls in targets]
    print(f"[forecast] {len(rows):,} whole submissions, {len(settled):,} "
          f"(reaction, element) already settled by the algebra, "
          f"{len(counts_of):,} metabolites with counts", flush=True)

    out, tally = build(rows, counts_of, settled, prior)
    df = pd.DataFrame(out, columns=list(FORECAST_COLS))
    pq.write_table(pa.Table.from_pandas(df, schema=FORECAST_SCHEMA, preserve_index=False),
                   args.out, compression="zstd")

    n_offered = int(df["offer"].sum()) if len(df) else 0
    base = set(df.loc[df["offer"], "base_mnxr"]) if len(df) else set()
    with_record = {m for m, _v, _c, _a, _cls in targets if m in covered}

    lines = ["kind\tkey\tn"]
    for k, v in sorted(tally.items()):
        lines.append(f"tally\t{k}\t{v}")
    lines.append(f"product\trows\t{len(df)}")
    lines.append(f"product\toffered\t{n_offered}")
    lines.append(f"product\toffered_reactions\t{len(base)}")
    lines.append(f"product\twhole_submissions\t{len(targets)}")
    # THE HONEST DENOMINATOR. A recall claim computed over the reactions the prior run
    # reached is not a recall claim over the universe, and the prior run did not converge.
    lines.append(f"empirical\tprior_logs_present\t{int(had_logs)}")
    lines.append(f"empirical\treactions_with_a_prior_record\t{len(with_record)}")
    lines.append(f"empirical\treactions_without_one\t{len(targets) - len(with_record)}")
    for m in MECHANISMS:
        lines.append(f"mechanism\t{m}\t{int((df['mechanism'] == m).sum()) if len(df) else 0}")
    lines.append(f"limit\tcontext_window_chars\t{CONTEXT_WINDOW_CHARS}")
    lines.append(f"limit\tatom_limit\t{W.ATOM_LIMIT}")
    lines.append(f"limit\tchar_limit\t{W.SMILES_LEN_LIMIT}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("FORECAST -- one prediction per (reaction, element)")
    print("=" * 60)
    for p in PREDICTIONS:
        n = int((df["predicted"] == p).sum()) if len(df) else 0
        print(f"  {p:<24} {n:>8,}")
    print(f"\n  {'offered a reduction':<24} {n_offered:>8,} "
          f"over {len(base):,} reactions")
    print(f"  {'no prior record':<24} {len(targets) - len(with_record):>8,} of "
          f"{len(targets):,} submissions -- only the deterministic rules speak for these")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--lookups", required=True, type=Path)
    p.add_argument("--worklist", required=True, type=Path)
    p.add_argument("--rescued", type=Path, default=None,
                   help="the rescue's completed universe. Its reactions are submitted "
                        "too, so they need a prediction of their own")
    p.add_argument("--element-counts", type=Path, default=None,
                   help="lookup::element_counts -- which elements a reaction carries at "
                        "all. Unknown counts as PRESENT: a structureless participant "
                        "states nothing about its sulfur")
    p.add_argument("--forced", type=Path, default=None,
                   help="the algebra's forced_pairs.parquet. A (reaction, element) it "
                        "already banked is predicted `settled` and offered nothing -- the "
                        "one case where an offer is suppressed, and it is suppressed "
                        "because the stack would discard the result unread")
    p.add_argument("--prior-logs", type=Path, default=None,
                   help="a previous bake's logs/ directory. The empirical half: which "
                        "reaction Indigo hung on is not a function of the string, and "
                        "does not need to be, because the last run wrote it down")
    p.add_argument("--out", required=True, help="the forecast, parquet")
    p.add_argument("--out-summary", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
