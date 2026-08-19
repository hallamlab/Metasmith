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

CONTEXT_WINDOW_CHARS = 512

MECHANISMS = ("char_cap", "atom_cap", "context_window",
              "prior_hang", "prior_timeout", "prior_empty")

PREDICTIONS = (
    "silent",
    "at_risk",
    "settled",
    "expected_ok",
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


def read_prior(logs: Path | None):
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


def elements_of(subs, prods, counts_of) -> list:
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


def classify(chars, atoms, verdict, mnxr, prior) -> list:
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
    if not fired:
        return "expected_ok"
    if "char_cap" in fired:
        return "silent"
    return "at_risk"


def build(rows, counts_of, settled, prior):
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
    wl = pd.read_parquet(worklist, columns=["mnxr", "verdict", "chars", "atoms"])
    whole = wl[wl["verdict"].isin(W.INDIGO_ADMITS)]
    rows = [(str(r.mnxr), str(r.verdict),
             None if pd.isna(r.chars) else int(r.chars),
             None if pd.isna(r.atoms) else int(r.atoms), "whole")
            for r in whole.itertuples(index=False)]

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
    if not forced or not Path(forced).exists() or Path(forced).stat().st_size == 0:
        return set()
    d = pd.read_parquet(forced, columns=["mnxr", "element"])
    return set(zip(d["mnxr"].astype(str), d["element"].astype(str)))


def participants(reactions_parquet) -> dict:
    rx = pd.read_parquet(Path(reactions_parquet),
                         columns=["mnxr", "substrates", "products"])

    def seq(v):
        if v is None:
            return []
        try:
            return list(v)
        except TypeError:
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
