"""Predict which reactions each thermo member will be silent on, BEFORE it runs.

WHY A FORECAST IS CHEAPER HERE THAN IT IS FOR THE MAPPERS. The AAM forecast has to
reason about a transformer's context window and about which reaction Indigo hangs
inside; it can only be right about half its universe and says so. This one is
almost entirely deterministic. Both thermo members abstain by walking a reaction's
participants in equation order and returning at the FIRST one they cannot use --
and every test they apply to a participant is a pure function of one row of
`chem_prop.tsv`. So the unit of prediction is the COMPOUND, evaluated once, and the
reaction inherits the verdict of its first failing participant. That is ~47.6k
compound evaluations rather than 83,795 reactions times mean participant count, and
it needs neither eQuilibrator nor torch.

THE MECHANISMS ARE NAMED, AND THAT IS THE POINT. A prediction with a score is a
number nobody can audit; a prediction with a mechanism is a claim about a specific
piece of machinery a reader can go and check. Each mechanism below is the literal
`reason` string the member returns, so the backtest is a string comparison against
a member table rather than a calibration.

  * `no_props` / `no_smiles` -- the participant has no InChIKey / no SMILES in
    chem_prop. EXACT: the members test exactly this.
  * `unparseable` / `wildcard` -- RDKit cannot read the participant's SMILES, or
    reads an R-group atom in it. EXACT: `thermo_dgbyg._has_wildcard` is called here
    on the same string.
  * `unbalanced` -- the heavy (non-hydrogen) atoms do not balance. SUFFICIENT, not
    necessary: measured against the r7 dGbyG table, every heavy-unbalanced reaction
    that reached the check was called unbalanced (0 false positives in 27,534), and
    63 of 2,481 real abstentions were heavy-balanced. dGbyG transforms protonation
    state at pH 7, so its hydrogen and charge ledger is not a function of the SMILES
    and is deliberately not predicted from one. `backtest` reports that residual.
  * `unresolved` -- eQuilibrator holds no compound for the participant's InChIKey.
    NOT a function of chem_prop: its cache is frozen at an older MetaNetX. This is
    the honest analogue of the AAM forecast's `prior_empty`, and it is MEASURED
    rather than guessed -- `resolve` runs the member's own `_compound` once per
    distinct participant and writes the answer down, which is a 24k-compound pass
    instead of the 83,795-reaction one.

WHAT IT DELIBERATELY DOES NOT PREDICT. eQuilibrator's `uninformative` (a returned
sigma past `SIGMA_CEILING`) is a property of its covariance matrix, reachable only
by asking it for the number. A reaction predicted `expected_ok` for eQ may still
land there, so `expected_ok` means "no abstention mechanism fires", never "a vote
is guaranteed". `backtest` measures that gap instead of hiding it.

THIS LANE IS READ-ONLY WITH RESPECT TO WHAT THE MEMBERS ARE ASKED. Unlike the AAM
forecast, which feeds a reduced-submission lane, nothing here narrows a member's
universe: `drive eval` still evaluates every reaction it is given. The forecast's
whole product is accounting -- which member will be silent, by which mechanism, and
on account of which compound.
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

import pandas as pd

from .refdata import load_mnxr_stoich, load_mnxm_props

# The closed set. A member's reason that is not in here is a new failure mode, and
# `backtest` names it rather than letting it land in an unnamed bucket.
MECHANISMS = ("no_stoich", "no_props", "unresolved",
              "no_smiles", "unparseable", "wildcard", "unbalanced",
              "expected_ok")

# Which mechanisms each member can possibly report. Asserted, so a mechanism cannot
# be attributed to a member whose code has no branch for it.
MEMBER_MECHANISMS = {
    "eq": ("no_stoich", "no_props", "unresolved", "expected_ok"),
    "dgbyg": ("no_stoich", "no_smiles", "unparseable", "wildcard", "unbalanced",
              "expected_ok"),
}

# The reasons a member can return that this lane does not attempt to predict. They
# are counted in the backtest as a named residual, never folded into agreement.
UNPREDICTED = {"eq": ("uninformative",), "dgbyg": ()}

FORECAST_COLS = ("mnxr", "member", "mechanism", "blocker", "n_participants")


# =====================================================================
# the compound pass -- every member test that is a function of one chem_prop row
# =====================================================================

class CompoundProfile:
    """Per-participant answers to every question a member asks about a compound.

    Built once per distinct MNXM. `heavy` is `{element: count}` over non-hydrogen
    atoms with explicit Hs added, which is what the balance check sums; it is None
    when there is no readable structure to count.
    """

    __slots__ = ("inchikey", "smiles", "parseable", "wildcard", "heavy")

    def __init__(self, inchikey, smiles, parseable, wildcard, heavy):
        self.inchikey = inchikey
        self.smiles = smiles
        self.parseable = parseable
        self.wildcard = wildcard
        self.heavy = heavy


def profile_compounds(participants, props):
    """`{mnxm: CompoundProfile}` -- one RDKit parse per distinct participant.

    RDKit is imported here rather than at module scope for the reason
    `thermo_dgbyg` does it: the direction images are equilibrator OR torch, and a
    module that cannot import in one of them takes the whole step down with it.
    """
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")

    out = {}
    for m in participants:
        p = props.get(m) or {}
        smiles = p.get("smiles")
        parseable, wildcard, heavy = None, None, None
        if smiles is not None:
            mol = Chem.MolFromSmiles(smiles)
            parseable = mol is not None
            if mol is not None:
                wildcard = any(a.GetAtomicNum() == 0 for a in mol.GetAtoms())
                heavy = collections.Counter(
                    a.GetSymbol() for a in Chem.AddHs(mol).GetAtoms()
                    if a.GetSymbol() != "H")
        out[m] = CompoundProfile(p.get("inchikey"), smiles, parseable, wildcard, heavy)
    return out


# =====================================================================
# the propagation -- first failing participant, in equation order
# =====================================================================

def predict_eq(stoich, profiles, resolved):
    """`(mechanism, blocker)` for the eQuilibrator member.

    Mirrors `EquilibratorMember.dgr`'s loop exactly: walk participants in the
    equation's own order and return at the first one that fails, because that is
    the reason the member will record. Order matters -- a reaction with one
    structureless participant and one unresolvable one reports whichever comes
    first, and a forecast that sorted the participants would disagree with the
    member on which mechanism was to blame while agreeing that it was silent.
    """
    for m in stoich:
        p = profiles.get(m)
        if p is None or not p.inchikey:
            return "no_props", m
        if resolved is not None and not resolved.get(m, True):
            return "unresolved", m
    return "expected_ok", ""


def predict_dgbyg(stoich, profiles):
    """`(mechanism, blocker)` for the dGbyG member, same short-circuit.

    The balance test runs only after every participant passed, which is the
    member's own order: an unreadable participant means there is no ledger to
    balance, so `no_smiles` beats `unbalanced` rather than competing with it.
    """
    for m in stoich:
        p = profiles.get(m)
        if p is None or p.smiles is None:
            return "no_smiles", m
        if not p.parseable:
            return "unparseable", m
        if p.wildcard:
            return "wildcard", m

    tot = collections.Counter()
    for m, coeff in stoich.items():
        for el, n in profiles[m].heavy.items():
            tot[el] += coeff * n
    off = sorted(el for el, v in tot.items() if abs(v) > 1e-9)
    if off:
        return "unbalanced", ",".join(off)
    return "expected_ok", ""


def build(universe, stoich_by_mnxr, profiles, resolved, subs=None):
    """One row per (reaction, member). `rows, tally`.

    `subs` rewrites the equation the same way `drive.cmd_eval` will, so the forecast
    describes the chemistry the run will actually be handed. Omitting it here and passing
    it there is the failure mode this argument exists to prevent: the accounting would
    describe a bake that was never built.
    """
    rows, tally = [], collections.Counter()
    for mnxr in universe:
        s = stoich_by_mnxr.get(mnxr)
        if s is None:
            for member in ("eq", "dgbyg"):
                rows.append((mnxr, member, "no_stoich", "", 0))
                tally[f"{member} no_stoich"] += 1
            continue
        st = subs.rewrite(s[0]) if subs is not None else s[0]
        for member, (mech, blocker) in (("eq", predict_eq(st, profiles, resolved)),
                                        ("dgbyg", predict_dgbyg(st, profiles))):
            assert mech in MEMBER_MECHANISMS[member], (
                f"{member} cannot report {mech!r}; the mechanism set and the "
                f"member's branches have drifted apart")
            rows.append((mnxr, member, mech, blocker, len(st)))
            tally[f"{member} {mech}"] += 1
    return rows, tally


# =====================================================================
# verbs
# =====================================================================

def _participants(stoich_by_mnxr, universe):
    out = set()
    for mnxr in universe:
        s = stoich_by_mnxr.get(mnxr)
        if s is not None:
            out |= set(s[0])
    return out


def _universe(path, stoich_by_mnxr):
    if path:
        return json.load(open(path))
    return sorted(stoich_by_mnxr)


def _props(chem_prop, mnxm_only):
    props = load_mnxm_props(chem_prop)
    if mnxm_only:
        dropped = [k for k in props if not k.startswith("MNXM")]
        for k in dropped:
            del props[k]
        print(f"[forecast] --mnxm-only: reproducing the pre-fix namespace filter, "
              f"{len(dropped)} accession(s) withheld: {sorted(dropped)}", flush=True)
    return props


def cmd_resolve(args):
    """Ask eQuilibrator once per distinct participant whether it holds the compound.

    The member memoises this per process and a sharded run fragments that cache N
    ways; doing it here instead makes it a 24k-compound pass whose answer is a file.
    Uses `EquilibratorMember._compound` rather than a reimplementation, so the
    InChI-first / InChIKey-fallback precedence cannot drift out of step with the
    member whose behaviour this is predicting.
    """
    from .thermo_eq import EquilibratorMember

    stoich = load_mnxr_stoich(args.reac_prop)
    universe = _universe(args.universe, stoich)
    props = _props(args.chem_prop, args.mnxm_only)
    cpds = sorted(m for m in _participants(stoich, universe)
                  if (props.get(m) or {}).get("inchikey"))
    print(f"[resolve] {len(cpds):,} distinct participants carry an InChIKey", flush=True)

    # CHECKPOINTED, because this pass is long enough to be interrupted and holds
    # nothing that has to be recomputed together: each answer is one compound's,
    # independent of the rest. Writing the whole table only at the end means an
    # interruption at 60% costs 60%, and the cache load alone is 15 s of it.
    rows = []
    if args.resume and Path(args.resume).exists():
        done = pd.read_parquet(args.resume)
        rows = list(done.itertuples(index=False, name=None))
        have = set(done["mnxm"])
        cpds = [m for m in cpds if m not in have]
        print(f"[resolve] resuming: {len(rows):,} already answered, "
              f"{len(cpds):,} left", flush=True)

    def flush():
        pd.DataFrame(rows, columns=["mnxm", "inchikey", "resolved"]).to_parquet(
            args.out, index=False)

    member = EquilibratorMember()
    for i, m in enumerate(cpds, 1):
        p = props[m]
        rows.append((m, p["inchikey"], member._compound(p["inchikey"], p.get("inchi")) is not None))
        if i % 2000 == 0:
            flush()
            print(f"[resolve] {i:,}/{len(cpds):,} (checkpointed)", flush=True)
    flush()
    df = pd.read_parquet(args.out)
    n = int(df["resolved"].sum())
    print(f"[resolve] {n:,}/{len(df):,} ({n/max(1,len(df)):.1%}) resolve -> {args.out}",
          flush=True)
    return 0


def cmd_build(args):
    from .refdata import load_mnxm_names
    from . import substitute

    stoich = load_mnxr_stoich(args.reac_prop)
    universe = _universe(args.universe, stoich)
    props = _props(args.chem_prop, args.mnxm_only)
    # `Substitutions()` with no tables covers nothing, so the default path is the one the
    # baselines were taken under -- not a mode, an empty table.
    subs = substitute.load(args.substitutions, props,
                           load_mnxm_names(args.chem_prop) if args.substitutions else {})
    props = subs.props(props)
    parts = _participants(stoich, universe) | set(subs.models)
    print(f"[forecast] {len(universe):,} reactions, {len(parts):,} distinct "
          f"participants, {len(props):,} chem_prop rows with a structure"
          + (f", {len(subs):,} substitutions" if len(subs) else ""), flush=True)

    profiles = profile_compounds(parts, props)

    resolved = None
    if args.resolution:
        r = pd.read_parquet(args.resolution)
        resolved = dict(zip(r["mnxm"].astype(str), r["resolved"].astype(bool)))
        # `resolve` CHECKPOINTS INTO ITS OWN --out, so a partial table is indistinguishable
        # from a finished one by inspection. Building on one under-reports `unresolved`
        # and inflates the eq arm's expected_ok, and every downstream count inherits it
        # without a warning anywhere. Same universe rule as `cmd_resolve`.
        want = {m for m in parts if (props.get(m) or {}).get("inchikey")}
        if not want <= set(resolved):
            raise SystemExit(
                f"[forecast] {args.resolution} answers {len(resolved):,} compounds but "
                f"{len(want):,} participants carry an InChIKey ({len(want - set(resolved)):,} "
                f"missing). A `resolve` pass that was interrupted leaves exactly this -- "
                f"finish it (--resume {args.resolution}) before building on it")
        print(f"[forecast] eQuilibrator resolution for {len(resolved):,} compounds; "
              f"{int(r['resolved'].sum()):,} resolve", flush=True)
    else:
        print("[forecast] no --resolution: `unresolved` is NOT predicted, so the eq "
              "arm's `expected_ok` is an upper bound on what it will answer",
              flush=True)

    rows, tally = build(universe, stoich, profiles, resolved, subs)
    df = pd.DataFrame(rows, columns=list(FORECAST_COLS))
    df.to_parquet(args.out, index=False)

    silent = (df[df["mechanism"] != "expected_ok"]
              .groupby("mnxr").size())
    both_silent = int((silent == 2).sum())

    lines = ["kind\tkey\tn"]
    for k, v in sorted(tally.items()):
        lines.append(f"tally\t{k}\t{v}")
    lines.append(f"product\trows\t{len(df)}")
    lines.append(f"product\treactions\t{len(universe)}")
    # THE NUMBER THE RE-BAKE IS ABOUT. A reaction both members are silent on has no
    # thermo vote at all, so it can only reach tier 3 (curated) or tier 0.
    lines.append(f"product\tboth_members_silent\t{both_silent}")
    lines.append(f"product\tat_least_one_member_speaks\t{len(universe) - both_silent}")
    lines.append(f"input\tparticipants\t{len(parts)}")
    lines.append(f"input\tparticipants_with_inchikey\t"
                 f"{sum(1 for m in parts if profiles[m].inchikey)}")
    lines.append(f"input\tparticipants_with_smiles\t"
                 f"{sum(1 for m in parts if profiles[m].smiles is not None)}")
    lines.append(f"input\tparticipants_parseable\t"
                 f"{sum(1 for m in parts if profiles[m].parseable)}")
    lines.append(f"input\tparticipants_wildcard\t"
                 f"{sum(1 for m in parts if profiles[m].wildcard)}")
    lines.append(f"input\tresolution_supplied\t{int(resolved is not None)}")
    lines.append(f"input\tsubstitutions\t{len(subs)}")
    lines.append(f"input\tmnxm_only\t{int(bool(args.mnxm_only))}")
    for member, mechs in sorted(MEMBER_MECHANISMS.items()):
        for m in mechs:
            lines.append(f"mechanism\t{member}:{m}\t{tally.get(f'{member} {m}', 0)}")
    lines.append(f"limit\theavy_atom_tolerance\t1e-9")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("DIRECTION FORECAST -- one prediction per (reaction, member)")
    print("=" * 60)
    for member in sorted(MEMBER_MECHANISMS):
        print(f"  {member}")
        for m in MEMBER_MECHANISMS[member]:
            print(f"    {m:<20} {tally.get(f'{member} {m}', 0):>8,}")
    print(f"\n  both members silent      {both_silent:>8,} of {len(universe):,}")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


def cmd_backtest(args):
    """Score the forecast against member tables a real run already wrote.

    NO RUN IS NEEDED FOR THIS. The deployed bake's two member parquets carry the
    real `reason` for all 83,795 reactions, so the forecast's accuracy is a join.
    Score the forecast built with `--mnxm-only` against a pre-water-fix bake, and
    without it against a post-fix one; scoring the wrong pair measures the fix
    rather than the forecast, and the summary records which was asked for.
    """
    fc = pd.read_parquet(args.forecast)
    lines = ["kind\tkey\tn"]
    for member, path in (("eq", args.member_eq), ("dgbyg", args.member_dgbyg)):
        if not path:
            continue
        got = pd.read_parquet(path)[["mnxr", "reason"]]
        # `ok` is the member's name for what the forecast calls `expected_ok`; every
        # other reason should be the mechanism verbatim.
        got["actual"] = got["reason"].where(got["reason"] != "ok", "expected_ok")
        j = fc[fc["member"] == member].merge(got, on="mnxr", how="inner")
        unknown = sorted(set(j["reason"]) - set(MECHANISMS) - {"ok"}
                         - set(UNPREDICTED[member]))
        agree = j["mechanism"] == j["actual"]
        print(f"\n=== {member}: {len(j):,} reactions in both ===")
        print(f"  exact mechanism agreement  {int(agree.sum()):>8,} "
              f"({agree.mean():.2%})")
        # SILENT-VS-SPEAKS is the coarser claim, and it is the one the rescue scope
        # rests on: getting the mechanism name wrong costs an explanation, getting
        # the silence wrong costs a count.
        pred_silent = j["mechanism"] != "expected_ok"
        real_silent = j["actual"] != "expected_ok"
        print(f"  silent/speaks agreement    {int((pred_silent == real_silent).sum()):>8,} "
              f"({(pred_silent == real_silent).mean():.2%})")
        print(f"  predicted silent, spoke    {int((pred_silent & ~real_silent).sum()):>8,}")
        print(f"  predicted to speak, silent {int((~pred_silent & real_silent).sum()):>8,}")
        for r in UNPREDICTED[member]:
            n = int((j["reason"] == r).sum())
            print(f"  of those, `{r}` (never predicted): {n:,}")
            lines.append(f"residual\t{member}:{r}\t{n}")
        if unknown:
            print(f"  UNNAMED reasons in the member table: {unknown}")
        lines.append(f"agreement\t{member}:exact\t{int(agree.sum())}")
        lines.append(f"agreement\t{member}:silence\t{int((pred_silent == real_silent).sum())}")
        lines.append(f"agreement\t{member}:rows\t{len(j)}")
        lines.append(f"agreement\t{member}:unnamed_reasons\t{len(unknown)}")
        print("\n  confusion (rows where they differ):")
        conf = j.loc[~agree].groupby(["mechanism", "actual"]).size().sort_values(ascending=False)
        print(conf.head(12).to_string() if len(conf) else "    none")
        for (p, a), n in conf.items():
            lines.append(f"confusion\t{member}:{p}->{a}\t{n}")
    if args.out_summary:
        Path(args.out_summary).write_text("\n".join(lines) + "\n")
        print(f"\nwrote {args.out_summary}", flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("resolve"); p.set_defaults(fn=cmd_resolve)
    p.add_argument("--universe", default=None,
                   help="the reaction list; defaults to every MNXR in reac_prop")
    p.add_argument("--reac-prop", required=True)
    p.add_argument("--chem-prop", required=True)
    p.add_argument("--mnxm-only", action="store_true",
                   help="reproduce the pre-fix namespace filter, which withheld "
                        "WATER. For backtesting against a member table written "
                        "before the fix; never for a forecast of a future run.")
    p.add_argument("--resume", default=None,
                   help="a partial table from an interrupted run (usually the same "
                        "path as --out). Its compounds are skipped.")
    p.add_argument("--out", required=True)

    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--universe", default=None)
    p.add_argument("--reac-prop", required=True)
    p.add_argument("--chem-prop", required=True)
    p.add_argument("--resolution", default=None,
                   help="`resolve`'s table. Without it `unresolved` is not "
                        "predicted at all, which is stated in the summary rather "
                        "than absorbed into the other mechanisms.")
    p.add_argument("--mnxm-only", action="store_true",
                   help="see `resolve --mnxm-only`")
    p.add_argument("--substitutions", default=None,
                   help="a substitution table directory. Omit for the configuration\n"
                        "every baseline was taken under. MUST match what `drive eval`\n"
                        "is given, or the accounting describes a bake nobody built.")
    p.add_argument("--out", required=True)
    p.add_argument("--out-summary", required=True)

    p = sub.add_parser("backtest"); p.set_defaults(fn=cmd_backtest)
    p.add_argument("--forecast", required=True)
    p.add_argument("--member-eq", default=None)
    p.add_argument("--member-dgbyg", default=None)
    p.add_argument("--out-summary", default=None)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
