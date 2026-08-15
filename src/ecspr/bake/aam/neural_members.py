"""The two neural AAM members: build reaction SMILES, map them, cache the result.

Ported from the deployed pair ``19b_rxnmapper_universe.py`` and
``18_localmapper_inference.py``. Merged into one module because the two members must see
the SAME reaction SMILES: they are the CORRELATED pair whose agreement the combiner
discounts, and that discount only means anything if the disagreement it measures is
between mappers rather than between two slightly different SMILES builders.

THE TWO SCRIPTS DID NOT DO THE SAME JOB, and reading them as one shape with two mapper
calls is what produced this lane's worst bug. RXNMapper swept the universe; LocalMapper
ran over a 487-reaction GAP -- reactions the pipeline could reach and had no mapping for
-- and every one of its 401 contributions to the deployed table comes from that set.
Generalising it into a 57,522-reaction sweep bought nothing and cost a 24-hour lane and
two OOM kills. Hence ``--covered``: the gap is what the other members did not reach, and
the caller decides which role a member plays.

Reaction SMILES are built from ``reac_prop``'s MNXR equation and ``chem_prop``'s
per-metabolite SMILES, stoichiometry expanded (a coefficient of 2 writes the metabolite
twice), so an atom map's indices line up with the participants the equation names. A
reaction whose participants are not all structurally known is not mapped at all -- a
partial reaction SMILES would map the atoms it does have onto the wrong destinations.

RESUMABLE, and it has to be: this is the longest single step in the reference build
(~57.5k reactions, hours per member). The cache is an append-only TSV keyed on ``mnxr``;
a restart re-reads it and does only what is missing, so a killed run costs the reaction
it was on and nothing else.

Output is one TSV per member, in the schema ``aam_combine.load_mapper`` reads:

    mnxr | rxn_smiles | mapped_rxn_smiles | confidence

No tau cut is applied here. The combiner is what decides how much a member's confidence
is worth, and a low-confidence map is still a vote to dilute against -- dropping it here
would remove the disagreement the ensemble is built to record.
"""
from __future__ import annotations

import argparse
import re
import signal
import sys
import time
from pathlib import Path

import pandas as pd

from . import shard as aam_shard

# The equation term grammar, verbatim from the deployed builder. NOTE it matches only
# `MNXM...@compartment` terms -- specials like `WATER@MNXD1` and `BIOMASS@MNXD1` do NOT
# match, which is deliberate here (they have no chem_prop SMILES) and is also the exact
# mismatch that made the extractor refuse every water-bearing reaction when the same
# regex was reused downstream. See ecspr_atom_pairs.EQ_TERM for that side of it.
EQ_TERM = re.compile(r"(\d+(?:\.\d+)?)\s+(MNXM\w+)@\w+")

# A reaction SMILES past this length is a polymer/macromolecule the transformers cannot
# usefully attend over, and attempting it costs minutes for a map nothing will trust.
SMILES_LEN_LIMIT = 8000

COLUMNS = ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence")

# Seconds per reaction before LocalMapper's map is recorded as a timeout rather than
# waited on. Sized from the run this bound exists because of: the bulk of the gap set
# went through at 1-17 s per reaction, and the ones that did not took tens of minutes
# each with no ceiling in sight. 240 s is an order of magnitude above the slow end of
# normal and still bounds a shard's worst case to something a declared duration covers.
DEFAULT_LM_TIMEOUT_S = 240


class _Timeout(Exception):
    pass


def _alarm(_sig, _frm):
    raise _Timeout()


def parse_equation(eq: str):
    if "=" not in eq:
        return None
    lhs, rhs = eq.split("=", 1)

    def side(s):
        return [(float(c), m) for c, m in EQ_TERM.findall(s)]

    L, R = side(lhs), side(rhs)
    if not L or not R:
        return None
    return L, R


def load_smiles(chem_prop: Path, mnxm_set: set[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 9:
                continue
            mnxm = parts[0]
            if mnxm not in mnxm_set:
                continue
            smi = parts[8].strip()
            if smi:
                out[mnxm] = smi
    return out


def load_all_equations(reac_prop: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with open(reac_prop) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            mnxr = parts[0]
            if mnxr == "EMPTY":
                continue
            out[mnxr] = parts[1]
    return out


def build_rxn_smiles(eq: str, smi_map: dict[str, str]):
    parsed = parse_equation(eq)
    if parsed is None:
        return None, "unparseable_eq"
    L, R = parsed

    def side(side_list):
        out = []
        for coef, m in side_list:
            s = smi_map.get(m)
            if not s:
                # One unknown structure poisons the whole reaction: mapping the rest
                # would hand the missing participant's atoms to whatever is left.
                return None, f"no_smiles:{m}"
            n = max(1, int(round(coef)))
            for _ in range(n):
                out.append(s)
        return out, None

    Ls, err = side(L)
    if err:
        return None, err
    Rs, err = side(R)
    if err:
        return None, err
    return ".".join(Ls) + ">>" + ".".join(Rs), None


def build_universe(reac_prop: Path, chem_prop: Path) -> dict[str, str]:
    """``{mnxr -> reaction SMILES}`` for every reaction both members will be asked to map."""
    equations = load_all_equations(reac_prop)
    needed = set()
    for eq in equations.values():
        for _, m in EQ_TERM.findall(eq):
            needed.add(m)
    smi_map = load_smiles(chem_prop, needed)
    print(f"[aam] {len(equations):,} equations, {len(needed):,} MNXM needed, "
          f"{len(smi_map):,} with SMILES", flush=True)

    rxn_smiles: dict[str, str] = {}
    n_unbuild = 0
    for mnxr, eq in equations.items():
        s, err = build_rxn_smiles(eq, smi_map)
        if err or not s or len(s) > SMILES_LEN_LIMIT:
            n_unbuild += 1
            continue
        rxn_smiles[mnxr] = s
    print(f"[aam] buildable reaction SMILES: {len(rxn_smiles):,}  "
          f"unbuildable/too-large: {n_unbuild:,}", flush=True)
    return rxn_smiles


def universe_from_worklist(worklist_parquet: Path, exclude=None) -> dict[str, str]:
    """``{mnxr -> reaction SMILES}`` for the reactions `aam_worklist` ADJUDICATED as
    mappable.

    THE SMILES MUST BE THE SAME STRING FOR EVERY MEMBER, and that is the whole reason
    this path exists. When each member rebuilds the universe from reac_prop + chem_prop
    it re-runs a builder that could drift; then two members "disagree" partly because
    they were shown two different strings, and the ensemble's disagreement rate stops
    measuring what it claims to. One table, built once, read by all.

    THE `verdict` COLUMN IS REQUIRED, and the refusal is the point. This used to read
    `lookup::reactions` and apply its own filter -- not null, under the length limit --
    which is a universe each member derived for itself and, worse, a universe with no
    row for anything it excluded. Pointing a member at the raw lookup now fails loudly
    rather than quietly mapping the oversized tail that OOM-killed this lane twice.

    `exclude` restricts to what a lower layer has NOT already claimed.
    """
    d = pd.read_parquet(worklist_parquet)
    if "verdict" not in d.columns:
        raise SystemExit(
            f"[aam] {worklist_parquet} has no `verdict` column, so it is not a worklist. "
            f"Members read `interm::aam_worklist` (or the rescued universe, which carries "
            f"the same column) -- never `lookup::reactions` directly, which has no row "
            f"for the reactions the adjudication refused and no record of why.")
    n_all = len(d)
    d = d[(d["verdict"] == "mappable") & d["rxn_smiles"].notna()]
    out = {r.mnxr: r.rxn_smiles for r in d.itertuples(index=False)}
    over = [m for m, s in out.items() if len(s) > SMILES_LEN_LIMIT]
    if over:
        # Belt and braces: the worklist applies this same cap, so a hit here means the
        # two limits have drifted apart rather than that a long reaction slipped
        # through. It is checked against whichever string the worklist wrote -- for a
        # collapsed reaction that is the collapsed one, which is what the member will
        # actually be handed and therefore what the cap is about.
        raise SystemExit(
            f"[aam] {len(over):,} mappable reactions exceed SMILES_LEN_LIMIT "
            f"({SMILES_LEN_LIMIT}), e.g. {over[0]}. The worklist and this module "
            f"disagree about the character cap; fix the constant, do not filter here.")
    n_collapsed = int(d["collapsed"].sum()) if "collapsed" in d.columns else 0
    n_map = len(out)
    if exclude:
        out = {m: s for m, s in out.items() if m not in exclude}
    print(f"[aam] worklist: {n_all:,} reactions adjudicated, {n_map:,} mappable"
          + (f" ({n_collapsed:,} as a stoichiometric collapse)" if n_collapsed else "")
          + (f", {n_map - len(out):,} already claimed by a lower layer" if exclude else ""),
          flush=True)
    return out


def covered_by(pairs_parquets) -> set[str]:
    """Reactions some other member already produced pairs for.

    THE GAP-FILLER'S INPUT. In the deployed chain LocalMapper never swept the universe:
    it ran over 487 reactions that the rest of the pipeline could reach but had no
    mapping for, and all 401 of its contributions to the deployed table come from that
    set -- zero outside it. Absence from every other member's pairs table is that same
    "reach AND no rxn" gap, expressed as something the graph can compute instead of
    something a person assembled by hand.
    """
    done = set()
    for p in pairs_parquets:
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            raise SystemExit(
                f"[aam] {p} is missing or empty. The gap is defined as what the OTHER "
                f"members did not reach, so an absent member would make the gap the "
                f"whole universe -- which is exactly the sweep this lane stopped being.")
        done |= set(pd.read_parquet(p, columns=["mnxr"])["mnxr"].astype(str))
    print(f"[aam] {len(done):,} reactions already covered by the members handed in",
          flush=True)
    return done


def _resume(out_tsv: Path) -> set[str]:
    if not out_tsv.exists() or out_tsv.stat().st_size == 0:
        return set()
    try:
        prev = pd.read_csv(out_tsv, sep="\t")
        done = set(prev["mnxr"].astype(str))
        print(f"[aam] resume: {len(done):,} reactions already cached", flush=True)
        return done
    except Exception as e:                                   # a truncated final line
        print(f"[aam] resume failed ({e}); starting over", file=sys.stderr, flush=True)
        return set()


def _writer(out_tsv: Path):
    write_header = not out_tsv.exists() or out_tsv.stat().st_size == 0
    fh = open(out_tsv, "a", buffering=1)                     # line-buffered: a kill -9
    if write_header:                                          # loses at most one row
        fh.write("\t".join(COLUMNS) + "\n")

    def emit(mnxr, smi, mapped, conf):
        vals = []
        for v in (mnxr, smi, mapped, conf):
            s = "" if v is None else str(v)
            vals.append(s.replace("\t", " ").replace("\n", " ").replace("\r", " "))
        fh.write("\t".join(vals) + "\n")

    return fh, emit


def _address_space_guard(budget_gb: float):
    """Cap this process's address space at `budget_gb` ABOVE what the loaded model
    already reserved, and return the peak-RSS reader.

    THE CAP GOES ON AFTER THE MODEL LOADS, and that ordering is not incidental: torch and
    DGL reserve a large virtual arena at import, so a cap applied before the import
    refuses the import itself and the lane dies having mapped nothing.

    What this buys is a per-reaction cost bound. A reaction that would have taken the
    node instead raises MemoryError inside the mapper call, which the caller's
    `except Exception` already records as an abstention -- the same outcome as any other
    reaction the mapper declines, and one the ensemble can read. Measured on the real
    image: ~0.65 GB at 100 atoms, 3.3 GB at 1,000, 8.1 GB at 2,751. With the worklist
    cutting at 600 atoms nothing should come near the budget; this is what happens when
    something does.
    """
    import resource

    def vmsize_gb():
        try:
            with open("/proc/self/status") as fh:
                for ln in fh:
                    if ln.startswith("VmSize:"):
                        return int(ln.split()[1]) / 2**20
        except OSError:
            pass
        return None

    base = vmsize_gb()
    if base is None:
        print("[aam] no /proc/self/status; running without an address-space guard",
              file=sys.stderr, flush=True)
        return
    cap = int((base + budget_gb) * 2**30)
    try:
        resource.setrlimit(resource.RLIMIT_AS, (cap, cap))
    except (ValueError, OSError) as e:
        print(f"[aam] could not set RLIMIT_AS ({e}); running unguarded",
              file=sys.stderr, flush=True)
        return
    print(f"[aam] address-space guard: model reserved {base:.1f}G, cap +{budget_gb}G",
          flush=True)


def run_rxnmapper(todo: list[tuple[str, str]], emit, chunk_size: int = 4,
                  sidecar=None):
    from rxnmapper import RXNMapper
    m = RXNMapper()
    t0 = time.time()
    n_ok = n_fail = 0
    i = 0
    while i < len(todo):
        j = min(i + chunk_size, len(todo))
        chunk = todo[i:j]
        smis = [s for _, s in chunk]
        if sidecar is not None:
            # The WHOLE chunk before the whole chunk: a kill lands inside one reaction
            # but the batch call gives no way to know which, so all of them are recorded
            # as attempted. The cost of that pessimism is at most chunk_size - 1
            # reactions never retried; the cost of optimism is a resume loop.
            for mnxr, _ in chunk:
                sidecar.mark(mnxr)
        try:
            res = m.get_attention_guided_atom_maps(smis)
        except Exception:
            # Per-reaction fallback so one bad reaction does not kill a whole chunk.
            res = []
            for one in smis:
                try:
                    r = m.get_attention_guided_atom_maps([one])
                    res.append(r[0] if r else {})
                except Exception:
                    res.append({"mapped_rxn": "", "confidence": float("nan")})
        for (mnxr, smi), r in zip(chunk, res):
            mapped = r.get("mapped_rxn", "")
            n_ok, n_fail = (n_ok + 1, n_fail) if mapped else (n_ok, n_fail + 1)
            emit(mnxr, smi, mapped, float(r.get("confidence", float("nan"))))
        i = j
        if i % 400 == 0 or i == len(todo):
            dt = time.time() - t0
            rate = i / dt if dt > 0 else 0
            print(f"      {i}/{len(todo)}  ok={n_ok} fail={n_fail}  "
                  f"({dt:.0f}s, {rate:.1f} rxn/s, eta {(len(todo)-i)/rate if rate else 0:.0f}s)",
                  flush=True)


def run_localmapper(todo: list[tuple[str, str]], emit, sidecar=None, budget_gb=None,
                    timeout_s: int = DEFAULT_LM_TIMEOUT_S, timeout_log=None):
    """Map the gap set, bounding EVERY reaction in time as well as in memory.

    THE MEMORY GUARD WAS ONLY HALF THE BOUND. `--mem-budget-gb` catches a reaction that
    grows; nothing caught one that simply does not finish. On 2026-07-28 this lane spent
    81 minutes on THREE reactions after clearing 818, at 99.9% CPU and a flat 13 GB, with
    323 to go -- upward of fifty hours, so the run was killed.

    THE CAUSE IS THE PROCESS, NOT THE REACTION, which is the opposite of what it looked
    like. Sharded six ways over the same universe, every reaction finished inside the
    budget and NONE hit it -- including MNXR198828, one of the two the single process was
    grinding on when it died. The single run's rate fell monotonically as it went (0.19,
    0.86, 0.094, 0.061 rxn/s); something accumulates in-process, and the fix that matters
    is capping how many reactions one process handles. See `bake/localmapper.py`.

    SO WHY KEEP THE TIMEOUT. Because it makes the worst case computable, and it is free:
    it fired zero times on the full gap set. Without it a single reaction can still hold
    a shard indefinitely and the lane has no upper bound of any kind -- which is exactly
    the state that cost a run. A tighter SIZE cap would not have helped either: the
    apparent culprit carries 280 atoms and 600-atom reactions had already gone through.

    A TIMEOUT IS A RECORDED OUTCOME. The row is written with an empty map, which every
    consumer already reads as an abstention, and the mnxr is additionally appended to
    `timeout_log` so the evidence distinguishes "LocalMapper could not" from "LocalMapper
    never finished". That distinction is invisible in the cache alone: a hang writes no
    row at all and shows up as sidecar-minus-cache, but a timeout writes one, so without
    this file it would be indistinguishable from an ordinary template failure.
    """
    from localmapper import localmapper
    mapper = localmapper(device="cpu")
    if budget_gb:
        _address_space_guard(budget_gb)
    t0 = time.time()
    n_ok = n_fail = n_timeout = 0
    for i, (mnxr, smi) in enumerate(todo, 1):
        if sidecar is not None:
            sidecar.mark(mnxr)
        old = signal.signal(signal.SIGALRM, _alarm)
        signal.alarm(timeout_s)
        try:
            r = mapper.get_atom_map(smi, return_dict=True)
            mapped = r.get("mapped_rxn", "") if isinstance(r, dict) else (r or "")
            # LocalMapper reports a template-match confidence under one of two names
            # depending on version; absent means it did not score, not that it scored 0.
            conf = float(r.get("confident", r.get("confidence", float("nan")))) \
                if isinstance(r, dict) else float("nan")
        except _Timeout:
            mapped, conf = "", float("nan")
            n_timeout += 1
            if timeout_log is not None:
                timeout_log.write(f"{mnxr}\n")
        except Exception:
            mapped, conf = "", float("nan")
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old)
        n_ok, n_fail = (n_ok + 1, n_fail) if mapped else (n_ok, n_fail + 1)
        emit(mnxr, smi, mapped, conf)
        if i % 200 == 0 or i == len(todo):
            dt = time.time() - t0
            rate = i / dt if dt > 0 else 0
            print(f"      {i}/{len(todo)}  ok={n_ok} fail={n_fail} timeout={n_timeout}  "
                  f"({dt:.0f}s, {rate:.1f} rxn/s, eta {(len(todo)-i)/rate if rate else 0:.0f}s)",
                  flush=True)
    if n_timeout:
        print(f"[aam:localmapper] {n_timeout} reaction(s) hit the {timeout_s}s budget and "
              f"abstained", flush=True)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--member", required=True, choices=["rxnmapper", "localmapper"])
    ap.add_argument("--worklist", type=Path, default=None,
                    help="interm::aam_worklist (or the rescued universe, same schema) -- "
                         "the adjudicated todo list every member shares")
    ap.add_argument("--reac-prop", type=Path, default=None)
    ap.add_argument("--chem-prop", type=Path, default=None)
    ap.add_argument("--exclude", type=Path, default=None,
                    help="a parquet with an `mnxr` column whose reactions a LOWER layer "
                         "has already claimed; they are not re-mapped")
    ap.add_argument("--covered", type=Path, nargs="*", default=None,
                    help="other members' pairs parquets. With this the lane is a "
                         "GAP-FILLER: it maps only what those members did not reach, "
                         "which is the role LocalMapper actually had in the deployed "
                         "chain and the reason it cost 487 reactions rather than a day")
    ap.add_argument("--shard", default=None,
                    help="i/n -- deterministic by crc32 of mnxr")
    ap.add_argument("--sidecar", type=Path, default=None,
                    help="the attempted-ids log, written BEFORE each attempt so a kill "
                         "costs the reaction it landed on rather than looping on it")
    ap.add_argument("--mem-budget-gb", type=float, default=None,
                    help="per-reaction address-space cap ABOVE what the loaded model "
                         "reserved; a reaction that exceeds it abstains instead of "
                         "taking the node. LocalMapper only")
    ap.add_argument("--timeout", type=int, default=DEFAULT_LM_TIMEOUT_S,
                    help="per-reaction seconds before LocalMapper abstains. The other "
                         "half of the bound the memory guard only half provided -- a "
                         "search that does not converge is not caught by a memory cap. "
                         "LocalMapper only")
    ap.add_argument("--timeout-log", type=Path, default=None,
                    help="append the mnxr of every timed-out reaction here. A timeout "
                         "WRITES a row (empty map), so unlike a hang it is invisible in "
                         "sidecar-minus-cache; this file is the only record separating "
                         "it from an ordinary template failure")
    ap.add_argument("--merge-from", type=Path, nargs="+", default=None,
                    help="shard caches to concatenate into --out and nothing else. "
                         "Rows with an EMPTY map are kept: an abstention is a recorded "
                         "outcome the extractor's status table reports, and dropping it "
                         "here would make 'the mapper declined' indistinguishable from "
                         "'the reaction was never in this member's shard'")
    ap.add_argument("--out", type=Path, required=True, help="the member's cached TSV")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap the number of reactions mapped this invocation; the cache "
                         "is resumable, so this splits one long run rather than "
                         "shrinking the member")
    a = ap.parse_args(argv)

    if a.merge_from:
        missing = [str(p) for p in a.merge_from if not Path(p).exists()]
        if missing:
            raise SystemExit(
                f"[aam:{a.member}] shard cache(s) missing: {missing}. A merge over a "
                f"short set of shards produces a member with a silent coverage hole, "
                f"which is the one failure the shard discipline exists to prevent.")
        parts = [pd.read_csv(p, sep="\t") for p in a.merge_from]
        d = pd.concat(parts, ignore_index=True).drop_duplicates("mnxr", keep="first")
        d[list(COLUMNS)].to_csv(a.out, sep="\t", index=False)
        n_mapped = int((d["mapped_rxn_smiles"].astype(str).str.len() > 0).sum())
        print(f"[aam:{a.member}] merged {len(a.merge_from)} shards -> {len(d):,} rows, "
              f"{n_mapped:,} carry a map -> {a.out}", flush=True)
        return 0

    exclude = None
    if a.exclude and a.exclude.exists():
        exclude = set(pd.read_parquet(a.exclude, columns=["mnxr"])["mnxr"])
    if a.worklist:
        universe = universe_from_worklist(a.worklist, exclude)
    elif a.reac_prop and a.chem_prop:
        universe = build_universe(a.reac_prop, a.chem_prop)
        if exclude:
            universe = {m: s for m, s in universe.items() if m not in exclude}
    else:
        raise SystemExit("[aam] need --worklist, or both --reac-prop and --chem-prop")

    if a.covered:
        done_elsewhere = covered_by(a.covered)
        n = len(universe)
        universe = {m: s for m, s in universe.items() if m not in done_elsewhere}
        print(f"[aam:{a.member}] gap: {len(universe):,} of {n:,} mappable reactions "
              f"were not reached by the members handed in", flush=True)

    shard = aam_shard.parse_spec(a.shard)
    universe = aam_shard.select(universe, shard)

    a.out.parent.mkdir(parents=True, exist_ok=True)
    # RESUME OFF BOTH: the cache holds what FINISHED, the sidecar holds what was STARTED,
    # and the difference is precisely the reaction a kill landed in. Resuming off the
    # cache alone re-attempts it, dies again, and never makes progress.
    done = _resume(a.out)
    if a.sidecar:
        done |= aam_shard.read_sidecar(a.sidecar, shard, who=f"aam:{a.member}")
    todo = [(m, s) for m, s in universe.items() if m not in done]
    if a.limit:
        todo = todo[:a.limit]
    print(f"[aam:{a.member}] todo: {len(todo):,}", flush=True)
    if not todo:
        print(f"[aam:{a.member}] nothing to do", flush=True)
        # An empty cache is still a cache: the header has to exist or the extractor's
        # read of a lane that had nothing to do fails on a zero-byte file.
        if not a.out.exists() or a.out.stat().st_size == 0:
            _writer(a.out)[0].close()
        return 0

    side = aam_shard.Sidecar(a.sidecar, shard) if a.sidecar else None
    fh, emit = _writer(a.out)
    # Append, line-buffered, for the same reason the cache is: this lane gets killed, and
    # a timeout record that only lands at the end is a record that does not survive.
    tlog = open(a.timeout_log, "a", buffering=1) if a.timeout_log else None
    try:
        if a.member == "rxnmapper":
            run_rxnmapper(todo, emit, sidecar=side)
        else:
            run_localmapper(todo, emit, sidecar=side,
                            budget_gb=a.mem_budget_gb,
                            timeout_s=a.timeout, timeout_log=tlog)
    finally:
        fh.close()
        if tlog:
            tlog.close()
        if side:
            side.close()

    df = pd.read_csv(a.out, sep="\t")
    n_mapped = int((df["mapped_rxn_smiles"].astype(str).str.len() > 0).sum())
    print(f"[aam:{a.member}] {len(df):,} rows cached, {n_mapped:,} carry a map "
          f"-> {a.out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
