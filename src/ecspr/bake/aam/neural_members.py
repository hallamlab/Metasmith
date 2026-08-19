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
from . import worklist

EQ_TERM = re.compile(r"(\d+(?:\.\d+)?)\s+(MNXM\w+)@\w+")

SMILES_LEN_LIMIT = 8000

COLUMNS = ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence")

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


def load_universe(universe_parquet: Path, exclude=None):
    d = pd.read_parquet(universe_parquet)
    if "verdict" not in d.columns:
        raise SystemExit(
            f"[aam] {universe_parquet} has no `verdict` column, so it is not a submission "
            f"table. Members read `interm::aam_universe` -- never `lookup::reactions` "
            f"directly, which has no row for the submissions the adjudication refused "
            f"and no record of why.")
    n_all = len(d)
    d = d[d["verdict"].isin(worklist.NEURAL_ADMITS) & d["rxn_smiles"].notna()]
    out = {r.mnxr: r.rxn_smiles for r in d.itertuples(index=False)}
    meta = {}
    has_base = "base_mnxr" in d.columns
    for r in d.itertuples(index=False):
        base = str(r.base_mnxr) if has_base else str(r.mnxr)
        el = getattr(r, "element", None)
        meta[r.mnxr] = (base, None if el is None or pd.isna(el) else str(el))
    over = [m for m, s in out.items() if len(s) > SMILES_LEN_LIMIT]
    if over:
        raise SystemExit(
            f"[aam] {len(over):,} admitted submissions exceed SMILES_LEN_LIMIT "
            f"({SMILES_LEN_LIMIT}), e.g. {over[0]}. The universe and this module "
            f"disagree about the character cap; fix the constant, do not filter here.")
    n_collapsed = int(d["collapsed"].sum()) if "collapsed" in d.columns else 0
    by_class = (d["submission_class"].value_counts().to_dict()
                if "submission_class" in d.columns else {})
    n_map = len(out)
    if exclude:
        out = {m: s for m, s in out.items() if m not in exclude}
    print(f"[aam] universe: {n_all:,} submissions, {n_map:,} admitted "
          + (f"{by_class} " if by_class else "")
          + (f"({n_collapsed:,} as a stoichiometric collapse)" if n_collapsed else "")
          + (f", {n_map - len(out):,} already claimed by a lower layer" if exclude else ""),
          flush=True)
    return out, meta


def covered_by(pairs_parquets) -> set:
    done = set()
    for p in pairs_parquets:
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            raise SystemExit(
                f"[aam] {p} is missing or empty. The gap is defined as what the OTHER "
                f"members did not reach, so an absent member would make the gap the "
                f"whole universe -- which is exactly the sweep this lane stopped being.")
        d = pd.read_parquet(p, columns=["mnxr", "element"])
        done |= set(zip(d["mnxr"].astype(str), d["element"].astype(str)))
    print(f"[aam] {len({m for m, _ in done}):,} reactions / {len(done):,} "
          f"(reaction, element) already covered by the members handed in", flush=True)
    return done


def gap_of(universe: dict, meta: dict, covered: set) -> dict:
    reached = {m for m, _el in covered}
    out = {}
    for key, smi in universe.items():
        base, el = meta.get(key, (key, None))
        if el is None:
            if base not in reached:
                out[key] = smi
        elif (base, el) not in covered:
            out[key] = smi
    return out


def _resume(out_tsv: Path) -> set[str]:
    if not out_tsv.exists() or out_tsv.stat().st_size == 0:
        return set()
    try:
        prev = pd.read_csv(out_tsv, sep="\t")
        done = set(prev["mnxr"].astype(str))
        print(f"[aam] resume: {len(done):,} reactions already cached", flush=True)
        return done
    except Exception as e:
        print(f"[aam] resume failed ({e}); starting over", file=sys.stderr, flush=True)
        return set()


def _writer(out_tsv: Path):
    write_header = not out_tsv.exists() or out_tsv.stat().st_size == 0
    fh = open(out_tsv, "a", buffering=1)
    if write_header:
        fh.write("\t".join(COLUMNS) + "\n")

    def emit(mnxr, smi, mapped, conf):
        vals = []
        for v in (mnxr, smi, mapped, conf):
            s = "" if v is None else str(v)
            vals.append(s.replace("\t", " ").replace("\n", " ").replace("\r", " "))
        fh.write("\t".join(vals) + "\n")

    return fh, emit


def _address_space_guard(budget_gb: float):
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
            for mnxr, _ in chunk:
                sidecar.mark(mnxr)
        try:
            res = m.get_attention_guided_atom_maps(smis)
        except Exception:
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


def _lm_map_one(mapper, smi: str, timeout_s: int):
    old = signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(timeout_s)
    try:
        r = mapper.get_atom_map(smi, return_dict=True)
        mapped = r.get("mapped_rxn", "") if isinstance(r, dict) else (r or "")
        conf = float(r.get("confident", r.get("confidence", float("nan")))) \
            if isinstance(r, dict) else float("nan")
        return (mapped, conf, "ok") if mapped else ("", conf, "empty")
    except _Timeout:
        return "", float("nan"), "timeout"
    except Exception:
        return "", float("nan"), "error"
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def _lm_serve_with(budget_gb):
    def _serve(conn, timeout_s: int):
        from localmapper import localmapper
        mapper = localmapper(device="cpu")
        if budget_gb:
            _address_space_guard(budget_gb)
        try:
            while True:
                smi = conn.recv()
                if smi is None:
                    return
                conn.send(_lm_map_one(mapper, smi, timeout_s))
        except (EOFError, KeyboardInterrupt):
            return
    return _serve


def run_localmapper(todo: list[tuple[str, str]], emit, sidecar=None, budget_gb=None,
                    timeout_s: int = DEFAULT_LM_TIMEOUT_S, timeout_log=None,
                    mapper=None):
    from .indigo_member import Mapper

    m = mapper if mapper is not None else Mapper(timeout_s,
                                                 serve=_lm_serve_with(budget_gb))
    t0 = time.time()
    n_ok = n_fail = n_timeout = n_killed = 0
    try:
        for i, (mnxr, smi) in enumerate(todo, 1):
            if sidecar is not None:
                sidecar.mark(mnxr)
            r = m.map_one(smi)
            mapped, conf, status = r if len(r) == 3 else (r[0], float("nan"), r[1])
            if status in ("timeout", "killed"):
                n_timeout += 1
                n_killed += status == "killed"
                if timeout_log is not None:
                    timeout_log.write(f"{mnxr}\t{status}\n")
            n_ok, n_fail = (n_ok + 1, n_fail) if mapped else (n_ok, n_fail + 1)
            emit(mnxr, smi, mapped, conf)
            if i % 200 == 0 or i == len(todo):
                dt = time.time() - t0
                rate = i / dt if dt > 0 else 0
                print(f"      {i}/{len(todo)}  ok={n_ok} fail={n_fail} "
                      f"timeout={n_timeout} killed={n_killed}  "
                      f"({dt:.0f}s, {rate:.1f} rxn/s, "
                      f"eta {(len(todo)-i)/rate if rate else 0:.0f}s)", flush=True)
    finally:
        if mapper is None:
            m.close()
    if n_timeout:
        print(f"[aam:localmapper] {n_timeout} reaction(s) hit the {timeout_s}s budget and "
              f"abstained ({n_killed} of them only to SIGKILL)", flush=True)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--member", required=True, choices=["rxnmapper", "localmapper"])
    ap.add_argument("--universe", type=Path, default=None,
                    help="interm::aam_universe -- the one submission table every member "
                         "shares, carrying all three submission classes")
    ap.add_argument("--cache-dir", type=Path, default=None,
                    help="the STAGED durable cache for this member: prior runs' "
                         "cache.tsv and *.attempted. A row is reused only when its "
                         "submission STRING matches the one this run would send, and "
                         "the sidecars are re-partitioned onto this run's shard spec "
                         "rather than refused across the change")
    ap.add_argument("--reac-prop", type=Path, default=None)
    ap.add_argument("--chem-prop", type=Path, default=None)
    ap.add_argument("--exclude", type=Path, default=None,
                    help="a parquet with an `mnxr` column whose reactions a LOWER layer "
                         "has already claimed; they are not re-mapped")
    ap.add_argument("--covered", type=Path, nargs="*", default=None,
                    help="other members' pairs parquets. With this the lane is a "
                         "GAP-FILLER: it maps only the submissions those members did "
                         "not answer, at (reaction, element) grain -- which is the role "
                         "LocalMapper actually had in the deployed chain and the reason "
                         "it cost 487 reactions rather than a day")
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
    meta = {}
    if a.universe:
        universe, meta = load_universe(a.universe, exclude)
    elif a.reac_prop and a.chem_prop:
        universe = build_universe(a.reac_prop, a.chem_prop)
        if exclude:
            universe = {m: s for m, s in universe.items() if m not in exclude}
    else:
        raise SystemExit("[aam] need --universe, or both --reac-prop and --chem-prop")

    if a.covered:
        done_elsewhere = covered_by(a.covered)
        n = len(universe)
        universe = gap_of(universe, meta, done_elsewhere)
        print(f"[aam:{a.member}] gap: {len(universe):,} of {n:,} admitted submissions "
              f"were not reached by the members handed in", flush=True)

    shard = aam_shard.parse_spec(a.shard)
    universe = aam_shard.select(universe, shard)

    a.out.parent.mkdir(parents=True, exist_ok=True)
    prior_caches, prior_sides = aam_shard.cache_files(a.cache_dir)
    carried, _stale, _foreign = aam_shard.read_cache(prior_caches, universe,
                                                     who=f"aam:{a.member}")
    if carried:
        fh0, emit0 = _writer(a.out)
        here = _resume(a.out)
        for row in carried:
            if row[0] in here:
                continue
            emit0(row[0], row[1], row[2] if len(row) > 2 else "",
                  row[3] if len(row) > 3 else "")
        fh0.close()

    done = _resume(a.out)
    done |= aam_shard.read_sidecars(prior_sides + ([a.sidecar] if a.sidecar else []),
                                    shard, who=f"aam:{a.member}")
    todo = [(m, s) for m, s in universe.items() if m not in done]
    if a.limit:
        todo = todo[:a.limit]
    print(f"[aam:{a.member}] todo: {len(todo):,}", flush=True)
    if not todo:
        print(f"[aam:{a.member}] nothing to do", flush=True)
        if not a.out.exists() or a.out.stat().st_size == 0:
            _writer(a.out)[0].close()
        return 0

    side = aam_shard.Sidecar(a.sidecar, shard) if a.sidecar else None
    fh, emit = _writer(a.out)
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
