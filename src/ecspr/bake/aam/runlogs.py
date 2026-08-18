"""The finished bake's own run logs, one row per lane per reaction.

WHAT THIS EXISTS TO STOP. `forecast.read_prior` reads a *previous* bake's per-reaction
records to decide which reactions get an element-reduced submission built for them, and
until now that directory was harvested by hand out of whatever scratch tree the run had
happened in. A hand-harvest is a generation that never gets rewritten: the logs beside a
bake described the run before it, so a gapfill inherited its parent's silences and the two
drifted apart with nothing to say they had.

DEDUPLICATED TO THE LATEST RUN, WHICH THE CACHE ALREADY IS. A member's `cache.tsv` is
cumulative -- it carries its reusable prior rows into its own output -- so the reactions a
gapfill served from cache and the reactions it re-mapped are already one set with the newer
row winning. Nothing here merges an old log file into a new one; the merge happened in the
lane, and this reads the result.

THE RUN STATUS SURVIVES ONLY IN THE PER-SHARD TABLES. Indigo records `ok`/`timeout`/
`error`/`killed` per attempt and the merge step drops that column, so the shard tables are
read in preference to the merged one. The two neural members write no status at all, which
is why their tables are `derived_status`: a missing confidence is the only signal they
leave that they declined.

ONE ROW PER REACTION, NOT PER SUBMISSION. The cache is keyed on `(mnxr, submission
string)` and one reaction has up to four -- whole, collapsed, rescue-completed, element
reduced -- but every reader joins these tables on reaction ids. So a reaction's submissions
collapse to one row: the unreduced submission's outcome where it has one, otherwise the
worst of its reductions. Taking the best would report a reaction as answered because a
repair of it was.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

LANES = ("indigo", "rxnmapper", "localmapper")

# Worst-wins, and the order is the readers' order rather than a severity opinion:
# `forecast.read_prior` sorts `timeout` into a budget statement and `error`/`empty` into a
# refusal, and `killed` into neither -- a killed reaction is a run that died inside it,
# which is what `indigo_unreturned.tsv` means, so it is carried there as well.
STATUS_RANK = ("ok", "empty", "error", "timeout", "killed")
DERIVED_RANK = ("ok", "no_confidence")

# One step log per transform invocation, and one of them is the pair stack's, which is two
# orders of magnitude larger than every other. Truncated rather than dropped: the head and
# tail are what a reader wants and the elision says what is missing.
STEP_MAX_BYTES = 2_000_000
ELISION = "\n... [{n:,} bytes elided by ecspr.bake.aam.runlogs] ...\n"


def base_id(key: str) -> str:
    """`MNXR123#C` -> `MNXR123`. The submission key's reaction half."""
    return key.split("#", 1)[0]


def _rows(path: Path):
    """`(header, row dicts)` from a TSV, or `(None, [])` for an absent or empty file."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return None, []
    with path.open() as fh:
        header = fh.readline().rstrip("\n").split("\t")
        out = []
        for line in fh:
            f = line.rstrip("\n").split("\t")
            f += [""] * (len(header) - len(f))
            out.append(dict(zip(header, f)))
    return header, out


def collapse(seen: dict, key: str, value: str, rank: tuple, extra=None) -> None:
    """Fold one submission's outcome into its reaction's row, worst-wins.

    `seen[mnxr] = (value, extra, from_whole)`. A whole submission's outcome is never
    displaced by a reduction's, in either direction -- the reductions are statements about
    a repair of the reaction, not about the reaction.
    """
    mnxr, is_whole = base_id(key), "#" not in key
    prev = seen.get(mnxr)
    if prev is None:
        seen[mnxr] = (value, extra, is_whole)
        return
    p_value, p_extra, p_whole = prev
    if p_whole and not is_whole:
        return
    if is_whole and not p_whole:
        seen[mnxr] = (value, extra, True)
        return
    if rank.index(value) > rank.index(p_value):
        seen[mnxr] = (value, extra if extra is not None else p_extra, is_whole)


def shard_tables(evidence: Path, lane: str) -> list[tuple[int, Path]]:
    """`(shard, path)` for a lane's per-shard tables under a retrieved evidence tree.

    The merged `<lane>.tsv` sits beside them and is deliberately not matched: it is the
    file the status column was dropped from.
    """
    out = []
    for p in sorted(Path(evidence).glob(f"{lane}/*/{lane}_[0-9]*.tsv")):
        try:
            out.append((int(p.stem.rsplit("_", 1)[1]), p))
        except (IndexError, ValueError):
            continue
    return sorted(out)


def cache_table(cache: Path, lane: str) -> Path:
    return Path(cache) / lane / "cache.tsv"


def indigo_status(cache: Path, evidence: Path | None) -> tuple[list, list]:
    """`(status rows, unreturned rows)` for the lane that can hang.

    A row the shard table carries with no status is one re-emitted from the cache, and
    what it is saying is that a previous run answered: `ok` where it holds a mapped
    string, `empty` where it does not. The reactions with a status are this run's
    attempts.
    """
    tables = shard_tables(evidence, "indigo") if evidence else []
    seen, shard_of = {}, {}
    if tables:
        for shard, p in tables:
            for r in _rows(p)[1]:
                key, status = r["mnxr"], (r.get("status") or "").strip()
                if not status:
                    status = "ok" if r.get("mapped_rxn_smiles") else "empty"
                shard_of.setdefault(base_id(key), shard)
                collapse(seen, key, status, STATUS_RANK, extra=shard)
    else:
        # No run workspace to read: the cache still says who answered, and losing the
        # timeout/hang split is better than reporting a run that recorded no failures.
        for r in _rows(cache_table(cache, "indigo"))[1]:
            collapse(seen, r["mnxr"],
                     "ok" if r.get("mapped_rxn_smiles") else "empty", STATUS_RANK)

    status_rows = [(m, v[1] if v[1] is not None else shard_of.get(m, ""), v[0])
                   for m, v in sorted(seen.items())]

    # ATTEMPTED MINUS RETURNED plus the kills. Both are the same statement -- the run went
    # into this reaction and did not come out of it with an answer -- and `read_prior`
    # has no other route for a kill, because the status vocabulary it sorts does not
    # carry one.
    returned = set(seen)
    attempted = {}
    for p in sorted(Path(cache).glob("indigo/*.attempted")):
        try:
            shard = int(p.stem.rsplit("_", 1)[1])
        except (IndexError, ValueError):
            shard = ""
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                attempted.setdefault(base_id(line), shard)
    unret = {m: s for m, s in attempted.items() if m not in returned}
    unret.update({m: sh for m, sh, st in status_rows if st == "killed"})
    return status_rows, sorted(unret.items())


def derived_status(cache: Path, lane: str) -> list:
    """`(mnxr, derived_status)` for a member that writes no status of its own."""
    seen = {}
    for r in _rows(cache_table(cache, lane))[1]:
        conf = (r.get("confidence") or "").strip()
        collapse(seen, r["mnxr"], "ok" if conf else "no_confidence", DERIVED_RANK)
    return [(m, v[0]) for m, v in sorted(seen.items())]


def write_tsv(path: Path, header: tuple, rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        fh.write("\t".join(header) + "\n")
        for r in rows:
            fh.write("\t".join(str(x) for x in r) + "\n")
    return len(rows)


def copy_attempted(cache: Path, out: Path) -> int:
    """Indigo's sidecars alone, because `read_prior` subtracts them from ONE returned set.

    Every `*.attempted` under `logs/attempted/` is unioned and the indigo status table's
    reactions are subtracted from it, so a neural member's sidecar dropped in here would
    read as an indigo hang for every reaction indigo was not given.
    """
    dest = out / "attempted"
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    for p in sorted(Path(cache).glob("indigo/*.attempted")):
        shutil.copy2(p, dest / p.name)
        n += 1
    return n


def copy_steps(runs, out: Path, max_bytes: int) -> int:
    """One log per transform invocation, named for the run that produced it.

    The run id lives in the sandbox path rather than in the filename, so it is prefixed
    here -- six runs each hold a `p01__*.log` and a flat copy would keep one of them.
    """
    dest = out / "steps"
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    for run in runs or ():
        run = Path(run)
        tag = run.name.lstrip("_")
        for p in sorted(run.glob("_metadata/logs.latest/steps/*.log")):
            target = dest / f"{tag}__{p.name}"
            size = p.stat().st_size
            if size <= max_bytes:
                shutil.copy2(p, target)
            else:
                half = max_bytes // 2
                with p.open("rb") as fh:
                    head = fh.read(half)
                    fh.seek(size - half)
                    tail = fh.read(half)
                with target.open("wb") as fh:
                    fh.write(head)
                    fh.write(ELISION.format(n=size - 2 * half).encode())
                    fh.write(tail)
            n += 1
    return n


def cmd_build(args) -> int:
    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    status_rows, unret = indigo_status(Path(args.cache), args.evidence)
    n = write_tsv(out / "indigo_status.tsv", ("mnxr", "shard", "status"), status_rows)
    n_unret = write_tsv(out / "indigo_unreturned.tsv", ("mnxr", "shard"), unret)
    tally = {}
    for _m, _s, st in status_rows:
        tally[st] = tally.get(st, 0) + 1
    print(f"[runlogs] indigo        {n:,} reactions {tally}, {n_unret:,} unreturned")

    for lane in ("rxnmapper", "localmapper"):
        rows = derived_status(Path(args.cache), lane)
        write_tsv(out / f"{lane}_derived_status.tsv", ("mnxr", "derived_status"), rows)
        t = {}
        for _m, st in rows:
            t[st] = t.get(st, 0) + 1
        print(f"[runlogs] {lane:<13} {len(rows):,} reactions {t}")

    if args.curated_status:
        curated = out / "L1_metacyc_status.tsv"
        shutil.copy2(args.curated_status, curated)
        n_curated = sum(1 for _ in curated.open()) - 1
        print(f"[runlogs] curated       {n_curated:,} reactions")

    print(f"[runlogs] attempted     {copy_attempted(Path(args.cache), out)} sidecar(s)")
    print(f"[runlogs] steps         "
          f"{copy_steps(args.runs, out, args.step_max_bytes)} log(s)")
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("build")
    p.set_defaults(fn=cmd_build)
    p.add_argument("--cache", required=True, type=Path,
                   help="the member cache directory: one subdirectory per lane, each "
                        "holding the cumulative cache.tsv and its sidecars")
    p.add_argument("--evidence", type=Path,
                   help="a retrieved tool-output tree, holding <lane>/<version>/ per "
                        "lane. Without it the run status is unavailable and indigo's "
                        "table degrades to ok/empty")
    p.add_argument("--curated-status", type=Path,
                   help="the curated layer's own per-reaction status table")
    p.add_argument("--runs", type=Path, nargs="*",
                   help="metasmith run sandboxes to gather step logs from")
    p.add_argument("--out", required=True, type=Path,
                   help="the logs directory to write; replaced if it exists")
    p.add_argument("--step-max-bytes", type=int, default=STEP_MAX_BYTES)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
