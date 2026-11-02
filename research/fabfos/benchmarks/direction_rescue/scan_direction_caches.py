"""Which bake does each decoded direction cache in the workspace actually hold?

`REBAKE.md` names the bug class this lane keeps producing: a derived artifact that does
not record which bake it came from. It is silent by construction -- the consumer gets a
plausible table from the previous generation and no error. This is that hazard made
executable, run across every worktree at once, because the caches live in the sibling
scopes and not in the one doing the re-bake.

Two independent readings per file, and the pair is the point:

  the stamp   -- `<file>.from`, written by `bake_identity.keep`. Authoritative when
                 present, absent on anything built before this instrument existed.
  the content -- a hash over the (mnxr, ratio) rows themselves, compared against a fresh
                 decode of the reference bake. Works on unstamped files, which is most of
                 the interesting ones.

A stamped file whose content disagrees with its stamp means somebody edited a cache in
place. That is a separate and worse problem, so it gets its own verdict rather than being
folded into "stale".

Run it before taking any baseline a re-bake will be judged against:

    PYTHONPATH=src mamba run -n msm python \\
        research/fabfos/benchmarks/direction_rescue/scan_direction_caches.py
"""
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_identity                                                          # noqa: E402

PRUNE = {".git", ".dvc", ".awm", "__pycache__", "node_modules", ".venv", ".mypy_cache"}
PATTERN = "direction_ratios"


def worktrees() -> list[Path]:
    out = subprocess.run(["git", "worktree", "list", "--porcelain"],
                         capture_output=True, text=True, check=True).stdout
    paths, bare = [], False
    for line in out.splitlines():
        if line.startswith("worktree "):
            paths.append(Path(line[len("worktree "):]))
            bare = False
        elif line.strip() == "bare":
            bare = True
            paths.pop()
    return paths


def find_caches(root: Path) -> list[Path]:
    hits = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in PRUNE]
        for f in filenames:
            if f.startswith(PATTERN) and f.endswith(".parquet"):
                hits.append(Path(dirpath) / f)
    return sorted(hits)


def content_hash(df: pd.DataFrame) -> str:
    # A hash of the decoded table's meaning, not its bytes.
    #
    # Sorted, and over the float64 ratios rather than their repr, so it is invariant to the
    # two decode spellings in this tree (a vocab `map` and a vocab `merge`) and to whether
    # the writer kept an index.
    d = df[["mnxr", "ratio"]].sort_values("mnxr")
    h = hashlib.sha256()
    h.update("\n".join(d.mnxr.astype(str)).encode())
    h.update(np.asarray(d.ratio, dtype="float64").tobytes())
    return h.hexdigest()


def summarise(df: pd.DataFrame) -> str:
    n1 = int((df.ratio == 1.0).sum())
    return f"{len(df):,} rows, {n1:,} at 1.0"


def verdict(stamp: str | None, want_stamp: str, same_content: bool) -> str:
    if stamp is None:
        return "unstamped, content current" if same_content else "UNSTAMPED, STALE CONTENT"
    if stamp == want_stamp:
        return "current" if same_content else "STAMP LIES -- edited in place"
    return "stale (stamped, older bake)" if not same_content else \
        "STAMP LIES -- content is current"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bake", type=Path, default=bake_identity.DEPLOYED,
                    help="the bake to compare against (default: this scope's deployed trio)")
    ap.add_argument("--root", type=Path, nargs="*", default=None,
                    help="trees to scan (default: every worktree of this repo)")
    args = ap.parse_args()

    want_stamp = bake_identity.identity(args.bake)
    ref = bake_identity.decode_direction_ratios(args.bake)
    want_content = content_hash(ref)
    print(f"reference: {args.bake}\n           {want_stamp}\n"
          f"           {summarise(ref)}, content {want_content[:12]}\n")

    roots = args.root or worktrees()
    rows, bad = [], 0
    for root in roots:
        for path in find_caches(root):
            try:
                df = pd.read_parquet(path)
                same = content_hash(df) == want_content
                note = summarise(df)
            except Exception as exc:
                same, note = False, f"UNREADABLE: {exc}"
            v = verdict(bake_identity.stamp_of(path), want_stamp, same)
            bad += v != "current"
            rows.append((str(path.relative_to(root)), root.name, v, note))

    w = max((len(r[0]) for r in rows), default=1)
    for rel, scope, v, note in rows:
        print(f"{rel:<{w}}  {scope:<24}  {v:<32}  {note}")
    print(f"\n{len(rows)} caches, {bad} not current against this bake")


if __name__ == "__main__":
    main()
