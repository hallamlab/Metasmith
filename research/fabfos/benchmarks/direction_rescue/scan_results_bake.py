#!/usr/bin/env python3
"""Which bake each ECSPr RESULT chunk was computed on, and how strong that evidence is.

    PYTHONPATH=src mamba run -n ecspr python \
        research/fabfos/benchmarks/direction_rescue/scan_results_bake.py

`scan_direction_caches.py` beside this one covers DECODE CACHES -- files named
`direction_ratios`, which carry a `.from` stamp and can be checked exactly. Nothing
covered the results, and results are where the staleness is read as a finding: a chunk
holds numbers somebody quotes, carries no stamp, and says nothing about the basis it was
measured on. This is the missing half.

THREE GRADES OF EVIDENCE, and the report says which one each row rests on.

  measured   the chunk carries a basis table of its own -- a composed `direction.parquet`
             under `networks/` -- so its ratios are compared to the deployed bake's
             directly. This is the only grade that proves anything.
  stamped    a `.from` sidecar written through `bake_identity`.
  dated      neither: the chunk's pin commit and its file mtimes are bracketed against
             the promote dates below. A DATE IS NOT A PROOF. It says which bake was
             deployed when the bytes were written, not which one the writer read -- a
             script pointed at a staged chunk, or one serving a stale decode cache, dates
             the same as one that read the deployed bake.

A chunk can be BOTH r10 and not, and that is the case worth catching: nostoc's composed
networks were recut on r10 while the measurements taken on them were not re-run, so the
basis and the numbers beside it come from two different bakes inside one pin.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin"))

import bake_identity  # noqa: E402
import bake_pairs  # noqa: E402

#: When each generation became the deployed bake, from `git log` on the pin. Used only for
#: the `dated` grade, and only to name a generation -- never to assert one.
PROMOTES = [("2026-08-24", "r10"), ("2026-08-18", "r9"), ("2026-08-17", "r7"),
            ("2026-08-14", "pre-bake")]

CHUNKS = [
    "data/fabfos/runs/aska/ecspr",
    "data/fabfos/runs/e_coli_epi300/ecspr",
    "data/fabfos/runs/eydallin_clones/ecspr",
    "data/fabfos/runs/scadc_fosmids/ecspr",
    "data/fabfos/runs/scadc_metagenome/ecspr",
    "data/fabfos/runs/scadc_metagenome/nulls",
    "data/fabfos/runs/scales/ecspr",
    "data/fabfos/nostoc/ecspr",
]


def generation(date: str) -> str:
    for since, name in PROMOTES:
        if date >= since:
            return name
    return "pre-monorepo"


def pin_date(chunk: Path) -> str:
    dvc = chunk.with_name(chunk.name + ".dvc")
    if not dvc.exists():
        return ""
    out = subprocess.run(["git", "log", "-1", "--format=%ad", "--date=short", "--",
                          str(dvc.relative_to(REPO))],
                         cwd=REPO, capture_output=True, text=True).stdout.strip()
    return out


def mtimes(chunk: Path) -> tuple:
    ts = sorted(pd.Timestamp(p.stat().st_mtime, unit="s").strftime("%Y-%m-%d")
                for p in chunk.rglob("*") if p.is_file())
    return (ts[0], ts[-1], len(ts)) if ts else ("", "", 0)


def measured_basis(chunk: Path, deployed: pd.Series) -> str:
    """Compare any composed direction table under the chunk to the deployed bake.

    Composed tables carry member-prefixed ids (`ERY:MNXR100024`), so the join is on the
    bare id -- a prefix is a private copy of the same reaction, not a different one. A
    multi-member table therefore carries one bare id PER MEMBER, all with the same ratio
    by construction, so the duplicates are dropped rather than joined against.
    """
    found = sorted(chunk.rglob("direction.parquet"))
    if not found:
        return ""
    hits = []
    for p in found[:8]:
        d = pd.read_parquet(p)
        if "ratio" not in d.columns or "mnxr" not in d.columns:
            continue
        bare = d.mnxr.astype(str).str.split(":").str[-1]
        here = pd.Series(d.ratio.values, index=bare.values)
        here = here[~here.index.duplicated()]
        j = pd.concat([here.rename("here"), deployed.rename("dep")],
                      axis=1, join="inner")
        if len(j):
            hits.append(((j.here - j.dep).abs() < 1e-12).mean())
    if not hits:
        return ""
    lo, hi = min(hits), max(hits)
    if lo > 0.9999:
        return f"basis IS the deployed bake ({len(found)} table(s), {hi:.2%} identical)"
    return (f"basis is NOT the deployed bake ({len(found)} table(s), "
            f"{lo:.2%}-{hi:.2%} identical)")


def main() -> int:
    print(f"deployed bake: {bake_identity.identity()}")
    deployed = pd.read_parquet(bake_pairs.direction_ratios()).set_index("mnxr").ratio

    rows = []
    for rel in CHUNKS:
        chunk = REPO / rel
        if not chunk.exists():
            rows.append(dict(chunk=rel, grade="absent", generation="", evidence=""))
            continue
        stamps = {bake_identity.stamp_of(p.with_suffix(""))
                  for p in chunk.rglob("*.from")} - {None}
        lo, hi, n = mtimes(chunk)
        pinned = pin_date(chunk)
        basis = measured_basis(chunk, deployed)

        if basis:
            grade = "measured"
            gen = "r10" if "IS the" in basis else "older than r10"
            evidence = basis
            # THE SPLIT IS THE FINDING, so it may not collapse into one generation. A
            # chunk whose basis was recut on the deployed bake while the measurements
            # taken on it were not re-run holds two bakes under one pin, and reading the
            # basis alone reports it as current.
            res = [p for p in chunk.rglob("*") if p.is_file()
                   and "results" in p.relative_to(chunk).parts]
            if gen == "r10" and res:
                newest = max(pd.Timestamp(p.stat().st_mtime, unit="s") for p in res)
                if newest.strftime("%Y-%m-%d") < PROMOTES[0][0]:
                    grade, gen = "SPLIT", "basis r10, results older"
                    evidence += (f"; but results/ newest is "
                                 f"{newest.strftime('%Y-%m-%d')}, before the r10 promote")
        elif stamps:
            grade = "stamped"
            gen = "r10" if stamps == {bake_identity.identity()} else "not r10"
            evidence = "; ".join(sorted(s[:16] + ":" + s[-16:] for s in stamps))
        else:
            grade, gen = "dated", generation(hi)
            evidence = f"pin {pinned or '-'}, mtimes {lo}..{hi}"
            # A CHUNK IS NOT ONE GENERATION JUST BECAUSE ITS NEWEST FILE IS. Adding one
            # refreshed table beside three retired ones leaves the directory reading as
            # current on its newest mtime, which is exactly how a stale number keeps
            # getting quoted. Grade the span, not the maximum.
            if generation(lo) != gen:
                grade = "MIXED"
                gen = f"{generation(lo)}..{gen}"
        rows.append(dict(chunk=rel, files=n, grade=grade, generation=gen,
                         evidence=evidence))

    df = pd.DataFrame(rows)
    print()
    print(df.to_string(index=False))
    print("\n`dated` rows are NOT evidence about which bake was read -- only about which "
          "one was deployed when the bytes were written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
