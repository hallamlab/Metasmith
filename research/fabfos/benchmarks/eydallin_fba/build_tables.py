#!/usr/bin/env python3
"""The cohort as the two tables `ecspr fba` reads, plus the medium as a third.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin_fba/build_tables.py

Everything this arm used to hold in Python -- which GPR table is the host, which rows are a
clone, which reactions the carve keeps -- becomes rows in `out/tables/`. The point is not
tidiness. The conductance arm reads the same conditions table, so a construction measured
both ways is now the SAME construction rather than two implementations that agreed by hand.

THREE TABLES OUT.

  conditions.parquet  one baseline per channel, plus one condition per clone: the host's
                      rows unioned with that clone's. That is the `add` convention, stated
                      as set algebra instead of as an insertion loop.
  gem_tests.parquet   the cross product of condition, medium and growth pin. `read_against`
                      names the baseline test of the same channel, medium and pin.
  media.parquet       M9 and Kornberg as exchange bounds on this frame. `kornberg.py` keeps
                      the derivation and the paper's own words; this writes down what it
                      computes, so the medium becomes an input rather than a function call.

**CAUTION** THE GPR TABLES ARE SYMLINKED UNDER NEW NAMES. A conditions table names its
tables by basename and both the host and the clone table are called `gpr_gem.parquet`, so
staging them as they lie makes one of them unreachable -- `load_corpus` refuses the pair
rather than picking one.

**CAUTION** These tables carry the migrated `llr`. The committed numbers in README.md were
produced when this arm's inputs held `raw_score`, so a difference is expected and is not a
regression. The carve still reads `min_lanes`, which is a lane COUNT and sees no weight at
all; `carve_threshold` is the column that does.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))
import common as C  # noqa: E402
import kornberg as K  # noqa: E402

from ecspr.model import conditions as cond_mod  # noqa: E402
from ecspr.model import fba as fba_mod  # noqa: E402
from ecspr.model import tests as test_mod  # noqa: E402
from ecspr.model import mnx as mnx_mod  # noqa: E402

TABLES = C.OUT / "tables"
MNX_DIR = C.MNX

FRAME = Path(C.GEM_DH1).stem
TARGET = C.GLYCOGEN_MET
PINS = (0.9, 0.5, 0.1)
MEDIA = ("kornberg", "m9")
MIN_LANES = 1

#: `<staged name>: <path>` per channel. The host table is the background of every
#: condition; the clone table is what a perturbation selects from.
SOURCES = {
    "gem": ("ag1_gem.parquet", C.HOST_GEM, "clones_gem.parquet", C.CLONE_GEM),
    "denovo": ("ag1_denovo.parquet", C.HOST_DENOVO, "clones_denovo.parquet",
               C.CLONE_DENOVO),
}


def stage() -> dict:
    """Symlink each GPR table under a name unique across the staged set."""
    TABLES.mkdir(parents=True, exist_ok=True)
    out = {}
    for channel, (host_name, host, clone_name, clone) in SOURCES.items():
        for name, src in ((host_name, host), (clone_name, clone)):
            link = TABLES / name
            if link.is_symlink() or link.exists():
                link.unlink()
            link.symlink_to(Path(src).resolve())
            out[name] = link
    return out


def media_rows(model, release) -> list:
    """M9 and Kornberg on this frame, as `(media, exchange, lower_bound, upper_bound)`."""
    formulas = K.formula_by_bigg(release.chem_prop, release.bigg)
    kb, absent = K.kornberg(model, formulas)
    rows = []
    for name, med in (("m9", K.m9(model)), ("kornberg", kb)):
        for rid, uptake in sorted(med.items()):
            if rid not in model.reactions:
                continue
            r = model.reactions.get_by_id(rid)
            rows.append((name, rid, -float(uptake), float(r.upper_bound)))
    return rows, absent


def build_conditions(corpus: dict) -> list:
    """One baseline per channel, and one condition per clone on top of it."""
    out = []
    for channel, (host_name, _h, clone_name, _c) in SOURCES.items():
        background = (cond_mod.Step(table=host_name, type="", set=(),
                                    operation="union"),)
        out.append(cond_mod.Condition(f"{channel}:baseline", background))
        clones = corpus[clone_name]
        for cid in dict.fromkeys(clones.condition_id.dropna().astype(str)):
            out.append(cond_mod.Condition(
                f"{channel}:{cid}",
                background + (cond_mod.Step(table=clone_name, type="condition_id",
                                            set=(cid,), operation="union"),)))
    return out


def build_tests(conditions) -> list:
    """Condition x medium x pin, each read against its own channel's baseline."""
    out = []
    for c in conditions:
        channel = c.condition_id.split(":", 1)[0]
        is_baseline = c.condition_id.endswith(":baseline")
        for medium in MEDIA:
            for pin in PINS:
                base = f"{channel}.{medium}.p{pin}.{channel}:baseline"
                out.append(test_mod.Test(
                    test_id=f"{channel}.{medium}.p{pin}.{c.condition_id}",
                    network=c.condition_id, arm="gem",
                    read_against="" if is_baseline else base,
                    settings=dict(frame=FRAME, media=medium, target_hub=TARGET,
                                  growth_pin=pin, min_lanes=MIN_LANES),
                    meta=dict(cohort=channel, is_control=False)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mnx", default=str(MNX_DIR))
    a = ap.parse_args()

    staged = stage()
    corpus = {name: pd.read_parquet(p) for name, p in staged.items()}

    conds = build_conditions(corpus)
    ts = build_tests(conds)
    cp = cond_mod.write(conds, TABLES / "conditions.parquet")
    tp = test_mod.write(ts, TABLES / "gem_tests.parquet", "gem")

    release = mnx_mod.Release(a.mnx, cache=C.CACHE)
    model = C.load_model()
    rows, absent = media_rows(model, release)
    mp = fba_mod.write_media(rows, TABLES / "media.parquet")

    print(f"{len(conds)} conditions -> {cp}")
    print(f"{len(ts)} tests -> {tp}")
    print(f"{len(rows)} media bounds -> {mp} "
          f"({len(absent)} yeast-extract compounds the frame cannot take up)")
    for channel, (host_name, _h, clone_name, _c) in SOURCES.items():
        print(f"  {channel}: host {len(corpus[host_name]):,} rows, "
              f"clones {len(corpus[clone_name]):,} rows over "
              f"{corpus[clone_name].condition_id.nunique()} conditions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
