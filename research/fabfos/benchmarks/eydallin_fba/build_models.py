#!/usr/bin/env python3
"""Build the host models this arm compares clones against, one per GPR channel.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin_fba/build_models.py --report

THE CONSTRUCTION MOVED INTO `ecspr.model.fba`. This module is now the study's driver over
it: it names the frame, the medium and the two baseline conditions, and the carve, the
MetaNetX insertion, the cycle closure and the gapfill happen in the library that both arms
share. Run `build_tables.py` first -- it writes the conditions, tests and media tables this
reads.

Both channels use `iECDH1ME8569_1439` as the stoichiometric frame, because a GPR table
carries gene-to-reaction evidence and no stoichiometry at all. What differs is which of
that frame's reactions each channel's evidence keeps, and what each channel adds.

**The GEM host** keeps what the AG1 host GPR table asserts. AG1 borrows DH1's model and the
borrow withholds one reaction, GTPDPK, for relA1. Two independent sources say so and the
build asserts against both: `check_ag1_identity.py`'s declared `EXPECTED_LOST_REACTIONS`,
and the table's own coverage of the frame. A future strain edit that moves one source and
not the other fails here rather than silently.

**The de-novo host** carves to lane-supported reactions, inserts lane-asserted MNXR the GEM
never named, closes the energy-generating cycles insertion creates, gapfills back to
biomass and glycogen feasibility, and closes the cycles again.

**WARNING** The carve is deep. At two lanes it keeps 1,260 of 2,755 reactions, 530 of them
ungoverned, and biomass goes to zero. Gapfill therefore does real work and its count is a
result rather than a detail.

**CAUTION** `min_lanes` is the one knob that moves everything downstream, and it is a lane
COUNT that reads no weight at all. REPORT.md documents two lanes as the fix for the
ProtBERT lane's promiscuity, and two is the right default for a question about annotation
quality in general. It is the wrong default here: no two lanes agree on any reaction of the
glycogen pathway, so a two-lane carve removes the phenotype under study and only gapfill
puts it back. This arm defaults to one lane and `--sweep` builds all three.

**CAUTION** The gapfill runs in M9 for every model, whatever medium the model is later
measured in. That makes each host a claim about the organism rather than about the broth.
Repairing under Kornberg instead is not equivalent: it draws 53 reactions rather than 58,
and the cycle closure that follows then takes growth back to zero.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))
import common as C  # noqa: E402

from ecspr.model import fba as F  # noqa: E402
from ecspr.model import mnx as MX  # noqa: E402

EXPECTED_LOST = {"GTPDPK"}
MODELS = C.OUT / "models"
TABLES = C.OUT / "tables"
GAPFILL_MEDIA = "m9"


def _inputs():
    """The corpus, the conditions, the media and the MetaNetX release, from the tables.

    **CAUTION** THE CURATED TABLE IS STAGED FOR THE DE-NOVO CHANNEL TOO, and not as a
    background. It is the only source of a frame-reaction-to-MNXR crosswalk in this
    project's pinned MetaNetX release: the frame's own `metanetx.reaction` annotation is
    from whichever release BiGG published against, and the two disagree on 1,231 of 2,751
    reactions here. Carving on the frame's annotation keeps 1,095 reactions where the
    pinned crosswalk keeps 1,720, and nothing raises.
    """
    from ecspr.model import conditions as cond_mod
    from ecspr.model.probes import load_corpus
    if not (TABLES / "conditions.parquet").exists():
        raise SystemExit(f"run build_tables.py first: {TABLES} has no conditions table")
    corpus = load_corpus(sorted(TABLES.glob("*_gem.parquet"))
                         + sorted(TABLES.glob("*_denovo.parquet")))
    conds = {c.condition_id: c for c in cond_mod.read(TABLES / "conditions.parquet")}
    media = F.load_media(TABLES / "media.parquet")
    return corpus, conds, media, MX.Release(C.MNX, cache=C.CACHE)


def gem_host(corpus, conds, media):
    """The frame minus whatever the AG1 host GPR table does not assert."""
    model, census = F.build(C.GEM_DH1, conds["gem:baseline"], corpus,
                            media[GAPFILL_MEDIA], min_lanes=1, target=C.GLYCOGEN_MET,
                            release=None, do_gapfill=False)
    lost = set(census.get("carved_away_ids", []))
    if lost != EXPECTED_LOST:
        raise SystemExit(
            f"the AG1 host GPR omits {sorted(lost)}; check_ag1_identity.py declares "
            f"{sorted(EXPECTED_LOST)}. The two sources disagree, so one of them moved.")
    model.id = "ag1_gem"
    census["channel"] = "gem"
    return model, census


def denovo_host(corpus, conds, media, release, min_lanes: int = 1, gapfill: bool = True):
    model, census = F.build(C.GEM_DH1, conds["denovo:baseline"], corpus,
                            media[GAPFILL_MEDIA], min_lanes=min_lanes,
                            target=C.GLYCOGEN_MET, release=release, do_gapfill=gapfill)
    model.id = f"ag1_denovo_l{min_lanes}"
    census["channel"] = "denovo"
    census["min_lanes"] = min_lanes
    return model, census


def _growth(model, medium) -> float:
    with model:
        F.apply_medium(model, medium)
        v = model.slim_optimize()
    return float(v) if v is not None and v == v else 0.0


def _max_glycogen(model, medium, pin: float) -> float:
    with model:
        F.apply_medium(model, medium)
        mu = model.slim_optimize()
        if mu is None or mu != mu:
            return 0.0
        for r in model.reactions:
            if r.objective_coefficient:
                r.lower_bound = max(r.lower_bound, pin * mu)
        model.objective = C.DEMAND
        v = model.slim_optimize()
    return float(v) if v is not None and v == v else 0.0


def profile(model, label: str, media: dict) -> dict:
    # `model.genes` keeps every gene the frame shipped with, carved or not, so count the
    # ones that still govern a reaction.
    live = {g.id for r in model.reactions for g in r.genes}
    out = {"model": label,
           "reactions": len(model.reactions),
           "metabolites": len({m.id for r in model.reactions for m in r.metabolites}),
           "genes": len(live)}
    for name in ("m9", "kornberg"):
        med = media[name]
        out[f"growth_{name}"] = round(_growth(model, med), 6)
        for pin in (0.9, 0.5, 0.1):
            out[f"glycogen_{name}_pin{pin}"] = round(_max_glycogen(model, med, pin), 6)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-lanes", type=int, default=1)
    ap.add_argument("--sweep", action="store_true",
                    help="build the de-novo host at one, two and three lanes")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--no-gapfill", action="store_true")
    a = ap.parse_args()

    MODELS.mkdir(parents=True, exist_ok=True)
    import cobra

    corpus, conds, media, release = _inputs()
    census, profiles = [], []

    gem, c = gem_host(corpus, conds, media)
    census.append(c)
    cobra.io.save_json_model(gem, str(MODELS / "ag1_gem.json"))
    profiles.append(profile(gem, "ag1_gem", media))

    for k in ((1, 2, 3) if a.sweep else (a.min_lanes,)):
        dn, c = denovo_host(corpus, conds, media, release, k,
                            gapfill=not a.no_gapfill)
        census.append(c)
        cobra.io.save_json_model(dn, str(MODELS / f"ag1_denovo_l{k}.json"))
        profiles.append(profile(dn, f"ag1_denovo_l{k}", media))

    (C.OUT / "host_census.json").write_text(json.dumps(census, indent=2, default=str))
    (C.OUT / "host_profile.json").write_text(json.dumps(profiles, indent=2))

    if a.report:
        print(json.dumps(census, indent=2, default=str))
        print()
        keys = ["model", "reactions", "metabolites", "genes",
                "growth_m9", "growth_kornberg",
                "glycogen_m9_pin0.1", "glycogen_kornberg_pin0.1"]
        print("\t".join(keys))
        for p in profiles:
            print("\t".join(str(p.get(k)) for k in keys))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
