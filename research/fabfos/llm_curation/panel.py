"""Freeze the reactions the LLM lane is measured on, once, so a later run scores the same cases.

Three files come out of here and none of them is regenerated casually: `dev.jsonl` is
what prompt revisions iterate against, `heldout.jsonl` is scored exactly once with the
frozen prompt, and `controls.jsonl` is the regression gate. Re-running with the same
`--seed` reproduces all three; re-running with a different one silently invalidates every
scoreboard row taken before it, which is why the seed is written into `PANEL.md`.

**The sample is drawn from the whole residual, and reported in three strata.** 696 of the
12,417 residual reactions carry no blockers at all — the mapper was handed a fully
structured equation and returned nothing anyway — and a further few percent have an empty
side. No rewrite reaches either: there is nothing to remove in the first and nothing to
balance against in the second. Dropping them would flatter the lane; folding them silently
into one number would make iteration chase strata it cannot move. So they stay in the
denominator and carry a `stratum` field, and the scoreboard reports each. Same discipline
as `measure_rescue.py`'s two denominators, for the same reason.

The three structurally hopeless outcomes (`no_transfer`, `non_molecule`,
`unparseable_equation`) are excluded before sampling: they are not reactions with a hard
equation, they are non-reactions.

**Every participant is annotated with whether MetaNetX has a structure for it, and the
SMILES when it does.** That is the whole of defect I3 — the pilot's agent was never told
which accessions were unusable, so in ten of twelve hard failures it rebuilt the answer
out of the very blockers it was asked to remove, four times reporting `unchanged` at high
confidence. The record shape below makes that failure unavailable rather than unlikely.

    PYTHONPATH=src mamba run -n rdkit-scratch python research/fabfos/llm_curation/panel.py
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/direction_rescue"))

from measure_rescue import MNX, load_names                                    # noqa: E402
from ecspr.bake.direction.refdata import (load_mnxm_props,                    # noqa: E402
                                          load_mnxr_stoich)

# Outcomes a rewrite could in principle reach. The three excluded ones -- no_transfer,
# non_molecule, unparseable_equation -- are not hard reactions but non-reactions.
RESIDUAL_OUTCOMES = ("rescue_declined", "mapped_nothing",
                     "partial_declined", "rescued_nothing")

# A control has to have enough of an equation to be wrecked. An empty or two-term
# equation balances trivially and would inflate the control pass rate for free.
CONTROL_MIN_TERMS = 4


def balances(rec: dict, res) -> bool:
    """Does the equation, exactly as MetaNetX writes it, balance on heavy atoms?

    Controls are screened on this and it is not a formality. Banking a reaction does not
    require it to balance -- the AAM lane banks the elements that mapped -- so a third of
    the fully-structured banked population does not, and a control that never balanced
    cannot be "left balanced" by anything the model does. Without this screen the gate
    reports regressions that are properties of MetaNetX rather than of the lane, which is
    exactly what the first run of it did.
    """
    from arbiter import as_terms
    left, lerr = res.side(as_terms(rec["left"]))
    right, rerr = res.side(as_terms(rec["right"]))
    if lerr or rerr or not left or not right:
        return False
    return all(abs(right.get(k, 0) - left.get(k, 0)) < 1e-9
               for k in set(left) | set(right))


def term(mnxm: str, coef: float, names: dict, props: dict) -> dict:
    """One participant, with the fact the pilot's prompt was missing."""
    smiles = (props.get(mnxm) or {}).get("smiles")
    return {
        "id": mnxm,
        "coef": float(coef),
        "name": names.get(mnxm) or mnxm,
        "has_structure": smiles is not None,
        "smiles": smiles,
    }


def record(mnxr: str, st: dict, names: dict, props: dict,
           led: pd.DataFrame, wl: pd.DataFrame, tier: dict) -> dict | None:
    """One panel record, or None if MetaNetX has no stoichiometry for the reaction at all.

    A reaction whose participants all sit on one side gets a record too, tagged
    `one_sided`. No rewrite balances an equation with an empty side, so it is a ceiling
    on coverage rather than a failure of the prompt -- and a ceiling that is dropped
    rather than counted is a ceiling nobody sees.
    """
    stoich = st.get(mnxr)
    if not stoich:
        return None
    coefs = stoich[0]
    left = [term(k, -c, names, props) for k, c in coefs.items() if c < 0]
    right = [term(k, c, names, props) for k, c in coefs.items() if c > 0]
    w = wl.loc[mnxr] if mnxr in wl.index else None
    blockers = [t["id"] for t in left + right if not t["has_structure"]]
    if not left or not right:
        stratum = "one_sided"
    elif blockers:
        stratum = "movable"
    else:
        stratum = "no_lever"
    return {
        "mnxr": mnxr,
        "left": left,
        "right": right,
        "n_terms": len(left) + len(right),
        "blockers": blockers,
        "n_blockers": len(blockers),
        "blocker_families": sorted(set(w.blocker_families)) if w is not None
                            and w.blocker_families is not None else [],
        "outcome": str(led.loc[mnxr, "outcome"]),
        "n_pairs": int(led.loc[mnxr, "n_pairs"]),
        "dir_tier": tier.get(mnxr),
        "stratum": stratum,
    }


def write(path: Path, rows: list[dict]) -> None:
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def strata(rows: list[dict]) -> dict:
    return dict(Counter(r["stratum"] for r in rows))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bake", default="data/fabfos/processed/metabolism_bake")
    ap.add_argument("--frac", type=float, default=0.10)
    ap.add_argument("--dev", type=int, default=400)
    ap.add_argument("--controls", type=int, default=200)
    ap.add_argument("--seed", type=int, default=20260818)
    ap.add_argument("--out", type=Path, default=HERE / "panel")
    a = ap.parse_args()

    seams = ROOT / a.bake / "seams"
    led = pd.read_parquet(seams / "aam_ledger.parquet").set_index("mnxr")
    wl = pd.read_parquet(seams / "aam_worklist.parquet").set_index("mnxr")
    ann = pd.read_parquet(seams / "direction_annotation.parquet").set_index("mnxr")
    tier = ann["dir_tier"].astype(int).to_dict()

    names = load_names()
    st = load_mnxr_stoich(MNX / "reac_prop.tsv")
    props = load_mnxm_props(MNX / "chem_prop.tsv")

    residual = sorted(led.index[led.outcome.isin(RESIDUAL_OUTCOMES)])
    banked = led.index[(led.outcome == "banked") & (led.n_pairs > 0)]

    rng = random.Random(a.seed)
    n = round(len(residual) * a.frac)
    picked = rng.sample(residual, n)
    rng.shuffle(picked)

    rows = [r for r in (record(m, st, names, props, led, wl, tier) for m in picked) if r]
    dev, heldout = rows[:a.dev], rows[a.dev:]

    # Controls are screened before sampling, not after, so the requested count is the
    # delivered count. The balance screen needs a structure recount per candidate, so the
    # pool is walked in shuffled order and stopped at the target rather than scored whole.
    from arbiter import Resolver
    res = Resolver(props)
    cand = [m for m in banked
            if m in st and len(st[m][0]) >= CONTROL_MIN_TERMS
            and all((props.get(k) or {}).get("smiles") for k in st[m][0])]
    rng.shuffle(cand)
    crows, screened = [], 0
    for m in cand:
        if len(crows) >= a.controls:
            break
        r = record(m, st, names, props, led, wl, tier)
        screened += 1
        if r and balances(r, res):
            r["orig_balanced"] = True
            crows.append(r)
    pool = cand

    a.out.mkdir(parents=True, exist_ok=True)
    write(a.out / "dev.jsonl", dev)
    write(a.out / "heldout.jsonl", heldout)
    write(a.out / "controls.jsonl", crows)

    (a.out / "PANEL.md").write_text(
        f"""# The frozen panel

Built by `panel.py` from `{a.bake}` with `--seed {a.seed} --frac {a.frac} --dev {a.dev}
--controls {a.controls}`. **Re-running with a different seed invalidates every scoreboard
row taken before it.** Regenerate only with these arguments.

| split | file | n | movable | no_lever | one_sided |
|---|---|---:|---:|---:|---:|
| dev | `dev.jsonl` | {len(dev)} | {strata(dev).get('movable', 0)} | {strata(dev).get('no_lever', 0)} | {strata(dev).get('one_sided', 0)} |
| held-out | `heldout.jsonl` | {len(heldout)} | {strata(heldout).get('movable', 0)} | {strata(heldout).get('no_lever', 0)} | {strata(heldout).get('one_sided', 0)} |
| controls | `controls.jsonl` | {len(crows)} | — | — | — |

Sampled from {len(residual)} residual reactions — every reaction whose ledger outcome is
one of {', '.join(f'`{o}`' for o in RESIDUAL_OUTCOMES)}.

Controls are drawn from the {len(pool)} banked reactions that carry at least
{CONTROL_MIN_TERMS} participants and a structure for every one of them, **and that balance
on heavy atoms exactly as MetaNetX writes them** — {len(crows)} kept from {screened}
screened, so roughly {1 - len(crows) / max(screened, 1):.0%} of the fully-structured
banked population does not balance as written. Banking does not require balance; the AAM
lane banks the elements that mapped. A control that never balanced cannot be left balanced
by anything the model does, and without this screen the gate reports regressions that are
properties of MetaNetX rather than of the lane.

Two strata are ceilings on coverage rather than failures of the prompt, and both stay in
the denominator so the ceiling stays visible. `no_lever` reactions carry no blockers: the
mapper was handed a fully structured equation and returned nothing anyway, so there is
nothing for a rewrite to remove. `one_sided` reactions have an empty side, which no
rewrite balances. Iterating against either is wasted GPU; hiding either flatters the lane.
Report per stratum.

Each record annotates every participant with `has_structure` and, where one exists,
`smiles`. That is not decoration: the pilot's prompt withheld it, and the agent rebuilt
its answers out of the very blockers it was asked to remove in ten of twelve failures.
""")

    print(f"residual {len(residual)}  banked {len(banked)}  control pool {len(pool)}")
    print(f"dev      {len(dev):>5}  {strata(dev)}")
    print(f"heldout  {len(heldout):>5}  {strata(heldout)}")
    print(f"controls {len(crows):>5}")
    print(f"-> {a.out}")


if __name__ == "__main__":
    main()
