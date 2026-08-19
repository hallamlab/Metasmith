"""Recount the elements. The model's opinion of its own answer is not consulted.

`curation.py` is already built this way -- eleven ways of arguing from a name, one way of
deciding -- and this is the same shape one layer earlier: whatever the model proposes, the
verdict is a per-element recount from structures, and a rewrite that does not balance is
rejected rather than shipped. That makes the lane's precision 100% by construction, so the
only thing prompt iteration can move is coverage.

Four verdicts, and the distinction between the last two is the one that matters:

    balanced    every element cancels across the rewritten equation
    unbalanced  it does not -- real chemistry, wrongly done
    unusable    a term could not be resolved to a structure at all
    refused     the model declined, which costs coverage and never correctness

`unusable` was ten of the pilot's twelve hard failures and every one of them was the same
input-design bug rather than a chemistry error. It is kept separate for that reason: it
counts against the harness first and the model second.

Hydrogen is excluded. MetaNetX equations are frequently not proton-balanced as written, so
gating on H would fail almost everything for a reason that has nothing to do with the
rewrite. A `*` residue IS counted as an element, which is deliberate: a rewrite whose
purpose is to remove residue placeholders should not balance by leaving matching ones on
both sides, and counting them means it cannot.

Runs in `rdkit-scratch` -- rdkit is there and nowhere else, and httpx is everywhere else,
which is why this reads the runner's JSONL rather than calling the model itself.

    PYTHONPATH=src mamba run -n rdkit-scratch python \\
        research/fabfos/llm_curation/arbiter.py --run runs/aam_r1.dev.jsonl
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/direction_rescue"))

from measure_rescue import MNX                                                # noqa: E402
from ecspr.bake.direction.refdata import load_mnxm_props                      # noqa: E402

from rdkit import Chem, RDLogger                                             # noqa: E402

RDLogger.DisableLog("rdApp.*")

VERDICTS = ("balanced", "unbalanced", "unusable", "refused")


def heavy_counts(smiles: str) -> collections.Counter | None:
    """Heavy-atom element counts, or None if rdkit will not parse the SMILES."""
    m = Chem.MolFromSmiles(smiles)
    if m is None:
        return None
    return collections.Counter(a.GetSymbol() for a in m.GetAtoms()
                               if a.GetSymbol() != "H")


class Resolver:
    """Turns a proposed term into element counts, or says why it cannot."""

    def __init__(self, props: dict):
        self.props = props
        self._cache: dict[str, collections.Counter | None] = {}

    def _of_smiles(self, smiles: str) -> collections.Counter | None:
        if smiles not in self._cache:
            self._cache[smiles] = heavy_counts(smiles)
        return self._cache[smiles]

    def term(self, t: dict) -> tuple[collections.Counter | None, str | None]:
        n = float(t.get("n", 1) or 1)
        mnxm, smiles = t.get("id"), t.get("smiles")
        # An id is checked first and its OWN structure used: a term carrying both an
        # accession and a SMILES that disagree is the model asserting something MetaNetX
        # contradicts, and the bake's structure is the one every other lane reads.
        if mnxm:
            s = (self.props.get(str(mnxm)) or {}).get("smiles")
            if not s:
                return None, f"id {mnxm} has no structure in MetaNetX"
            c = self._of_smiles(s)
            if c is None:
                return None, f"id {mnxm} structure unparseable"
            return collections.Counter({k: v * n for k, v in c.items()}), None
        if smiles:
            c = self._of_smiles(smiles)
            if c is None:
                return None, f"smiles {smiles!r} unparseable"
            return collections.Counter({k: v * n for k, v in c.items()}), None
        return None, "term carries neither id nor smiles"

    def side(self, terms: list) -> tuple[collections.Counter, list[str]]:
        tot, errs = collections.Counter(), []
        for t in terms or []:
            c, e = self.term(t)
            if e:
                errs.append(e)
            else:
                tot.update(c)
        return tot, errs


def as_terms(side: list[dict]) -> list[dict]:
    """A panel side in the runner's term shape.

    The panel writes `coef` and the model writes `n`, and `Resolver.term` reads `n`. Left
    unconverted the panel's coefficients silently read as 1 apiece, so an equation echoed
    back verbatim compares unequal to itself -- which is how the control gate first
    reported four regressions that were arithmetic rather than chemistry.
    """
    return [{"id": t["id"], "n": t["coef"]} for t in side]


def judge(rec: dict, res: Resolver) -> dict:
    """Verdict for one model response, with the residual that produced it."""
    if rec.get("error"):
        return {"verdict": "unusable", "why": [f"harness: {rec['error']}"]}
    if rec.get("action") == "refuse":
        return {"verdict": "refused", "why": [rec.get("reason", "")[:120]]}

    left, lerr = res.side(rec.get("left"))
    right, rerr = res.side(rec.get("right"))
    if lerr or rerr:
        return {"verdict": "unusable", "why": lerr + rerr}
    if not left or not right:
        return {"verdict": "unusable", "why": ["a side of the rewrite is empty"]}

    residual = {k: right.get(k, 0) - left.get(k, 0)
                for k in set(left) | set(right)}
    residual = {k: v for k, v in residual.items() if abs(v) > 1e-9}
    out = {"verdict": "balanced" if not residual else "unbalanced",
           "residual": residual,
           "totals": {k: left.get(k, 0) for k in sorted(left)}}
    if "*" in left or "*" in right:
        out["residual_wildcards"] = True
    return out


def control_outcome(rec: dict, panel: dict, res: Resolver) -> str:
    """Controls: `refused` and `preserved` are both fine; anything else is a regression.

    A control is a reaction that banks today, so the lane must not make it worse. A
    refusal cannot -- in production a banked reaction never reaches this lane at all, so
    over-refusal on controls costs nothing. What would cost something is a rewrite that
    fails to balance, or one that balances at different element totals, because that is
    the model editing chemistry it was not asked to touch.
    """
    # A reaction the harness never got an answer for is not a regression. Scoring it as
    # one fails a good revision for an infrastructure reason -- a prompt that overran its
    # slot, a dropped connection -- and the gate is only worth having if it means chemistry.
    if rec.get("error"):
        return "unscorable"
    j = judge(rec, res)
    if j["verdict"] == "refused":
        return "refused"
    if j["verdict"] != "balanced":
        return "regressed"
    orig, errs = res.side(as_terms(panel["left"]))
    if errs:
        return "unscorable"
    got = collections.Counter(j["totals"])
    same = all(abs(got.get(k, 0) - orig.get(k, 0)) < 1e-9
               for k in set(got) | set(orig))
    return "preserved" if same else "regressed"


def load_jsonl(p: Path) -> list[dict]:
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


SCOREBOARD_COLS = ("revision", "model", "split", "n", "balanced", "unbalanced",
                   "unusable", "refused", "movable_n", "movable_balanced",
                   "prompt_tokens", "completion_tokens", "tokens_per_rxn",
                   "seconds", "rxn_per_min", "harness_errors", "note")


def append_scoreboard(path: Path, summary: dict, meta: dict, note: str) -> None:
    """One row per revision, with cost beside coverage so the two are read together.

    Wall-clock comes from the run's `.meta.json` rather than being re-derived: it is what
    converts a token count into the GPU-hours a universe-scale run would bill, and it is
    only knowable at the moment the run happened.
    """
    mv = summary.get("movable", {})
    n = summary.get("n", 0) or 1
    secs = meta.get("seconds", 0) or 0
    tok = summary.get("prompt_tokens", 0) + summary.get("completion_tokens", 0)
    row = {
        "revision": meta.get("revision", "?"), "model": meta.get("model", "?"),
        "split": meta.get("split", "?"), "n": summary.get("n", 0),
        "balanced": summary.get("balanced", summary.get("preserved", 0)),
        "unbalanced": summary.get("unbalanced", summary.get("regressed", 0)),
        "unusable": summary.get("unusable", summary.get("unscorable", 0)),
        "refused": summary.get("refused", 0),
        "movable_n": sum(mv.values()), "movable_balanced": mv.get("balanced", 0),
        "prompt_tokens": summary.get("prompt_tokens", 0),
        "completion_tokens": summary.get("completion_tokens", 0),
        "tokens_per_rxn": round(tok / n),
        "seconds": round(secs, 1),
        "rxn_per_min": round(n / secs * 60, 1) if secs else "",
        "harness_errors": meta.get("harness_errors", ""),
        "note": note,
    }
    new = not path.exists()
    with path.open("a") as f:
        if new:
            f.write("\t".join(SCOREBOARD_COLS) + "\n")
        f.write("\t".join(str(row[c]) for c in SCOREBOARD_COLS) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", type=Path, required=True, help="the runner's JSONL output")
    ap.add_argument("--panel", type=Path, required=True,
                    help="the split the run was taken over")
    ap.add_argument("--controls", action="store_true",
                    help="score as the control gate rather than as coverage")
    ap.add_argument("--detail", type=Path, help="write a per-reaction TSV here")
    ap.add_argument("--json", type=Path, help="write the summary as JSON here")
    ap.add_argument("--scoreboard", type=Path,
                    help="append a row here; wall-clock is read from the run's .meta.json")
    ap.add_argument("--note", default="", help="what changed in this revision")
    a = ap.parse_args()

    props = load_mnxm_props(MNX / "chem_prop.tsv")
    res = Resolver(props)
    panel = {r["mnxr"]: r for r in load_jsonl(a.panel)}
    runs = load_jsonl(a.run)

    seen, rows = set(), []
    by_stratum = collections.defaultdict(collections.Counter)
    ctrl = collections.Counter()

    for rec in runs:
        m = rec.get("mnxr")
        if m not in panel:
            print(f"  ! {m} is not in this panel split", file=sys.stderr)
            continue
        seen.add(m)
        p = panel[m]
        if a.controls:
            o = control_outcome(rec, p, res)
            ctrl[o] += 1
            rows.append((m, "control", o, "", rec.get("prompt_tokens", 0),
                         rec.get("completion_tokens", 0)))
            continue
        j = judge(rec, res)
        by_stratum[p["stratum"]][j["verdict"]] += 1
        rows.append((m, p["stratum"], j["verdict"],
                     json.dumps(j.get("residual", j.get("why", ""))),
                     rec.get("prompt_tokens", 0), rec.get("completion_tokens", 0)))

    missing = sorted(set(panel) - seen)
    tok_in = sum(r[4] for r in rows)
    tok_out = sum(r[5] for r in rows)

    if a.controls:
        n = sum(ctrl.values())
        print(f"=== CONTROL GATE  n={n}  (unanswered {len(missing)})")
        for k in ("preserved", "refused", "regressed", "unscorable"):
            print(f"  {k:<12} {ctrl[k]}")
        print(f"\n  REGRESSIONS: {ctrl['regressed']}  "
              f"({'PASS -- the gate is zero' if not ctrl['regressed'] else 'FAIL'})")
        summary = {"kind": "controls", "n": n, **dict(ctrl),
                   "unanswered": len(missing),
                   "prompt_tokens": tok_in, "completion_tokens": tok_out}
    else:
        allv = collections.Counter()
        for c in by_stratum.values():
            allv.update(c)
        n = sum(allv.values())
        print(f"=== COVERAGE  n={n} answered of {len(panel)} in the split "
              f"(unanswered {len(missing)})")
        print(f"{'stratum':<12} " + "".join(f"{v:>12}" for v in VERDICTS))
        for s in sorted(by_stratum):
            c = by_stratum[s]
            print(f"{s:<12} " + "".join(f"{c[v]:>12}" for v in VERDICTS))
        print(f"{'ALL':<12} " + "".join(f"{allv[v]:>12}" for v in VERDICTS))
        mv = by_stratum.get("movable", collections.Counter())
        nmv = sum(mv.values()) or 1
        print(f"\n  yield over the whole split : {allv['balanced']}/{len(panel)} "
              f"= {allv['balanced'] / max(len(panel), 1):.1%}")
        print(f"  yield over `movable` only  : {mv['balanced']}/{nmv} "
              f"= {mv['balanced'] / nmv:.1%}")
        summary = {"kind": "coverage", "n": n, "split_n": len(panel),
                   "unanswered": len(missing),
                   **{v: allv[v] for v in VERDICTS},
                   "movable": dict(mv),
                   "prompt_tokens": tok_in, "completion_tokens": tok_out}

    print(f"\n  tokens: {tok_in:,} in + {tok_out:,} out = {tok_in + tok_out:,}")

    if a.detail:
        with a.detail.open("w") as f:
            f.write("mnxr\tstratum\tverdict\tdetail\tprompt_tokens\tcompletion_tokens\n")
            for r in rows:
                f.write("\t".join(str(x) for x in r) + "\n")
        print(f"  -> {a.detail}")
    if a.json:
        a.json.write_text(json.dumps(summary, indent=1))
        print(f"  -> {a.json}")
    if a.scoreboard:
        mp = a.run.with_suffix(".meta.json")
        meta = json.loads(mp.read_text()) if mp.exists() else {}
        if not meta:
            print(f"  ! no {mp.name}: the row will carry no wall-clock", file=sys.stderr)
        append_scoreboard(a.scoreboard, summary, meta, a.note)
        print(f"  -> {a.scoreboard}")


if __name__ == "__main__":
    main()
