"""Ask each reaction twice, once with the sides swapped, and keep only what survives.

The direction pilot's finding is the reason this file is shaped the way it is. Asked for a
direct opinion, the model did not call direction at all — it ratified whichever orientation
MetaNetX happened to write, scoring 100% on reactions written left-to-right and 15–30% on
those written right-to-left. That is a lift of 7–13% over the trivial baseline of answering
"as written" every time, and it is a *bias*, not noise: three independent opinions would
raise apparent confidence while leaving accuracy exactly where it was.

**So the second opinion is a second orientation, not a second sampling.** The same reaction
is presented as written and with its sides exchanged, and only the disagreement is kept:

    labels differ   the model gave the opposite answer to the reversed question, which is
                    what answering the chemistry looks like. Keep the as-written call.
    labels agree    it answered the layout -- "left to right" is true of any equation as
                    presented. Abstain.
    either unsure   abstain.

Agreement is the failure signal here, which inverts the usual reading of a consistency
check and is the whole point.

**MetaCyc is the answer key and never appears in the prompt.** `biocyc_category` is a
curated physiological call, and grading a model on something it was handed measures
nothing. The panel is balanced by *written* orientation and every number is reported per
class, because a single aggregate over an unbalanced pool hides exactly the bias this is
built to detect: the trivial baseline scores 100% on one class and 0% on the other, so it
scores whatever the class mix is.

    PYTHONPATH=src mamba run -n rdkit-scratch python .../direction.py panel
    PYTHONPATH=src mamba run -n ecspr        python .../direction.py run   --split ...
    PYTHONPATH=src mamba run -n rdkit-scratch python .../direction.py score --run ...
"""

from __future__ import annotations

import argparse
import collections
import json
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/direction_rescue"))
sys.path.insert(0, str(HERE))

TRUTH = {
    "LEFT-TO-RIGHT": "left_to_right", "PHYSIOL-LEFT-TO-RIGHT": "left_to_right",
    "RIGHT-TO-LEFT": "right_to_left", "PHYSIOL-RIGHT-TO-LEFT": "right_to_left",
}
FLIP = {"left_to_right": "right_to_left", "right_to_left": "left_to_right"}


def side(terms: list[dict]) -> str:
    bits = []
    for t in terms:
        n = t["coef"]
        pre = "" if abs(n - 1) < 1e-9 else f"{n:g} "
        bits.append(f"{pre}{t['name']}")
    return " + ".join(bits)


def cmd_panel(a) -> int:
    from measure_rescue import MNX, load_names
    from ecspr.bake.direction.refdata import load_mnxr_stoich

    ann = pd.read_parquet(ROOT / a.bake / "seams/direction_annotation.parquet")
    ann = ann[ann.biocyc_category.isin(TRUTH)]
    names = load_names()
    st = load_mnxr_stoich(MNX / "reac_prop.tsv")

    by_class = collections.defaultdict(list)
    for r in ann.itertuples(index=False):
        s = st.get(r.mnxr)
        if not s:
            continue
        coefs = s[0]
        left = [{"id": k, "coef": -c, "name": names.get(k) or k}
                for k, c in coefs.items() if c < 0]
        right = [{"id": k, "coef": c, "name": names.get(k) or k}
                 for k, c in coefs.items() if c > 0]
        if not left or not right or len(coefs) < 3:
            continue
        by_class[TRUTH[r.biocyc_category]].append({
            "mnxr": r.mnxr, "left": left, "right": right,
            "truth": TRUTH[r.biocyc_category],
            "biocyc_category": r.biocyc_category,
            "dir_tier": int(r.dir_tier), "ratio": float(r.ratio),
        })

    # Equal counts per class, whatever the pool's own mix is. An unbalanced pool would let
    # the trivial baseline score the class mix instead of 50%, and every accuracy below it
    # would inherit that.
    # Equal counts per class in EACH split, not merely overall. Balancing only the pool
    # would leave the dev/heldout shuffle to decide each split's mix, and then the `ALL`
    # row of either scores that mix rather than the model.
    rng = random.Random(a.seed)
    per = min(a.dev + a.heldout, 2 * min(len(v) for v in by_class.values())) // 2
    dev, heldout = [], []
    for cls in ("left_to_right", "right_to_left"):
        take = rng.sample(by_class[cls], per)
        dev += take[:a.dev // 2]
        heldout += take[a.dev // 2:]
    rng.shuffle(dev)
    rng.shuffle(heldout)

    a.out.mkdir(parents=True, exist_ok=True)
    for nm, rows in (("dir_dev.jsonl", dev), ("dir_heldout.jsonl", heldout)):
        (a.out / nm).write_text("\n".join(json.dumps(r) for r in rows) + "\n")
        c = collections.Counter(r["truth"] for r in rows)
        print(f"  {nm:<18} {len(rows):>4}  {dict(c)}")
    print(f"pool by class: { {k: len(v) for k, v in by_class.items()} }")
    print(f"-> {a.out}")
    return 0


def cmd_run(a) -> int:
    from client import BadJSON, LLMClient, LLMConfig, SchemaRejected
    from run_panel import split_prompt
    from schema import DIRECTION

    system, template = split_prompt(a.prompt.read_text())
    recs = [json.loads(l) for l in a.split.read_text().splitlines() if l.strip()]
    if a.limit:
        recs = recs[:a.limit]
    cfg = LLMConfig(base_url=a.base_url, model=a.model, alias=a.alias or a.model,
                    dialect=a.dialect, max_tokens=a.max_tokens,
                    enable_thinking=a.thinking)

    lock, done, t0 = threading.Lock(), [0], time.perf_counter()
    with LLMClient(cfg) as client:
        if not client.health():
            raise SystemExit(f"no server at {a.base_url}")

        def ask(rec, swapped: bool):
            left, right = (rec["right"], rec["left"]) if swapped else (rec["left"], rec["right"])
            user = template.format(mnxr=rec["mnxr"],
                                   equation=f"{side(left)} = {side(right)}")
            try:
                obj, u = client.complete_json(system=system, user=user,
                                              schema=DIRECTION, name="direction")
                return {"call": obj.get("call"), "reason": obj.get("reason", ""),
                        "prompt_tokens": u.prompt_tokens,
                        "completion_tokens": u.completion_tokens, "error": None}
            except (BadJSON, SchemaRejected) as e:
                return {"call": None, "error": f"{type(e).__name__}: {e}",
                        "prompt_tokens": 0, "completion_tokens": 0}
            except Exception as e:
                return {"call": None, "error": f"{type(e).__name__}: {e}",
                        "prompt_tokens": 0, "completion_tokens": 0}

        def work(rec):
            out = {"mnxr": rec["mnxr"], "truth": rec["truth"],
                   "as_written": ask(rec, False), "swapped": ask(rec, True)}
            with lock:
                done[0] += 1
                if done[0] % 25 == 0 or done[0] == len(recs):
                    print(f"  {done[0]}/{len(recs)}  "
                          f"{time.perf_counter() - t0:.0f}s", flush=True)
            return out

        with ThreadPoolExecutor(max_workers=a.parallel) as pool:
            rows = list(pool.map(work, recs))
    wall = time.perf_counter() - t0

    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    tin = sum(r[k]["prompt_tokens"] for r in rows for k in ("as_written", "swapped"))
    tout = sum(r[k]["completion_tokens"] for r in rows for k in ("as_written", "swapped"))
    a.out.with_suffix(".meta.json").write_text(json.dumps({
        "revision": a.prompt.stem, "model": cfg.alias, "split": a.split.stem,
        "n": len(rows), "calls": 2 * len(rows), "prompt_tokens": tin,
        "completion_tokens": tout, "seconds": round(wall, 1),
        "parallel": a.parallel}, indent=1))
    print(f"\n{len(rows)} reactions x 2 orientations in {wall:.0f}s")
    print(f"  tokens {tin:,} in + {tout:,} out = "
          f"{(tin + tout) / max(len(rows), 1):.0f}/reaction")
    print(f"  -> {a.out}")
    return 0


def cmd_score(a) -> int:
    rows = [json.loads(l) for l in a.run.read_text().splitlines() if l.strip()]
    per = collections.defaultdict(collections.Counter)
    baseline = collections.defaultdict(collections.Counter)
    naive = collections.defaultdict(collections.Counter)

    for r in rows:
        cls, aw, sw = r["truth"], r["as_written"].get("call"), r["swapped"].get("call")
        baseline[cls]["correct" if cls == "left_to_right" else "wrong"] += 1
        # The single-pass model, ungated -- what the pilot measured.
        if aw in FLIP:
            naive[cls]["correct" if aw == cls else "wrong"] += 1
        else:
            naive[cls]["unsure"] += 1

        if aw not in FLIP or sw not in FLIP:
            per[cls]["abstain_unsure"] += 1
        elif aw == sw:
            per[cls]["abstain_layout"] += 1
        else:
            per[cls]["correct" if aw == cls else "wrong"] += 1

    def show(title, d):
        print(f"\n=== {title}")
        keys = sorted({k for c in d.values() for k in c})
        print(f"{'class':<16}" + "".join(f"{k:>16}" for k in keys) + f"{'accuracy':>12}")
        tot = collections.Counter()
        for cls in sorted(d):
            c = d[cls]
            tot.update(c)
            called = c["correct"] + c["wrong"]
            acc = f"{c['correct'] / called:.1%}" if called else "—"
            print(f"{cls:<16}" + "".join(f"{c[k]:>16}" for k in keys) + f"{acc:>12}")
        called = tot["correct"] + tot["wrong"]
        acc = f"{tot['correct'] / called:.1%}" if called else "—"
        print(f"{'ALL':<16}" + "".join(f"{tot[k]:>16}" for k in keys) + f"{acc:>12}")
        return tot

    show("TRIVIAL BASELINE — answer 'as written' every time", baseline)
    show("SINGLE PASS, ungated — what the pilot measured", naive)
    tot = show("DUAL ORIENTATION — kept only where the two answers disagree", per)

    called = tot["correct"] + tot["wrong"]
    print(f"\n  coverage: {called}/{len(rows)} reactions kept = "
          f"{called / max(len(rows), 1):.1%}")
    print(f"  abstained on layout agreement: {tot['abstain_layout']}"
          f"  (this is the bias being caught, not a failure)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("panel")
    p.add_argument("--bake", default="data/fabfos/processed/metabolism_bake")
    p.add_argument("--dev", type=int, default=200)
    p.add_argument("--heldout", type=int, default=400)
    p.add_argument("--seed", type=int, default=20260818)
    p.add_argument("--out", type=Path, default=HERE / "panel")
    p.set_defaults(fn=cmd_panel)

    p = sub.add_parser("run")
    p.add_argument("--prompt", type=Path, required=True)
    p.add_argument("--split", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--base-url", default="http://127.0.0.1:8080/v1")
    p.add_argument("--model", default="qwen3-32b")
    p.add_argument("--alias", default=None)
    p.add_argument("--dialect", default="llamacpp", choices=("llamacpp", "vllm"))
    p.add_argument("--parallel", type=int, default=8)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--max-tokens", type=int, default=512)
    p.add_argument("--thinking", action="store_true")
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("score")
    p.add_argument("--run", type=Path, required=True)
    p.set_defaults(fn=cmd_score)

    a = ap.parse_args()
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
