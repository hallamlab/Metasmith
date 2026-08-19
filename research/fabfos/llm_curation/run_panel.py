"""Push one prompt revision through one split, and bill it.

The unit of iteration is a *revision*: a prompt file under `prompts/`, run over a split,
scored by `arbiter.py`, appended to the scoreboard as one row. Everything that would make
two rows incomparable is therefore recorded in the row -- the revision, the model, the
split, and the token and wall-clock cost -- rather than remembered.

**Cost is collected from the first run, not retrofitted.** llama.cpp returns a `usage`
block on every OpenAI-compatible response, the client hands it back per call, and the
totals land in the run's `.meta.json` alongside wall-clock. A revision whose coverage rose
because it spent triple the tokens is a different trade from one that did not, and there
is no way to see that afterwards from a coverage number alone.

The prompt file is markdown split by `## SYSTEM` and `## USER` headings. The USER half is
a `str.format` template over the fields built in `render` below, so a revision can change
what the model is shown as well as what it is told -- which is the whole of defect I3, and
not something a system-prompt-only design could have fixed.

Concurrency is threads against a server started with `--parallel`: a held allocation bills
wall-clock whether or not the GPU is busy, so the panel goes through it in a batch rather
than one request at a time.

    PYTHONPATH=src mamba run -n ecspr python research/fabfos/llm_curation/run_panel.py \\
        --prompt prompts/aam_r1.md --split panel/dev.jsonl --out runs/aam_r1.dev.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from client import BadJSON, LLMClient, LLMConfig, SchemaRejected, Usage    # noqa: E402
from schema import SIMPLIFY                                                # noqa: E402

_SECTION = re.compile(r"^##\s+(SYSTEM|USER)\s*$", re.M)


def split_prompt(text: str) -> tuple[str, str]:
    parts = _SECTION.split(text)
    if len(parts) < 5:
        raise SystemExit("prompt file needs both a '## SYSTEM' and a '## USER' heading")
    out = {}
    for i in range(1, len(parts) - 1, 2):
        out[parts[i]] = parts[i + 1].strip()
    return out["SYSTEM"], out["USER"]


def _side(terms: list[dict]) -> str:
    bits = []
    for t in terms:
        n = t["coef"]
        pre = "" if abs(n - 1) < 1e-9 else f"{n:g} "
        bits.append(f"{pre}{t['name']} [{t['id']}]")
    return " + ".join(bits)


def render(rec: dict) -> dict:
    avail, blocked = [], []
    for t in rec["left"] + rec["right"]:
        if t["has_structure"]:
            avail.append(f"  {t['id']}  {t['name']}\n      SMILES: {t['smiles']}")
        else:
            blocked.append(f"  {t['id']}  {t['name']}   <-- NO STRUCTURE")
    return {
        "mnxr": rec["mnxr"],
        "equation": f"{_side(rec['left'])} = {_side(rec['right'])}",
        "available": "\n".join(avail) or "  (none)",
        "blocked": "\n".join(blocked) or "  (none)",
        "n_blockers": rec["n_blockers"],
        "families": ", ".join(rec["blocker_families"]) or "none",
    }


def one(client: LLMClient, system: str, template: str, rec: dict) -> dict:
    out = {"mnxr": rec["mnxr"], "stratum": rec["stratum"],
           "prompt_tokens": 0, "completion_tokens": 0, "seconds": 0.0, "error": None}
    try:
        obj, u = client.complete_json(system=system, user=template.format(**render(rec)),
                                      schema=SIMPLIFY, name="simplify")
        out.update(obj)
        out.update(prompt_tokens=u.prompt_tokens, completion_tokens=u.completion_tokens,
                   seconds=u.seconds)
    except (BadJSON, SchemaRejected) as e:
        out["error"] = f"{type(e).__name__}: {e}"
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prompt", type=Path, required=True)
    ap.add_argument("--split", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--base-url", default="http://127.0.0.1:8080/v1")
    ap.add_argument("--model", default="qwen3-32b")
    ap.add_argument("--alias", default=None, help="scoreboard tag; defaults to --model")
    ap.add_argument("--dialect", default="llamacpp", choices=("llamacpp", "vllm"))
    ap.add_argument("--parallel", type=int, default=8,
                    help="match the server's --parallel; a held card idles otherwise")
    ap.add_argument("--limit", type=int, default=0, help="first N records only, for a probe")
    ap.add_argument("--max-tokens", type=int, default=2048)
    ap.add_argument("--thinking", action="store_true",
                    help="leave Qwen3's reasoning preamble on; it is billed either way")
    a = ap.parse_args()

    system, template = split_prompt(a.prompt.read_text())
    recs = [json.loads(l) for l in a.split.read_text().splitlines() if l.strip()]
    if a.limit:
        recs = recs[:a.limit]

    cfg = LLMConfig(base_url=a.base_url, model=a.model, alias=a.alias or a.model,
                    dialect=a.dialect, max_tokens=a.max_tokens,
                    enable_thinking=a.thinking)
    a.out.parent.mkdir(parents=True, exist_ok=True)

    lock = threading.Lock()
    done = [0]
    t0 = time.perf_counter()
    with LLMClient(cfg) as client:
        if not client.health():
            raise SystemExit(f"no server at {a.base_url} -- is the tunnel up?")

        def work(rec):
            r = one(client, system, template, rec)
            with lock:
                done[0] += 1
                if done[0] % 25 == 0 or done[0] == len(recs):
                    print(f"  {done[0]}/{len(recs)}  "
                          f"{time.perf_counter() - t0:.0f}s", flush=True)
            return r

        with ThreadPoolExecutor(max_workers=a.parallel) as pool:
            rows = list(pool.map(work, recs))
    wall = time.perf_counter() - t0

    with a.out.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

    total = Usage(sum(r["prompt_tokens"] for r in rows),
                  sum(r["completion_tokens"] for r in rows), wall)
    errs = sum(1 for r in rows if r["error"])
    meta = {
        "revision": a.prompt.stem, "model": cfg.alias, "split": a.split.stem,
        "n": len(rows), "harness_errors": errs,
        "prompt_tokens": total.prompt_tokens,
        "completion_tokens": total.completion_tokens,
        "seconds": round(wall, 1), "parallel": a.parallel,
        "thinking": a.thinking, "max_tokens": a.max_tokens,
    }
    a.out.with_suffix(".meta.json").write_text(json.dumps(meta, indent=1))

    per = (total.prompt_tokens + total.completion_tokens) / max(len(rows), 1)
    print(f"\n{len(rows)} reactions in {wall:.0f}s "
          f"({len(rows) / max(wall, 1e-9) * 60:.1f}/min, {a.parallel}-wide)")
    print(f"  tokens {total.prompt_tokens:,} in + {total.completion_tokens:,} out "
          f"= {per:.0f}/reaction")
    if errs:
        print(f"  !! {errs} harness errors -- a constrained decode should not produce these")
    print(f"  -> {a.out}\n  -> {a.out.with_suffix('.meta.json')}")


if __name__ == "__main__":
    main()
