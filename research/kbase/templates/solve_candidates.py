#!/usr/bin/env python3
"""Turn each curated narrative shape into a metasmith spec and try to solve it.

This is the question the whole port exists to answer: can metasmith plan the
workflows KBase users actually ran?

One candidate becomes one solve. The shape's terminal apps give the targets, its
root apps give the deferred inputs, and the mask is the shape's own apps -- the
tightest honest library view. An unmasked solve is meaningless here: 46 apps are
pure sources, so every target resolves to "import it from staging".

Every failure is assigned a class rather than counted, because a failure class is
a work item and a count is not.

    solve_candidates.py [--limit N] [--workers N]
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
LIB = REPO / "research" / "kbase" / "library"
CATALOG = REPO / "research" / "kbase" / "catalog"
CANDIDATES = REPO / "research" / "kbase" / "curation" / "r1" / "candidates.jsonl"
OUT = HERE / "solve_results.jsonl"

sys.path.insert(0, str(LIB))
import mask  # noqa: E402

from metasmith.python_api import DataInstanceLibrary, Spec, DEFERRED  # noqa: E402


def type_of(ws: str) -> str | None:
    if "." not in ws: return None
    m, _, n = ws.partition(".")
    return f"{m}_{n}" if m and n and "." not in n else None


def union_index():
    """members tuple -> union type name, exactly as the generator named them."""
    sys.path.insert(0, str(LIB))
    import _generate
    apps = [json.loads(l) for l in (CATALOG / "apps.jsonl").read_text().splitlines() if l]
    unions, _tokens = _generate.build_unions(apps)
    return {members: name for name, (_t, members) in unions.items()}


def param_type(entry, unions) -> str | None:
    members = tuple(sorted(t for t in entry["ws_types"] if type_of(t)))
    if not members: return None
    return type_of(members[0]) if len(members) == 1 else unions.get(members)


def shape_plan(cand, cat, dag, unions):
    """(targets, external inputs, apps to drop from the mask) for one shape.

    The workflow's inputs are what its ROOT cells consume -- the cells nothing
    else feeds. Deriving them as "consumed but not produced anywhere in the
    shape" is wrong: a step's input type is often produced by a later step, and
    a union input is satisfied by any member, so the first step's own requirement
    disappears.

    A root that is a pure source -- an uploader whose input is a staging path
    rather than a typed object -- has no typed input to offer. Its product is the
    workflow's real starting point, so that becomes the deferred input and the
    uploader leaves the mask.

    Targets are the terminal cells' products, falling back to the report when a
    terminal produces only one. That is an answer rather than a gap: a
    differential-expression step's product IS its report.
    """
    in_shape = {a for a in cand["apps"] if a in cat}
    incoming = {e["to"] for e in dag["edges"]}
    outgoing = {e["from"] for e in dag["edges"]}

    roots, terminals = [], []
    for n, app in enumerate(dag["apps"]):
        if app not in in_shape: continue
        if n not in incoming: roots.append(app)
        if n not in outgoing: terminals.append(app)

    succ = {}
    for e in dag["edges"]:
        succ.setdefault(e["from"], []).append(e["to"])
    root_idx = [n for n, a in enumerate(dag["apps"]) if a in in_shape and n not in incoming]

    def typed_inputs(app):
        return [t for t in (param_type(e, unions) for e in cat[app]["inputs"]) if t]

    external, seen, drop = [], set(), set()
    for n in root_idx or [None]:
        app = dag["apps"][n] if n is not None else sorted(in_shape)[0]
        typed = typed_inputs(app)
        if not typed:
            # A pure source: an uploader whose input is a staging path. Its
            # product is the workflow's real start -- unless it declares no typed
            # product either, in which case the cells it feeds are the start.
            drop.add(app)
            typed = [t for t in (type_of(ws) for e in cat[app]["outputs"]
                                 for ws in e["ws_types"]) if t][:1]
            if not typed and n is not None:
                for m in succ.get(n, []):
                    nxt = dag["apps"][m]
                    if nxt in cat: typed += typed_inputs(nxt)
        for t in typed:
            if t not in seen:
                seen.add(t); external.append(t)

    # One target per output PARAMETER, never one per declared type. A parameter
    # accepting several types is a product group: the app produces one of them,
    # so demanding all four makes the plan unsatisfiable rather than ambitious.
    # And a target is a solver slot -- name only what nothing else reaches.
    targets, tseen = [], set()
    for app in terminals or sorted(in_shape)[:1]:
        if app in drop: continue
        for e in cat[app]["outputs"]:
            t = next((type_of(ws) for ws in e["ws_types"] if type_of(ws)), None)
            if t and t not in tseen:
                tseen.add(t); targets.append(t)
    if not targets:
        targets = ["report"]
    return targets, external, drop


def classify(err: str, task, targets) -> str:
    if task is not None and not task.ok:
        return "no_plan_all_targets" if not task.plan.steps else "no_plan_some_targets"
    e = err.lower()
    if "memory" in e or "killed" in e: return "out_of_memory"
    if "timeout" in e or "timed out" in e: return "timeout"
    if "no such type" in e or "gettype" in e: return "missing_type"
    return "raised"


def one(cand, cat, dags, workdir, unions):
    dag = dags[cand["representative"]]
    targets, inputs, drop = shape_plan(cand, cat, dag, unions)
    rec = {"shape_id": cand["shape_id"], "copies": cand["copies"],
           "n_steps": cand["n_steps"], "apps": cand["apps"],
           "representative": cand["representative"],
           "n_targets": len(targets), "n_inputs": len(inputs), "dropped_sources": sorted(drop),
           "targets": targets[:12], "inputs": inputs[:12]}
    if not targets:
        return {**rec, "ok": False, "reason": "no_typed_target", "steps": 0, "seconds": 0.0}
    if not inputs:
        return {**rec, "ok": False, "reason": "no_typed_input", "steps": 0, "seconds": 0.0}

    t0 = time.time()
    try:
        d = mask.materialise(workdir / f"{cand['shape_id']}_lib", set(cand["apps"]) - drop)
        lib = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix="msm-c-")))
        lib.AddTypeLibrary(LIB / "data_types" / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        for t in inputs:
            lib.AddItem(DEFERRED, f"kbase::{t}", parents={sample})
        spec = Spec(input_library=lib, sample_type="kbase::sample",
                    target_types=[f"kbase::{t}" for t in targets],
                    transform_libraries=[d],
                    resource_libraries=[LIB / "resources" / "env"])
        task = spec.Solve()
        return {**rec, "ok": bool(task.ok), "steps": len(task.plan.steps),
                "dropped": sorted(str(x) for x in task.plan.dropped_targets)[:8],
                "reason": "solved" if task.ok else classify("", task, targets),
                "seconds": round(time.time() - t0, 2)}
    except Exception as e:
        return {**rec, "ok": False, "steps": 0,
                "reason": classify(f"{e}\n{traceback.format_exc()}", None, targets),
                "error": repr(e)[:300], "seconds": round(time.time() - t0, 2)}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--resume", action="store_true")
    a = ap.parse_args()

    cat = {x["app_id"]: x for x in
           (json.loads(l) for l in (CATALOG / "apps.jsonl").read_text().splitlines() if l)}
    dags = {d["ref"]: d for d in
            (json.loads(l) for l in
             (REPO / "data/kbase/narratives/dags.jsonl").read_text().splitlines() if l)}
    cands = [json.loads(l) for l in CANDIDATES.read_text().splitlines() if l]
    if a.limit: cands = cands[:a.limit]
    # Resumable: this run is long enough to be interrupted, and it has been.
    done = set()
    if a.resume and OUT.exists():
        done = {json.loads(l)["shape_id"] for l in OUT.read_text().splitlines() if l}
        cands = [c for c in cands if c["shape_id"] not in done]
        print(f"resuming: {len(done)} already solved, {len(cands)} to go")

    unions = union_index()
    work = Path(tempfile.mkdtemp(prefix="msm-cand-"))
    done = 0
    with OUT.open("a" if a.resume else "w") as fh:
        for c in cands:
            r = one(c, cat, dags, work, unions)
            fh.write(json.dumps(r) + "\n"); fh.flush()
            shutil.rmtree(work / f"{c['shape_id']}_lib", ignore_errors=True)
            done += 1
            if done % 20 == 0:
                print(f"  {done}/{len(cands)}", flush=True)
    print(f"wrote {OUT.relative_to(REPO)} ({done} candidates)")


if __name__ == "__main__":
    main()
