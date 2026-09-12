#!/usr/bin/env python3
"""Enumerate every public KBase Narrative and extract its app cells.

A Narrative is a Jupyter notebook whose app cells record, per step, the app that
ran, the parameters it was given, and the workspace references of the objects it
produced. That is enough to rebuild the analysis as a dependency graph, which is
what makes a story convertible into a metasmith template.

The prose, the code output and the rendered widgets are discarded at extraction:
they are most of the bytes and none of the workflow. The raw object is kept
gzipped so re-extraction costs nothing.

    narratives.py list                 # enumerate public narratives
    narratives.py fetch [--limit N]    # fetch + extract, resumable
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import threading
from pathlib import Path

import kbase_api as K

RAW = K.REPO / "data" / "kbase" / "narratives"
OUT = K.REPO / "research" / "kbase" / "narratives"
INDEX = RAW / "index.jsonl"
# Extracted cells are corpus, not analysis: 135 MB of parameters and job
# results. They live with the blobs under data/ and are pinned by DVC.
CELLS = RAW / "cells.jsonl"

WS_REF = re.compile(r"^\d+/\d+(/\d+)?$")


def enumerate_public() -> list[dict]:
    query = {"bool": {"must": [{"term": {"is_public": True}}]}}
    found, seen, offset = [], set(), 0
    while True:
        r = K.search("search_objects", {
            "query": query, "indexes": ["narrative"], "size": 100, "from": offset,
            "sort": [{"timestamp": {"order": "asc"}}], "track_total_hits": True,
        })
        hits = r["hits"]
        if not hits: break
        for h in hits:
            d = h["doc"]
            key = (d["access_group"], d["obj_id"])
            if key in seen: continue
            seen.add(key)
            found.append({
                "ws_id": d["access_group"], "obj_id": d["obj_id"],
                "ref": f"{d['access_group']}/{d['obj_id']}",
                "title": d.get("narrative_title"), "owner": d.get("owner"),
                "creator": d.get("creator"), "modified_at": d.get("modified_at"),
                "total_cells": d.get("total_cells"),
                "static_narrative_ref": d.get("static_narrative_ref"),
                "data_objects": len(d.get("data_objects") or []),
            })
        offset += len(hits)
        print(f"  listed {len(found)}/{r['count']}", flush=True)
        if offset >= r["count"]: break
    RAW.mkdir(parents=True, exist_ok=True)
    INDEX.write_text("".join(json.dumps(x) + "\n" for x in found))
    print(f"public narratives: {len(found)}")
    return found


def _refs_in(obj) -> list[str]:
    out = []
    def walk(o):
        if isinstance(o, str):
            if WS_REF.match(o): out.append(o)
        elif isinstance(o, dict):
            for v in o.values(): walk(v)
        elif isinstance(o, list):
            for v in o: walk(v)
    walk(obj)
    return sorted(set(out))


def extract(meta: dict, data: dict) -> dict:
    cells = []
    for i, c in enumerate(data.get("cells") or []):
        kb = (c.get("metadata") or {}).get("kbase") or {}
        ac = kb.get("appCell") or {}
        app = ac.get("app") or {}
        kind = kb.get("type") or c.get("cell_type")
        rec = {"i": i, "kind": kind}
        if app.get("id"):
            js = ((ac.get("exec") or {}).get("jobState") or {})
            result = js.get("result") or js.get("job_output")
            rec.update({
                "kind": "app",
                "app_id": app.get("id"),
                "tag": app.get("tag"),
                "version": app.get("version"),
                "params": ac.get("params"),
                "job_status": js.get("status") or js.get("job_state"),
                "result_refs": _refs_in(result),
                "result": result,
            })
        elif kind == "code":
            src = c.get("source") or ""
            # A narrative can also drive apps from a code cell through the app
            # manager, which leaves no app-cell metadata. Flag it rather than parse it.
            rec["drives_apps"] = ("AppManager" in src) or ("run_app" in src)
        cells.append(rec)

    app_cells = [c for c in cells if c["kind"] == "app"]
    return {
        **meta,
        "n_cells": len(cells),
        "n_app_cells": len(app_cells),
        "n_code_driving_apps": sum(1 for c in cells if c.get("drives_apps")),
        "apps_used": sorted({c["app_id"] for c in app_cells}),
        "cells": cells,
    }


_lock = threading.Lock()


def fetch_all(limit: int | None = None, workers: int = 8):
    index = [json.loads(l) for l in INDEX.read_text().splitlines() if l]
    RAW.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)

    done = set()
    if CELLS.exists():
        for l in CELLS.read_text().splitlines():
            if l: done.add(json.loads(l)["ref"])
    todo = [m for m in index if m["ref"] not in done]
    if limit: todo = todo[:limit]
    print(f"index {len(index)}, already extracted {len(done)}, fetching {len(todo)}")

    out = CELLS.open("a")

    def one(meta):
        blob = RAW / f"{meta['ws_id']}_{meta['obj_id']}.json.gz"
        if blob.exists():
            data = json.loads(gzip.decompress(blob.read_bytes()))
        else:
            # Deliberately NOT K.workspace(): the generic cache would keep a second,
            # uncompressed copy of every narrative, which is 2.4 GB against 823 MB
            # for the gzipped blobs that are the actual corpus.
            got = K.call("ws-uncached", K.WORKSPACE, "Workspace.get_objects2",
                         [{"objects": [{"ref": meta["ref"]}]}],
                         timeout=180, retries=3, no_cache=True)
            data = got["data"][0]["data"]
            blob.write_bytes(gzip.compress(json.dumps(data).encode(), 6))
        rec = extract(meta, data)
        with _lock:
            out.write(json.dumps(rec) + "\n")
            out.flush()
        return rec["n_app_cells"]

    got = K.fanout(one, todo, workers=workers, label="narratives")
    out.close()
    failed = [(m["ref"], repr(e)[:120]) for m, _r, e in got if e]
    print(f"fetched {len(got) - len(failed)} ok, {len(failed)} failed")
    for r, e in failed[:10]: print(f"  FAIL {r}: {e}")
    if failed:
        (OUT / "fetch_failures.jsonl").write_text(
            "".join(json.dumps({"ref": r, "error": e}) + "\n" for r, e in failed))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("phase", choices=["list", "fetch"])
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    if a.phase == "list": enumerate_public()
    else: fetch_all(a.limit, a.workers)


if __name__ == "__main__":
    main()
