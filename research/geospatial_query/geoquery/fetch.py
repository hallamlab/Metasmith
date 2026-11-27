"""One source over every AOI, one table out: a row per AOI x feature found, a status row per AOI without."""
import argparse, json, os, re, time, concurrent.futures as cf
import pandas as pd
from . import pipeline as P
from .registry import SOURCES, UNITS, specs

COLUMNS = ["aoi", "source", "observation_type", "time", "feature", "class", "family", "value",
           "value_num", "SI_unit", "licence", "native_res_m", "release", "item", "url", "status", "reason"]


# Shared public APIs throttle and stall when every source runs at once; these failures are retried.
TRANSIENT = re.compile(r"429|Too Many Requests|[Tt]imeout|timed out|HTTP 5\d\d|5\d\d Server Error|50[234]|ConnectionError|Max retries exceeded")
BACKOFF_S = (15, 60, 180)
# inside fetch_source's 30-minute limit: a stalled API (Overpass) must cost its AOIs, not the run
DEADLINE_S = 20 * 60


def records(v):
    """An adapter's feature value: a plain value, {value, class?, unit?}, or a list of those, one per class."""
    return v if isinstance(v, list) else [v if isinstance(v, dict) else {"value": v}]


def rows(rec, src):
    base = {"aoi": rec["aoi_id"], "source": rec["source"], "family": src["family"],
            "observation_type": src["observation_type"], "time": rec.get("obs_date"),
            "licence": rec["licence"], "native_res_m": rec.get("res_m"), "release": rec.get("release"),
            "item": rec.get("item"), "url": rec.get("url"), "status": rec["status"], "reason": rec["reason"]}
    if rec["status"] != "found":
        return [base]
    return [{**base, "feature": k, "class": r.get("class"), "value": r["value"],
             "SI_unit": r.get("unit", UNITS.get(k, ""))}
            for k, v in rec["features"].items() for r in records(v)]


def _run(todo, nwork, end):
    """run_one over todo until `end`; an AOI still running then is recorded, not awaited."""
    ex = cf.ThreadPoolExecutor(nwork)
    futs = [ex.submit(P.run_one, *t) for t in todo]
    cf.wait(futs, timeout=max(0.0, end - time.time()))
    ex.shutdown(wait=False, cancel_futures=True)
    return [f.result() if f.done() and not f.cancelled() else
            {"aoi_id": a["aoi_id"], "source": s[1], "licence": s[3], "status": "unavailable",
             "reason": f"DeadlineExceeded: the source did not answer within {DEADLINE_S // 60} minutes", "features": {}}
            for f, (a, s) in zip(futs, todo)]


def fetch(aois, source, nwork):
    todo = [(a, s) for s in specs() if s[1] == source for a in aois]
    if not todo:
        raise SystemExit(f"unknown source: {source}")
    end = time.time() + DEADLINE_S
    recs = _run(todo, nwork, end)
    for wait in BACKOFF_S:
        retry = [i for i, r in enumerate(recs)
                 if r["status"] == "unavailable" and TRANSIENT.search(r["reason"] or "")]
        if not retry or time.time() + wait >= end:
            break
        time.sleep(wait)
        # one at a time, so a throttled API sees one request at a time
        for i, r in zip(retry, _run([todo[i] for i in retry], 1, end)):
            recs[i] = r
    out = [row for r in recs for row in rows(r, SOURCES[source])]
    t = pd.DataFrame(out).reindex(columns=COLUMNS)
    t["value_num"] = pd.to_numeric(t["value"], errors="coerce")
    # identifiers are text: parsing 090805 as 90805 would corrupt the join key
    t.loc[t.feature.astype(str).str.contains(r"(?:_id|uid|_code)$"), "value_num"] = float("nan")
    return t


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--aois", required=True)
    ap.add_argument("--source", required=True, help="JSON file or string: {\"adapter\": name}")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    spec = json.load(open(a.source)) if os.path.exists(a.source) else json.loads(a.source)
    t = fetch(P.load_aois(a.aois), spec["adapter"], int(os.environ.get("NWORK", 4)))
    t.to_csv(a.out, index=False)
    print(f"{spec['adapter']}: {t.status.value_counts().to_dict()} -> {a.out}", flush=True)
    os._exit(0)  # do not wait on threads still stuck in a stalled request


if __name__ == "__main__":
    main()
