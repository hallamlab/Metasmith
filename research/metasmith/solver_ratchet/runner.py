"""Run one msm_solver binary over a named payload set and report deterministic counts.

The reply carries no timings, so its sha256 is an exact identity for "this build made
this plan" -- which is what the default-path gate compares. Wall clock is recorded but
is only meaningful when the box is quiet; the ranking metrics are the counts.
"""
import argparse, hashlib, json, os, subprocess, sys, time
from pathlib import Path

def run_one(binary, payload_file, max_iter, max_refine, mem, timeout, env, reply_out):
    doc = json.loads(Path(payload_file).read_text())
    doc["max_iter"] = max_iter
    doc["max_refine"] = max_refine
    blob = json.dumps(doc).encode()
    cmd = ["systemd-run", "--user", "--scope", "-q", "-p", f"MemoryMax={mem}",
           str(binary), "solve"]
    e = dict(os.environ)
    e.pop("MSM_SOLVER_PUCT", None)
    e.update(env)
    t0 = time.monotonic()
    try:
        p = subprocess.run(cmd, input=blob, capture_output=True, timeout=timeout, env=e)
        rc, out, err = p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return {"rc": "timeout", "wall": timeout, "steps": None, "complete": False,
                "iterations": None, "refiner_iters": None, "digest": None, "err": "timeout"}
    wall = time.monotonic() - t0
    row = {"rc": rc, "wall": round(wall, 3), "digest": hashlib.sha256(out).hexdigest()[:16]}
    if rc != 0:
        row.update(steps=None, complete=False, iterations=None, refiner_iters=None,
                   err=err.decode(errors="replace")[-400:])
        return row
    if reply_out:
        Path(reply_out).write_bytes(out)
    d = json.loads(out)
    row.update(steps=len(d["steps"]), complete=bool(d["complete"]),
               iterations=d.get("iterations"),
               refiner_iters=sum(b for _, b in d.get("refiner_iterations") or []))
    return row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--payloads", required=True)
    ap.add_argument("--set", default="fast")
    ap.add_argument("--out", required=True)
    ap.add_argument("--replies", default=None)
    ap.add_argument("--puct", default=None)
    ap.add_argument("--mem", default="8G")
    a = ap.parse_args()

    pdir = Path(a.payloads)
    manifest = json.loads((pdir / "manifest.json").read_text())
    entries = manifest[a.set]
    env = {}
    if a.puct:
        env["MSM_SOLVER_PUCT"] = a.puct
    if a.replies:
        Path(a.replies).mkdir(parents=True, exist_ok=True)

    rows = {}
    for ent in entries:
        name = ent["name"]
        reply_out = str(Path(a.replies) / f"{name}.reply.json") if a.replies else None
        rows[name] = run_one(a.bin, pdir / ent["file"], ent["iter"], ent["refine"],
                             a.mem, ent["timeout"], env, reply_out)
    doc = {"set": a.set, "puct": a.puct, "rows": rows}
    Path(a.out).write_text(json.dumps(doc, indent=2))
    print(json.dumps(doc["rows"], indent=2))

if __name__ == "__main__":
    main()
