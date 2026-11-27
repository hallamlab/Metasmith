import json, os, sys, time, glob, subprocess
API = "https://search.foldseek.com/api"
DBS = ["pdb100", "afdb-swissprot", "afdb50"]
os.makedirs("foldseek", exist_ok=True)
def sh(args):
    return subprocess.run(args, capture_output=True, text=True, timeout=300).stdout
for pdb in sorted(glob.glob("structures/*.pdb")):
    gid = os.path.basename(pdb)[:-4]
    out = f"foldseek/{gid}.json"
    if os.path.exists(out): continue
    args = ["curl", "-s", "-m", "120", "-X", "POST", "-F", f"q=@{pdb}", "-F", "mode=3diaa"]
    for d in DBS: args += ["-F", f"database[]={d}"]
    try:
        t = json.loads(sh(args + [f"{API}/ticket"]))
    except Exception as e:
        print(gid, "submit failed", e, flush=True); time.sleep(30); continue
    tid = t.get("id")
    for _ in range(120):
        try:
            st = json.loads(sh(["curl", "-s", "-m", "60", f"{API}/ticket/{tid}"]) or "{}").get("status")
        except Exception:
            st = None
        if st in ("COMPLETE", "ERROR"): break
        time.sleep(5)
    if st != "COMPLETE":
        print(gid, "status", st, flush=True); continue
    try:
        res = json.loads(sh(["curl", "-s", "-m", "120", f"{API}/result/{tid}/0"]))
    except Exception:
        print(gid, "result unreadable", flush=True); time.sleep(30); continue
    json.dump(res, open(out, "w"))
    n = {r["db"]: len(r.get("alignments") or []) for r in res.get("results", [])}
    print(gid, n, flush=True)
    time.sleep(5)
