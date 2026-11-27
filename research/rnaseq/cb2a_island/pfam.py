import json, sys, time, urllib.request
API = "https://www.ebi.ac.uk/Tools/hmmer/api/v1"
def post(seqid, seq):
    body = json.dumps({"database": "pfam", "input": f">{seqid}\n{seq}"}).encode()
    req = urllib.request.Request(f"{API}/search/hmmscan", body, {"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=120))["id"]
def get(jid):
    for _ in range(120):
        r = json.load(urllib.request.urlopen(f"{API}/result/{jid}", timeout=120))
        if r.get("status") in ("SUCCESS", "FAILURE"): return r
        time.sleep(3)
recs, cur = {}, None
for l in open("targets.faa"):
    l = l.strip()
    if l.startswith(">"): cur = l[1:]; recs[cur] = ""
    else: recs[cur] += l
out = {}
for k, s in recs.items():
    for attempt in range(3):
        try:
            out[k] = get(post(k, s)); break
        except Exception as e:
            print(k, "retry", e, file=sys.stderr); time.sleep(10)
    print(k, len(out.get(k, {}).get("result", {}).get("hits", [])), flush=True)
json.dump(out, open("pfam_raw.json", "w"))
