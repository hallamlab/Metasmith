"""List a producer step's work dirs whose every consumer task finished, to feed prune_work.sbatch.

    step_refs.py <run dir> <producer step> <out list> [--need STEP ...]

A consumer is any non-array-parent task dir whose .command.sh names the producer dir.
A producer dir is prunable when no consumer of it is open, and every --need step has an ok consumer of it.
"""
import argparse, collections, os, re
ap = argparse.ArgumentParser()
ap.add_argument("run"); ap.add_argument("step"); ap.add_argument("out")
ap.add_argument("--need", action="append", default=[])
a = ap.parse_args()
run = os.path.realpath(a.run); W = os.path.join(run, "nxf_work"); rid = os.path.basename(run)
pat = re.compile(re.escape(rid) + r"/nxf_work/([0-9a-f]{2}/[0-9a-f]{30})")
jn = re.compile(r"^#SBATCH -J nf-p\d+__([A-Za-z0-9_]+?)_\(", re.M)
tasks = {}
for d2 in os.listdir(W):
    if len(d2) != 2: continue
    for h in os.listdir(os.path.join(W, d2)):
        d = f"{d2}/{h}"; p = os.path.join(W, d)
        try:
            run_txt = open(os.path.join(p, ".command.run")).read()
        except OSError:
            continue
        if "#SBATCH -o /dev/null" in run_txt: continue
        m = jn.search(run_txt); step = m.group(1) if m else "?"
        ec = os.path.join(p, ".exitcode")
        state = ("ok" if open(ec).read().strip() == "0" else "fail") if os.path.exists(ec) else "open"
        tasks[d] = (step, state, p)
prod = {d for d, (s, st, _) in tasks.items() if s == a.step and st == "ok"}
refs = collections.defaultdict(list)
for d, (s, st, p) in tasks.items():
    if s == a.step: continue
    try: sh = open(os.path.join(p, ".command.sh")).read()
    except OSError: continue
    for f in set(pat.findall(sh)) & prod:
        refs[f].append((s, st))
print("producer ok dirs:", len(prod))
print("consumer refs by (step,state):", dict(collections.Counter(x for v in refs.values() for x in v)))
need = set(a.need); out, blocked = [], collections.Counter()
for f in sorted(prod):
    v = refs.get(f, [])
    if any(st == "open" for _, st in v): blocked["open"] += 1; continue
    miss = need - {s for s, st in v if st == "ok"}
    if miss: blocked["missing:" + ",".join(sorted(miss))] += 1; continue
    out.append(os.path.join(W, f))
print("prunable", len(out), "blocked", dict(blocked))
open(a.out, "w").write("".join(x + "\n" for x in out))
