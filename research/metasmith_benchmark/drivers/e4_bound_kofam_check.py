import sys
from pathlib import Path
sys.argv = [sys.argv[0]]
exec(open(Path(__file__).with_name("e4_orf_mnx.py")).read().split("w = csv.writer")[0])
b = sys.stdin.readline().strip()
if not b:
    print(next(iter(sorted(kofam_raw)))); sys.exit()
ours = set()
for l in open("/scratch/phyberos/e4_ablation/evidence/bound/check/ko_check.tsv"):
    f = l.split("\t")
    if f[0] == "*":
        ours.add((f[1], f[2]))
theirs = {(o, k) for o, ks in kos_by_orf(kofam_raw[b]).items() for k in ks}
print(f"{b}: kofamscan {len(theirs)} calls, single-pass {len(ours)}, shared {len(ours & theirs)}, "
      f"only kofamscan {len(theirs - ours)}, only single-pass {len(ours - theirs)}")
