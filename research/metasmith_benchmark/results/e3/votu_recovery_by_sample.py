import csv, os, sys
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import votu_recovery_curve as v
keys = v.run_keys()
labels = v.load_labels(sys.argv[2])
pub = v.load_published(set(keys.values()))
best_any, best_same, src = {}, {}, defaultdict(Counter)
with open(sys.argv[1], newline="") as f:
    for row in csv.DictReader(f, delimiter="\t"):
        q = row["Query_name"].split()[0]
        if q not in pub: continue
        ani, af = float(row["ANI"]), float(row["Align_fraction_query"])
        if ani < 95 or af < 85: continue
        run = labels.get(row["Ref_name"].split("|")[0])
        best_any[q] = 1
        if keys.get(run) == pub[q]["sample"]:
            best_same[q] = 1
        else:
            src[pub[q]["sample"]][keys.get(run)] += 1
per = defaultdict(lambda: [0, 0, 0])
for q, r in pub.items():
    p = per[(r["sample"], r["lane"])]
    p[0] += 1; p[1] += q in best_any; p[2] += q in best_same
runs_of = defaultdict(list)
for run, k in keys.items(): runs_of[k].append(run)
print("sample\tlane\tn\tany\tsame\truns\ttop_other_sources")
for (s, lane), (n, a, sm) in sorted(per.items()):
    print(f"{s}\t{lane}\t{n}\t{a/n:.3f}\t{sm/n:.3f}\t{','.join(runs_of[s])}\t{src[s].most_common(3)}")
