import csv, gzip, re, sys
from collections import Counter, defaultdict
from pathlib import Path
import statistics

exec(open(Path(__file__).with_name("e4_orf_mnx.py")).read().split("w = csv.writer")[0])

defn = {}
for r in csv.DictReader(open("/project/6004975/phyberos/lib/kofamscan/ko_list.tsv"), delimiter="\t"):
    defn[r["knum"]] = r["definition"]


def kind(ko):
    t = defn.get(ko, "")
    if re.search(r"transport|permease|ABC|porin|symporter|antiporter|exporter|importer|channel|efflux|secretion|PTS system|TonB", t, re.I):
        return "transport"
    if "[EC:" in t:
        return "enzyme_EC"
    if re.search(r"ribosom|tRNA|RNA polymerase|translation|elongation factor", t, re.I):
        return "translation"
    if re.search(r"regulat|transcription|sensor|response|two-component|sigma", t, re.I):
        return "regulation"
    if re.search(r"uncharacterized|hypothetical|unknown|DUF", t, re.I):
        return "unknown"
    return "other"


ec_mnxr = defaultdict(set)
for line in open(MNX / "reac_prop.tsv"):
    p = line.rstrip("\n").split("\t")
    if line.startswith("#") or len(p) < 5 or not p[0].startswith("MNXR") or not p[3]:
        continue
    for ec in p[3].split(";"):
        if ec.strip():
            ec_mnxr[ec.strip()].add(p[0])


def ec_reach(ko):
    ecs = re.findall(r"\d+\.\d+\.\d+\.\d+", " ".join(re.findall(r"\[EC:([^\]]+)\]", defn.get(ko, ""))))
    return set().union(*(ec_mnxr.get(e, set()) for e in ecs)) if ecs else set()


per = defaultdict(list)
total = Counter()
for faa in sorted(glob.glob("/scratch/phyberos/e4_ablation/published/*/proteins/*.faa")):
    b = Path(faa).stem
    dh = W / f"models/hits_diamond/{b}.tsv.gz"
    if not dh.exists() or b not in kofam_raw:
        continue
    d = set(diamond_by_orf(dh))
    c = Counter()
    for o, ks in kos_by_orf(kofam_raw[b]).items():
        if o in d:
            continue
        if any(ko_mnxr.get(x) for x in ks):
            c["has_mnxr"] += 1
            continue
        k0 = kind(sorted(ks)[0])
        c[k0] += 1
        if k0 == "enzyme_EC":
            reach = set().union(*(ec_reach(x) for x in ks))
            if reach:
                c["ec_has_mnxr"] += 1
                c["ec_mnxr_total"] += len(reach)
    for k in ("has_mnxr", "transport", "enzyme_EC", "translation", "regulation", "unknown", "other", "ec_has_mnxr", "ec_mnxr_total"):
        per[k].append(c[k])
    total.update(c)
for k, v in per.items():
    print(f"{k:12s} median {statistics.median(v):6.0f}   pooled {total[k]:8d}")
