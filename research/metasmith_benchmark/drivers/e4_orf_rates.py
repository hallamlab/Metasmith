import csv, glob, gzip, re, sys
from pathlib import Path
import yaml

sys.path.insert(0, "/scratch/phyberos/bench/checkout/41382ec2/research/metasmith_benchmark/drivers")
from e4_evidence import cache_shards, product_file
from e4_tally import bins_of

W = Path("/scratch/phyberos/e4_ablation/evidence")
R = Path("/scratch/phyberos/e4_ablation/metasmith/runs/mxQRJ8Dc/results")
MODELS = {"M": W / "rung_models/M", "kofam": W / "models/kofam", "both": W / "models/both",
          "top1": W / "models_top1/diluted_top1"}

index = yaml.load(open(R / "_metadata/index.yml"), Loader=yaml.CSafeLoader)["manifest"]
owner = bins_of(index)
shards = cache_shards(R.parents[2])
kofam_raw = {}
for path, e in index.items():
    if e.get("type") == "annotation::kofamscan_results_chunk" and owner[path]:
        kofam_raw[owner[path]] = product_file(R, path, shards)

if sys.argv[1:] == ["peek"]:
    b, p = next(iter(kofam_raw.items()))
    print(b, p)
    print(open(p).read()[:1500])
    sys.exit()


def orfs_of(path):
    op = gzip.open if str(path).endswith(".gz") else open
    with op(path, "rt") as fh:
        return {l.split("\t", 1)[0] for l in fh if l.strip() and not l.startswith("#")}


def ko_orfs(path):
    hits = set()
    for r in csv.DictReader(open(path)):
        if r["thrshld"] != "-" and float(r["score"]) >= float(r["thrshld"]):
            hits.add(r["gene_name"])
    return hits


def model(path):
    x = gzip.open(path, "rt").read()
    genes = set(re.findall(r'<fbc:geneProduct [^>]*fbc:id="([^"]+)"', x)) - {"G_spontaneous"}
    rx = re.findall(r'<reaction .*?</reaction>', x, re.S)
    exch = sum(1 for r in rx if re.search(r'id="R_(EX|sink|DM)_', r))
    with_gpr = sum(1 for r in rx if "geneProductRef" in r)
    return len(genes), len(rx), with_gpr, exch


out = csv.writer(sys.stdout, delimiter="\t")
cols = ["bin", "orfs", "ko_orfs", "kofam_bigg_orfs", "diamond_orfs", "either_orfs"]
for lane in MODELS:
    cols += [f"{lane}_genes", f"{lane}_rxns", f"{lane}_gpr_rxns", f"{lane}_exch"]
out.writerow(cols)
for faa in sorted(glob.glob("/scratch/phyberos/e4_ablation/published/*/proteins/*.faa")):
    b = Path(faa).stem
    dh, kh = W / f"models/hits_diamond/{b}.tsv.gz", W / f"models/hits_kofam/{b}.tsv.gz"
    if not dh.exists():
        continue
    n = sum(1 for l in open(faa) if l.startswith(">"))
    d, k = orfs_of(dh), orfs_of(kh)
    ko = ko_orfs(kofam_raw[b]) if b in kofam_raw else set()
    row = [b, n, len(ko), len(k), len(d), len(d | k)]
    for lane, root in MODELS.items():
        p = root / f"{b}.xml.gz"
        row += list(model(p)) if p.exists() else ["", "", "", ""]
    out.writerow(row)
