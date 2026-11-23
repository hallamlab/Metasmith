import csv, sys

# KofamScan's call, from one hmmsearch over every KO profile: a hit is significant when its
# full-sequence score (score_type "full") or best-domain score ("domain") reaches the KO's threshold.
# Rows are KofamScan's detail-tsv shape, significant ones starred.
tblout, ko_list, out = sys.argv[1:4]
ko = {r["knum"]: r for r in csv.DictReader(open(ko_list), delimiter="\t")}
with open(out, "w") as fh:
    for l in open(tblout):
        if l.startswith("#"):
            continue
        f = l.split()
        gene, k, e = f[0], f[2], f[4]
        full, dom = float(f[5]), float(f[8])
        r = ko.get(k)
        if r is None or r["threshold"] == "-":
            continue
        score = dom if r["score_type"] == "domain" else full
        star = "*" if score >= float(r["threshold"]) else ""
        fh.write(f"{star}\t{gene}\t{k}\t{r['threshold']}\t{score}\t{e}\t{r['definition']}\n")
