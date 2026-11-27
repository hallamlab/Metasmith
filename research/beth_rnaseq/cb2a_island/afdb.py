import csv, json, sys, time, urllib.request, urllib.parse
rows = list(csv.DictReader(open("targets.tsv"), delimiter="\t"))
def get(url, js=True):
    for a in range(3):
        try:
            r = urllib.request.urlopen(url, timeout=120).read()
            return json.loads(r) if js else r.decode()
        except urllib.error.HTTPError as e:
            if e.code == 404: return None
            time.sleep(5)
        except Exception:
            time.sleep(5)
out = []
for r in rows:
    q = urllib.parse.quote(r["protein_id"].split(".")[0])
    tsv = get(f"https://rest.uniprot.org/uniparc/search?query={q}&fields=upi,accession&format=tsv&size=1", js=False) or ""
    lines = tsv.strip().split("\n")[1:]
    upi, accs = (lines[0].split("\t") + [""])[:2] if lines else ("", "")
    accs = [a.split(".")[0] for a in accs.split("; ") if a]
    model = None
    for a in accs[:25]:
        p = get(f"https://alphafold.ebi.ac.uk/api/prediction/{a}")
        if p:
            p = p[0]; model = (a, p.get("pdbUrl"), p.get("globalMetricValue")); break
    out.append({"gene_id": r["gene_id"], "protein_id": r["protein_id"], "upi": upi, "n_uniprot": len(accs),
                "afdb_acc": model[0] if model else "", "afdb_pdb": model[1] if model else "", "plddt": model[2] if model else ""})
    print(out[-1]["gene_id"], upi, len(accs), out[-1]["afdb_acc"], out[-1]["plddt"], flush=True)
    if model:
        open(f"structures/{r['gene_id']}.pdb", "w").write(get(model[1], js=False))
json.dump(out, open("afdb.json", "w"), indent=1)
