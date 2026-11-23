import csv, json, time, urllib.request, urllib.parse
rows = list(csv.DictReader(open("element_cds.tsv"), delimiter="\t"))
out = []
for r in rows:
    q = urllib.parse.quote(r["pid"].split(".")[0])
    url = f"https://rest.uniprot.org/uniparc/search?query={q}&fields=upi,organism&format=tsv&size=1"
    for a in range(3):
        try:
            t = urllib.request.urlopen(url, timeout=60).read().decode(); break
        except Exception:
            time.sleep(5); t = ""
    lines = t.strip().split("\n")[1:]
    upi, orgs = (lines[0].split("\t") + [""])[:2] if lines else ("", "")
    out.append({"tag": r["tag"], "start": r["start"], "product": r["product"], "upi": upi, "organisms": orgs})
    print(r["tag"], orgs[:160], flush=True)
json.dump(out, open("uniparc_orgs.json", "w"), indent=1)
