"""Files on disk against the manifest, per in-scope study, with the size ratio the .sra
route produces. Not a verifier -- verify_sra.py is."""
import csv
from pathlib import Path

rows = list(csv.DictReader(open("manifest.tsv"), delimiter="\t"))
inscope = [r for r in rows if r["dataset"] != "sunagawa2015"]
th = tb = tw = 0
print("dataset          files      GB disk   vs manifest   sra staged")
for ds in ("li2019", "korem2015", "karlsson2013", "bissett_base"):
    r = [x for x in inscope if x["dataset"] == ds]
    have = [x for x in r if (Path(ds)/x["relpath"]).is_file()]
    got = sum((Path(ds)/x["relpath"]).stat().st_size for x in have)
    want = sum(int(x["bytes"]) for x in have)
    sra = len(list(Path(".sra", ds).glob("*.sra"))) if Path(".sra", ds).is_dir() else 0
    runs = sum(1 for _ in open(f"runs.{ds}.tsv")) - 1
    th += len(have); tb += got; tw += want
    ratio = f"{got / want * 100:.1f}%" if want else "-"
    print(f"{ds:<14} {len(have):>4}/{len(r):<5} {got / 1e9:>9.1f} {ratio:>12}   {sra}/{runs}")
ratio = f"{tb / tw * 100:.1f}%" if tw else "-"
print(f"{'TOTAL':<14} {th:>4}/{len(inscope):<5} {tb / 1e9:>9.1f} {ratio:>12}")
