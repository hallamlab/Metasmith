"""Tabulate every embed run's timing json under <bench>/out as TSV on stdout."""
import glob
import json
import os
import sys

B = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))
cols = ["name", "mode", "procs", "threads", "cores", "pin", "sdpa", "ipex", "toks", "orfs", "residues",
        "embed_wall_s", "orf_per_s", "orf_per_s_per_core", "res_per_s_per_core", "per_proc_wall",
        "maxsep_wall_s", "t_maxsep_stock", "t_maxsep_cachedgmm", "t_fast_total", "maxrss_gb"]
print("\t".join(cols))
for js in sorted(glob.glob(f"{B}/out/*.json") + glob.glob(f"{B}/out/*/*.json")):
    m = json.load(open(js))
    if "embed_wall_s" not in m:
        continue
    m["name"] = os.path.relpath(js, f"{B}/out")[:-5]
    m["cores"] = m["procs"] * m["threads"]
    m["res_per_s_per_core"] = round(m["residues"] / m["embed_wall_s"] / m["cores"], 2)
    m.update(m.get("maxsep_timings") or {})
    print("\t".join(str(m.get(c, "")) for c in cols))
