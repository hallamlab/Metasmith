"""Build the objective-1 page: does metasmith reproduce nf-core/mag? Reads results/ and f1/, writes obj1/index.html.

Run f1_dag_parity.py first; the page references its SVGs and reads its edge check.
"""

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
RES = HERE.parent / "results"
OUT = HERE / "obj1"

ORDER = {
    "short": ["fastqc_raw", "fastp", "fastqc_trimmed", "megahit", "bowtie2_binning_bam",
              "metabat2", "semibin2", "comebin", "das_tool", "checkm2"],
    "long": ["porechop_abi", "chopper", "flye", "minimap2_binning_bam",
             "metabat2", "semibin2", "comebin", "das_tool", "checkm2"],
}
BINNERS = ["DASTool", "MetaBAT2", "SemiBin2", "COMEBin"]
SCORING = {"amber", "gold_standard"}


def rows(path):
    return list(csv.DictReader(open(path), delimiter="\t"))


def task_counts():
    """Tasks in the final deliverable, per transform: E1 short by final_success, E1 long every COMPLETED attempt
    (one run plus the b19 rerun), E2 unique (step, ordinal) across run keys."""
    e1 = defaultdict(Counter)
    procs = defaultdict(lambda: defaultdict(set))
    for r in rows(RES / "e1/e1_slurm_tasks.tsv"):
        final = r["final_success"] == "True" if r["arm"] == "short" else r["State"] == "COMPLETED"
        if final:
            key = r["e2_transform"] or "bookkeeping"
            e1[r["arm"]][key] += 1
            procs[r["arm"]][key].add(r["process"])
    e2 = defaultdict(Counter)
    seen = set()
    for r in rows(RES / "e2/e2_slurm_tasks.tsv"):
        k = (r["arm"], r["step"], r["sample_ordinal_in_batch"])
        if r["State"] == "COMPLETED" and r["transform"] not in SCORING and k not in seen:
            seen.add(k)
            e2[r["arm"]][r["transform"]] += 1
    out = {}
    for arm, order in ORDER.items():
        out[arm] = {
            "rows": [dict(tool=t, e1=e1[arm][t], e2=e2[arm][t], e1_processes=sorted(procs[arm][t])) for t in order],
            "bookkeeping": dict(e1=e1[arm]["bookkeeping"], processes=len(procs[arm]["bookkeeping"])),
        }
    return out


def runtime():
    return {r["arm"]: dict(e1_h=float(r["e1_tool_bearing_h"]), e2_h=float(r["e2_tool_bearing_h"]),
                           book_h=float(r["e1_bookkeeping_h"]))
            for r in rows(RES / "metrics/runtime_totals.tsv")}


def match_rates():
    src = {"ctl": RES / "compare_arms_ctl/e1_e1ctl/summary.tsv",
           "e2_25": RES / "compare_arms_ctl/e1_e2/summary.tsv",
           "e2_all": RES / "compare_arms/summary.tsv"}
    out = defaultdict(dict)
    for key, path in src.items():
        for r in rows(path):
            out[f'{r["arm"]}/{r["binner"]}'][key] = dict(
                e1=float(r["E1_matched_pct"]), other=float(r["E2_matched_pct"]),
                e1_mags=int(r["E1_mags"]), other_mags=int(r["E2_mags"]), samples=int(r["samples"]))
    return out


def tiers():
    t = defaultdict(lambda: [0] * 6)
    cols = ["E1_high", "E1_medium", "E1_below", "E2_high", "E2_medium", "E2_below"]
    for r in rows(RES / "compare_arms/per_sample.tsv"):
        v = t[f'{r["arm"]}/{r["binner"]}']
        for i, c in enumerate(cols):
            v[i] += int(r[c])
    return {k: dict(zip(["e1_hq", "e1_mq", "e1_lq", "e2_hq", "e2_mq", "e2_lq"], v)) for k, v in t.items()}


def edge_check():
    out = defaultdict(list)
    for r in rows(HERE / "f1/edge_check.tsv"):
        out[r["arm"]].append(dict(src=r["src"], dst=r["dst"], edge_in=r["edge_in"],
                                  implied=r["implied_by_other"] == "True"))
    return out


def assemblies():
    out = {}
    for r in rows(RES / "compare_arms/summary.tsv"):
        mn, med = r["asm_bp_frac_id99_min/median"].split(" / ")
        smn, smed = r["asm_shared_bp_frac_min/median"].split(" / ")
        out[r["arm"]] = dict(id99_min=float(mn), id99_med=float(med), exact_min=float(smn), exact_med=float(smed))
    return out


def main():
    data = dict(dag=json.loads((HERE / "f1/summary.json").read_text()), tasks=task_counts(), runtime=runtime(), match=match_rates(), tiers=tiers(),
                edges=edge_check(), assemblies=assemblies(), binners=BINNERS)
    OUT.mkdir(exist_ok=True)
    template = (HERE / "obj1_template.html").read_text()
    (OUT / "index.html").write_text(template.replace("/*DATA*/null", json.dumps(data, separators=(",", ":"))))
    for arm in ("short", "long"):
        t = data["tasks"][arm]
        print(arm, "E1", sum(r["e1"] for r in t["rows"]), "+", t["bookkeeping"]["e1"], " E2", sum(r["e2"] for r in t["rows"]))


if __name__ == "__main__":
    main()
