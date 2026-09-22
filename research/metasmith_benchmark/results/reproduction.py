"""Build the E1-vs-E2 reproduction tables from the committed result tables.

Writes reproduction_{mags,jobs,steps}.tsv beside this script. With --markdown, also prints
each table as markdown for findings/E1_E2_REPRODUCTION.md.
"""
import argparse
import csv
import gzip
import statistics
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
BINNERS = ("COMEBin", "MetaBAT2", "SemiBin2", "DASTool")
SCORING = {"amber", "gold_standard"}


def read(path):
    op = gzip.open if path.suffix == ".gz" else open
    with op(path, "rt", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def quartiles(xs):
    if len(xs) < 2:
        return ", ".join(f"{x:.1f}" for x in xs * 3) if xs else ""
    q = statistics.quantiles(xs, n=4, method="inclusive")
    return ", ".join(f"{x:.1f}" for x in q)


def tier(completeness, contamination):
    if completeness > 90 and contamination < 5:
        return "HQ"
    if completeness >= 50 and contamination < 10:
        return "MQ"
    return "LQ"


def groups(rows):
    by = {}
    for r in rows:
        by.setdefault((r["dataset"], r["arm"]), []).append(r)
        by.setdefault((f"all {r['arm']}", r["arm"]), []).append(r)
    order = sorted(by, key=lambda k: (k[1] == "long", k[0].startswith("all"), k[0]))
    for k in order:
        yield k[0], k[1], by[k]


def mags():
    e1 = read(HERE / "e1" / "e1_checkm2.tsv.gz")
    e2 = read(HERE / "e2" / "e2_checkm2.tsv.gz")
    for r in e1:
        r["pipeline"] = "E1"
    for r in e2:
        r["pipeline"] = "E2"
    head = ["dataset", "arm", "binner", "samples"]
    for m in ("bins", "HQ", "MQ", "LQ", "completeness_q1_med_q3", "contamination_q1_med_q3"):
        head += [f"E1_{m}", f"E2_{m}"]
    out = []
    for ds, arm, rows in groups(e1 + e2):
        for b in BINNERS:
            sel = {p: [r for r in rows if r["binner"] == b and r["pipeline"] == p] for p in ("E1", "E2")}
            samples = len({r["sample"] for p in sel for r in sel[p]})
            line = [ds, arm, b, samples]
            stats = {}
            for p, rs in sel.items():
                comp = [float(r["Completeness"]) for r in rs]
                cont = [float(r["Contamination"]) for r in rs]
                tiers = [tier(a, c) for a, c in zip(comp, cont)]
                stats[p] = [len(rs), tiers.count("HQ"), tiers.count("MQ"), tiers.count("LQ"), quartiles(comp), quartiles(cont)]
            for i in range(6):
                line += [stats["E1"][i], stats["E2"][i]]
            out.append(line)
    return head, out


def ts(s):
    return datetime.fromisoformat(s) if s and s != "Unknown" else None


def jobs():
    head = ["pipeline", "arm", "scope", "run_keys", "jobs_submitted", "completed", "failed", "cancelled",
            "elapsed_h", "cpu_h", "first_submit", "last_end"]
    out = []
    sets = []
    for r in read(HERE / "e1" / "e1_slurm_tasks.tsv"):
        sets.append(("E1", r["arm"], "pipeline", r))
    for r in read(HERE / "e2" / "e2_slurm_tasks.tsv"):
        sets.append(("E2", r["arm"], "scoring" if r["transform"] in SCORING else "pipeline", r))
    keys = sorted({k[:3] for k in sets}, key=lambda k: (k[0], k[1] == "long", k[2] == "scoring"))
    for k in keys:
        rs = [r for *g, r in sets if tuple(g) == k]
        state = [r["State"].split()[0] for r in rs]
        el = [int(r["elapsed_s"] or 0) for r in rs]
        cpu = sum(e * int(r["AllocCPUS"] or 0) for e, r in zip(el, rs))
        submit = [t for t in (ts(r["Submit"]) for r in rs) if t]
        end = [t for t in (ts(r["End"]) for r in rs) if t]
        run_keys = sorted({r["run_key"] or ("b19" if r["process"].startswith(("B19", "METABAT2_B19")) else "unattributed") for r in rs})
        out.append([*k, " ".join(run_keys), len(rs), state.count("COMPLETED"), state.count("FAILED"),
                    state.count("CANCELLED"), f"{sum(el) / 3600:.0f}", f"{cpu / 3600:.0f}",
                    min(submit).isoformat(timespec="minutes"), max(end).isoformat(timespec="minutes")])
    return head, out


def steps():
    e1 = read(HERE / "e1" / "e1_slurm_tasks.tsv")
    e2 = read(HERE / "e2" / "e2_slurm_tasks.tsv")
    head = ["arm", "e2_transform", "nfcore_processes", "E1_completed_tasks", "E2_completed_tasks"]
    out = []
    for arm in ("short", "long"):
        procs, n1, n2 = {}, {}, {}
        for r in e1:
            if r["arm"] != arm:
                continue
            t = r["e2_transform"] or "(no transform)"
            procs.setdefault(t, set()).add(r["process"])
            n1[t] = n1.get(t, 0) + (r["State"] == "COMPLETED")
        for r in e2:
            if r["arm"] == arm:
                n2[r["transform"]] = n2.get(r["transform"], 0) + (r["State"] == "COMPLETED")
        mapped = sorted(t for t in procs if t != "(no transform)")
        for t in mapped + sorted(set(n2) - set(procs)) + ["(no transform)"]:
            names = " + ".join(sorted(procs.get(t, []))) or "(scoring, outside nf-core/mag)"
            out.append([arm, t, names, n1.get(t, 0), n2.get(t, 0)])
    return head, out


def write(name, head, rows):
    with open(HERE / f"reproduction_{name}.tsv", "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(head)
        w.writerows(rows)


def markdown(head, rows):
    cols, i = [], 0
    while i < len(head):
        pair = i + 1 < len(head) and head[i].startswith("E1_") and head[i + 1] == "E2_" + head[i][3:]
        cols.append((f"{head[i][3:]} (E1 / E2)", (i, i + 1)) if pair else (head[i], (i,)))
        i += 2 if pair else 1
    lines = ["| " + " | ".join(c for c, _ in cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(" / ".join(str(r[j]) for j in idx) for _, idx in cols) + " |" for r in rows]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--markdown", action="store_true")
    a = p.parse_args()
    for name, build in (("mags", mags), ("jobs", jobs), ("steps", steps)):
        head, rows = build()
        write(name, head, rows)
        if a.markdown:
            print(f"## {name}\n\n{markdown(head, rows)}\n")


if __name__ == "__main__":
    main()
