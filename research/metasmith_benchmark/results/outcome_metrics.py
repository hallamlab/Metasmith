#!/usr/bin/env python3
"""Compute the E1-vs-E2 outcome metrics that findings/E1_E2_REPRODUCTION.md does not present.

Stdlib only, no metasmith import, every input a committed table. It runs nothing and reads
nothing outside this repository, so it is reproducible from a clone. Inputs are the pairwise
comparison tables produced by drivers/compare_arms_sample.py on fir and gathered under
results/compare_arms/, imported to this branch from feat/engine/bench-E2 at 5f45c500.

Writes results/metrics/*.tsv; --markdown prints the tables that findings/E1_E2_METRICS.md quotes.
"""

import argparse
import csv
import gzip
import statistics
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "metrics"

BINNERS = ["DASTool", "COMEBin", "MetaBAT2", "SemiBin2"]
ARMS = ["short", "long"]


def read_tsv(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", newline="") as f:
        header = f.readline().rstrip("\n").split("\t")
        # mag_matches.tsv.gz repeats partner_completeness/contamination/tier; the first copy wins.
        seen, keep = set(), []
        for i, name in enumerate(header):
            if name not in seen:
                seen.add(name)
                keep.append((i, name))
        for line in f:
            row = line.rstrip("\n").split("\t")
            yield {name: row[i] for i, name in keep if i < len(row)}


def as_int(value):
    """Blank means no completed attempt: E1 short's QUAST row is 0 tasks and 20 failures."""
    return int(value) if value else 0


def quantile(values, q):
    """Linear-interpolated quantile on a sorted copy; q in [0, 1]."""
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    pos = q * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def dist(values):
    return {
        "n": len(values),
        "min": min(values) if values else None,
        "p10": quantile(values, 0.10),
        "median": quantile(values, 0.50),
        "p90": quantile(values, 0.90),
        "max": max(values) if values else None,
    }


def fmt(x, places=4):
    if x is None:
        return ""
    if isinstance(x, int):
        return str(x)
    if abs(x) >= 1e6 and float(x).is_integer():
        return f"{int(x)}"
    return f"{x:.{places}f}".rstrip("0").rstrip(".") if places else str(x)


def write_rows(name, fieldnames, rows):
    OUT.mkdir(exist_ok=True)
    path = OUT / name
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return path


def markdown(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join("" if c is None else str(c) for c in r) + " |")
    return "\n".join(out)


# ---------------------------------------------------------------- assemblies

def assemblies(md):
    """Absolute assembly size and contiguity per arm and pipeline, plus the paired delta.

    The report gives only relative differences. These are the figures themselves, which is
    what a reader comparing against a published assembly needs.
    """
    rows = list(read_tsv(HERE / "compare_arms" / "assemblies.tsv"))
    per_arm, deltas = [], []
    for arm in ARMS:
        a = [r for r in rows if r["arm"] == arm]
        for pipeline, pre in (("E1", "e1"), ("E2", "e2")):
            for metric, col, scale in (
                ("total bp", "bp", 1),
                ("contigs", "contigs", 1),
                ("N50", "n50", 1),
                ("L50", "l50", 1),
                ("longest contig", "longest", 1),
            ):
                vals = [int(r[f"{pre}_{col}"]) * scale for r in a]
                d = dist(vals)
                per_arm.append({"arm": arm, "pipeline": pipeline, "metric": metric, **d})
        # paired per-sample deltas, signed E2 - E1
        for metric, col in (("total bp", "bp"), ("contigs", "contigs"), ("N50", "n50")):
            vals = [int(r[f"e2_{col}"]) - int(r[f"e1_{col}"]) for r in a]
            rel = [
                (int(r[f"e2_{col}"]) - int(r[f"e1_{col}"])) / int(r[f"e1_{col}"])
                for r in a
                if int(r[f"e1_{col}"])
            ]
            deltas.append(
                {
                    "arm": arm,
                    "metric": metric,
                    "samples": len(vals),
                    "e2_larger": sum(1 for v in vals if v > 0),
                    "e1_larger": sum(1 for v in vals if v < 0),
                    "equal": sum(1 for v in vals if v == 0),
                    "abs_median": quantile([abs(v) for v in vals], 0.5),
                    "rel_median_signed": quantile(rel, 0.5),
                    "rel_p90_abs": quantile([abs(v) for v in rel], 0.9),
                }
            )
    write_rows(
        "assembly_size.tsv",
        ["arm", "pipeline", "metric", "n", "min", "p10", "median", "p90", "max"],
        per_arm,
    )
    write_rows(
        "assembly_delta.tsv",
        [
            "arm",
            "metric",
            "samples",
            "e2_larger",
            "e1_larger",
            "equal",
            "abs_median",
            "rel_median_signed",
            "rel_p90_abs",
        ],
        deltas,
    )
    if md:
        print("\n### Assembly size and contiguity\n")
        print(
            markdown(
                ["arm", "pipeline", "metric", "min", "p10", "median", "p90", "max"],
                [
                    [
                        r["arm"],
                        r["pipeline"],
                        r["metric"],
                        fmt(r["min"], 0),
                        fmt(r["p10"], 1),
                        fmt(r["median"], 1),
                        fmt(r["p90"], 1),
                        fmt(r["max"], 0),
                    ]
                    for r in per_arm
                ],
            )
        )
        print("\n### Paired per-sample difference, E2 minus E1\n")
        print(
            markdown(
                ["arm", "metric", "samples", "E2 larger", "E1 larger", "equal",
                 "median abs diff", "median signed rel", "p90 abs rel"],
                [
                    [
                        r["arm"], r["metric"], r["samples"], r["e2_larger"], r["e1_larger"],
                        r["equal"], fmt(r["abs_median"], 1),
                        f"{r['rel_median_signed']:+.2e}", f"{r['rel_p90_abs']:.2e}",
                    ]
                    for r in deltas
                ],
            )
        )
    return per_arm


# ------------------------------------------------------- % of contigs binned

def contigs_binned(md):
    """The share of each assembly a binner placed in a bin, by contig count and by base pairs.

    Within one binner a sample's bins are disjoint, so summing over bins is the binned total.
    Bins named unbinned/lowDepth/tooShort are absent from mags.tsv.gz (checked: zero rows), so
    no pseudo-bin inflates the numerator. DAS Tool's bins are drawn from the other three and
    trimmed, so its share is a selection rate rather than a coverage rate.
    """
    asm = {}
    for r in read_tsv(HERE / "compare_arms" / "assemblies.tsv"):
        asm[(r["arm"], r["sample"])] = r

    binned = defaultdict(lambda: [0, 0])  # (arm, sample, pipeline, binner) -> [contigs, bp]
    for r in read_tsv(HERE / "compare_arms" / "raw" / "mags.tsv.gz"):
        key = (r["arm"], r["sample"], r["pipeline"], r["binner"])
        binned[key][0] += int(r["contigs"])
        binned[key][1] += int(r["bp"])

    per_sample, summary = [], []
    grouped = defaultdict(list)
    for (arm, sample, pipeline, binner), (nc, nbp) in binned.items():
        a = asm[(arm, sample)]
        pre = "e1" if pipeline == "E1" else "e2"
        tot_c, tot_bp = int(a[f"{pre}_contigs"]), int(a[f"{pre}_bp"])
        row = {
            "arm": arm,
            "sample": sample,
            "pipeline": pipeline,
            "binner": binner,
            "assembly_contigs": tot_c,
            "assembly_bp": tot_bp,
            "binned_contigs": nc,
            "binned_bp": nbp,
            "pct_contigs_binned": 100.0 * nc / tot_c,
            "pct_bp_binned": 100.0 * nbp / tot_bp,
        }
        per_sample.append(row)
        grouped[(arm, binner, pipeline)].append(row)

    for arm in ARMS:
        for binner in BINNERS:
            for pipeline in ("E1", "E2"):
                rows = grouped.get((arm, binner, pipeline), [])
                if not rows:
                    continue
                c = dist([r["pct_contigs_binned"] for r in rows])
                b = dist([r["pct_bp_binned"] for r in rows])
                summary.append(
                    {
                        "arm": arm,
                        "binner": binner,
                        "pipeline": pipeline,
                        "samples": c["n"],
                        "pct_contigs_median": c["median"],
                        "pct_contigs_min": c["min"],
                        "pct_contigs_max": c["max"],
                        "pct_bp_median": b["median"],
                        "pct_bp_min": b["min"],
                        "pct_bp_max": b["max"],
                    }
                )

    # paired per-sample difference, so the comparison is within a sample rather than across
    paired = []
    by_key = {(r["arm"], r["sample"], r["binner"], r["pipeline"]): r for r in per_sample}
    for arm in ARMS:
        for binner in BINNERS:
            diffs_c, diffs_b = [], []
            for (a, s, bn, p), r in by_key.items():
                if p != "E1" or a != arm or bn != binner:
                    continue
                other = by_key.get((a, s, bn, "E2"))
                if not other:
                    continue
                diffs_c.append(other["pct_contigs_binned"] - r["pct_contigs_binned"])
                diffs_b.append(other["pct_bp_binned"] - r["pct_bp_binned"])
            if diffs_c:
                paired.append(
                    {
                        "arm": arm,
                        "binner": binner,
                        "samples": len(diffs_c),
                        "d_pct_contigs_median": quantile(diffs_c, 0.5),
                        "d_pct_contigs_p90abs": quantile([abs(v) for v in diffs_c], 0.9),
                        "d_pct_bp_median": quantile(diffs_b, 0.5),
                        "d_pct_bp_p90abs": quantile([abs(v) for v in diffs_b], 0.9),
                    }
                )

    write_rows(
        "contigs_binned_per_sample.tsv",
        ["arm", "sample", "pipeline", "binner", "assembly_contigs", "assembly_bp",
         "binned_contigs", "binned_bp", "pct_contigs_binned", "pct_bp_binned"],
        sorted(per_sample, key=lambda r: (r["arm"], r["binner"], r["sample"], r["pipeline"])),
    )
    write_rows(
        "contigs_binned.tsv",
        ["arm", "binner", "pipeline", "samples", "pct_contigs_median", "pct_contigs_min",
         "pct_contigs_max", "pct_bp_median", "pct_bp_min", "pct_bp_max"],
        summary,
    )
    write_rows(
        "contigs_binned_paired.tsv",
        ["arm", "binner", "samples", "d_pct_contigs_median", "d_pct_contigs_p90abs",
         "d_pct_bp_median", "d_pct_bp_p90abs"],
        paired,
    )
    if md:
        print("\n### Share of the assembly binned\n")
        print(
            markdown(
                ["arm", "binner", "samples", "% contigs binned (E1 / E2)", "% bp binned (E1 / E2)"],
                [
                    [
                        arm, binner,
                        next(r["samples"] for r in summary if r["arm"] == arm and r["binner"] == binner),
                        " / ".join(
                            f"{next(r['pct_contigs_median'] for r in summary if r['arm'] == arm and r['binner'] == binner and r['pipeline'] == p):.2f}"
                            for p in ("E1", "E2")
                        ),
                        " / ".join(
                            f"{next(r['pct_bp_median'] for r in summary if r['arm'] == arm and r['binner'] == binner and r['pipeline'] == p):.2f}"
                            for p in ("E1", "E2")
                        ),
                    ]
                    for arm in ARMS
                    for binner in BINNERS
                    if any(r["arm"] == arm and r["binner"] == binner for r in summary)
                ],
            )
        )
        print("\n### Paired difference in binned share, E2 minus E1, percentage points\n")
        print(
            markdown(
                ["arm", "binner", "samples", "median d% contigs", "p90 |d%| contigs",
                 "median d% bp", "p90 |d%| bp"],
                [
                    [r["arm"], r["binner"], r["samples"],
                     f"{r['d_pct_contigs_median']:+.2f}", f"{r['d_pct_contigs_p90abs']:.2f}",
                     f"{r['d_pct_bp_median']:+.2f}", f"{r['d_pct_bp_p90abs']:.2f}"]
                    for r in paired
                ],
            )
        )
    return summary


# ------------------------------------------------------------- MAG contiguity

def mag_size(md):
    """MAG size and fragmentation per binner.

    mags.tsv.gz carries each MAG's contig count and base count but not its contig lengths, so
    a true per-MAG N50 is not computable here. Contigs per MAG and mean contig length are the
    fragmentation proxies these columns do support; the gap is stated in the findings doc.
    """
    rows = list(read_tsv(HERE / "compare_arms" / "raw" / "mags.tsv.gz"))
    out = []
    for arm in ARMS:
        for binner in BINNERS:
            for pipeline in ("E1", "E2"):
                sel = [r for r in rows if r["arm"] == arm and r["binner"] == binner and r["pipeline"] == pipeline]
                if not sel:
                    continue
                bp = [int(r["bp"]) for r in sel]
                nc = [int(r["contigs"]) for r in sel]
                mean_len = [int(r["bp"]) / int(r["contigs"]) for r in sel if int(r["contigs"])]
                out.append(
                    {
                        "arm": arm,
                        "binner": binner,
                        "pipeline": pipeline,
                        "mags": len(sel),
                        "bp_median": quantile(bp, 0.5),
                        "bp_p90": quantile(bp, 0.9),
                        "contigs_median": quantile(nc, 0.5),
                        "contigs_p90": quantile(nc, 0.9),
                        "mean_contig_len_median": quantile(mean_len, 0.5),
                        "total_bp": sum(bp),
                    }
                )
    write_rows(
        "mag_size.tsv",
        ["arm", "binner", "pipeline", "mags", "bp_median", "bp_p90", "contigs_median",
         "contigs_p90", "mean_contig_len_median", "total_bp"],
        out,
    )
    if md:
        print("\n### MAG size and fragmentation\n")
        print(
            markdown(
                ["arm", "binner", "MAGs (E1 / E2)", "median bp (E1 / E2)",
                 "median contigs (E1 / E2)", "median mean contig len (E1 / E2)"],
                [
                    [
                        arm, binner,
                        " / ".join(str(g(out, arm, binner, p, "mags")) for p in ("E1", "E2")),
                        " / ".join(f"{g(out, arm, binner, p, 'bp_median'):,.0f}" for p in ("E1", "E2")),
                        " / ".join(f"{g(out, arm, binner, p, 'contigs_median'):.1f}" for p in ("E1", "E2")),
                        " / ".join(f"{g(out, arm, binner, p, 'mean_contig_len_median'):,.0f}" for p in ("E1", "E2")),
                    ]
                    for arm in ARMS
                    for binner in BINNERS
                    if any(r["arm"] == arm and r["binner"] == binner for r in out)
                ],
            )
        )
    return out


def g(rows, arm, binner, pipeline, field):
    return next(r[field] for r in rows if r["arm"] == arm and r["binner"] == binner and r["pipeline"] == pipeline)


# ------------------------------------------------------- ANI distance distribution

def ani_distribution(md):
    """The distance distribution over matched MAG pairs, reported beside its censoring.

    Each matched pair appears twice in mag_matches.tsv.gz, once per pipeline; the E1 row is
    taken so a pair counts once. A distribution over matched pairs alone is conditional on
    matching, so the unmatched share rides in the same table -- an uncensored reading of the
    ANI column is the single easiest way to overstate agreement.
    """
    rows = list(read_tsv(HERE / "compare_arms" / "mag_matches.tsv.gz"))
    edges = [0.0, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    out, hist = [], []
    for arm in ARMS:
        for binner in BINNERS:
            sel = [r for r in rows if r["arm"] == arm and r["binner"] == binner]
            e1 = [r for r in sel if r["pipeline"] == "E1"]
            e2 = [r for r in sel if r["pipeline"] == "E2"]
            pairs = [r for r in e1 if r["matched"] == "True"]
            d = [100.0 - float(r["ani"]) for r in pairs if r["ani"]]
            if not d:
                continue
            out.append(
                {
                    "arm": arm,
                    "binner": binner,
                    "e1_mags": len(e1),
                    "e2_mags": len(e2),
                    "matched_pairs": len(pairs),
                    "e1_matched_pct": 100.0 * len(pairs) / len(e1) if e1 else None,
                    "e2_matched_pct": 100.0 * sum(1 for r in e2 if r["matched"] == "True") / len(e2) if e2 else None,
                    "ani_dist_median": quantile(d, 0.5),
                    "ani_dist_p90": quantile(d, 0.9),
                    "ani_dist_p99": quantile(d, 0.99),
                    "ani_dist_max": max(d),
                    "pairs_ani_100": sum(1 for v in d if v == 0.0),
                    "pairs_ani_ge_99_9": sum(1 for v in d if v <= 0.1),
                }
            )
            counts = [0] * (len(edges))
            for v in d:
                placed = False
                for i in range(len(edges) - 1):
                    if edges[i] <= v < edges[i + 1]:
                        counts[i] += 1
                        placed = True
                        break
                if not placed:
                    counts[-1] += 1
            labels = [f"[{edges[i]}, {edges[i+1]})" for i in range(len(edges) - 1)] + [f">= {edges[-1]}"]
            for label, c in zip(labels, counts):
                hist.append(
                    {
                        "arm": arm,
                        "binner": binner,
                        "ani_distance_bin": label,
                        "matched_pairs": c,
                        "pct_of_matched": 100.0 * c / len(d),
                    }
                )
    write_rows(
        "ani_distance.tsv",
        ["arm", "binner", "e1_mags", "e2_mags", "matched_pairs", "e1_matched_pct",
         "e2_matched_pct", "ani_dist_median", "ani_dist_p90", "ani_dist_p99",
         "ani_dist_max", "pairs_ani_100", "pairs_ani_ge_99_9"],
        out,
    )
    write_rows(
        "ani_distance_histogram.tsv",
        ["arm", "binner", "ani_distance_bin", "matched_pairs", "pct_of_matched"],
        hist,
    )
    if md:
        print("\n### ANI distance over matched MAG pairs, with its censoring\n")
        print(
            markdown(
                ["arm", "binner", "matched pairs", "matched % (E1 / E2)", "median distance",
                 "p90", "p99", "max", "pairs at ANI 100", "pairs at ANI >= 99.9"],
                [
                    [
                        r["arm"], r["binner"], r["matched_pairs"],
                        f"{r['e1_matched_pct']:.1f} / {r['e2_matched_pct']:.1f}",
                        f"{r['ani_dist_median']:.4f}", f"{r['ani_dist_p90']:.3f}",
                        f"{r['ani_dist_p99']:.2f}", f"{r['ani_dist_max']:.2f}",
                        r["pairs_ani_100"],
                        f"{r['pairs_ani_ge_99_9']} ({100.0 * r['pairs_ani_ge_99_9'] / r['matched_pairs']:.1f}%)",
                    ]
                    for r in out
                ],
            )
        )
        print("\n### Distance histogram, share of matched pairs\n")
        bins = sorted({r["ani_distance_bin"] for r in hist}, key=lambda b: hist.index(next(x for x in hist if x["ani_distance_bin"] == b)))
        print(
            markdown(
                ["arm", "binner"] + bins,
                [
                    [arm, binner] + [
                        f"{next((h['pct_of_matched'] for h in hist if h['arm'] == arm and h['binner'] == binner and h['ani_distance_bin'] == b), 0.0):.1f}%"
                        for b in bins
                    ]
                    for arm in ARMS
                    for binner in BINNERS
                    if any(h["arm"] == arm and h["binner"] == binner for h in hist)
                ],
            )
        )
    return out


# --------------------------------------------------------------- runtime per step

def runtime(md):
    """E1's per-process runtime joined to E2's per-transform runtime.

    Both sides are summed Slurm Elapsed, which excludes queue time. Three E2 transforms each
    hold two nf-core processes, so E1's side is summed over the processes that map to one
    transform; a per-process comparison of those rows is meaningless in either direction.
    """
    e1 = list(read_tsv(HERE / "e1" / "e1_runtime_by_process.tsv"))
    e2 = {(r["arm"], r["transform"]): r for r in read_tsv(HERE / "e2" / "e2_runtime_by_transform.tsv")}

    agg = defaultdict(lambda: {"processes": [], "n_tasks": 0, "sum_elapsed_s": 0, "max_elapsed_s": 0})
    unmapped = defaultdict(lambda: {"processes": [], "n_tasks": 0, "sum_elapsed_s": 0})
    for r in e1:
        key = (r["arm"], r["e2_transform"])
        target = agg[key] if r["e2_transform"] else unmapped[r["arm"]]
        target["processes"].append(r["process"])
        target["n_tasks"] += as_int(r["n_tasks"])
        target["sum_elapsed_s"] += as_int(r["sum_elapsed_s"])
        if "max_elapsed_s" in target:
            target["max_elapsed_s"] = max(target["max_elapsed_s"], as_int(r["max_elapsed_s"]))

    rows = []
    for (arm, transform), v in sorted(agg.items()):
        t = e2.get((arm, transform))
        rows.append(
            {
                "arm": arm,
                "e2_transform": transform,
                "nfcore_processes": " + ".join(sorted(v["processes"])),
                "e1_tasks": v["n_tasks"],
                "e2_tasks": as_int(t["n_tasks"]) if t else None,
                "e1_sum_elapsed_h": v["sum_elapsed_s"] / 3600.0,
                "e2_sum_elapsed_h": as_int(t["sum_elapsed_s"]) / 3600.0 if t else None,
                "ratio_e2_over_e1": (as_int(t["sum_elapsed_s"]) / v["sum_elapsed_s"]) if t and v["sum_elapsed_s"] else None,
                "e1_median_task_s": None,
                "e2_median_task_s": as_int(t["median_elapsed_s"]) if t else None,
            }
        )
    # E1's median is only meaningful for a single-process transform
    for r in rows:
        if " + " not in r["nfcore_processes"]:
            src = next(x for x in e1 if x["arm"] == r["arm"] and x["process"] == r["nfcore_processes"])
            r["e1_median_task_s"] = as_int(src["median_elapsed_s"])

    totals = []
    for arm in ARMS:
        mapped_e1 = sum(v["sum_elapsed_s"] for (a, _), v in agg.items() if a == arm)
        mapped_e2 = sum(
            as_int(t["sum_elapsed_s"])
            for (a, transform), t in e2.items()
            if a == arm and (a, transform) in agg
        )
        book = unmapped.get(arm, {"sum_elapsed_s": 0, "n_tasks": 0, "processes": []})
        totals.append(
            {
                "arm": arm,
                "e1_tool_bearing_h": mapped_e1 / 3600.0,
                "e2_tool_bearing_h": mapped_e2 / 3600.0,
                "e1_bookkeeping_h": book["sum_elapsed_s"] / 3600.0,
                "e1_bookkeeping_tasks": book["n_tasks"],
                "e1_bookkeeping_processes": len(book["processes"]),
                "ratio_e2_over_e1": mapped_e2 / mapped_e1 if mapped_e1 else None,
            }
        )

    write_rows(
        "runtime_by_step.tsv",
        ["arm", "e2_transform", "nfcore_processes", "e1_tasks", "e2_tasks",
         "e1_sum_elapsed_h", "e2_sum_elapsed_h", "ratio_e2_over_e1",
         "e1_median_task_s", "e2_median_task_s"],
        rows,
    )
    write_rows(
        "runtime_totals.tsv",
        ["arm", "e1_tool_bearing_h", "e2_tool_bearing_h", "e1_bookkeeping_h",
         "e1_bookkeeping_tasks", "e1_bookkeeping_processes", "ratio_e2_over_e1"],
        totals,
    )
    if md:
        print("\n### Runtime per step, summed Slurm Elapsed, queue excluded\n")
        print(
            markdown(
                ["arm", "step", "nf-core processes", "tasks (E1 / E2)", "hours (E1 / E2)",
                 "E2/E1", "median task s (E1 / E2)"],
                [
                    [
                        r["arm"], r["e2_transform"], r["nfcore_processes"],
                        f"{r['e1_tasks']} / {r['e2_tasks']}",
                        f"{r['e1_sum_elapsed_h']:.1f} / {r['e2_sum_elapsed_h']:.1f}" if r["e2_sum_elapsed_h"] is not None else f"{r['e1_sum_elapsed_h']:.1f} / -",
                        f"{r['ratio_e2_over_e1']:.2f}" if r["ratio_e2_over_e1"] else "",
                        f"{r['e1_median_task_s']} / {r['e2_median_task_s']}" if r["e1_median_task_s"] is not None else f"- / {r['e2_median_task_s']}",
                    ]
                    for r in rows
                ],
            )
        )
        print("\n### Total runtime excluding queue, tool-bearing steps only\n")
        print(
            markdown(
                ["arm", "E1 tool-bearing h", "E2 tool-bearing h", "E2/E1",
                 "E1 bookkeeping h", "E1 bookkeeping tasks", "E1 bookkeeping processes"],
                [
                    [
                        r["arm"], f"{r['e1_tool_bearing_h']:.1f}", f"{r['e2_tool_bearing_h']:.1f}",
                        f"{r['ratio_e2_over_e1']:.2f}", f"{r['e1_bookkeeping_h']:.1f}",
                        r["e1_bookkeeping_tasks"], r["e1_bookkeeping_processes"],
                    ]
                    for r in totals
                ],
            )
        )
    return rows


# ----------------------------------------------------------------- reads mapped

def reads_mapped(md):
    """E1's short-arm read-mapping rate against its own assembly.

    E1's long arm has no mapping figure in any form: its MultiQC consumed CheckM2 only and its
    BAMs are absent from both archive manifests. E2's side is not in this repository at all --
    the rate lives in the bowtie2 summary inside each alignment shard's logs/.command.out in
    fir's live E2 cache. Until that is read, this column is E1-only and says so.
    """
    path = HERE / "e1" / "e1_short_bowtie2_assembly_align.tsv"
    if not path.exists():
        return []
    rows = list(read_tsv(path))
    rates = [float(r["overall_alignment_rate"]) for r in rows if r.get("overall_alignment_rate")]
    d = dist(rates)
    out = [{"arm": "short", "pipeline": "E1", "samples": d["n"], "source": "MultiQC bowtie2 assembly log",
            **{k: d[k] for k in ("min", "p10", "median", "p90", "max")},
            "mean": statistics.fmean(rates)}]
    write_rows(
        "reads_mapped.tsv",
        ["arm", "pipeline", "samples", "source", "min", "p10", "median", "p90", "max", "mean"],
        out,
    )
    if md:
        print("\n### Reads mapped to the assembly, overall alignment rate\n")
        print(
            markdown(
                ["arm", "pipeline", "samples", "source", "min", "p10", "median", "p90", "max"],
                [
                    [r["arm"], r["pipeline"], r["samples"], r["source"],
                     f"{r['min']:.2f}", f"{r['p10']:.2f}", f"{r['median']:.2f}",
                     f"{r['p90']:.2f}", f"{r['max']:.2f}"]
                    for r in out
                ],
            )
        )
        print("\nE2 short, E1 long and E2 long are absent. See findings/E1_E2_METRICS.md.")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--markdown", action="store_true", help="print the tables the findings doc quotes")
    ap.add_argument(
        "--only",
        choices=["assemblies", "binned", "mags", "ani", "runtime", "mapped"],
        help="run one section",
    )
    args = ap.parse_args()

    sections = {
        "assemblies": assemblies,
        "binned": contigs_binned,
        "mags": mag_size,
        "ani": ani_distribution,
        "runtime": runtime,
        "mapped": reads_mapped,
    }
    for name, fn in sections.items():
        if args.only and name != args.only:
            continue
        fn(args.markdown)
    if not args.markdown:
        print(f"wrote {len(list(OUT.glob('*.tsv')))} tables to {OUT}", file=sys.stderr)


if __name__ == "__main__":
    main()
