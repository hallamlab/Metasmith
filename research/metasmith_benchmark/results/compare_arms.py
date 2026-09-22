"""Compare E1 (nf-core/mag) and E2 (metasmith) sample by sample: assemblies by sequence, MAGs by
reciprocal skani match, and CheckM2 quality across matched MAG pairs.

Reads compare_arms/raw/*.tsv.gz (gathered from drivers/compare_arms_sample.py) and the per-bin CheckM2
tables in e1/ and e2/. Writes compare_arms/*.tsv. A MAG's partner in the other arm is the pair that
maximises ANI x min(AF_e1, AF_e2). Two MAGs match when each is the other's partner and the pair passes
both thresholds.
"""
import argparse
import csv
import gzip
import re
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
RAW = HERE / "compare_arms" / "raw"
OUT = HERE / "compare_arms"
BINNERS = ("DASTool", "COMEBin", "MetaBAT2", "SemiBin2")
PIPES = ("E1", "E2")


def read(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write(name, rows, head=None, gz=False):
    rows = list(rows)
    path = OUT / f"{name}.tsv{'.gz' if gz else ''}"
    with (gzip.open(path, "wt", newline="") if gz else open(path, "w", newline="")) as f:
        w = csv.DictWriter(f, fieldnames=head or list(rows[0]), delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    return rows


def tier(comp, cont):
    if comp > 90 and cont < 5:
        return "high"
    if comp >= 50 and cont < 10:
        return "medium"
    return "below"


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else ""


def fmt(x, d=4):
    return f"{x:.{d}f}" if isinstance(x, float) else x


def assemblies():
    rows = read(RAW / "assembly.tsv.gz")
    near = {}
    for r in read(RAW / "contig_matches.tsv.gz"):
        k = (r["sample"], r["pipeline"])
        n = near.setdefault(k, [0, 0])
        length = int(r["length"])
        n[0] += length
        n[1] += round(length * float(r["cov_id99"]))
    out = []
    for r in rows:
        row = dict(r)
        for p in PIPES:
            unshared, covered = near.get((r["sample"], p), [0, 0])
            row[f"{p.lower()}_unshared_bp_covered_id99"] = covered
            row[f"{p.lower()}_bp_frac_identical_or_id99"] = fmt(
                (int(r[f"{p.lower()}_bp"]) - unshared + covered) / int(r[f"{p.lower()}_bp"]), 6)
        out.append(row)
    return write("assemblies", out)


def partners(pairs, side):
    best = {}
    for p in pairs:
        score = float(p["ani"]) * min(float(p["af_e1"]), float(p["af_e2"]))
        k = p[side]
        if k not in best or score > best[k][0]:
            best[k] = (score, p)
    return {k: v[1] for k, v in best.items()}


def source_bin(name, have):
    m = re.match(r"^(MEGAHIT|FLYE)-DASTool-[^.]+\.(.+)$", name)
    stem = re.sub(r"_sub$", "", m.group(2) if m else name)
    m = re.match(r"^(MEGAHIT|FLYE)-(COMEBin|MetaBAT2|SemiBin2)(?:Refined)?-(.+?)[._](\d+)$", stem)
    asm, binner, sample, i = m.groups()
    hits = [(binner, c) for c in (f"{asm}-{binner}-{sample}.{i}", f"{asm}-{binner}-{sample}_{i}") if (binner, c) in have]
    assert len(hits) == 1, (name, hits)
    return hits[0]


def trace_dastool(per_mag):
    index = {(r["pipeline"], r["sample"], r["binner"], r["bin"]): r for r in per_mag}
    have = {}
    for r in per_mag:
        have.setdefault((r["pipeline"], r["sample"]), set()).add((r["binner"], r["bin"]))
    kept = {}
    for r in per_mag:
        if r["binner"] == "DASTool":
            r["source_binner"], r["source_bin"] = source_bin(r["bin"], have[(r["pipeline"], r["sample"])])
            kept.setdefault((r["pipeline"], r["sample"]), set()).add((r["source_binner"], r["source_bin"]))
    for r in per_mag:
        if r["binner"] != "DASTool" or r["matched"]:
            continue
        src = index[(r["pipeline"], r["sample"], r["source_binner"], r["source_bin"])]
        other = "E2" if r["pipeline"] == "E1" else "E1"
        if not src["matched"]:
            r["cause"] = "source bin unmatched"
        elif (r["source_binner"], src["partner"]) in kept.get((other, r["sample"]), set()):
            r["cause"] = "source bins matched, DAS Tool trimmed them differently"
        else:
            r["cause"] = "source bins matched, other arm's DAS Tool dropped its partner"


def mags(min_ani, min_af):
    inv = read(RAW / "mags.tsv.gz")
    checkm = {}
    for p, path in (("E1", HERE / "e1" / "e1_checkm2.tsv.gz"), ("E2", HERE / "e2" / "e2_checkm2.tsv.gz")):
        for r in read(path):
            checkm[(p, r["sample"], r["binner"], r["bin"])] = (float(r["Completeness"]), float(r["Contamination"]))
    pairs = {}
    for r in read(RAW / "skani_pairs.tsv.gz"):
        pairs.setdefault((r["sample"], r["binner"]), []).append(r)

    per_mag = []
    for r in inv:
        k = (r["pipeline"], r["sample"], r["binner"], r["bin"])
        assert k in checkm, f"no CheckM2 row for {k}"
    by_group = {}
    for r in inv:
        by_group.setdefault((r["arm"], r["sample"], r["binner"]), []).append(r)
    for (arm, s, b), group in sorted(by_group.items()):
        ps = pairs.get((s, b), [])
        best_e1, best_e2 = partners(ps, "e1_bin"), partners(ps, "e2_bin")
        bp = {(r["pipeline"], r["bin"]): int(r["bp"]) for r in group}
        for r in group:
            pipe, name = r["pipeline"], r["bin"]
            hit = (best_e1 if pipe == "E1" else best_e2).get(name)
            mine, theirs = ("e1_bin", "e2_bin") if pipe == "E1" else ("e2_bin", "e1_bin")
            partner = hit[theirs] if hit else ""
            back = (best_e2 if pipe == "E1" else best_e1).get(partner)
            reciprocal = bool(hit) and back is not None and back[mine] == name
            passes = bool(hit) and float(hit["ani"]) >= min_ani and min(float(hit["af_e1"]), float(hit["af_e2"])) >= min_af
            comp, cont = checkm[(pipe, s, b, name)]
            row = {"arm": arm, "sample": s, "binner": b, "pipeline": pipe, "bin": name, "bp": bp[(pipe, name)],
                   "completeness": comp, "contamination": cont, "tier": tier(comp, cont),
                   "partner": partner, "ani": hit["ani"] if hit else "",
                   "af_self": hit[f"af_{pipe.lower()}"] if hit else "",
                   "af_partner": hit[f"af_{'e2' if pipe == 'E1' else 'e1'}"] if hit else "",
                   "reciprocal": reciprocal, "matched": reciprocal and passes}
            if row["matched"]:
                other = "E2" if pipe == "E1" else "E1"
                pc, pn = checkm[(other, s, b, partner)]
                row.update(partner_completeness=pc, partner_contamination=pn, partner_tier=tier(pc, pn))
            per_mag.append(row)
    trace_dastool(per_mag)
    head = list(per_mag[0]) + ["partner_completeness", "partner_contamination", "partner_tier",
                               "source_binner", "source_bin", "cause"]
    write("mag_matches", per_mag, head, gz=True)
    write("unmatched_mags", [r for r in per_mag if not r["matched"]], head)
    return per_mag


def sample_table(per_mag):
    groups = {}
    for r in per_mag:
        groups.setdefault((r["arm"], r["sample"], r["binner"]), []).append(r)
    out = []
    for (arm, s, b), rs in sorted(groups.items()):
        row = {"arm": arm, "sample": s, "binner": b}
        for p in PIPES:
            mine = [r for r in rs if r["pipeline"] == p]
            matched = [r for r in mine if r["matched"]]
            row[f"{p}_mags"] = len(mine)
            row[f"{p}_bp"] = sum(r["bp"] for r in mine)
            row[f"{p}_matched"] = len(matched)
            row[f"{p}_only"] = len(mine) - len(matched)
            row[f"{p}_bp_matched_frac"] = fmt(sum(r["bp"] for r in matched) / row[f"{p}_bp"]) if row[f"{p}_bp"] else ""
            for t in ("high", "medium", "below"):
                row[f"{p}_{t}"] = sum(r["tier"] == t for r in mine)
        e2m = [r for r in rs if r["pipeline"] == "E2" and r["matched"]]
        ani = [float(r["ani"]) for r in e2m]
        af = [min(float(r["af_self"]), float(r["af_partner"])) for r in e2m]
        dcomp = [r["completeness"] - r["partner_completeness"] for r in e2m]
        dcont = [r["contamination"] - r["partner_contamination"] for r in e2m]
        row.update({
            "ani_mean": fmt(statistics.mean(ani)) if ani else "", "ani_median": fmt(statistics.median(ani)) if ani else "",
            "ani_min": fmt(min(ani)) if ani else "", "ani_p5": fmt(pct(ani, 0.05)) if ani else "",
            "af_mean": fmt(statistics.mean(af), 2) if af else "", "af_min": fmt(min(af), 2) if af else "",
            "median_dcompleteness": fmt(statistics.median(dcomp), 2) if dcomp else "",
            "median_dcontamination": fmt(statistics.median(dcont), 2) if dcont else "",
            "tier_changed": sum(r["tier"] != r["partner_tier"] for r in e2m),
            "tier_changed_mags": " ".join(f"{r['bin']}:{r['partner_tier']}->{r['tier']}" for r in e2m if r["tier"] != r["partner_tier"]),
        })
        out.append(row)
    return write("per_sample", out)


def dastool_causes(per_mag):
    counts = {}
    for r in per_mag:
        if r["binner"] == "DASTool" and not r["matched"]:
            c = counts.setdefault((r["arm"], r["pipeline"], r["cause"]), [0, 0])
            c[0] += 1
            c[1] += r["tier"] == "high"
    return write("dastool_unmatched_causes", ({"arm": a, "pipeline": p, "cause": c, "mags": n, "high_quality": h}
                                              for (a, p, c), (n, h) in sorted(counts.items())))


def summary(asm, per_mag):
    out = []
    for arm in ("short", "long"):
        a = [r for r in asm if r["arm"] == arm]
        identical = sum(r["verdict"] != "different" for r in a)
        frac = sorted(min(float(r["e1_shared_bp_frac"]), float(r["e2_shared_bp_frac"])) for r in a)
        near = sorted(min(float(r["e1_bp_frac_identical_or_id99"]), float(r["e2_bp_frac_identical_or_id99"])) for r in a)
        for b in BINNERS:
            rs = [r for r in per_mag if r["arm"] == arm and r["binner"] == b]
            row = {"arm": arm, "binner": b, "samples": len(a),
                   "assemblies_identical": identical,
                   "asm_shared_bp_frac_min/median": f"{fmt(frac[0])} / {fmt(statistics.median(frac))}",
                   "asm_bp_frac_id99_min/median": f"{fmt(near[0])} / {fmt(statistics.median(near))}"}
            for p in PIPES:
                mine = [r for r in rs if r["pipeline"] == p]
                matched = [r for r in mine if r["matched"]]
                row[f"{p}_mags"] = len(mine)
                row[f"{p}_matched_pct"] = fmt(100 * len(matched) / len(mine), 1) if mine else ""
                row[f"{p}_unmatched"] = len(mine) - len(matched)
                row[f"{p}_unmatched_high"] = sum(r["tier"] == "high" for r in mine if not r["matched"])
            e2m = [r for r in rs if r["pipeline"] == "E2" and r["matched"]]
            row["matched_ani_median"] = fmt(statistics.median(float(r["ani"]) for r in e2m)) if e2m else ""
            row["median_abs_dcompleteness"] = fmt(statistics.median(abs(r["completeness"] - r["partner_completeness"]) for r in e2m), 2) if e2m else ""
            row["tier_changed"] = sum(r["tier"] != r["partner_tier"] for r in e2m)
            out.append(row)
    return write("summary", out)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--min-ani", type=float, default=95.0)
    p.add_argument("--min-af", type=float, default=50.0, help="percent, applied to both directions")
    a = p.parse_args()
    asm = assemblies()
    per_mag = mags(a.min_ani, a.min_af)
    sample_table(per_mag)
    dastool_causes(per_mag)
    for r in summary(asm, per_mag):
        print("\t".join(str(v) for v in r.values()))


if __name__ == "__main__":
    main()
