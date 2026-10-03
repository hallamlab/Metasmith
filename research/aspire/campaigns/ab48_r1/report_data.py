#!/usr/bin/env python3
"""Build the lab ASV timeline page from the pinned ASPIRE results and the bioreactor logs.

    python research/aspire/campaigns/ab48_r1/report_data.py [--out PAGE] [--study lab_r1] [--v4-study purify_v4_r1]

Reads the study's pinned results under data/aspire and the sheets and reactor log in
data/aspire/hallam_16s_inputs, and fills report_template.html with the result.
"""

import argparse
import json
import math
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DATA = REPO / "data" / "aspire"
SHEETS = DATA / "hallam_16s_inputs"
GTDB = Path.home() / "agentic_workspace/projects/cyanoverse/ab48/data/revio/taxonomy/gtdbtk_r232/gtdbtk.bac120.summary.tsv"

BAR_TAXA = 7
DOT_TAXA = 16
BIN_HOURS = 3
RANKS = ["Domain", "Phylum", "Class", "Order", "Family", "Genus"]
# The logger holds a dose or harvest value across the rows of one event and reads 0 between
# events, so a bin keeps its largest value; a mean would dilute an event and a sum count it many times.
EVENT_COLUMNS = {"Nutrients Added (mL)", "Harvest (L)"}
LOG_PANELS = [
    ("Temperature", "°C", ["Temperature (°C)", "Temperature Setpoint (°C)"]),
    ("pH", "pH", ["pH", "pH Setpoint (°C)"]),
    ("CO₂ injections", "per 10 min", ["CO2(Injections / 10 min)"]),
    ("Relative density", "", ["Relative Density"]),
    ("Growth rate", "", ["Growth Rate"]),
    ("Light intensity", "%", ["Light Intensity (%)"]),
    ("Tank volume", "L", ["Tank Volume (L)"]),
    ("Nutrients added", "mL, 3-h max", ["Nutrients Added (mL)"]),
    ("Harvest", "L, 3-h max", ["Harvest (L)"]),
    ("Nutrient feed mix", "proportion", ["Nutrient A Proportion", "Nutrient B Proportion", "Nutrient C Proportion"]),
]
SERIES_NAMES = {
    "Temperature (°C)": "measured", "Temperature Setpoint (°C)": "setpoint",
    "pH": "measured", "pH Setpoint (°C)": "setpoint",
    "Nutrient A Proportion": "A", "Nutrient B Proportion": "B", "Nutrient C Proportion": "C",
}


def one(directory: Path) -> Path:
    files = [p for p in directory.iterdir() if p.is_file()]
    assert len(files) == 1, directory
    return files[0]


def taxon_name(lineage: str) -> tuple[str, str]:
    parts = {p.split("__", 1)[0]: p.split("__", 1)[1] for p in lineage.split("; ") if "__" in p}
    ranks = dict(zip("dpcofg", RANKS))
    named = [(ranks[k], v) for k, v in parts.items() if k in ranks and v and not v.lower().startswith("uncultured")]
    phylum = parts.get("p", "") or "Unassigned"
    if not named:
        return "Unassigned", phylum
    rank, value = named[-1]
    value = value.replace("_", " ")
    return (value if rank == "Genus" else f"{value} ({rank.lower()})"), phylum


def study_tables(name: str):
    root = DATA / name
    counts = pd.read_csv(one(root / "aspire-counts_clean"), sep="\t", index_col=0)
    tax = pd.read_csv(one(root / "amplicon-asv_taxonomy"), sep="\t", index_col=0)
    names = tax["Taxon"].map(taxon_name)
    asv_taxon = names.map(lambda t: t[0]).reindex(counts.index).fillna("Unassigned")
    phyla = dict(zip(names.map(lambda t: t[0]), names.map(lambda t: t[1])))
    rel = counts / counts.sum(axis=0)
    by_taxon = rel.groupby(asv_taxon).sum()
    return counts, rel, asv_taxon, by_taxon, phyla


def rank_taxa(by_taxon: pd.DataFrame, n: int) -> list[str]:
    score = by_taxon.drop(index="Unassigned", errors="ignore").max(axis=1) + by_taxon.mean(axis=1)
    return score.sort_values(ascending=False).index[:n].tolist()


def crosswalk() -> pd.DataFrame:
    cw = pd.read_csv(SHEETS / "sample_crosswalk.tsv", sep="\t", dtype=str)
    return cw[cw.is_control == "False"]


# The 2026 purify run amplified V4 alone and is its own study, so its samples join the V4-V5
# samples here, at the taxon level, and never by ASV.
def purify_section(studies: list[tuple[str, pd.DataFrame, pd.DataFrame]]):
    cw = crosswalk().drop_duplicates("asv_table_id").set_index("asv_table_id")
    meta = pd.read_csv(SHEETS / "Purify_Metadata.csv", dtype=str).set_index("ID")
    samples, frames, asvs = [], [], {}
    for study, counts, by_taxon in studies:
        ids = [s for s in counts.columns if s in cw.index and cw.loc[s, "cohort"].startswith("purify")]
        for sid in ids:
            row = cw.loc[sid]
            label = row.display_label
            m = meta.loc[label] if label in meta.index else None
            samples.append({
                "id": sid, "label": label.replace("_", " "),
                "date": m["Date"] if m is not None else row.date,
                "condition": row.condition, "washed": "Washed" in label,
                "reads": int(counts[sid].sum()), "study": study,
            })
        frames.append(by_taxon[ids])
        asvs[study] = int((counts[ids].sum(axis=1) > 0).sum())
    samples.sort(key=lambda x: (x["date"], x["washed"], x["label"]))
    frame = pd.concat(frames, axis=1).fillna(0.0)[[x["id"] for x in samples]]
    return samples, frame[frame.sum(axis=1) > 0], asvs


def gtdb_names() -> dict[str, str]:
    out = {}
    for _, r in pd.read_csv(GTDB, sep="\t").iterrows():
        ranks = dict(p.split("__", 1) for p in r["classification"].split(";"))
        bin_id = r["user_genome"].split(".")[0].rsplit("-", 1)[0]
        out[r["user_genome"].lower()] = f"{ranks.get('s') or ranks.get('g') or ranks.get('f')} (bin {bin_id})"
    return out


def ab48_section(study: str, counts, asv_taxon, by_taxon):
    cw = crosswalk().drop_duplicates("asv_table_id").set_index("asv_table_id")
    present = [s for s in counts.columns if s in cw.index and not cw.loc[s, "cohort"].startswith("purify")]
    keys = cw.loc[present, ["cohort", "date"]].apply(tuple, axis=1)
    groups = []
    columns = {}
    for (cohort, date), ids in keys.groupby(keys).groups.items():
        ids = list(ids)
        label = "Historical" if cohort == "ab48_historical" else cohort.rsplit("_", 1)[-1]
        key = f"{cohort}|{date}"
        groups.append({"id": key, "label": label, "date": date, "cohort": cohort, "n": len(ids),
                       "reads": int(counts[ids].sum().sum())})
        columns[key] = by_taxon[ids].mean(axis=1)
    groups.sort(key=lambda g: (g["date"], g["cohort"]))
    grouped = pd.DataFrame(columns)[[g["id"] for g in groups]]
    grouped = grouped[grouped.sum(axis=1) > 0]

    pairing = pd.read_csv(one(DATA / study / "aspire-asv_mag_pairing"), sep="\t")
    filtered = pairing.ASV_ID.nunique()
    pairing = pairing[pairing.pairing_status != "unpaired"]
    names = gtdb_names()
    reads = counts.sum(axis=1)
    mags = {}
    for asv, rows in pairing.groupby("ASV_ID"):
        taxon = asv_taxon.get(asv)
        if taxon is None:
            continue
        for _, r in rows.iterrows():
            entry = mags.setdefault(taxon, {})
            name = names.get(r.genome_id, r.genome_id)
            entry[name] = entry.get(name, 0) + int(reads[asv])
    mags = {t: [n for n, _ in sorted(v.items(), key=lambda kv: -kv[1])] for t, v in mags.items()}
    stats = {"samples": len(present), "asvs": int((counts[present].sum(axis=1) > 0).sum()), "filtered_asvs": int(filtered),
             "paired_asvs": int(pairing.ASV_ID.nunique())}
    return groups, grouped, mags, stats


def log_section():
    d = pd.read_csv(SHEETS / "pbr_logs_merged.csv", parse_dates=["Date"])
    cols = [c for _, _, cs in LOG_PANELS for c in cs]
    live = d[(d.select_dtypes("number") != 0).any(axis=1)].set_index("Date").sort_index()
    rule = f"{BIN_HOURS}h"
    state = live[[c for c in cols if c not in EVENT_COLUMNS]].resample(rule).mean()
    events = live[[c for c in cols if c in EVENT_COLUMNS]].resample(rule).max()
    logged = live[cols[0]].resample(rule).count() > 0
    binned = pd.concat([state, events], axis=1)[cols].where(logged)
    binned[list(EVENT_COLUMNS)] = binned[list(EVENT_COLUMNS)].fillna(0.0).where(logged)
    t0 = binned.index[0]

    def clean(v):
        return None if v is None or (isinstance(v, float) and math.isnan(v)) else float(f"{v:.4g}")

    panels = []
    for title, unit, cs in LOG_PANELS:
        panels.append({"title": title, "unit": unit, "event": cs[0] in EVENT_COLUMNS, "series": [
            {"name": SERIES_NAMES.get(c, title), "column": c, "values": [clean(v) for v in binned[c]]}
            for c in cs]})
    runs = {run: [g.index.min().isoformat(), g.index.max().isoformat()] for run, g in live.groupby("run")}
    dropped = [c for c in d.select_dtypes("number").columns if (live[c] == 0).all()]
    return {"t0": t0.isoformat(), "step_hours": BIN_HOURS, "n": len(binned), "panels": panels,
            "runs": runs, "rows": len(live), "dropped": dropped,
            "first": live.index.min().isoformat(), "last": live.index.max().isoformat()}


# A taxon in both figures takes the same slot in each. The rest fill each figure's free slots in
# its own order, so neither figure repeats a hue and neither needs more than its bar count.
def assign_colors(sections: list[tuple[pd.DataFrame, list[str]]]) -> list[dict[str, int]]:
    shared = [t for t in sections[0][1] if all(t in bars for _, bars in sections[1:])]
    shared.sort(key=lambda t: -sum(float(f.loc[t].mean()) for f, _ in sections))
    out = []
    for _, bars in sections:
        slots = {t: i for i, t in enumerate(shared)}
        for t in bars:
            slots.setdefault(t, len(slots))
        out.append(slots)
    return out


def frame_json(frame: pd.DataFrame, taxa: list[str]) -> dict[str, list[float]]:
    return {t: [round(float(v), 5) for v in frame.loc[t]] for t in taxa if t in frame.index}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=REPO / "cache" / "aspire" / "report" / "lab_asv_timeline.html")
    ap.add_argument("--study", default="lab_r1")
    ap.add_argument("--v4-study", default="purify_v4_r1")
    args = ap.parse_args()

    counts, _, asv_taxon, by_taxon, phyla = study_tables(args.study)
    v4_counts, _, _, v4_by_taxon, v4_phyla = study_tables(args.v4_study)
    phyla = {**v4_phyla, **phyla}
    p_samples, p_frame, p_asvs = purify_section([(args.study, counts, by_taxon), (args.v4_study, v4_counts, v4_by_taxon)])
    a_groups, a_frame, mags, a_stats = ab48_section(args.study, counts, asv_taxon, by_taxon)
    p_bars, a_bars = rank_taxa(p_frame, BAR_TAXA), rank_taxa(a_frame, BAR_TAXA)
    p_slots, a_slots = assign_colors([(p_frame, p_bars), (a_frame, a_bars)])
    p_rows, a_rows = rank_taxa(p_frame, DOT_TAXA), rank_taxa(a_frame, DOT_TAXA)

    data = {
        "study": {"name": args.study, "v4": args.v4_study, "samples": int(counts.shape[1]), "asvs": int(len(counts))},
        "phyla": {t: phyla.get(t, "") for t in set(p_rows + a_rows)},
        "purify": {"samples": p_samples, "slots": p_slots, "bars": p_bars, "rows": p_rows, "asvs": p_asvs,
                   "rel": frame_json(p_frame, sorted(set(p_rows + p_bars)))},
        "ab48": {"groups": a_groups, "slots": a_slots, "bars": a_bars, "rows": a_rows, "stats": a_stats,
                 "mags": {t: mags.get(t, []) for t in a_rows},
                 "rel": frame_json(a_frame, sorted(set(a_rows + a_bars)))},
        "logs": log_section(),
    }
    html = (HERE / "report_template.html").read_text().replace("/*DATA*/null", json.dumps(data, ensure_ascii=False))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html)
    print(f"{args.out} {len(html) / 1e6:.2f} MB")
    print("purify", p_slots)
    print("ab48", a_slots)


if __name__ == "__main__":
    main()
