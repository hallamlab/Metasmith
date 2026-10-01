#!/usr/bin/env python3
"""ASPIRE over the Hallam lab's AB48 and purify 16S time series on sockeye.

    python research/aspire/campaigns/ab48_r1/run_ab48.py --study ab48_e5 list
    python research/aspire/campaigns/ab48_r1/run_ab48.py --study ab48_e5 run [--plan-only]
    python research/aspire/campaigns/ab48_r1/run_ab48.py --study ab48_e5 status
    python research/aspire/campaigns/ab48_r1/run_ab48.py --study ab48_e5 retrieve

Three studies, one driver:

    ab48_e5   the 2025-07-23 Enrichment5 run alone, 96 samples on one instrument
    ab48      AB48's historical and seven lab cohorts, 200 samples
    purify    the purify Spirulina samples that carry primers, with the bioreactor's sensor
              readings as measurements

The reads were staged on sockeye by the asv_task project, one directory per sequencing run.
The sheets come from capella and are pinned at data/aspire/hallam_16s_inputs. The AB48 MAGs
are the 24 dereplicated quality bins of cyanoverse/ab48's revio assembly, staged under ROOT.
Every AB48 and purify primer pair is 515F-Y/926R, which is the ASPIRE preset's amplicon, so
the preset is the params file unchanged.
"""

import csv
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from _campaign import REFS, REPO, Campaign, Sample, main  # noqa: E402
from metasmith.python_api import Duration, Resources, Size  # noqa: E402

ROOT = os.environ.get("ASPIRE_AB48_ROOT", "/scratch/st-shallam-1/txyliu/aspire_ab48")
STAGED = os.environ.get("ASPIRE_ASV_TASK_STAGING", "/scratch/st-shallam-1/pwy_group/staging/asv_task")
SHEETS = REPO / "data" / "aspire" / "hallam_16s_inputs"
PRESET = REPO / "research" / "aspire" / "presets" / "aspire.yml"

AB48_COHORTS = ["ab48_historical", "lab_19-05-01_to_23-05-01_Legacy", "lab_23-07-11_Enrichment1",
                "lab_23-08-15_Enrichment2", "lab_23-11-09_Enrichment3", "lab_24-01-15_Profile",
                "lab_24-06-15_Enrichment4", "lab_25-07-23_Enrichment5"]
# purify_2026_03_30 is trimmed of its primers and the other two runs are not, and the params
# file trims a fixed length, so the study keeps the runs that carry primers. The 2025_10_20 run
# repeats the 2025-06-02 run's s11 and s115 files under later dates; the first copy is kept.
PURIFY_COHORTS = ["purify_2025-06-02_Enrichment", "purify_2025_10_20_Enrichment"]
TAXA_LABELS = ["Type", "Strain", "Condition", "NaCl", "NaOH", "Stock", "Extraction"]
PURIFY_LABELS = ["Condition", "Round", "Glycerol", "DMSO"]
# The bioreactor log columns averaged into each sample's measurements, and the window before
# the sampling date they are averaged over. Setpoints and flows are configuration, not state.
SENSORS = {"Temperature (°C)": "temperature_c", "pH": "ph", "CO2(Injections / 10 min)": "co2_injections",
           "Relative Density": "relative_density", "Light Intensity (%)": "light_pct",
           "Tank Volume (L)": "tank_volume_l", "Growth Rate": "growth_rate"}
WINDOW = timedelta(days=7)

TARGETS = {
    "ab48_e5": ["aspire::counts_clean", "aspire::read_fate", "aspire::indicspecies_results",
                "aspire::diversity_outputs", "aspire::network_outputs", "aspire::asv_mag_pairing",
                "aspire::master_long"],
    "ab48": ["aspire::counts_clean", "aspire::read_fate", "aspire::indicspecies_results",
             "aspire::diversity_outputs", "aspire::network_outputs", "aspire::asv_mag_pairing",
             "aspire::asv_mag_network_outputs", "aspire::master_long"],
    "purify": ["aspire::counts_clean", "aspire::read_fate", "aspire::measurement_association_outputs"],
}
RESOURCE_OVERRIDES = {
    "sina_trim": Resources(cpus=16, memory=Size.GB(48), duration=Duration(hours=6)),
    "taxonomy": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=6)),
    "denoise": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=12)),
}


def _tsv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader((ln for ln in f if not ln.startswith("#")), delimiter="\t"))


def _csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def staged_reads(cohort: str) -> dict[str, Sample]:
    out = {}
    for row in _tsv(SHEETS / "manifests" / f"{cohort}.tsv"):
        sid = row["sample-id"]
        base = f"{STAGED}/{cohort}/reads"
        out[sid] = Sample(sid, f"{base}/{row['forward-absolute-filepath']}",
                          f"{base}/{row['reverse-absolute-filepath']}")
    return out


def crosswalk() -> list[dict]:
    return [r for r in _tsv(SHEETS / "sample_crosswalk.tsv") if r["is_control"] == "False"]


def _norm(s: str) -> str:
    return re.sub(r"[^0-9a-z]", "", s.lower())


def taxa_metadata(row: dict, by_id: dict) -> dict:
    """Taxa_Metadata.csv keys some IDs whole, truncates others to 15 characters, and gives the
    2025 run its read file prefix, so the join tries each in turn."""
    label = row["display_label"]
    for key in (label, label[:15]):
        if key in by_id:
            return by_id[key]
    want = _norm(row["asv_table_id"].removeprefix("s"))
    for norm, rec in by_id["_norm"]:
        if norm.startswith(want):
            return rec
    return {}


def _label(value: str) -> str:
    return "" if value.strip() in ("", "N/A", "NA") else value.strip()


def sheet(rows: list[dict], columns: list[str]) -> str:
    """The study sheet keeps only labels with at least two levels."""
    kept = [c for c in columns if len({r[c] for r in rows if r[c]}) >= 2]
    lines = ["\t".join(["sample", *kept])]
    lines += ["\t".join([r["sample"], *(r[c] for c in kept)]) for r in rows]
    return "\n".join(lines) + "\n"


def ab48_study(cohorts: list[str]):
    taxa = _csv(SHEETS / "Taxa_Metadata.csv")
    by_id = {r["ID"]: r for r in taxa}
    by_id["_norm"] = [(_norm(r["ID"]), r) for r in taxa]
    reads, rows = {}, []
    for cohort in cohorts:
        reads.update(staged_reads(cohort))
    for r in crosswalk():
        if r["cohort"] not in cohorts:
            continue
        meta = taxa_metadata(r, by_id)
        rows.append({"sample": r["asv_table_id"], "cohort": r["cohort"],
                     **{c: _label(meta.get(c, "")) for c in TAXA_LABELS}})
    samples = [reads[r["sample"]] for r in rows]
    return samples, sheet(rows, ["cohort", *TAXA_LABELS])


def pbr_measurements(rows: list[dict]) -> str:
    dates = {r["sample"]: datetime.fromisoformat(r["date"]) for r in rows if r["Condition"] == "PBR"}
    sums = {sid: {k: [0.0, 0] for k in SENSORS.values()} for sid in dates}
    with open(SHEETS / "pbr_logs_merged.csv", encoding="utf-8-sig") as f:
        for rec in csv.DictReader(f):
            when = datetime.fromisoformat(rec["Date"])
            values = {SENSORS[k]: float(rec[k]) for k in SENSORS if rec.get(k) not in (None, "")}
            if not any(values.values()):
                continue
            for sid, day in dates.items():
                if day - WINDOW <= when < day + timedelta(days=1):
                    for k, v in values.items():
                        sums[sid][k][0] += v
                        sums[sid][k][1] += 1
    cols = list(SENSORS.values())
    lines = ["\t".join(["sample", *cols])]
    for sid in dates:
        vals = [f"{s / n:.6g}" if n else "" for s, n in (sums[sid][c] for c in cols)]
        lines.append("\t".join([sid, *vals]))
    return "\n".join(lines) + "\n"


def purify_study():
    meta = {r["ID"]: r for r in _csv(SHEETS / "Purify_Metadata.csv")}
    reads, rows = {}, []
    for cohort in PURIFY_COHORTS:
        for sid, s in staged_reads(cohort).items():
            reads.setdefault(sid, s)
    seen = set()
    for r in crosswalk():
        sid = r["asv_table_id"]
        if r["cohort"] not in PURIFY_COHORTS or sid in seen:
            continue
        seen.add(sid)
        m = meta.get(r["display_label"], {})
        rows.append({"sample": sid, "date": m.get("Date", r["date"]),
                     **{c: _label(m.get(c, "")) for c in PURIFY_LABELS}})
    samples = [reads[r["sample"]] for r in rows]
    return samples, sheet(rows, PURIFY_LABELS), pbr_measurements(rows)


def campaign(args) -> Campaign:
    common = dict(here=HERE, params=PRESET.read_text(), targets=TARGETS[args.study],
                  mito_reference=f"{REFS}/refseq_mitochondrion.fasta",
                  contaminant_reference=f"{REFS}/contaminants.fasta",
                  resource_overrides=RESOURCE_OVERRIDES)
    if args.study == "purify":
        samples, study, measures = purify_study()
        return Campaign(name="purify_r1", root=f"{ROOT}/purify", samples=samples, study_sheet=study,
                        measurements=measures, **common)
    cohorts = AB48_COHORTS if args.study == "ab48" else ["lab_25-07-23_Enrichment5"]
    samples, study = ab48_study(cohorts)
    return Campaign(name=f"{args.study}_r1", root=ROOT, samples=samples, study_sheet=study,
                    switches_on={"spieceasi", "network_modules", "asv_mag_link", "graph_network"},
                    mag_collection=f"{ROOT}/mag_collection", **common)


def cmd_list(c: Campaign, _args):
    print(c.study_sheet, end="")
    if c.measurements:
        print(c.measurements, end="")
    print(f"{len(c.samples)} samples")
    return 0


def add_args(ap):
    ap.add_argument("--study", choices=sorted(TARGETS), default="ab48_e5")


if __name__ == "__main__":
    raise SystemExit(main(__doc__, campaign, {"list": cmd_list}, add_args=add_args))
