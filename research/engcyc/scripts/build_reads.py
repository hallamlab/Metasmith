"""Locate the read files of every Engcyc dataset that is not on a node metasmith reads yet.

Writes data/engcyc/reads/<dataset_id>.tsv. Saanich and Line P get one row per sample, joined
across NCBI SRA, the JGI Data Portal, the January 2025 sharknado filesystem index and the chinook
QC copies. The other sharknado datasets and the metaGEM studies get one row per file.
Inputs are the pinned listings in data/engcyc/sources/reads/.
"""
import collections
import csv
import gzip
import sys
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "data/engcyc/sources/reads"
OUT = ROOT / "data/engcyc/reads"

SHARKNADO_BACKUP = "worker-01:/mnt/sharknado_backup/Shamwow_Home/"
CHINOOK_LAB = "globus:bae74145-f2ef-41e6-ae73-38d06110819d:"
TREES = {
    "saanich_metagenome": ["OMZs/Saanich/RawData/Joint_Genome_Institute/Illumina_metagenomes/"],
    "linep_metagenome": [
        "OMZs/LineP/RawData/Joint_Genome_Institute/Illumina_metagenomes/",
        "OMZs/Saanich_and_LineP/RawData/Joint_Genome_Institute/Illumina_metagenomes/",
    ],
}
CHINOOK_DIR = {"saanich_metagenome": "/Saanich_QC_Reads/MetaGs/", "linep_metagenome": "/LineP_QC_Reads/"}

# JGI names a sequencing unit <run>.<lane>.<flowcell>.<barcode>. Old units carry a library-code prefix.
SEQ_UNIT = re.compile(r"(?<!\d)(\d{4,5}\.\d\.\d{4,6}(?:\.[ACGTN]+(?:-[ACGTN]+)?)?)\.(?:anqdp\.|filter-[A-Z]+\.)?f(?:ast)?q")
MONTH = {m[:3].lower(): m[0].upper() for m in
         ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
          "November", "December"]}
MONTH["jun"], MONTH["jul"], MONTH["jan"] = "J", "Jl", "Ja"


def read_tsv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def seq_unit(name):
    m = SEQ_UNIT.search(name)
    return m.group(1) if m else ""


def saanich_key(text, cruise="", depth=""):
    text = text.replace(" ", "_")
    c = re.search(r"SI_?(?:S\d_)?0*(\d{2,3})", text) or re.match(r"^(\d{2})_\d\d/", text)
    if cruise:
        c = re.match(r"SI0*(\d+)", cruise)
    if not c:
        return None
    s = re.search(r"_S(\d)(?:LV|_)", text) or re.search(r"_SI(\d)_", text)
    d = depth or (re.findall(r"_(\d+)(?:m|MG|_MG|_DNA|$)", text) or [""])[-1]
    rep = re.search(r"_(\d+)m_([AB])(?:_|$)", text)
    return (int(c.group(1)), s.group(1) if s else "", int(d) if d else None, rep.group(2) if rep else "")


def linep_key(text):
    t = text.replace(" ", "_")
    m = (re.search(r"(?:LP|sample)_([AFJ])_?(\d{2})_P(\d+)_(\d+)", t)
         or re.search(r"LP_([AFJ])(\d{2})_P(\d+)_(\d+)m", t)
         or re.search(r"LP_([AFJ])_20(\d{2})_P(\d+)_(\d+)m", t))
    if m:
        season, yy, st, d = m.groups()
    else:
        m = re.search(r"(?:P(\d+)_)?([A-Z][a-z]+)_?(20\d\d)_?(?:P(\d+)_)?(\d+)m", t)
        if not m:
            m2 = re.search(r"LP_J_20(\d\d)_(\d+)m", t)
            return ("J", m2.group(1), None, int(m2.group(2)), "") if m2 else None
        st1, mon, yyyy, st2, d = m.groups()
        season, yy, st = MONTH.get(mon[:3].lower(), mon[0]), yyyy[2:], st1 or st2
    rep = re.search(r"_(\d+)m_([AB])(?:_|$)", t)
    return (season[0], yy, int(st) if st else None, int(d), rep.group(2) if rep else "")


KEY = {"saanich_metagenome": saanich_key, "linep_metagenome": linep_key}


def sharknado_files(dataset):
    files = collections.defaultdict(lambda: {"raw": [], "qc": []})
    every = []
    by_lib = collections.defaultdict(lambda: {"raw": [], "qc": []})
    with gzip.open(SRC / "sharknado_read_paths.txt.gz", "rt") as f:
        for line in f:
            p = line.rstrip("\n")
            rel = p.removeprefix("sharknado/LimsData/")
            if not any(rel.startswith(t) for trees in TREES.values() for t in trees):
                continue
            name = rel.rsplit("/", 1)[-1]
            parts = rel.split("/")
            if "QC_and_Genome_Assembly" in parts or re.search(r"unmapped|ReadsMapping|\.bam$|input\.corr|\.trim\.", name):
                continue
            kind = "qc" if ("QC_Filtered_Raw_Data" in parts or re.search(r"anqdp|filter-|qtrimmed|_pe\.[12]\.|unpaired", name)) else "raw"
            loc = SHARKNADO_BACKUP + rel
            every.append((loc, kind))
            su = seq_unit(name)
            if su:
                files[su][kind].append(loc)
            lib = re.match(r"^([A-Z]{4,5})\.", name)
            if lib:
                by_lib[lib.group(1)][kind].append(loc)
            elif not su:
                folder = next((x for x in parts if re.match(r"^\d{7}_[A-Z]{4,5}_", x)), "")
                if folder:
                    by_lib[folder.split("_")[1]][kind].append(loc)
    return files, by_lib, every


def sample_folder(loc):
    parts = loc.split("Illumina_metagenomes/", 1)[1].split("/")[:-1]
    return "/".join(x for x in parts if x not in ("Raw_Data", "QC_Filtered_Raw_Data", "forward_reads", "reverse_reads"))


def public_units(dataset):
    """One record per sequencing unit, from SRA first and the JGI portal for units SRA lacks."""
    region = "saanich" if dataset == "saanich_metagenome" else "linep"
    units = {}
    for r in read_tsv(SRC / f"sra_{region}.tsv"):
        su = seq_unit(r["sra_original_file"]) or f"lib:{r['jgi_library']}"
        u = units.setdefault(su, {"title": r["lab_sample_id"] or r["sample_title"], "sra_title": r["sample_title"],
                                  "cruise": r["cruise"], "depth": r["depth_m"], "lib": r["jgi_library"],
                                  "runs": [], "bases": 0, "sra_mb": 0, "jgi": "", "notes": r["notes"]})
        u["runs"].append(r["run"])
        u["bases"] += int(r["bases"] or 0)
        u["sra_mb"] += int(r["size_mb"] or 0)
    for r in read_tsv(SRC / f"jgi_portal_{region}.tsv"):
        if r["kind"] != "metagenome" or r["in_sra_run"]:
            continue
        title = r["ap_name"]
        if re.search(r"MetaT|RNA|metatranscriptome|viral", title, re.I):
            continue
        su = seq_unit(r["raw_fastq"])
        if not su or su in units:
            continue
        units[su] = {"title": title, "sra_title": "", "cruise": "", "depth": "", "lib": "", "runs": [], "bases": 0,
                     "sra_mb": 0, "jgi": f"{r['jgi_ap_id']} {r['raw_fastq']} {r['raw_size_gb']} GB {r['raw_status']}",
                     "notes": "JGI portal only, not in SRA"}
    sra_title = {u["lib"]: u["title"] for u in units.values() if u["lib"]}
    for r in read_tsv(SRC / "jgi_portal_libs.tsv"):
        if dataset_of_title(r["title"]) != dataset or re.search(r"combined|MetaT|RNA", r["title"]):
            continue
        for raw in r["raw_fastq"].split("; "):
            su = seq_unit(raw)
            if su in units:
                units[su]["lib"] = units[su]["lib"] or r["library"]
                continue
            units[su] = {"title": sra_title.get(r["library"], r["title"]), "sra_title": "", "cruise": "", "depth": "",
                         "lib": r["library"], "runs": [], "bases": 0, "sra_mb": 0,
                         "jgi": f"{r['jgi_project']} {raw}", "notes": "unit not in SRA"}
    return units


def dataset_of_title(title):
    return "saanich_metagenome" if re.search(r"Saanich", title) else "linep_metagenome"


def chinook_files(dataset):
    out = collections.defaultdict(list)
    for r in read_tsv(SRC / "chinook_hallam_lab_data.tsv"):
        if r["path"].startswith(CHINOOK_DIR[dataset]):
            name = r["path"].rsplit("/", 1)[-1].split("_pe.")[0]
            out[name].append((CHINOOK_LAB + r["path"], int(r["bytes"])))
    return out


def sample_key(dataset, u):
    if dataset == "saanich_metagenome":
        return saanich_key(u["title"], *( (u["cruise"], u["depth"]) if not re.match(r"SI0\d\d_", u["title"]) else ("", "")))
    return linep_key(u["title"]) or linep_key(u["cruise"] + "_" + str(u["depth"]) + "m")


def loose_match(dataset, k, by_key):
    """Match ignoring the replicate letter, and the station where one side omits it."""
    if dataset == "saanich_metagenome":
        hits = [s for kk, s in by_key.items() if kk[0] == k[0] and kk[2] == k[2] and "" in (k[1], kk[1], k[1] if k[1] == kk[1] else None)]
    else:
        hits = [s for kk, s in by_key.items() if kk[:2] == k[:2] and kk[3] == k[3] and k[2] in (None, kk[2])]
    return hits[0] if len(hits) == 1 else None


def claimed_by(dataset):
    """Sharknado files whose unit or library a public record of this dataset names."""
    sk_units, sk_libs, _ = sharknado_files(dataset)
    out = set()
    for su, u in public_units(dataset).items():
        for src in (sk_units.get(su), sk_libs.get(u["lib"]) if u["lib"] else None):
            if src:
                out.update(src["raw"] + src["qc"])
    return out


def build(dataset):
    sk_units, sk_libs, sk_every = sharknado_files(dataset)
    units = public_units(dataset)
    samples = collections.OrderedDict()
    used_sk = set()
    for su, u in units.items():
        key = sample_key(dataset, u) or ("unparsed", u["title"])
        s = samples.setdefault(key, {"names": [], "units": [], "runs": [], "bases": 0, "sra_mb": 0, "jgi": [],
                                     "sk_raw": [], "sk_qc": [], "notes": []})
        s["names"].append(u["title"])
        s["units"].append(su)
        s["runs"] += u["runs"]
        s["bases"] += u["bases"]
        s["sra_mb"] += u["sra_mb"]
        if u["jgi"]:
            s["jgi"].append(u["jgi"])
        if u["notes"]:
            s["notes"].append(u["notes"])
        for src in (sk_units.get(su), sk_libs.get(u["lib"]) if u["lib"] else None):
            if src:
                s["sk_raw"] += src["raw"]
                s["sk_qc"] += src["qc"]
                used_sk.update(src["raw"] + src["qc"])
    own = tuple(SHARKNADO_BACKUP + t for t in TREES[dataset])
    claimed = {x for other in TREES if other != dataset for x in claimed_by(other)}
    by_key = {k: s for k, s in samples.items() if k[0] != "unparsed"}
    leftover = collections.defaultdict(lambda: {"raw": [], "qc": []})
    for loc, kind in sk_every:
        if loc not in used_sk and loc not in claimed and loc.startswith(own):
            leftover[sample_folder(loc)][kind].append(loc)
    for folder, f in leftover.items():
        k = KEY[dataset](folder.rsplit("/", 1)[-1])
        hit = (by_key.get(k) or loose_match(dataset, k, by_key)) if k else None
        if hit is None:
            hit = samples[("sharknado_only", folder)] = {
                "names": [folder], "units": sorted({seq_unit(x.rsplit("/", 1)[-1]) for x in f["raw"] + f["qc"]} - {""}),
                "runs": [], "bases": 0, "sra_mb": 0, "jgi": [], "sk_raw": [], "sk_qc": [],
                "notes": ["sharknado only, no public unit matched"]}
        hit["sk_raw"] += f["raw"]
        hit["sk_qc"] += f["qc"]

    chin = chinook_files(dataset)
    for name, files in chin.items():
        k = KEY[dataset](name)
        hit = (by_key.get(k) or loose_match(dataset, k, by_key)) if k else None
        if hit is None:
            key = ("chinook_only", name)
            hit = samples[key] = {"names": [name], "units": [], "runs": [], "bases": 0, "sra_mb": 0, "jgi": [],
                                  "sk_raw": [], "sk_qc": [], "notes": ["chinook QC copy only"]}
        hit.setdefault("chinook", []).extend(files)

    for s in samples.values():
        if any("filter-MTF" in x for x in s["sk_qc"]):
            s["notes"].append("metatranscriptome (JGI filter-MTF), not a metagenome")
        if any("ESP_" in n for n in s["names"]):
            s["notes"].append("ESP sampler, not a cruise depth profile")

    OUT.mkdir(parents=True, exist_ok=True)
    cols = ["sample_key", "names", "seq_units", "sra_runs", "sra_bases", "sra_mb", "jgi_portal", "sharknado_raw",
            "sharknado_qc", "chinook_qc", "chinook_qc_bytes", "notes"]
    with open(OUT / f"{dataset}.tsv", "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(cols)
        for k, s in samples.items():
            ch = s.get("chinook", [])
            w.writerow(["_".join(str(x) for x in k if x not in ("", None)), "; ".join(dict.fromkeys(s["names"])),
                        "; ".join(s["units"]), "; ".join(s["runs"]), s["bases"] or "", s["sra_mb"] or "",
                        "; ".join(s["jgi"]), "; ".join(dict.fromkeys(s["sk_raw"])), "; ".join(dict.fromkeys(s["sk_qc"])),
                        "; ".join(p for p, _ in ch), sum(b for _, b in ch) or "", " ".join(dict.fromkeys(s["notes"]))])
    return samples


METAGEM_ARCHIVE = "chinook:/Workspace_backups/Tony_Liu/fir_metagem/metagem/"


def role(rel):
    name = rel.rsplit("/", 1)[-1]
    parts = rel.split("/")
    if "Raw_Data" in parts:
        return "raw"
    if "QC_Filtered_Raw_Data" in parts or re.search(r"anqdp|filter-|qtrimmed|artifactRm|\.filtered\.", name):
        return "qc_filtered"
    if name.endswith(".bam"):
        return "alignment"
    if re.search(r"unmapped|ReadMapping|ReadsMapping|input\.corr|\.trim\.", name):
        return "derived"
    if re.search(r"_pe\.[12]\.f|unpaired|/trim/", rel):
        return "qc_lab"
    return "reads"


def build_files(manifest):
    """One row per file for the sharknado datasets outside TREES and for the metaGEM studies."""
    prefixes = []
    for r in read_tsv(manifest):
        if r["status"] == "available" or r["dataset_id"] in TREES:
            continue
        for loc in r["locations"].split("; "):
            if loc.startswith(SHARKNADO_BACKUP):
                prefixes.append((loc.removeprefix(SHARKNADO_BACKUP).rstrip("/") + "/", r["dataset_id"]))
    prefixes.sort(key=lambda x: -len(x[0]))
    rows = collections.defaultdict(list)
    with gzip.open(SRC / "sharknado_read_paths.txt.gz", "rt") as f:
        for line in f:
            rel = line.rstrip("\n").removeprefix("sharknado/LimsData/")
            for pre, ds in prefixes:
                if rel.startswith(pre):
                    rows[ds].append((SHARKNADO_BACKUP + rel, "", "", role(rel[len(pre):])))
                    break
    for r in read_tsv(ROOT / "research/metagem/manifest.tsv"):
        rows[r["dataset"]].append((METAGEM_ARCHIVE + r["dataset"] + "/" + r["relpath"], r["url"], r["md5"], "raw"))
    for oid in (SRC / "engcyc_legacy_oids.txt").read_text().split():
        rows["engcyc_legacy"].append((f"sharknado:/ENGCYC/input/engcyc_{oid}.fasta.zip",
                                      f"https://img.jgi.doe.gov/cgi-bin/m/main.cgi?section=TaxonDetail&taxon_oid={oid}",
                                      "", "assembly"))
    for ds, files in rows.items():
        with open(OUT / f"{ds}.tsv", "w", newline="") as f:
            w = csv.writer(f, delimiter="\t", lineterminator="\n")
            w.writerow(["location", "public_url", "md5", "role"])
            w.writerows(sorted(files))
        print(ds, dict(collections.Counter(x[3] for x in files)))


if __name__ == "__main__":
    build_files(sys.argv[1] if len(sys.argv) > 1 else ROOT / "data/engcyc/manifest.tsv")
    for ds in TREES:
        samples = build(ds)
        n = collections.Counter()
        for s in samples.values():
            n["samples"] += 1
            n["sra"] += bool(s["runs"])
            n["jgi_only"] += bool(s["jgi"]) and not s["runs"]
            n["sharknado_raw"] += bool(s["sk_raw"])
            n["sharknado_qc_only"] += bool(s["sk_qc"]) and not s["sk_raw"]
            n["chinook_qc"] += bool(s.get("chinook"))
            n["no_public"] += not s["runs"] and not s["jgi"]
        print(ds, dict(n))
