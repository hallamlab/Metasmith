"""Count the samples, files, reads and bases of every Engcyc dataset, and its metagenome equivalents.

    python build_inventory.py [<xlsx_out_dir>]

Writes data/engcyc/inventory/datasets.tsv (one row per dataset, or per study where a manifest row
pools several) and the per-sample presence tables for Saanich and Line P. With an output
directory it also writes inventory.xlsx, whose metagenome equivalents are formulas over the two
ratios on its params sheet.
"""
import collections
import csv
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_reads as reads  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "data/engcyc"
SRC = DATA / "sources"
OUT = DATA / "inventory"

FOSMIDS_PER_GENOME = 100
GENOMES_PER_METAGENOME = 100

STUDY_NAMES = {
    "sunagawa2015": "Tara Oceans prokaryote fractions (Sunagawa 2015)",
    "karlsson2013": "Swedish women gut, glucose control (Karlsson 2013)",
    "korem2015": "In-vitro mixed-culture time course MIX7 (Korem 2015)",
    "li2019": "Peanut rhizosphere (Li 2019)",
    "bissett_base": "Australian BASE soils (Bissett 2016)",
}
COLUMNS = ["dataset_id", "study", "collection", "modal", "origin", "status", "accession",
           "expected_samples", "expected_basis", "samples_found", "runs_or_libraries", "read_files",
           "reads", "bases", "bytes", "count_source", "metagenomes", "genomes", "fosmid_clones",
           "fosmid_sequencing", "where", "notes"]


def tsv(path):
    return reads.read_tsv(path)


def num(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


# --- Saanich and Line P: the union of every source that names a sample ----------------------

def saanich_union():
    def key(*a):
        k = reads.saanich_key(*a)
        if not k or k[2] is None:
            return None
        return (f"SI{k[0]:03d}", f"S{k[1]}" if k[0] == 37 and k[1] else "", k[2])

    src = collections.defaultdict(set)
    for r in tsv(SRC / "reads/sra_saanich.tsv"):
        if r["sample_title"].startswith("Na_"):
            continue
        t = r["lab_sample_id"] or r["sample_title"]
        args = ("", "") if re.match(r"SI0\d\d_", t) else (r["cruise"], r["depth_m"])
        if k := key(t, *args):
            src[k].add("sra")
    for r in tsv(SRC / "reads/jgi_portal_saanich.tsv"):
        t = r["ap_name"]
        if r["kind"] != "metagenome" or re.search(r"MetaT|RNA|ESP|combined|Na_|MT$", t):
            continue
        t = t.split(" - ")[-1].replace("Saanich Inlet ", "").strip()
        if m := re.search(r"SI0?(\d{2})(\d{2})(\d{2})_(\d+)", t):
            k = ("SI020", "", int(m.group(4))) if m.group(1) == "04" else None
        elif m := re.match(r"^(\d{2,3}) \d\d/\d\d/\d\d (\d+)m", t):
            k = key(f"SI{int(m.group(1)):03d}_{m.group(2)}m")
        else:
            k = key(t.replace(" ", "_").replace("_metaG", ""))
        if k:
            src[k].add("jgi_portal")
    for r in tsv(DATA / "metadata/saanich_metagenome.tsv"):
        if k := key(r["sample_ID"]):
            src[k].add("lab_map")
    for name in chinook_names("/Saanich"):
        if k := key(name):
            src[k].add("chinook_qc")
    for r in tsv(DATA / "reads/saanich_metagenome.tsv"):
        if (r["sharknado_raw"] or r["sharknado_qc"]) and r["sample_key"][0].isdigit():
            p = r["sample_key"].split("_")
            c = int(p[0])
            src[(f"SI{c:03d}", f"S{p[1]}" if c == 37 and len(p) == 3 else "", int(p[-1]))].add("sharknado")
    return src


def linep_union():
    duplicates = {("J", "11", None, 1000): ("J", "11", 26, 1000), ("J", "18", 4, 1300): ("J", "18", 4, 1400)}

    def key(t):
        t = re.sub(r"Marine microbial communities from expanding oxygen minimum zones in Line P, North Pacific Ocean - "
                   r"|Line P |JGI_LineP_", "", t).replace("sample F_", "sample_F_").replace(" ", "_")
        k = reads.linep_key(t)
        k = k[:4] if k else None
        return duplicates.get(k, k)

    src = collections.defaultdict(set)

    def add(name, tag):
        if not re.search(r"SI03|metaT|MetaT|viral|RNA|Saanich|combined", name) and (k := key(name)):
            src[k].add(tag)

    for r in tsv(SRC / "reads/sra_linep.tsv"):
        add(r["sample_title"], "sra")
    for r in tsv(SRC / "reads/jgi_portal_linep.tsv"):
        if r["kind"] == "metagenome":
            add(r["ap_name"], "jgi_portal")
    for r in tsv(SRC / "reads/jgi_portal_libs.tsv"):
        add(r["title"], "jgi_portal")
    for r in tsv(DATA / "metadata/linep_metagenome.tsv"):
        add(r["assy.SIG.3.Sample_Labels"], "lab_table3")
    for name in chinook_names("/LineP"):
        add(name, "chinook_qc")
    for r in tsv(DATA / "reads/linep_metagenome.tsv"):
        if (r["sharknado_raw"] or r["sharknado_qc"]) and r["sample_key"][0] in "AFJ":
            p = r["sample_key"].split("_")
            src[(p[0], p[1], int(p[2]), int(p[3]))].add("sharknado")
    return {(f"{s}{yy}", f"P{st}" if st else "", d): v for (s, yy, st, d), v in src.items()}


def chinook_names(prefix):
    return {r["path"].rsplit("/", 1)[1].split("_pe.")[0] for r in tsv(SRC / "reads/chinook_hallam_lab_data.tsv")
            if r["path"].startswith(prefix)}


def write_union(name, union, cols):
    tags = ["sra", "jgi_portal", "sharknado", "chinook_qc", "lab_map", "lab_table3"]
    with open(OUT / f"{name}_samples.tsv", "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(cols + tags)
        for k in sorted(union, key=lambda k: (k[0], k[1], k[2])):
            w.writerow(list(k) + ["1" if t in union[k] else "" for t in tags])


# --- one row per dataset ---------------------------------------------------------------------

def public_rows():
    out = collections.defaultdict(list)
    for r in tsv(SRC / "inventory/public_summary.tsv"):
        out[r["dataset_id"]].append(r)
    return out


def chinook_rows():
    path = SRC / "inventory/chinook_summary.tsv"
    return {r["dataset_id"]: r for r in tsv(path)} if path.exists() else {}


def fosmid_clones(dataset):
    path = DATA / "metadata" / f"{dataset}.tsv"
    if not path.exists() or not dataset.startswith(("fosdb_", "scadc")):
        return None, ""
    full = ends = 0
    for r in tsv(path):
        n = num(r.get("no_clones")) or 0
        if (r.get("found_full_sequence") or r.get("Full seq to JGI") or "").lower().startswith("yes"):
            full += n
        else:
            ends += n
    kinds = [f"{full:,} clones with a full-insert sequence in FosDB" if full else "", f"{ends:,} clones with no full-insert sequence in FosDB" if ends else ""]
    return full + ends, "; ".join(k for k in kinds if k)


def cyanoverse_split():
    by_source = collections.defaultdict(set)
    runs = collections.Counter()
    for r in tsv(SRC / "inventory/public_runs/cyanoverse.tsv"):
        src = r["library_source"]
        kind = "metagenome" if src == "METAGENOMIC" else "single_cell" if "SINGLE CELL" in src else "genome"
        by_source[kind].add(r["sample_accession"])
        runs[kind] += 1
    return {k: len(v) for k, v in by_source.items()}, runs


def build():
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = tsv(DATA / "manifest.tsv")
    public, chinook = public_rows(), chinook_rows()
    su, lu = saanich_union(), linep_union()
    write_union("saanich", su, ["cruise", "station", "depth_m"])
    write_union("linep", lu, ["cruise", "station", "depth_m"])
    rows = []
    for m in manifest:
        did = m["dataset_id"]
        row = {c: "" for c in COLUMNS}
        row.update(dataset_id=did, study=STUDY_NAMES.get(did, m["name"]), collection=m["collection"],
                   modal=m["modal"], origin=m["origin"], status=m["status"], accession=m["accession"],
                   expected_samples=m["n_samples"], expected_basis="manifest n_samples",
                   samples_found=m["n_samples"], bytes=m["size_bytes"],
                   count_source=f"manifest ({m['size_source'] or 'no size'})",
                   where="; ".join(sorted({loc.split(":", 1)[0] for loc in m["locations"].split("; ") if loc})))
        notes = []
        if p := public.get(did):
            tot = next((r for r in p if r["sub_dataset"].lower().startswith(("total", "all"))), None) or p[0]
            if did == "pratama2026" or did == "cyanoverse":
                tot = {k: sum(num(r[k]) or 0 for r in p if not r["sub_dataset"].lower().startswith(("total", "all")))
                       for k in ("runs", "samples", "read_count", "base_count", "fastq_bytes", "fastq_files")}
            row.update(runs_or_libraries=tot["runs"], samples_found=tot["samples"], reads=tot["read_count"],
                       bases=tot["base_count"], bytes=tot["fastq_bytes"], read_files=tot.get("fastq_files", ""),
                       count_source="ENA filereport (exact)")
            if num(tot.get("project_total_runs")) and num(tot["project_total_runs"]) != num(tot["runs"]):
                notes.append(f"The public project holds {tot['project_total_runs']} runs and "
                             f"{num(tot['project_total_bases']) / 1e9:,.0f} Gbp. This dataset uses {tot['runs']}.")
            if tot.get("notes") and did != "cyanoverse":
                notes.append(tot["notes"])
        if c := chinook.get(did):
            if not public.get(did):
                row.update(read_files=c["n_read_files"], bytes=c["read_bytes"], samples_found=c["n_samples_inferred"],
                           reads=c["total_reads"], bases=c["total_bases"],
                           count_source=f"chinook Globus listing; {c['count_source']}".strip("; "))
            if c.get("notes"):
                notes.append(c["notes"])
        if not public.get(did) and not chinook.get(did) and (DATA / "reads" / f"{did}.tsv").exists() \
                and did not in ("saanich_metagenome", "linep_metagenome"):
            roles = collections.Counter(r["role"] for r in tsv(DATA / "reads" / f"{did}.tsv"))
            row.update(read_files=sum(roles.values()),
                       count_source="sharknado index, Jan 2025 (names only); bytes from manifest")
            notes.append("Read-like files by role: " + ", ".join(f"{k} {v}" for k, v in roles.most_common()) + ".")
        if did in ("saanich_metagenome", "linep_metagenome"):
            union = su if did.startswith("saanich") else lu
            sra = [r for r in tsv(SRC / f"reads/sra_{'saanich' if did.startswith('saanich') else 'linep'}.tsv")
                   if not r["sample_title"].startswith("Na_")]
            row.update(expected_samples=len(union), samples_found=sum(1 for v in union.values() if v - {"jgi_portal", "lab_map", "lab_table3"} or "sra" in v),
                       expected_basis="union of SRA, JGI portal, lab sheets, chinook QC and sharknado (see per-sample sheet)",
                       runs_or_libraries=len(sra), reads=sum(num(r["spots"]) or 0 for r in sra),
                       bases=sum(num(r["bases"]) or 0 for r in sra),
                       count_source="sample count exact; reads and bases are the SRA runs only")
            only_jgi = sum(1 for v in union.values() if v == {"jgi_portal"})
            no_sra = sum(1 for v in union.values() if "sra" not in v)
            notes.append(f"{no_sra} samples are not in SRA. {only_jgi} of them exist only at JGI (tape, login needed).")
        measured = row["samples_found"]
        if measured != m["n_samples"] and num(measured) is not None and did not in ("saanich_metagenome", "linep_metagenome"):
            row["expected_samples"], row["expected_basis"] = measured, row["count_source"]
            notes.append(f"The manifest says {m['n_samples'] or 'nothing'}.")
        if m["status"] in ("missing", "candidate"):
            row["samples_found"] = 0
        if did == "engcyc_legacy":
            notes.append("These are JGI assemblies, not reads: 904 IMG assemblies (788 with 330 OIDs), 25 Knorr cruise "
                         "assemblies and Saanich bundles. The assemblies are not found. Their ORFs are on chinook (Hallam_Lab_Data "
                         "GSB_CroweHallam/ORFS_ALL, 1,198 engcyc_<OID>.qced.faa covering all 904 OIDs). JGI holds the "
                         "reads behind most IMG OIDs.")
        if did == "gem_catalog":
            notes.append("A candidate. The public product is 52,515 MAGs. The 10,450 source metagenomes are IMG IDs, and "
                         "their reads are not all public. Many may overlap engcyc_legacy.")
        if did == "cyanoverse":
            samples, runs = cyanoverse_split()
            row.update(metagenomes=samples.get("metagenome", 0),
                       genomes=samples.get("genome", 0) + samples.get("single_cell", 0))
            notes.append(f"By library source: {samples.get('genome', 0)} isolate genomes, {samples.get('single_cell', 0)} "
                         f"single cells and {samples.get('metagenome', 0)} metagenomes (biosamples).")
        elif m["modal"] == "metagenome":
            row["metagenomes"] = row["expected_samples"]
        elif m["modal"] == "genome":
            row["genomes"] = row["expected_samples"]
        elif m["modal"] == "fosmid":
            clones, kind = fosmid_clones(did)
            if m2 := re.search(r"FosDB lists ([\d,]+) clones", m["notes"]):
                clones = int(m2.group(1).replace(",", ""))
            row.update(fosmid_clones=clones or "", fosmid_sequencing=kind)
            if not clones:
                notes.append("Clone count unknown.")
        rows.append(row | {"notes": " ".join(notes)})
    with open(OUT / "datasets.tsv", "w", newline="") as f:
        w = csv.DictWriter(f, COLUMNS, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    return rows


# --- xlsx ------------------------------------------------------------------------------------

def export(out_dir):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    font, head = Font(name="Arial", size=10), Font(name="Arial", size=10, bold=True, color="FFFFFF")
    fill = PatternFill("solid", fgColor="1F4E5A")
    wb = Workbook()
    params = wb.active
    params.title = "params"
    params.append(["parameter", "value", "basis"])
    params.append(["fosmids_per_genome", FOSMIDS_PER_GENOME,
                   "A fosmid insert is 35-40 kb and a typical prokaryote genome is about 4 Mbp, so about 100 inserts."])
    params.append(["genomes_per_metagenome", GENOMES_PER_METAGENOME,
                   "The lab's Saanich and Line P assemblies are 434 and 547 Mbp median per sample, which is 110-135 "
                   "genomes of 4 Mbp."])

    def sheet(name, path, wrap=()):
        ws = wb.create_sheet(name)
        rows = list(csv.reader(open(path), delimiter="\t"))
        for i, r in enumerate(rows):
            ws.append([int(v) if i and re.fullmatch(r"-?\d{1,15}", v) else v for v in r])
        for c in ws[1]:
            c.font, c.fill = head, fill
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        for j, col in enumerate(rows[0], start=1):
            letter = get_column_letter(j)
            ws.column_dimensions[letter].width = 60 if col in wrap else min(45, max(10, len(col) + 2))
            for c in ws[letter][1:]:
                c.font = font
                if col in wrap:
                    c.alignment = Alignment(wrap_text=True, vertical="top")
                elif isinstance(c.value, int) and abs(c.value) >= 10000:
                    c.number_format = "#,##0"
        return ws

    ws = sheet("datasets", OUT / "datasets.tsv", wrap={"notes", "expected_basis", "count_source", "study"})
    cols = [c.value for c in ws[1]]
    me = len(cols) + 1
    ws.cell(row=1, column=me, value="metagenome_equivalents").font = head
    ws.cell(row=1, column=me).fill = fill
    L = {c: get_column_letter(cols.index(c) + 1) for c in ("metagenomes", "genomes", "fosmid_clones")}
    for r in range(2, ws.max_row + 1):
        c = ws.cell(row=r, column=me, value=(f"=N({L['metagenomes']}{r})+N({L['genomes']}{r})/params!$B$3"
                                              f"+N({L['fosmid_clones']}{r})/(params!$B$2*params!$B$3)"))
        c.number_format, c.font = "#,##0.0", font
    total = ws.max_row + 1
    ws.cell(row=total, column=1, value="TOTAL").font = Font(name="Arial", size=10, bold=True)
    for col in ("samples_found", "reads", "bases", "bytes", "metagenomes", "genomes", "fosmid_clones"):
        j = cols.index(col) + 1
        L2 = get_column_letter(j)
        ws.cell(row=total, column=j, value=f"=SUM({L2}2:{L2}{total - 1})").number_format = "#,##0"
    Lm = get_column_letter(me)
    ws.cell(row=total, column=me, value=f"=SUM({Lm}2:{Lm}{total - 1})").number_format = "#,##0.0"
    sheet("saanich_samples", OUT / "saanich_samples.tsv")
    sheet("linep_samples", OUT / "linep_samples.tsv")
    sheet("public_by_study", SRC / "inventory/public_summary.tsv", wrap={"title", "notes"})
    if (SRC / "inventory/chinook_summary.tsv").exists():
        sheet("chinook_listing", SRC / "inventory/chinook_summary.tsv", wrap={"notes", "location(s)"})
    commit = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                            capture_output=True, text=True).stdout.strip()
    about = wb.create_sheet("about")
    for line in [
        "Engcyc dataset inventory",
        f"Built from branch engcyc/dev, commit {commit}, by research/engcyc/scripts/build_inventory.py.",
        "datasets: one row per dataset. count_source says how each count was made: exact public metadata, a live listing, or the manifest.",
        "metagenome_equivalents counts what each dataset would contribute, from expected_samples, found or not. metagenome_equivalents = metagenomes + genomes / genomes_per_metagenome + fosmid_clones / (fosmids_per_genome x genomes_per_metagenome). Change the ratios on the params sheet.",
        "fosmid_clones counts every clone in the library. fosmid_sequencing says how many have full inserts sequenced. End sequences carry far less than a full insert.",
        "Amplicon datasets get no metagenome equivalents.",
        "saanich_samples and linep_samples: one row per sample, with a 1 for each place that holds or names it.",
        "The source of truth is the DVC-pinned TSVs under data/engcyc/. Edits made here do not flow back.",
    ]:
        about.append([line])
    about.column_dimensions["A"].width = 140
    wb.move_sheet("params", offset=1)
    out_dir.mkdir(parents=True, exist_ok=True)
    wb.save(out_dir / "inventory.xlsx")


if __name__ == "__main__":
    rows = build()
    print(f"{len(rows)} datasets")
    if len(sys.argv) > 1:
        export(Path(sys.argv[1]))
