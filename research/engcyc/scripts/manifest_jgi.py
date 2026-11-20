"""Write manifest.csv for a raw-data entry that holds JGI portal downloads.

Each library folder below the entry carries JGI's <label>_FD_files.xml, which gives every file's
md5, size and library code, and Metagenome_Report_Tables/, which gives the sample metadata.
"""
import argparse
import csv
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

COLUMNS = ("dataset_id,sample_id,source_name,replicate,file,archive_member,mate,lane,platform,format,"
           "content,role,bytes,md5,centre,run_id,library_id,seq_unit,accession,sample_type,site,"
           "depth_m,sampling_date,msm_type,notes").split(",")
SEQ_UNIT = re.compile(r"^(\d+)\.(\d+)\.(\d+)\.([ACGTN]+(?:-[ACGTN]+)?)")
COMPOUND = ("fastq.gz", "fq.gz", "sam.gz", "sff.gz", "tar.gz", "fasta.gz", "fna.gz")
CONTENT = {"fastq.gz": "fastq", "fq.gz": "fastq", "fastq": "fastq", "fasta": "fasta", "fna": "fasta",
           "faa": "fasta", "fa": "fasta", "sam.gz": "sam", "bam": "bam_aligned", "sff": "sff", "sff.gz": "sff"}


def file_format(name):
    for ext in COMPOUND:
        if name.endswith("." + ext):
            return ext
    return name.rsplit(".", 1)[-1] if "." in name else ""


def report_table(lib_dir, prefix):
    values = {}
    for path in lib_dir.glob(f"Metagenome_Report_Tables/{prefix}_*"):
        for line in path.read_text(errors="replace").splitlines():
            key, _, value = line.partition("  ")
            if value.strip() and not line.startswith("assy."):
                values[key.strip()] = value.strip()
    return values


def fd_index(lib_dir):
    index = {}
    for xml in lib_dir.glob("*_FD_files.xml"):
        for folder in ET.parse(xml).getroot().iter("folder"):
            for f in folder.findall("file"):
                index.setdefault(f.get("filename"), []).append(f.attrib)
    return index


def role_of(name, attrs, rel):
    kind = (attrs or {}).get("fileType", "")
    if kind == "Sequence" and name.endswith((".fastq.gz", ".fq.gz")):
        if "filter-" in name:
            return "qc_filtered", ""
        if name.startswith("input.corr"):
            return "derived", "JGI error-corrected reads, the assembly input"
        return "raw", ""
    if kind == "Alignment":
        return "alignment", "reads mapped back to the JGI assembly"
    if name.endswith(".chaff.tar"):
        return "derived", "reads removed by JGI QC"
    if name.endswith(("contigs.fasta", "contigs.fna")):
        return "assembly", "JGI assembly"
    if name == "covstats.txt":
        return "derived", "JGI assembly coverage"
    return "report", ""


def rows_for_library(entry, lib_dir, dataset_id, centre):
    index = fd_index(lib_dir)
    study = report_table(lib_dir, "Table_1")
    sample = report_table(lib_dir, "Table_2")
    accession = "; ".join(f"{k}:{v}" for k, v in (
        ("IMG", study.get("IMG ID (IMG TAXON OID)")), ("GOLD_biosample", sample.get("GOLD ID (BIOSAMPLE)")),
        ("GOLD_project", study.get("GOLD ID (SEQUENCING PROJECT)")), ("SRA", study.get("SRA ID"))) if v)
    latlon = sample.get("LATITUDE AND LONGITUDE", "")
    site = sample.get("GEOGRAPHIC LOCATION", "") + (f" ({latlon})" if latlon else "")
    seq_unit = next((m.group(0) for n in index for m in [SEQ_UNIT.match(n)] if m), "")
    run_id, lane = (seq_unit.split(".")[0], seq_unit.split(".")[1]) if seq_unit else ("", "")
    library = next((a["library"] for v in index.values() for a in v if a.get("library")), "")
    source = lib_dir.name
    sample_id = re.sub(r"[^A-Za-z0-9_]", "_", source)
    for path in sorted(p for p in lib_dir.rglob("*") if p.is_file()):
        rel = path.relative_to(entry).as_posix()
        size = path.stat().st_size
        candidates = index.get(path.name, [])
        attrs = next((a for a in candidates if a.get("sizeInBytes") == str(size)), None)
        notes = []
        if candidates and attrs is None:
            with open(path, "rb") as fh:
                is_html = fh.read(64).lstrip().startswith(b"<")
            notes.append("a JGI portal error page saved in place of this file" if is_html
                         else f"size differs from JGI index ({candidates[0].get('sizeInBytes')})")
            print(f"SIZE MISMATCH {rel}: {size} vs {candidates[0].get('sizeInBytes')}", file=sys.stderr)
        role, note = role_of(path.name, attrs or (candidates[0] if candidates else None), rel)
        if note:
            notes.append(note)
        fmt = file_format(path.name)
        content = CONTENT.get(fmt, "fastq" if path.name.endswith(".chaff.tar") else "")
        is_reads = role in ("raw", "qc_filtered", "derived") and content == "fastq" and fmt != "tar"
        if attrs and attrs.get("md5"):
            notes.append("md5 from the JGI download index")
        yield {
            "dataset_id": dataset_id, "sample_id": sample_id, "source_name": source, "replicate": "",
            "file": rel, "archive_member": "", "mate": "interleaved" if is_reads else "",
            "lane": lane if is_reads or role == "alignment" else "", "platform": "illumina",
            "format": fmt, "content": content, "role": role, "bytes": size,
            "md5": (attrs or {}).get("md5", ""), "centre": centre, "run_id": run_id, "library_id": library,
            "seq_unit": seq_unit, "accession": accession, "sample_type": "metagenome", "site": site,
            "depth_m": sample.get("VERTICAL DISTANCE DEPTH", ""), "sampling_date": sample.get("SAMPLE COLLECTION DATE", ""),
            "msm_type": "short_reads_pe" if is_reads else "", "notes": "; ".join(notes),
        }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("entry", type=Path)
    ap.add_argument("dataset_id")
    ap.add_argument("--centre", default="JGI")
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    libs = sorted(p.parent for p in args.entry.glob("*/*_FD_files.xml"))
    out = open(args.out or args.entry / "manifest.csv", "w", newline="")
    w = csv.DictWriter(out, COLUMNS)
    w.writeheader()
    n = 0
    for lib in libs:
        for row in rows_for_library(args.entry, lib, args.dataset_id, args.centre):
            w.writerow(row)
            n += 1
    listed = {p for lib in libs for p in lib.rglob("*") if p.is_file()}
    stray = [p for p in args.entry.rglob("*") if p.is_file() and p not in listed and p.name != "manifest.csv"]
    for p in stray:
        print(f"NOT IN A LIBRARY FOLDER {p.relative_to(args.entry)}", file=sys.stderr)
    print(f"{len(libs)} libraries, {n} rows, {len(stray)} files outside a library folder", file=sys.stderr)


if __name__ == "__main__":
    main()
