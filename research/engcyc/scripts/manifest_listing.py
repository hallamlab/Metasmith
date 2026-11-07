"""Write manifest.csv for a raw-data entry from a file listing, without the bytes on hand.

The listing is a TSV of bytes, md5 and file (relative to the entry), as the Globus successful-transfers
record of the upload gives it. The metadata directory holds the entry's small text files (JGI xml,
report tables, READMEs, sample sheets) at their paths in the entry.

Profiles:
  jgi        JGI portal downloads, one folder per library (metagenomes, metatranscriptomes)
  jgi_itags  JGI iTag runs, files named by sequencing unit, mapped to samples by <run>.txt sheets
  gq_sag     Genome Quebec single cells, HI.<run>.<lane>.<sample>_R<mate>.fastq.gz
  ont_tar    Nanopore runs, one uncompressed tar of fast5 files per run
  gq_454     Genome Quebec 454 deliveries: <region>.<run>.<MID|RL>.<label>.<key>.454Reads.{fna,qual} and sff
  jgi_pyrotags  One multiplexed JGI 454 pyrotag run, samples named in its .primers sheet
  sra_sag    Single cells fetched from SRA as <run>_<mate>.fastq.gz, named from an ENA run table (--runinfo)
"""
import argparse
import csv
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from manifest_jgi import COLUMNS, CONTENT, file_format

SAK_LABEL = re.compile(r"sak_(\d{4})_(\d{1,2})_(\d{1,2})_(\d+)m?")
SEQ_UNIT = re.compile(r"(?<![\d.])(\d{4,5})\.(\d)\.(\d{3,6})(?:\.([ACGTN]+(?:-[ACGTN]+)?))?(?=\.)")
GQ_SAG = re.compile(r"^(HI\.(\d+)\.(\d+))\.(.+)_R([12])\.fastq\.gz$")
SAKINAW = "Sakinaw Lake, British Columbia, Canada"
MD5_NOTE = "md5 verified end to end by the Globus upload"


def sanitize(name):
    return re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9_]", "_", name)).strip("_")


def read_listing(path):
    with open(path) as fh:
        return [(int(r["bytes"]), r["md5"], r["file"]) for r in csv.DictReader(fh, delimiter="\t")]


def sak_sample(label):
    m = SAK_LABEL.search(label)
    if not m:
        return "", ""
    y, mo, d, depth = m.groups()
    return f"{y}-{int(mo):02d}-{int(d):02d}", depth


def sak_sample_id(label, fallback):
    date, depth = sak_sample(label)
    return f"sak_{date.replace('-', '_')}_{depth}m" if date else sanitize(fallback)


def base_row(dataset_id, centre, **kw):
    row = dict.fromkeys(COLUMNS, "")
    row.update(dataset_id=dataset_id, centre=centre, site=SAKINAW)
    row.update(kw)
    return row


def file_fields(size, md5, rel):
    fmt = file_format(rel.rsplit("/", 1)[-1])
    return {"file": rel, "bytes": size, "md5": md5, "format": fmt, "content": CONTENT.get(fmt, "")}


def grep_value(path, pattern):
    if path and path.is_file():
        m = re.search(pattern, path.read_text(errors="replace"), re.M)
        if m:
            return m.group(1).strip()
    return ""


def jgi_xml(meta_lib):
    index = {}
    for xml in meta_lib.glob("*_FD_files.xml"):
        for f in ET.parse(xml).getroot().iter("file"):
            index.setdefault(f.get("filename"), []).append(f.attrib)
    return index


def jgi_role(rel, platform):
    name = rel.rsplit("/", 1)[-1]
    if name.endswith((".fastq.gz", ".fq.gz")):
        if ".filter-" in name or ".anqdp." in name:
            return "qc_filtered", ""
        return "raw", ""
    if name.endswith(".chaff.tar"):
        return "derived", "reads removed by JGI QC"
    if platform == "454" and name.endswith("454TrimmedReads.fna"):
        return "raw", "454 reads as fasta, quality scores in the .qual beside it"
    if name.endswith(".sorted.bam"):
        return "alignment", "reads mapped back to the JGI assembly"
    if name.endswith(("contigs.fasta", "contigs.fna", "contigs.fa", ".gt800.fa", "Contigs.lucy.fa", "AllContigs.lucy.fa")):
        return "assembly", "JGI assembly"
    if "/IMG_Data/" in f"/{rel}":
        return "derived", "IMG annotation"
    if name.endswith((".pdf", ".txt", ".sh", ".xml", ".log", ".yaml", ".ini", ".xlsx")):
        return "report", ""
    return "derived", ""


def jgi_rows(listing, meta, dataset_id, centre, sample_type):
    libs = {}
    for size, md5, rel in listing:
        if rel == "manifest.csv":
            continue
        libs.setdefault(rel.split("/", 1)[0] if "/" in rel else "", []).append((size, md5, rel))
    for lib, files in sorted(libs.items()):
        mlib = meta / lib
        xml = jgi_xml(mlib)
        labels = [a["label"] for v in xml.values() for a in v if a.get("label")]
        raw_label = next((l for l in labels if not re.search(r"Annotation|Expression", l)), labels[0] if labels else "")
        table3 = next(iter(mlib.glob("Metagenome_Report_Tables/Table_3_*")), None)
        label = raw_label or grep_value(table3, r"Sample_Labels=(.*)") or re.sub(r"_meta[GT]$", "", lib)
        label = re.sub(r"\s+meta[GT]$", "", label)
        date, depth = sak_sample(label) if SAK_LABEL.search(label) else sak_sample(lib)
        depth = depth or (re.search(r"_(\d+)m(?:_|$)", lib) or [None, ""])[1]
        readme = next(iter(mlib.glob("QC_and_Genome_Assembly/README.txt")), None)
        report = next(iter(mlib.glob("QC_Filtered_Raw_Data/*.filtered-report.txt")), None)
        library = (grep_value(report, r"^Library:\s*(\S+)") or grep_value(readme, r"Library Name\(s\):\s*(\S+)")
                   or next((a["library"] for v in xml.values() for a in v if a.get("library")), ""))
        project = grep_value(readme, r"Sequencing Project ID\(s\):\s*(\S+)") or next(
            (a.get("project") for v in xml.values() for a in v if a.get("label") == raw_label and a.get("project")), "")
        names = [r.rsplit("/", 1)[-1] for _, _, r in files]
        img = next((m.group(1) for n in names for m in [re.match(r"(?:Table_7_-_)?(33\d{8})\.", n)] if m), "")
        unit = next((m for n in names for m in [SEQ_UNIT.search(n)] if m), None)
        seq_unit = unit.group(0) if unit else ""
        accession = "; ".join(f"{k}:{v}" for k, v in (("IMG", img), ("JGI_sequencing_project", project)) if v)
        platform = "454" if "454" in lib else "illumina"
        sample_id = sak_sample_id(label, lib)
        for size, md5, rel in files:
            role, note = jgi_role(rel, platform)
            fields = file_fields(size, md5, rel)
            name = rel.rsplit("/", 1)[-1]
            if name.endswith(".chaff.tar"):
                fields["content"] = "fastq"
            is_reads = role in ("raw", "qc_filtered") and fields["content"] == "fastq"
            notes = [n for n in (note, MD5_NOTE if md5 else "") if n]
            for a in xml.get(name, []):
                if a.get("sizeInBytes") == str(size) and a.get("md5") and a["md5"] != md5:
                    notes.append(f"md5 differs from the JGI download index ({a['md5']})")
            if is_reads and not (unit and unit.group(4)):
                notes.append("mate layout assumed interleaved, as JGI delivers")
            yield base_row(
                dataset_id, centre, sample_id=sample_id, source_name=label, platform=platform,
                mate="interleaved" if is_reads and platform == "illumina" else ("single" if role == "raw" else ""),
                lane=unit.group(2) if unit and (is_reads or role == "alignment") else "",
                role=role, run_id=unit.group(1) if unit else "", library_id=library, seq_unit=seq_unit,
                accession=accession, sample_type=sample_type, depth_m=depth, sampling_date=date,
                msm_type="short_reads_pe" if is_reads and platform == "illumina" else "",
                notes="; ".join(notes), **fields)


def itag_rows(listing, meta, dataset_id, centre):
    sheet = {}
    for path in sorted(meta.glob("*.txt")):
        run = path.stem
        for line in path.read_text().splitlines():
            if "\t" in line:
                name, src = line.split("\t", 1)
                sheet[src.rsplit("/", 1)[-1].removesuffix(".fastq.gz")] = (name.strip(), run)
    for size, md5, rel in listing:
        if rel == "manifest.csv":
            continue
        fields = file_fields(size, md5, rel)
        unit = SEQ_UNIT.search(rel.rsplit("/", 1)[-1])
        seq_unit = unit.group(0) if unit else ""
        notes = [MD5_NOTE] if md5 else []
        if seq_unit:
            source, run = sheet.get(seq_unit, ("", ""))
            if not source:
                print(f"NO SAMPLE FOR {rel}", file=sys.stderr)
            date, depth = sak_sample(source)
            role, note = jgi_role(rel, "illumina")
            is_reads = fields["content"] == "fastq"
            notes = [n for n in (note, f"JGI iTag run {run}", *notes) if n]
            yield base_row(
                dataset_id, centre, sample_id=sak_sample_id(source, source), source_name=source,
                mate="interleaved" if is_reads else "", lane=unit.group(2), platform="illumina", role=role,
                run_id=unit.group(1), seq_unit=seq_unit, sample_type="amplicon_16S", depth_m=depth,
                sampling_date=date, msm_type="short_reads_pe" if is_reads else "", notes="; ".join(notes), **fields,
                **({} if source.startswith("sak_") else {"site": ""}))
        else:
            run = rel.split("/", 1)[0].removesuffix(".txt") if rel.split("/", 1)[0] in ("SZHZ", "TWUN", "SZHZ.txt", "TWUN.txt") else ""
            what = "sample sheet: sample name to JGI sequence unit" if rel.endswith((".txt", ".xlsx")) and "/" not in rel else "JGI iTag analysis"
            yield base_row(
                dataset_id, centre, sample_id=f"run_{run}" if run else "all_runs", platform="illumina",
                role="report" if fields["format"] in ("txt", "xlsx", "yaml", "ini", "log", "pdf") else "derived",
                sample_type="amplicon_16S", run_id=run,
                notes="; ".join([f"{what}, covers every sample of {'run ' + run if run else 'both runs'}", *notes]), **fields)


def gq_sag_rows(listing, dataset_id, centre):
    for size, md5, rel in listing:
        if rel == "manifest.csv":
            continue
        m = GQ_SAG.match(rel.rsplit("/", 1)[-1])
        if not m:
            print(f"UNPARSED {rel}", file=sys.stderr)
            continue
        lane, run, _, source, mate = m.groups()
        yield base_row(
            dataset_id, centre, sample_id=sanitize(source), source_name=source, mate=f"R{mate}", lane=lane,
            platform="illumina", role="raw", run_id=f"HI.{run}", sample_type="SAG",
            msm_type="zipped_forward_short_reads" if mate == "1" else "zipped_reverse_short_reads",
            notes=MD5_NOTE if md5 else "", **file_fields(size, md5, rel))


def ont_tar_rows(listing, dataset_id, centre):
    for size, md5, rel in listing:
        if rel == "manifest.csv":
            continue
        run = rel.rsplit("/", 1)[-1].removesuffix(".tar")
        date, depth = (re.search(r"m(20\d\d)_", run) or [None, ""])[1], (re.search(r"_(\d+)m", run) or [None, ""])[1]
        yield base_row(
            dataset_id, centre, sample_id=sanitize(f"sak_{date}_{depth}m_ont" if date else f"sak_{depth}m_ont_{run.split('_')[0]}"),
            source_name=run, mate="single",
            platform="ont", role="raw", run_id=run, sample_type="metagenome", depth_m=depth, sampling_date=date,
            notes="; ".join(n for n in ("fast5 inside; no metasmith type for fast5, basecall first",
                                        "" if date else "sampling year not in the run name",
                                        MD5_NOTE if md5 else "") if n),
            **{**file_fields(size, md5, rel), "content": "fast5"})


READS_454 = re.compile(r"^(\d+)\.(\d+)\.((?:MID|RL)\d+)\.(.+?)\.(?:GAC|TCA|TCAG)\.454Reads\.(fna|qual)\.gz$")
SFF_454 = re.compile(r"^([A-Z0-9]{9})(?:\d{2})?\.((?:MID|RL)\d+)\.(.+?)\.sff\.gz$")
BARE_454 = re.compile(r"^([A-Z]{4})?((?:sak|pl)_\d{4}_\d{1,2}_\d{1,2}_\d+m?)(?:\.?(?:GAC|TCA))?(?:\.454Reads)?\.(?:fna|fa|qual)\.gz$", re.I)
LAKE_LABEL = re.compile(r"^(sak|pl)_(\d{4})_(\d{1,2})_(\d{1,2})_(\d+)m?_?$", re.I)
LAKES = {"sak": SAKINAW, "pl": "Powell Lake, British Columbia, Canada"}


def lake_sample(label):
    m = LAKE_LABEL.match(label)
    if not m:
        return sanitize(label), "", "", ""
    lake, y, mo, d, depth = m.groups()
    date = f"{y}-{int(mo):02d}-{int(d):02d}"
    return f"{lake.lower()}_{date.replace('-', '_')}_{depth}m", date, depth, LAKES[lake.lower()]


def format_454(name):
    for ext in ("454Reads.fna.gz", "454Reads.qual.gz", "fa.qual.gz", "qual.gz", "fa.gz", "fna.gz", "fasta.gz",
                "sff.gz", "xls.gz", "txt.gz"):
        if name.endswith("." + ext):
            fmt = ext.removeprefix("454Reads.").replace("fa.qual", "qual")
            return fmt, {"fna.gz": "fasta", "fa.gz": "fasta", "fasta.gz": "fasta", "qual.gz": "qual",
                         "sff.gz": "sff"}.get(fmt, "")
    return file_format(name), ""


def gq_454_rows(listing, dataset_id, centre, sample_type):
    by_name = {}
    for size, md5, rel in listing:
        by_name.setdefault(rel.rsplit("/", 1)[-1], []).append(rel)
    for size, md5, rel in listing:
        if rel == "manifest.csv":
            continue
        name = rel.rsplit("/", 1)[-1]
        fmt, content = format_454(name)
        fields = {"file": rel, "bytes": size, "md5": md5, "format": fmt, "content": content}
        notes = [MD5_NOTE] if md5 else []
        if len(by_name[name]) > 1:
            notes.append("same name also at " + ", ".join(r for r in by_name[name] if r != rel))
        m, s, b = READS_454.match(name), SFF_454.match(name), BARE_454.match(name)
        if m or s or b:
            if m:
                region, run, barcode, label = m.group(1), m.group(2), m.group(3), m.group(4)
            elif s:
                region, run, barcode, label = "", s.group(1), s.group(2), s.group(3)
            else:
                region, run, barcode, label = "", "", b.group(1) or "", b.group(2)
                if b.group(1):
                    notes.append(f"{b.group(1)} reads as the JGI library code")
            sample_id, date, depth, site = lake_sample(label)
            if not site:
                notes.append("not a Sakinaw or Powell sample by name")
            if content == "sff":
                notes.append("454 flowgrams; no metasmith type for sff")
            elif content == "fasta":
                notes.append("454 reads as fasta, quality scores in the .qual beside it")
            yield base_row(
                dataset_id, centre, sample_id=sample_id, source_name=label, mate="single", lane=region,
                platform="454", role="raw", run_id=run, library_id=barcode,
                sample_type=sample_type, site=site, depth_m=depth, sampling_date=date,
                notes="; ".join(notes), **fields)
        else:
            yield base_row(dataset_id, centre, sample_id="all_samples", platform="454", role="report",
                           sample_type=sample_type, notes="; ".join(["covers every sample of the entry", *notes]),
                           **fields)


PRIMER_LABEL = re.compile(r"^Sak(\d{2})(\d{2})(\d{2})-(\d+)m")


def jgi_pyrotag_rows(listing, meta, dataset_id, centre):
    samples = []
    for primers in meta.rglob("*.primers"):
        for line in primers.read_text().splitlines():
            m = PRIMER_LABEL.match(line)
            if m:
                d, mo, y, depth = m.groups()
                samples.append(f"sak_20{y}_{mo}_{d}_{depth}m")
    for size, md5, rel in listing:
        if rel == "manifest.csv":
            continue
        name = rel.rsplit("/", 1)[-1]
        fmt, content = format_454(name)
        unit = re.search(r"\.(\d{5})\.(\d)\.", name)
        is_reads = content in ("sff", "fasta", "qual") and name.startswith("GWNO.")
        notes = [MD5_NOTE] if md5 else []
        if is_reads:
            notes.insert(0, f"multiplexed by barcode, {len(samples)} samples in the .primers sheet: " + " ".join(samples))
            if content == "sff":
                notes.append("no metasmith type for sff")
        yield base_row(
            dataset_id, centre, sample_id=f"run_{unit.group(1)}" if unit else "run_12063", platform="454",
            mate="single" if is_reads else "", lane=unit.group(2) if unit else "",
            role="raw" if is_reads else ("report" if fmt in ("primers", "") or name == "README" else "derived"),
            run_id=unit.group(1) if unit else "12063", library_id="GWNO", sample_type="amplicon_16S",
            notes="; ".join(notes if is_reads else [("JGI pyrotag analysis, 97% OTUs" if fmt not in ("primers", "")
                                                    else "run README and barcode sheet"), *notes]),
            file=rel, bytes=size, md5=md5, format=fmt, content=content)


def sra_sag_rows(listing, runinfo, dataset_id, centre):
    with open(runinfo) as fh:
        runs = {r["run_accession"]: r for r in csv.DictReader(fh, delimiter="\t")}
    for size, md5, rel in listing:
        if rel == "manifest.csv":
            continue
        m = re.match(r"^(SRR\d+)_([12])\.fastq\.gz$", rel.rsplit("/", 1)[-1])
        r = runs.get(m.group(1), {}) if m else {}
        if not r:
            print(f"NO RUN FOR {rel}", file=sys.stderr)
        cell = re.search(r"SCGC \S+", r.get("scientific_name", ""))
        yield base_row(
            dataset_id, centre, sample_id=sanitize(cell.group(0)) if cell else sanitize(r.get("sample_accession", rel)),
            source_name=r.get("scientific_name", ""), mate=f"R{m.group(2)}" if m else "", platform="illumina",
            role="raw", run_id=m.group(1) if m else "", library_id=r.get("library_name", ""),
            accession="; ".join(f"{k}:{r[f]}" for k, f in (("SRA_run", "run_accession"), ("BioProject", "study_accession"),
                                                         ("BioSample", "sample_accession")) if r.get(f)),
            sample_type="SAG", sampling_date=r.get("collection_date", ""),
            msm_type="zipped_forward_short_reads" if m and m.group(2) == "1" else "zipped_reverse_short_reads",
            notes="; ".join(n for n in ("fetched from SRA and gzipped by the lab, so md5 differs from SRA's",
                                        r.get("instrument_model", ""), MD5_NOTE if md5 else "") if n),
            **file_fields(size, md5, rel))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("profile", choices=("jgi", "jgi_itags", "gq_sag", "ont_tar", "gq_454", "jgi_pyrotags", "sra_sag"))
    ap.add_argument("listing", type=Path)
    ap.add_argument("dataset_id")
    ap.add_argument("--meta", type=Path, help="the entry's small text files, at their entry paths")
    ap.add_argument("--centre", default="JGI")
    ap.add_argument("--sample-type", default="metagenome")
    ap.add_argument("--runinfo", type=Path, help="ENA read_run table, for sra_sag")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    listing = read_listing(args.listing)
    if args.profile == "jgi":
        rows = jgi_rows(listing, args.meta, args.dataset_id, args.centre, args.sample_type)
    elif args.profile == "jgi_itags":
        rows = itag_rows(listing, args.meta, args.dataset_id, args.centre)
    elif args.profile == "gq_454":
        rows = gq_454_rows(listing, args.dataset_id, args.centre, args.sample_type)
    elif args.profile == "jgi_pyrotags":
        rows = jgi_pyrotag_rows(listing, args.meta, args.dataset_id, args.centre)
    elif args.profile == "sra_sag":
        rows = sra_sag_rows(listing, args.runinfo, args.dataset_id, args.centre)
    elif args.profile == "ont_tar":
        rows = ont_tar_rows(listing, args.dataset_id, args.centre)
    else:
        rows = gq_sag_rows(listing, args.dataset_id, args.centre)
    with open(args.out, "w", newline="") as out:
        w = csv.DictWriter(out, COLUMNS)
        w.writeheader()
        n = 0
        for row in rows:
            w.writerow(row)
            n += 1
    print(f"{n} rows of {len(listing)} listed files -> {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
