"""The E5 tool-comparison table, rendered into the page between the E5 SWAPS marker and the next table."""
import html

RUNS = ["E1", "E2", "Pratama", "E3", "metaGEM", "E4", "E5"]
HEAD = ["E1 nf-core/mag", "E2 msm<br>matches E1", "Pratama paper", "E3 msm<br>matches Pratama",
        "metaGEM paper", "E4 msm<br>from metaGEM MAGs", "E5"]

# (tool, {run: cell}, E5 reason). A run missing from the dict doesn't use the tool.
# Cell: True, a settings note, NEW / "NEW: note" for a tool the library lacks, or "fill".
# Reason: None (empty), "fill", "fill · note", or text.
NEW = "NEW"
GROUPS = [
    ("Read trimming and filtering", [
        ("fastp", {"E1": True, "E2": "E2 library", "metaGEM": True}, "Tony: E5 takes bbduk"),
        ("bbduk", {"Pratama": "qtrim=rl, trimq=20, minlen=50", "E3": "Pratama's settings", "E5": True}, "fill"),
    ]),
    ("Contaminant removal", [
        ("bowtie2 against phiX", {"E1": "switched off with --keep_phix"}, "Tony: a manual check when a sample needs it, not a pipeline step"),
    ]),
    ("Read QC metrics", [
        ("FastQC", {"E1": True, "E2": "raw and trimmed, per mate; E2 library"}, "Tony: seqkit for QC"),
        ("fastp report", {"Pratama": "report only", "E3": "report only"}, "Tony: seqkit for QC"),
        ("seqkit", {"E3": "metasmith read stats", "E5": True}, "Tony: seqkit for QC"),
    ]),
    ("Long-read processing", [
        ("Guppy basecalling", {"Pratama": "sup model"}, "Parity difference: SRA holds the basecalled FASTQ, so E3 starts after Guppy"),
        ("Porechop ABI", {"E1": True, "E2": "E2 library"}, "Tony: Chopper is more generic"),
        ("Chopper", {"E1": True, "E2": "E2 library", "E5": NEW}, "Tony: more generic than Porechop ABI"),
    ]),
    ("Assembly", [
        ("MEGAHIT", {"E1": True, "E2": True, "Pratama": "viral contigs only", "E3": "viral contigs only", "metaGEM": True, "E5": True}, "Performance over metaSPAdes"),
        ("metaSPAdes", {"Pratama": "-k 21,33,55,77", "E3": "-k 21,33,55,77"}, "Performance: MEGAHIT instead"),
        ("metaSPAdes --nanopore (hybrid)", {"Pratama": "per Illumina run, 17 assemblies", "E3": "NEW: spades.py takes short reads only"}, "fill"),
        ("Flye", {"E1": True, "E2": "preset from declared platform; memory from measured peaks", "E5": "preset from declared platform; memory from measured peaks"}, "Tony: long-read assembler"),
    ]),
    ("Read mapping and coverage", [
        ("bowtie2", {"E1": "short reads", "E2": "short reads; E2 library"}, "Tony: minimap2"),
        ("minimap2", {"E1": "long reads", "E2": "long reads", "E5": True}, "Tony: minimap2 with a coverage step"),
        ("assembly_stats", {"E3": "minimap2, samtools, bedtools; per-base coverage", "E5": "per-base coverage"}, "Tony: per-base coverage, better than CoverM"),
        ("bwa", {"metaGEM": True}, None),
        ("CoverM", {"Pratama": "MAG and vOTU abundance"}, "Tony: assembly_stats gives per-base coverage"),
    ]),
    ("Binners", [
        ("MetaBAT2", {"E1": True, "E2": True, "Pratama": True, "E3": "via MetaWRAP", "metaGEM": True, "E5": True}, "Tony: three binners into MAGScoT"),
        ("MaxBin2", {"Pratama": True, "E3": "via MetaWRAP", "metaGEM": True}, "Tony: three binners into MAGScoT"),
        ("CONCOCT", {"Pratama": True, "E3": "via MetaWRAP", "metaGEM": True}, "Tony: three binners into MAGScoT"),
        ("SemiBin2", {"E1": True, "E2": True, "E5": True}, "Tony: three binners into MAGScoT"),
        ("COMEBin", {"E1": True, "E2": True, "E5": True}, "Tony: three binners into MAGScoT"),
        ("BinSanity", {"Pratama": True, "E3": NEW}, "fill"),
        ("abawaca", {"Pratama": True, "E3": NEW}, "fill"),
    ]),
    ("Dereplication: one non-redundant bin set from overlapping candidates", [
        ("DAS Tool", {"E1": "per sample", "E2": "per sample", "E5": "NEW: per sample, over all three binners"}, "Tony: four interchangeable dereplicators for one step"),
        ("MetaWRAP bin_refinement", {"Pratama": "2 rounds, with CheckM filtering", "E3": "2 rounds; now 1, until BinSanity and abawaca", "metaGEM": "with CheckM filtering"}, "Tony: MAGScoT"),
        ("MAGScoT", {"E5": "NEW: per sample, over all three binners"}, "Tony: four interchangeable dereplicators for one step"),
        ("dRep", {"Pratama": "across samples", "E3": "NEW: across samples", "E5": "NEW: per sample, and per study"}, "Tony: four interchangeable dereplicators for one step"),
        ("skani_dedup", {"E5": "per sample, and per study; needs a study grouping"}, "Tony: four interchangeable dereplicators for one step"),
    ]),
    ("Reassembly", [
        ("MetaWRAP reassemble_bins", {"metaGEM": True}, "fill"),
    ]),
    ("Bin quality", [
        ("CheckM2", {"E1": True, "E2": True, "E5": "bench library, on all four bin sets"}, "Tony: CheckM2 only"),
        ("CheckM", {"Pratama": "inside MetaWRAP", "E3": "inside MetaWRAP", "metaGEM": "inside MetaWRAP"}, "Tony: CheckM2 only"),
    ]),
    ("MAG taxonomy", [
        ("GTDB-Tk", {"Pratama": "r202", "E3": "r232; package extracted on fir", "metaGEM": True, "E4": "r232; package extracted on fir", "E5": "r232; package extracted on fir"}, "Tony: r232 everywhere, even where it breaks parity"),
    ]),
    ("Gene calling", [
        ("Prodigal", {"E1": "standalone, unscored", "Pratama": "inside DRAM", "E3": True, "metaGEM": True, "E4": "first step, on published MAGs", "E5": True}, "Tony: both Prodigal and prodigal-gv"),
        ("prodigal-gv", {"E3": "viral contigs", "E5": True}, "Tony: both Prodigal and prodigal-gv"),
    ]),
    ("Virus identification", [
        ("DeepVirFinder", {"Pratama": "-l 1000", "E3": NEW, "E5": "-l 1000; score table only, its calls stay out of the vOTU set (no cut given)"}, "fill"),
        ("VIBRANT", {"Pratama": "-virome", "E3": "-virome", "E5": True}, "Taken from Pratama"),
        ("geNomad", {"Pratama": "two sensitivity flags", "E3": "Pratama's flags", "E5": True}, "Taken from Pratama"),
        ("VirSorter2", {"Pratama": "dsDNAphage, ssDNA", "E3": "Pratama's groups", "E5": True}, "Taken from Pratama"),
    ]),
    ("Viral genomes", [
        ("CheckV", {"Pratama": True, "E3": True, "E5": True}, "Taken from Pratama"),
        ("MMseqs2 vOTU clustering", {"Pratama": "cov-mode 0", "E3": "cov-mode 0", "E5": True}, "Taken from Pratama"),
        ("vConTACT3", {"Pratama": "db 220", "E3": True, "E5": True}, "Taken from Pratama"),
        ("MetaPop microdiversity", {"Pratama": "--min_cov 70", "E3": "NEW: needs every BAM against one shared reference", "E5": "--min_cov 70; per study, every sample mapped to the study's vOTU representatives with bowtie2"}, "Tony: add"),
        ("DRAM-v (AMGs)", {"Pratama": "manual curation, ≥10 kb", "E3": "no manual curation", "E5": "to decide after the pilot"}, "Taken from Pratama; confirm it goes or stays with DRAM"),
    ]),
    ("CRISPR spacers and host prediction", [
        ("minced", {"Pratama": True, "E3": NEW, "E5": "NEW: to decide after the pilot"}, "Its only E5 use was the dropped BLASTn links"),
        ("BLASTn spacers to contigs", {"Pratama": True, "E3": True}, "Tony: drop in E5"),
        ("GTDB-Tk de novo", {"Pratama": True, "E3": True, "E5": True}, "Taken from Pratama"),
        ("iPHoP", {"Pratama": "default and extra-MAGs databases", "E3": "both databases; now shipped only, until dRep", "E5": True}, "Tony: keep; the database costs little"),
    ]),
    ("Functional annotation", [
        ("DRAM (MAGs)", {"Pratama": True, "E3": "on MAGs"}, "Tony: the 4-lane panel replaces DRAM"),
        ("KOfamScan", {"E5": True}, "4-lane panel"),
        ("CLEAN", {"E5": True}, "4-lane panel"),
        ("DIAMOND UniRef50", {"E5": True}, "4-lane panel"),
        ("ProteinBERT", {"E5": True}, "4-lane panel"),
    ]),
    ("Metabolic models", [
        ("CarveMe", {"metaGEM": True, "E4": "12 h gapfill limit (B11)", "E5": True}, "fill"),
        ("CPLEX solver", {"metaGEM": True, "E4": "22.2"}, "Open-source solver"),
        ("Open-source solver (SCIP)", {"E5": "runtime risk: an open solver ran 1 h 49 m on the smallest bin without finishing"}, "Open-source solver"),
        ("MEMOTE", {"metaGEM": True, "E4": True, "E5": True}, "fill"),
        ("SMETANA", {"metaGEM": "detailed, 15 media, CPLEX", "E4": NEW, "E5": "detailed, 15 media, SCIP; one community per sample"}, "Tony: run it"),
    ]),
    ("Scoring", [
        ("AMBER", {"E1": "after the run", "E2": True, "E5": "after the run, outside the plan"}, "Tony: one plan can't give samples different targets"),
        ("skANI recovery vs published", {"E3": "vOTUs, MAGs", "E4": "planned", "E5": "after the run, outside the plan"}, "Tony: one plan can't give samples different targets"),
    ]),
]

FILL = '<span class="pick ask">fill</span>'
NEW_PILL = '<span class="pick long">new transform</span>'


def dim(text):
    return f'<br><span class="dim">{html.escape(text)}</span>'


def cell(tool, value):
    if value is None:
        return '<td class="absent"></td>'
    if value == "fill":
        return f"<td>{FILL}</td>"
    if value is True:
        return f"<td>{html.escape(tool)}</td>"
    if value == NEW or value.startswith(NEW + ": "):
        note = value.removeprefix(NEW).removeprefix(": ")
        return f"<td>{html.escape(tool)} {NEW_PILL}{dim(note) if note else ''}</td>"
    return f"<td>{html.escape(tool)}{dim(value)}</td>"


def reason(why):
    if why is None:
        return "<td></td>"
    if why == "fill":
        return "<td></td>"
    if why.startswith("fill · "):
        return f'<td><span class="dim">{html.escape(why.removeprefix("fill · "))}</span></td>'
    return f"<td>{html.escape(why)}</td>"


def render():
    rows = []
    for purpose, tools in GROUPS:
        rows.append(f'      <tr class="grp"><th colspan="{len(RUNS) + 2}" scope="rowgroup">{html.escape(purpose)}</th></tr>')
        for tool, runs, why in tools:
            cells = "".join(cell(tool, runs.get(r)) for r in RUNS)
            rows.append(f'      <tr><th scope="row">{html.escape(tool)}</th>{cells}{reason(why)}</tr>')
    head = "".join(f"<th>{h}</th>" for h in HEAD)
    return (
        '<div class="corpus">\n  <div class="scroll">\n  <table class="ds swaps">\n'
        f'    <thead><tr><th>Tool</th>{head}<th>E5 reason</th></tr></thead>\n'
        "    <tbody>\n" + "\n".join(rows) + "\n    </tbody>\n  </table>\n  </div>\n</div>"
    )


def insert(src):
    start = src.index("<!-- E5 SWAPS -->")
    table_start = src.index('<div class="corpus">', start)
    end_marker = "</table>\n  </div>\n</div>"
    table_end = src.index(end_marker, table_start) + len(end_marker)
    return src[:table_start] + render() + src[table_end:]
