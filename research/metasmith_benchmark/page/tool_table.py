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
        ("fastp", {"E1": True, "E2": "replaces bbduk; transform exists", "metaGEM": True}, "Tony: E5 takes bbduk"),
        ("bbduk", {"Pratama": "qtrim=rl, trimq=20, minlen=50", "E3": "Pratama's settings; now qtrim=r, trimq=0", "E5": True}, "fill"),
    ]),
    ("Contaminant removal", [
        ("bowtie2 against phiX", {"E1": "reference is Coliphage WA11, 95% to phiX174", "E2": "NEW: keep for parity, or drop? To decide"}, "Tony: a manual check when a sample needs it, not a pipeline step"),
    ]),
    ("Read QC metrics", [
        ("FastQC", {"E1": True, "E2": "transform exists"}, "Tony: seqkit for QC"),
        ("fastp report", {"Pratama": "report only", "E3": "fastp_qc transform exists"}, "Tony: seqkit for QC"),
        ("seqkit", {"E2": "metasmith read stats", "E3": "metasmith read stats", "E5": True}, "Tony: seqkit for QC"),
    ]),
    ("Long-read processing", [
        ("Guppy basecalling", {"Pratama": "sup model"}, "Parity difference: SRA holds the basecalled FASTQ, so E3 starts after Guppy"),
        ("Porechop ABI", {"E1": True, "E2": NEW}, "Tony: Chopper is more generic"),
        ("Chopper", {"E1": True, "E2": NEW, "E5": NEW}, "Tony: more generic than Porechop ABI"),
    ]),
    ("Assembly", [
        ("MEGAHIT", {"E1": True, "E2": True, "Pratama": "viral contigs only", "E3": "viral contigs only", "metaGEM": True, "E5": True}, "Performance over metaSPAdes"),
        ("metaSPAdes", {"Pratama": "-k 21,33,55,77", "E3": "spades.py exists; pin k-mers"}, "Performance: MEGAHIT instead"),
        ("metaSPAdes --nanopore (hybrid)", {"Pratama": "per Illumina run, 17 assemblies", "E3": "NEW: spades.py takes short reads only"}, "fill"),
        ("Flye", {"E1": True, "E2": "preset from declared platform; 96–128 GB (B12)", "E5": "preset from declared platform; 96–128 GB (B12)"}, "Tony: long-read assembler"),
    ]),
    ("Read mapping and coverage", [
        ("bowtie2", {"E1": "short reads", "E2": "NEW: library's bowtie2_align emits an RNA-seq type"}, "Tony: minimap2"),
        ("minimap2", {"E1": "long reads", "E2": "long reads", "E5": True}, "Tony: minimap2 with a coverage step"),
        ("assembly_stats", {"E2": "minimap2, samtools, bedtools", "E3": "minimap2, samtools, bedtools", "E5": "maybe split coverage out to CoverM"}, "fill"),
        ("bwa", {"metaGEM": True}, None),
        ("CoverM", {"Pratama": "MAG and vOTU abundance", "E3": NEW, "E5": "NEW: maybe, for coverage"}, "fill"),
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
    ("Bin refinement: best bins per sample, from several binners", [
        ("DAS Tool", {"E1": True, "E2": True, "E5": "feeds both dereplicators"}, "Tony: try all four. Refinement and dereplication are separate steps"),
        ("MetaWRAP bin_refinement", {"Pratama": "2 rounds", "E3": "2 rounds; now 1", "metaGEM": True}, "Tony: MAGScoT"),
        ("MetaWRAP reassemble_bins", {"metaGEM": True}, "fill"),
        ("MAGScoT", {"E5": "NEW: feeds both dereplicators"}, "Tony: try all four. Refinement and dereplication are separate steps"),
    ]),
    ("Bin quality", [
        ("CheckM2", {"E1": True, "E2": True, "E5": True}, "Tony: CheckM2 only"),
        ("CheckM", {"Pratama": "inside MetaWRAP", "E3": "inside MetaWRAP", "metaGEM": "inside MetaWRAP"}, "Tony: CheckM2 only"),
    ]),
    ("Dereplication: one genome per species, across samples", [
        ("dRep", {"Pratama": True, "E3": "NEW: replaces skani_dedup", "E5": "NEW: per study, on DAS Tool and on MAGScoT bins"}, "Tony: try all four. Refinement and dereplication are separate steps"),
        ("skani_dedup", {"E5": "per study, on DAS Tool and on MAGScoT bins; needs a study grouping"}, "Tony: try all four. Refinement and dereplication are separate steps"),
    ]),
    ("MAG taxonomy", [
        ("GTDB-Tk", {"Pratama": "r202", "E3": "r232; needs the skani database from Globus", "metaGEM": True, "E4": "r232; needs the skani database from Globus", "E5": "r232; needs the skani database from Globus"}, "Tony: r232 everywhere, even where it breaks parity"),
    ]),
    ("Gene calling", [
        ("Prodigal", {"E1": True, "E2": True, "Pratama": "inside DRAM", "E3": True, "metaGEM": True, "E4": "first step, on published MAGs", "E5": True}, "Tony: both Prodigal and prodigal-gv"),
        ("prodigal-gv", {"E3": "viral contigs", "E5": True}, "Tony: both Prodigal and prodigal-gv"),
    ]),
    ("Virus identification", [
        ("DeepVirFinder", {"Pratama": True, "E3": NEW, "E5": NEW}, "fill · no transform yet. It runs on CPU, so a GPU isn't what's missing"),
        ("VIBRANT", {"Pratama": "-virome", "E3": "-virome; now without", "E5": True}, "Taken from Pratama"),
        ("geNomad", {"Pratama": "two sensitivity flags", "E3": "Pratama's flags; now defaults", "E5": True}, "Taken from Pratama"),
        ("VirSorter2", {"Pratama": "dsDNAphage, ssDNA", "E3": "Pratama's groups; now wider", "E5": True}, "Taken from Pratama"),
    ]),
    ("Viral genomes", [
        ("CheckV", {"Pratama": True, "E3": True, "E5": True}, "Taken from Pratama"),
        ("MMseqs2 vOTU clustering", {"Pratama": "cov-mode 0", "E3": "cov-mode 0; now 0 and 1", "E5": True}, "Taken from Pratama"),
        ("vConTACT3", {"Pratama": "db 220", "E3": True, "E5": True}, "Taken from Pratama"),
        ("MetaPop microdiversity", {"Pratama": True, "E3": "NEW: needs every BAM against one shared reference", "E5": "NEW: per study, every sample mapped to the study's vOTU catalogue"}, "Tony: add"),
        ("DRAM-v (AMGs)", {"Pratama": "manual curation, ≥10 kb", "E3": "no manual curation", "E5": True}, "Taken from Pratama"),
    ]),
    ("CRISPR spacers and host prediction", [
        ("minced", {"Pratama": True, "E3": NEW, "E5": NEW}, "Tony: add"),
        ("BLASTn spacers to contigs", {"Pratama": True, "E3": True, "E5": True}, "Taken from Pratama"),
        ("GTDB-Tk de novo", {"Pratama": True, "E3": True, "E5": True}, "Taken from Pratama"),
        ("iPHoP", {"Pratama": "default and extra-MAGs databases", "E3": "both databases; now extra-MAGs only", "E5": True}, "Taken from Pratama"),
    ]),
    ("Functional annotation", [
        ("DRAM (MAGs)", {"Pratama": True, "E3": "on MAGs; now opt-in, whole assembly"}, "fill · 4-lane panel instead (Tony wrote BLAST: confirm)"),
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
        ("SMETANA", {"metaGEM": True, "E4": NEW, "E5": NEW}, "Tony: run it"),
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
