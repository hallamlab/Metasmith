#!/usr/bin/env python3
"""Join every annotation lane of one assembly into a wide table and a long table.

    aggregate_annotations.py --sample S --assembly FASTA --out-dir DIR
        [--port-run RUN_DIR ...] [--mp-out MP_SAMPLE_DIR] [--pgdb-table pgdb_genes.parquet ...]
        [--mag-map MAG_MAP]
    aggregate_annotations.py --write-legend lane_legend.tsv

A feature is a CDS, rRNA, tRNA, ncRNA, promoter or element (BGC, virus, plasmid, integron,
MGE), keyed by type and contig coordinates, so the two pipelines' calls of one gene share a
row only when their coordinates agree exactly. Every other CDS keeps its own row, and
cds_coordinate_mismatches.tsv names its nearest partner from the other pipeline.

--port-run reads a metasmith run's _metasmith/logs.latest/lineage.csv, which names every file
the run produced by data type, intermediates included; several runs of one assembly (the
MetaPathways port and its extra lane groups) are merged. --mp-out is MetaPathways 4.0.3's
<output>/<sample> directory. --pgdb-table takes export_pgdb.py output.

Writes annotations_wide.{parquet,tsv.gz} (one row per feature; <lane>_id, <lane>_desc and
<lane>_conf per lane, best hit only; contig lanes are copied onto every feature of the contig,
and element lanes onto every feature an element contains, which `elements` also lists),
annotations_long.{parquet,tsv.gz} (every hit: feature, lane, rank, id, desc, conf and the
source row's remaining columns as JSON in `extras`), cds_coordinate_mismatches.tsv,
lane_legend.tsv, summary.tsv and lane_presence.tsv.

lane_presence.tsv gives every lane of each pipeline given a status: ok, partial (a MAG lane
missing some bins' PGDBs), EMPTY (source files present, no hits), not_applicable, MISSING (no
source file) or UNMAPPED (hits that match no feature). The tables are written regardless, but
a MISSING or UNMAPPED lane makes the exit status nonzero.
"""

import argparse
import csv
import gzip
import hashlib
import io
import itertools
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

HIGH, LOW, NONE = "higher_is_better", "lower_is_better", "none"

LEGEND_COLUMNS = ["lane", "pipeline", "tool", "reference", "feature_type", "id", "desc",
                  "conf_measure", "conf_unit", "conf_direction", "source"]
LANES = {
    "swissprot": ("metapathways", "FAST", "UniProtKB/Swiss-Prot", "CDS", "target accession", "product", "blast score ratio", "bitscore / query self-bitscore, 0-1", HIGH, "blast_results/<sample>.swissprot.FASTout.parsed.txt"),
    "uniref50": ("metapathways", "FAST", "UniRef50", "CDS", "target accession", "product", "blast score ratio", "bitscore / query self-bitscore, 0-1", HIGH, "blast_results/<sample>.uniref50.FASTout.parsed.txt"),
    "uniref90": ("metapathways", "FAST", "UniRef90", "CDS", "target accession", "product", "blast score ratio", "bitscore / query self-bitscore, 0-1", HIGH, "blast_results/<sample>.uniref90.FASTout.parsed.txt"),
    "cazy": ("metapathways", "FAST", "dbCAN CAZyDB", "CDS", "target accession", "product", "blast score ratio", "bitscore / query self-bitscore, 0-1", HIGH, "blast_results/<sample>.cazy.FASTout.parsed.txt"),
    "eggnog": ("metapathways", "FAST", "eggNOG proteins", "CDS", "target accession", "product", "blast score ratio", "bitscore / query self-bitscore, 0-1", HIGH, "blast_results/<sample>.eggnog.FASTout.parsed.txt"),
    "metacyc": ("metapathways", "FAST", "MetaCyc proteins", "CDS", "target accession", "product", "blast score ratio", "bitscore / query self-bitscore, 0-1", HIGH, "blast_results/<sample>.metacyc.FASTout.parsed.txt"),
    "mp_product": ("metapathways", "MetaPathways consensus", "the reference_db in extras", "CDS", "target accession", "product", "none", "", NONE, "results/annotation_table/<sample>.functional_and_taxonomic_table.txt"),
    "mp_lca": ("metapathways", "per-database LCA", "NCBI taxonomy", "CDS", "LCA taxon", "reference database", "none", "", NONE, "results/annotation_table/<sample>.annotation_taxonomy.tsv; rank 1 is the database the consensus product came from"),
    "silva": ("metapathways", "barrnap and BLAST", "SILVA SSU and LSU", "rRNA", "deepest taxon", "SILVA taxonomy", "percent identity", "%", HIGH, "results/rRNA/<sample>.<db>.rRNA.stats.txt"),
    "mp_trna": ("metapathways", "tRNAscan-SE", "none", "tRNA", "tRNA isotype", "anticodon", "Infernal score", "bits", HIGH, "results/tRNA/<sample>.tRNA.results.txt"),
    "mp_pathologic": ("metapathways", "Pathway Tools PathoLogic", "MetaCyc", "CDS", "MetaCyc reactions (all, ;-joined in the wide table)", "MetaCyc pathways (all, ;-joined)", "none", "", NONE, "export_pgdb.py on results/pgdb/community/<sample>cyc.tar.bz2"),
    "mp_pathologic_mag": ("metapathways", "Pathway Tools PathoLogic", "MetaCyc", "CDS", "MetaCyc reactions (all, ;-joined)", "MetaCyc pathways (all, ;-joined)", "none", "", NONE, "export_pgdb.py on results/pgdb/MAGs/<bin>/<bin>cyc.tar.bz2; the bin is in extras"),
    "diamond_uniref50": ("port", "DIAMOND", "UniRef50", "CDS", "UniRef50 cluster", "cluster description", "blast score ratio", "bitscore / query self-bitscore, 0-1", HIGH, "annotation::diamond_uniref50_results + annotation::diamond_uniref50_descriptions"),
    "kofam": ("port", "KofamScan", "KOfam profiles", "CDS", "KO", "KO definition", "HMM bitscore (all rows clear the KO's own threshold)", "bits", HIGH, "annotation::kofamscan_results + annotation::kofamscan_descriptions"),
    "clean": ("port", "CLEAN", "CLEAN model", "CDS", "EC number", "", "CLEAN confidence", "0-1", HIGH, "annotation::clean_predictions"),
    "deepec": ("port", "DeepEC", "DeepEC model", "CDS", "EC number", "", "none (DeepEC reports no score)", "", NONE, "annotation::deepec_predictions"),
    "ezpred": ("port", "EZpred", "EZpred model on ESM-C 600M", "CDS", "EC term at any level", "head kind", "EZpred probability", "0-1", HIGH, "annotation::ezpred_predictions; rows scoring >= 0.1 only, the raw CSV keeps every term"),
    "proteinbert": ("port", "ProteinBERT label transfer", "label-transfer landmarks", "CDS", "MetaNetX reaction", "landmark", "transfer vote share", "0-1", HIGH, "channel pbert of annotation::gpr_table_7lane, else annotation::gpr_table"),
    "esm_c": ("port", "ESM-C 600M label transfer inside the 7-lane GPR, nearest landmark at cosine >= 0.985", "ESM-C landmarks", "CDS", "MetaNetX reaction", "landmark", "transfer vote share", "0-1", HIGH, "channel esmc of annotation::gpr_table_7lane"),
    "esmc_transfer": ("port", "ESM-C 600M nearest-landmark label transfer; a second opinion, not part of the gpr consensus", "222,019 Swiss-Prot 2026_02 landmarks, labels via fabfos mnxr_lookup", "CDS", "MetaNetX reaction of a landmark among the 30 nearest", "EC numbers of the reaction (mnxr_lookup)", "cosine to the nearest landmark carrying the reaction", "0-1, rows >= 0.97 only; README gives precision by cut-off", HIGH, "annotation::esm_c_embeddings + annotation::esm_c_index against ref::label_transfer_landmarks_esmc"),
    "gpr": ("port", "4-lane GPR merge (kofam, CLEAN, UniRef50, ProteinBERT)", "MetaNetX lookup", "CDS", "MetaNetX reaction with the most supporting channels", "supporting channels", "supporting channels", "count of 4", HIGH, "annotation::gpr_table; the long table keeps every channel row with its raw_score"),
    "gpr7": ("port", "7-lane GPR merge (adds DeepEC, EZpred, ESM-C)", "MetaNetX lookup", "CDS", "MetaNetX reaction with the most supporting channels", "supporting channels", "supporting channels", "count of 7", HIGH, "annotation::gpr_table_7lane; the long table keeps every channel row with its raw_score"),
    "port_pathologic": ("port", "Pathway Tools PathoLogic", "MetaCyc", "CDS", "MetaCyc reactions (all, ;-joined in the wide table)", "MetaCyc pathways (all, ;-joined)", "none", "", NONE, "export_pgdb.py on annotation::pgdb_archive"),
    "port_pathologic_mag": ("port", "Pathway Tools PathoLogic", "MetaCyc", "CDS", "MetaCyc reactions (all, ;-joined)", "MetaCyc pathways (all, ;-joined)", "none", "", NONE, "export_pgdb.py on annotation::bin_pgdb_archive; the bin is in extras"),
    "contig_lcastar": ("port", "LCA* vote", "GTDB via UniRef50, Metabuli and rRNA", "contig", "deepest taxon", "GTDB lineage", "LCA* p-value", "probability", LOW, "taxonomy::contig_lcastar"),
    "metabuli": ("port", "Metabuli", "GTDB", "contig", "deepest taxon", "GTDB lineage", "Metabuli score", "0-1", HIGH, "taxonomy::metabuli"),
    "rrna_gtdb": ("port", "barrnap and BLAST", "GTDB SSU", "rRNA", "deepest taxon", "GTDB lineage", "best percent identity", "%", HIGH, "taxonomy::rrna_gtdb"),
    "aragorn": ("port", "ARAGORN", "none", "tRNA", "tRNA product", "anticodon", "none", "", NONE, "annotation::trna_gff"),
    "genomad": ("port", "geNomad", "geNomad database", "element (virus, provirus, plasmid)", "geNomad sequence name", "class and taxonomy", "virus or plasmid score", "0-1", HIGH, "taxonomy::genomad_virus_summary + taxonomy::genomad_plasmid_summary"),
    "virsorter2": ("port", "VirSorter2", "VirSorter2 database", "element (virus)", "VirSorter2 sequence name", "best viral group", "max score", "0-1", HIGH, "annotation::virsorter2_boundary + annotation::virsorter2_scores"),
    "integronfinder": ("port", "IntegronFinder", "built-in models", "element (integron)", "replicon:integron", "integron type and elements", "best element e-value", "e-value", LOW, "annotation::integronfinder_integrons"),
    "mobileelementfinder": ("port", "MobileElementFinder", "built-in database", "element (MGE)", "element name", "element type", "identity", "fraction, 0-1", HIGH, "annotation::mobileelementfinder_results"),
    "bakta_ncrna": ("port", "Bakta non-coding", "Bakta light database", "ncRNA, tRNA, rRNA, CRISPR", "feature name", "product and cross-references", "none", "", NONE, "annotation::bakta_gff"),
    "promotech": ("port", "PromoTech", "built-in RF-HOT model", "promoter", "promoter", "", "promoter probability", "0-1", HIGH, "annotation::promotech_predictions"),
    "deeptfactor": ("port", "DeepTFactor", "built-in model", "CDS", "TF (predicted transcription factors only)", "", "TF probability", "0-1", HIGH, "annotation::deeptfactor_results"),
    "amrfinderplus": ("port", "AMRFinderPlus", "AMRFinderPlus database", "CDS", "element symbol", "element name and class", "percent identity to reference", "%", HIGH, "annotation::amrfinderplus_results"),
    "bacmet": ("port", "DIAMOND", "BacMet2", "CDS", "target", "target title", "bitscore", "bits", HIGH, "annotation::bacmet_diamond_results"),
    "megares": ("port", "DIAMOND", "MEGARes", "CDS", "target", "target title", "bitscore", "bits", HIGH, "annotation::megares_diamond_results"),
    "vfdb": ("port", "DIAMOND", "VFDB", "CDS", "target", "target title", "bitscore", "bits", HIGH, "annotation::vfdb_diamond_results"),
    "tcdb": ("port", "DIAMOND", "TCDB", "CDS", "target", "target title", "bitscore", "bits", HIGH, "annotation::diamond_tcdb_results"),
    "pathofact": ("port", "PathoFact", "PathoFact database", "CDS", "ARG", "AMR category", "none", "", NONE, "annotation::pathofact_amr"),
    "antismash": ("port", "antiSMASH", "antiSMASH database", "element (BGC)", "contig.region", "BGC products", "none (rule-based)", "", NONE, "annotation::antismash_json"),
    "eggnog_mapper": ("port", "eggNOG-mapper, best ortholog", "eggNOG 5.0.2", "CDS", "seed ortholog", "preferred name and description", "seed ortholog bitscore", "bits", HIGH, "annotation::eggnog_results"),
    "interproscan": ("port", "InterProScan", "InterProScan data", "CDS", "signature accession", "signature description and InterPro accession", "match score (e-value for most member databases)", "e-value", LOW, "annotation::interproscan_results + annotation::interproscan_descriptions"),
}
EZPRED_FLOOR = 0.1
ESMC_TRANSFER_FLOOR = 0.97
ESMC_TRANSFER_K = 30
EC4 = re.compile(r"^\d+\.\d+\.\d+\.n?\d+$")
MP_DBS = ("swissprot", "uniref50", "uniref90", "cazy", "eggnog", "metacyc")
ELEMENT_LANES = ("genomad", "virsorter2", "integronfinder", "mobileelementfinder", "antismash")
CONTIG_LANES = ("contig_lcastar", "metabuli")
JOINED_LANES = ("mp_pathologic", "mp_pathologic_mag", "port_pathologic", "port_pathologic_mag")
SOURCES = {
    **{db: [f"blast_results/{{s}}.{db}.FASTout.parsed.txt", f"blast_results/{{s}}.{db}.BLASTout.parsed.txt"] for db in MP_DBS},
    "mp_product": ["results/annotation_table/{s}.functional_and_taxonomic_table.txt"],
    "mp_lca": ["results/annotation_table/{s}.annotation_taxonomy.tsv", "results/annotation_table/{s}.functional_and_taxonomic_table.txt"],
    "silva": ["results/rRNA/{s}.*.rRNA.stats.txt"],
    "mp_trna": ["results/tRNA/{s}.tRNA.results.txt"],
    "mp_pathologic": ["results/pgdb/community/*cyc.tar.bz2"],
    "mp_pathologic_mag": ["results/pgdb/MAGs/*/*cyc.tar.bz2"],
    "diamond_uniref50": ["annotation::diamond_uniref50_results"],
    "kofam": ["annotation::kofamscan_results"],
    "clean": ["annotation::clean_predictions"],
    "deepec": ["annotation::deepec_predictions", "annotation::deepec_predictions_chunk"],
    "ezpred": ["annotation::ezpred_predictions"],
    "proteinbert": ["annotation::gpr_table_7lane", "annotation::gpr_table"],
    "esm_c": ["annotation::gpr_table_7lane"],
    "esmc_transfer": ["annotation::esm_c_embeddings"],
    "gpr": ["annotation::gpr_table"],
    "gpr7": ["annotation::gpr_table_7lane"],
    "port_pathologic": ["annotation::pgdb_archive"],
    "port_pathologic_mag": ["annotation::bin_pgdb_archive"],
    "contig_lcastar": ["taxonomy::contig_lcastar"],
    "metabuli": ["taxonomy::metabuli"],
    "rrna_gtdb": ["taxonomy::rrna_gtdb"],
    "aragorn": ["annotation::trna_gff"],
    "genomad": ["taxonomy::genomad_virus_summary", "taxonomy::genomad_plasmid_summary"],
    "virsorter2": ["annotation::virsorter2_boundary"],
    "integronfinder": ["annotation::integronfinder_integrons"],
    "mobileelementfinder": ["annotation::mobileelementfinder_results"],
    "bakta_ncrna": ["annotation::bakta_gff"],
    "promotech": ["annotation::promotech_predictions"],
    "deeptfactor": ["annotation::deeptfactor_results"],
    "amrfinderplus": ["annotation::amrfinderplus_results"],
    "bacmet": ["annotation::bacmet_diamond_results"],
    "megares": ["annotation::megares_diamond_results"],
    "vfdb": ["annotation::vfdb_diamond_results"],
    "tcdb": ["annotation::diamond_tcdb_results"],
    "pathofact": ["annotation::pathofact_amr"],
    "antismash": ["annotation::antismash_json"],
    "eggnog_mapper": ["annotation::eggnog_results"],
    "interproscan": ["annotation::interproscan_results"],
}
FAILING = ("MISSING", "UNMAPPED")
STD = ["ns", "key", "id", "desc", "conf"]
FEATURE_COLS = ["ns", "key", "feature_type", "feature_name", "contig", "start", "end", "strand"]
LONG_SCHEMA = pa.schema([("sample", pa.string()), ("feature_id", pa.string()), ("feature_type", pa.string()),
                         ("lane", pa.string()), ("rank", pa.int32()), ("id", pa.string()), ("desc", pa.string()),
                         ("conf", pa.float64()), ("key", pa.string()), ("extras", pa.string())])


def write_legend(path):
    rows = [dict(zip(LEGEND_COLUMNS, (lane, *info))) for lane, info in LANES.items()]
    pd.DataFrame(rows, columns=LEGEND_COLUMNS).to_csv(path, sep="\t", index=False)


class Assembly:
    def __init__(self, fasta):
        self.order, self.lengths = [], {}
        name, n = None, 0
        opener = gzip.open if str(fasta).endswith(".gz") else open
        with opener(fasta, "rt") as fh:
            for line in fh:
                if line.startswith(">"):
                    if name is not None:
                        self.lengths[name] = n
                    name, n = line[1:].split()[0], 0
                    self.order.append(name)
                else:
                    n += len(line.rstrip())
        if name is not None:
            self.lengths[name] = n

    def translate(self, names, lengths=None, by_order=False):
        lengths = lengths or {}
        out = {}
        for i, n in enumerate(names):
            if n in self.lengths:
                out[n] = n
                continue
            m = re.search(r"(\d+)$", n)
            idx = i if by_order else (int(m.group(1)) - 1 if m else -1)
            if 0 <= idx < len(self.order):
                cand = self.order[idx]
                if lengths.get(n) in (None, self.lengths[cand]):
                    out[n] = cand
        return out


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 22), b""):
            h.update(block)
    return h.hexdigest()


class PortFiles:
    def __init__(self, runs, assembly_path):
        self.files, self.notes = defaultdict(list), []
        want = Path(assembly_path).resolve()
        for run in runs:
            run = Path(run).resolve()
            home = run.parents[1]
            found = defaultdict(set)
            lineage = run / "_metasmith" / "logs.latest" / "lineage.csv"
            if lineage.is_file():
                with open(lineage, newline="") as fh:
                    reader = csv.reader(fh)
                    header = next(reader)
                    for row in reader:
                        for dtype, value in zip(header, row):
                            if value:
                                found[dtype].add(value)
            for d in (run / "results").glob("*"):
                if d.is_dir() and "-" in d.name and not d.name.startswith("_"):
                    found[d.name.replace("-", "::", 1)].update(str(p) for p in d.iterdir() if p.is_file())
            local = {t: {self.local(p, home) for p in v} for t, v in found.items()}
            asm = {p.resolve() for p in local.get("sequences::assembly", ()) if p.exists()}
            if asm and want not in asm:
                sys.exit(f"{run} annotates {sorted(map(str, asm))}, not {want}")
            for dtype, paths in local.items():
                self.files[dtype].extend(sorted(paths))
        self.unique = {}

    @staticmethod
    def local(p, home):
        return Path(str(home) + p[len("/msm_home"):]) if p.startswith("/msm_home") else Path(p)

    def distinct(self, dtype):
        if dtype not in self.unique:
            seen, out = set(), []
            for p in self.files.get(dtype, ()):
                if p.is_file() and (digest := (p.stat().st_size, md5(p))) not in seen:
                    seen.add(digest)
                    out.append(p)
            self.unique[dtype] = out
        return self.unique[dtype]

    def get(self, *dtypes):
        for dtype in dtypes:
            if self.distinct(dtype):
                return self.distinct(dtype)
        return []

    def one(self, dtype):
        paths = self.get(dtype)
        if len(paths) > 1:
            self.notes.append(f"{dtype}: {len(paths)} distinct files, using {paths[0]}")
        return paths[0] if paths else None


class MPFiles:
    def __init__(self, out, sample):
        self.out, self.sample = Path(out), sample
        self.contigs = {}
        mapping = self.path("preprocessed/{s}.mapping.txt")
        if mapping:
            with open(mapping) as fh:
                for line in fh:
                    f = line.rstrip("\n").split("\t")
                    m = re.search(r"C(\d+)$", f[0])
                    if m and len(f) >= 2:
                        self.contigs[m.group(1)] = f[1].split()[0] if f[1].split() else f[1]

    def path(self, pattern):
        p = self.out / pattern.format(s=self.sample)
        return p if p.is_file() else None

    def glob(self, pattern):
        return sorted(self.out.glob(pattern.format(s=self.sample)))

    def contig(self, name):
        m = re.search(r"(?:^|-)C(\d+)$", str(name)) or re.fullmatch(r"(\d+)", str(name))
        return self.contigs.get(m.group(1), name) if m else name


def mp_orf(orf):
    s = str(orf)
    m = re.search(r"C(\d+)-G(\d+)$", s) or re.search(r"(\d+)_(\d+)$", s)
    return f"C{m.group(1)}-G{m.group(2)}" if m else s


def read_tsv(path, **kw):
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False, na_values=[], **kw)


def concat(frames):
    frames = [f for f in frames if f is not None and len(f)]
    return pd.concat(frames, ignore_index=True) if frames else None


def num(s):
    return pd.to_numeric(s, errors="coerce")


def gff_rows(path):
    rows = []
    with open(path) as fh:
        for line in fh:
            if line.startswith("##FASTA"):
                break
            if line.startswith("#") or not line.strip():
                continue
            f = line.rstrip("\n").split("\t")
            if len(f) < 9:
                continue
            attrs = dict(kv.split("=", 1) for kv in f[8].strip().strip(";").split(";") if "=" in kv)
            rows.append((f[0], f[2], int(f[3]), int(f[4]), f[6], f[5], attrs))
    return rows


def nobatch(name):
    return re.sub(r"^.*~", "", str(name))


def last_taxon(lineage):
    parts = [p for p in re.split(r"[;|]", str(lineage)) if p.strip() and p.strip() not in ("-", "NA")]
    return parts[-1].strip() if parts else ""


def features_port(pf):
    out = []
    faa = pf.one("sequences::orfs")
    if faa:
        rows = []
        with open(faa) as fh:
            for line in fh:
                if line.startswith(">"):
                    f = line[1:].rstrip("\n").split(" # ")
                    if len(f) >= 4:
                        orf = f[0].split()[0]
                        rows.append(("port_cds", orf, "CDS", "", re.sub(r"_\d+$", "", orf), int(f[1]), int(f[2]),
                                     "+" if f[3].strip() == "1" else "-"))
        out.append(pd.DataFrame(rows, columns=FEATURE_COLS))
    for dtype, ns in (("annotation::rrna_gff", "port_rrna"), ("annotation::trna_gff", "port_trna")):
        for path in pf.get(dtype):
            rows = [(ns, a.get("ID", f"{c}:{s}-{e}"), t, a.get("product", ""), c, s, e, st)
                    for c, t, s, e, st, _, a in gff_rows(path)]
            out.append(pd.DataFrame(rows, columns=FEATURE_COLS))
    return out


def features_mp(mp):
    out = []
    gff = mp.path("orf_prediction/{s}.unannot.gff")
    if gff:
        rows = [("mp_cds", mp_orf(a.get("ID", "")), "CDS", "", mp.contig(c), s, e, st)
                for c, t, s, e, st, _, a in gff_rows(gff) if t == "CDS"]
        out.append(pd.DataFrame(rows, columns=FEATURE_COLS))
    rrna = mp.path("orf_prediction/{s}.rRNA.gff")
    if rrna:
        rows = [("mp_rrna", f"{mp.contig(c)}:{s - 1}-{e}({st})", "rRNA", a.get("product", a.get("Name", "")),
                 mp.contig(c), s, e, st) for c, t, s, e, st, _, a in gff_rows(rrna) if t == "rRNA"]
        out.append(pd.DataFrame(rows, columns=FEATURE_COLS))
    trna = mp_trna_table(mp)
    if trna is not None:
        out.append(trna[FEATURE_COLS])
    return out


def mp_trna_table(mp):
    path = mp.path("results/tRNA/{s}.tRNA.results.txt")
    if not path:
        return None
    rows = []
    with open(path) as fh:
        for line in fh:
            f = [x.strip() for x in line.rstrip("\n").split("\t")]
            if len(f) < 9 or not f[1].isdigit():
                continue
            b, e = int(f[2]), int(f[3])
            contig = mp.contig(f[0])
            rows.append(("mp_trna", f"{f[0]}.trna{f[1]}", "tRNA", f"tRNA-{f[4]}", contig, min(b, e), max(b, e),
                         "+" if b <= e else "-", f"tRNA-{f[4]}", f[5], f[8], line.rstrip("\n")))
    return pd.DataFrame(rows, columns=FEATURE_COLS + ["id", "desc", "conf", "raw"])


def parse_bakta(pf, asm):
    path = pf.one("annotation::bakta_gff")
    if not path:
        return None, None
    lengths = {}
    with open(path) as fh:
        for line in fh:
            if line.startswith("##sequence-region"):
                f = line.split()
                lengths[f[1]] = int(f[3])
    rows = [r for r in gff_rows(path) if r[1] not in ("region", "CDS", "gene", "sORF")]
    names = list(dict.fromkeys([r[0] for r in rows] + list(lengths)))
    tr = asm.translate(names, lengths)
    feats, hits = [], []
    for c, t, s, e, st, score, a in rows:
        key = a.get("ID") or f"{c}:{s}-{e}{st}"
        name = a.get("Name") or a.get("product", "")
        feats.append(("bakta", key, t, name, tr.get(c, c), s, e, st))
        desc = "; ".join(x for x in (a.get("product", ""), a.get("Dbxref", "")) if x)
        hits.append(("bakta", key, name, desc, score if score != "." else "", t, a.get("locus_tag", ""), c))
    hits = pd.DataFrame(hits, columns=STD + ["type", "locus_tag", "bakta_seqid"])
    hits["conf"] = num(hits["conf"])
    unmapped = sorted(set(names) - set(tr))
    note = f"bakta_ncrna: {len(unmapped)} sequence IDs not in the assembly" if unmapped else None
    return pd.DataFrame(feats, columns=FEATURE_COLS), (hits, note)


def parse_promotech(pf):
    frames = []
    for path in pf.get("annotation::promotech_predictions"):
        df = read_tsv(path)
        if len(df):
            frames.append(df)
    df = concat(frames)
    if df is None:
        return None, None
    df["start"], df["end"] = num(df["start"]).astype("Int64"), num(df["end"]).astype("Int64")
    lo, hi = np.minimum(df["start"], df["end"]), np.maximum(df["start"], df["end"])
    df["key"] = df["chrom"] + ":" + lo.astype(str) + "-" + hi.astype(str) + df["strand"]
    feats = pd.DataFrame({"ns": "promotech", "key": df["key"], "feature_type": "promoter", "feature_name": "",
                          "contig": df["chrom"], "start": lo.astype(int), "end": hi.astype(int), "strand": df["strand"]})
    hits = pd.DataFrame({"ns": "promotech", "key": df["key"], "id": "promoter", "desc": "", "conf": num(df["score"]),
                         "sequence": df.get("sequence", "")})
    return feats, hits


def element_frame(rows, lane):
    df = pd.DataFrame(rows, columns=["key", "contig", "start", "end", "strand", "element_class", "id", "desc", "conf", "raw_json"])
    df["ns"] = lane
    df["conf"] = num(df["conf"])
    return df


def parse_genomad(pf, asm):
    rows = []
    for kind, dtype, score in (("virus", "taxonomy::genomad_virus_summary", "virus_score"),
                               ("plasmid", "taxonomy::genomad_plasmid_summary", "plasmid_score")):
        for path in pf.get(dtype):
            df = read_tsv(path)
            for r in df.to_dict("records"):
                name = nobatch(r["seq_name"])
                contig = name.split("|provirus")[0]
                m = re.match(r"(\d+)-(\d+)$", r.get("coordinates", "") or "")
                start, end = (int(m.group(1)), int(m.group(2))) if m else (1, asm.lengths.get(contig, int(r.get("length") or 0)))
                cls = "provirus" if m else kind
                desc = f"{cls}: {r.get('taxonomy', '')}" if kind == "virus" else \
                    f"plasmid: conjugation {r.get('conjugation_genes', '') or 'none'}; AMR {r.get('amr_genes', '') or 'none'}"
                rows.append((name, contig, start, end, ".", cls, name, desc, r.get(score, ""), json.dumps(r)))
    return element_frame(rows, "genomad") if rows else None


def parse_virsorter2(pf):
    b = concat(read_tsv(p) for p in pf.get("annotation::virsorter2_boundary") if Path(p).stat().st_size)
    if b is None:
        return None
    s = concat(read_tsv(p) for p in pf.get("annotation::virsorter2_scores") if Path(p).stat().st_size)
    s = (s.assign(seqname=s["seqname"].map(nobatch)).drop_duplicates("seqname").set_index("seqname")
         if s is not None and "seqname" in s.columns else pd.DataFrame())
    seq = "seqname" if "seqname" in b.columns else "seqname_new"
    rows = []
    for r in b.to_dict("records"):
        new = nobatch(r.get("seqname_new") or r[seq])
        sc = s.loc[new].to_dict() if new in s.index else {}
        start = r.get("trim_bp_start") or r.get("full_bp_start")
        end = r.get("trim_bp_end") or r.get("full_bp_end")
        score = sc.get("max_score") or r.get("trim_pr_max") or r.get("trim_pr") or r.get("pr_full")
        group = sc.get("max_score_group") or r.get("final_max_score_group") or r.get("group", "")
        rows.append((new, nobatch(r[seq]).split("||")[0], int(float(start)), int(float(end)), ".", "virus", new, group, score,
                     json.dumps({**r, **{f"score_{k}": v for k, v in sc.items()}})))
    return element_frame(rows, "virsorter2") if rows else None


def parse_integronfinder(pf):
    rows = []
    for path in pf.get("annotation::integronfinder_integrons"):
        lines = [l for l in open(path) if not l.startswith("#") and l.strip()]
        if len(lines) < 2:
            continue
        df = pd.read_csv(io.StringIO("".join(lines)), sep="\t", dtype=str, keep_default_na=False)
        for (replicon, integron), g in df.groupby(["ID_replicon", "ID_integron"], sort=False):
            replicon = nobatch(replicon)
            start, end = num(g["pos_beg"]).min(), num(g["pos_end"]).max()
            kinds = g["type_elt"].value_counts().to_dict() if "type_elt" in g else {}
            annot = g["annotation"].value_counts().to_dict() if "annotation" in g else {}
            itype = g["type"].iloc[0] if "type" in g else ""
            desc = f"{itype}; " + ", ".join(f"{k} x{v}" for k, v in annot.items())
            ev = num(g["evalue"]).min() if "evalue" in g else np.nan
            rows.append((f"{replicon}:{integron}", replicon, int(start), int(end), ".", "integron",
                         f"{replicon}:{integron}", desc, ev, json.dumps({"elements": kinds, "annotations": annot})))
    return element_frame(rows, "integronfinder") if rows else None


def parse_mobileelementfinder(pf):
    rows = []
    for path in pf.get("annotation::mobileelementfinder_results"):
        lines = [l for l in open(path) if not l.startswith("#") and l.strip()]
        if len(lines) < 2:
            continue
        df = pd.read_csv(io.StringIO("".join(lines)), sep="\t", dtype=str, keep_default_na=False)
        for i, r in enumerate(df.to_dict("records")):
            contig = nobatch(r["contig"])
            key = f"{contig}:{r.get('start')}-{r.get('end')}:{r.get('name')}"
            desc = "; ".join(x for x in (r.get("type", ""), r.get("prediction", "")) if x)
            rows.append((key, contig, int(r["start"]), int(r["end"]), ".", "MGE", r.get("name", ""), desc,
                         r.get("identity", ""), json.dumps(r)))
    return element_frame(rows, "mobileelementfinder") if rows else None


def parse_antismash(pf, asm):
    rows, notes = [], []
    for path in pf.get("annotation::antismash_json"):
        with open(path) as fh:
            data = json.load(fh)
        records = data.get("records", [])
        names = [r.get("id", "") for r in records]
        lengths = {r.get("id", ""): len(r.get("seq", {}).get("data", "")) for r in records if r.get("seq", {}).get("data")}
        tr = asm.translate(names, lengths, by_order=not all(n in asm.lengths for n in names))
        for rec in records:
            contig = tr.get(rec.get("id", ""))
            if contig is None:
                notes.append(f"antismash: record {rec.get('id')} not matched to a contig")
                continue
            for feat in rec.get("features", []):
                if feat.get("type") != "region":
                    continue
                m = re.search(r"\[<?(\d+):>?(\d+)\]", feat.get("location", ""))
                if not m:
                    continue
                q = feat.get("qualifiers", {})
                num_ = (q.get("region_number") or ["0"])[0]
                rid = f"{contig}.region{int(num_):03d}"
                products = ",".join(q.get("product", []))
                rows.append((rid, contig, int(m.group(1)) + 1, int(m.group(2)), ".", "BGC", rid, products, np.nan,
                             json.dumps({"contig_edge": q.get("contig_edge", [""])[0], "antismash_record": rec.get("id")})))
    return (element_frame(rows, "antismash") if rows else None), notes


class Features:
    def __init__(self, frames, mags, asm):
        df = concat(frames)
        if df is None:
            sys.exit("no features found")
        df["start"], df["end"] = df["start"].astype(int), df["end"].astype(int)
        df["pipeline"] = np.where(df["ns"].str.startswith("mp_"), "mp", "port")
        df["feature_id"] = (df["feature_type"] + ":" + df["contig"] + ":" + df["start"].astype(str) + "-"
                            + df["end"].astype(str) + ":" + df["strand"])
        dup = df.groupby(["ns", "feature_id"]).cumcount()
        df.loc[dup > 0, "feature_id"] = df.loc[dup > 0, "feature_id"] + "#" + (dup[dup > 0] + 1).astype(str)
        self.members = df
        self.index = {ns: dict(zip(g["key"], g["feature_id"])) for ns, g in df.groupby("ns")}
        ids = lambda ns: df[df["ns"] == ns].set_index("feature_id")["key"]
        joined = lambda s: ";".join(sorted(set(x for x in s if x)))
        multi = df["feature_id"].duplicated(keep=False)
        single = (df[~multi].set_index("feature_id")
                  .rename(columns={"ns": "sources", "pipeline": "pipelines"})
                  [["feature_type", "contig", "start", "end", "strand", "feature_name", "sources", "pipelines"]])
        merged = (df[multi].groupby("feature_id", sort=False)
                  .agg(feature_type=("feature_type", "first"), contig=("contig", "first"), start=("start", "first"),
                       end=("end", "first"), strand=("strand", "first"), feature_name=("feature_name", joined),
                       sources=("ns", joined), pipelines=("pipeline", joined)))
        base = pd.concat([single, merged])
        base.index.name = "feature_id"
        base["length"] = base["end"] - base["start"] + 1
        base["bin"] = base["contig"].map(mags).fillna("")
        base["port_id"] = ids("port_cds").groupby(level=0).first().reindex(base.index).fillna("")
        base["mp_id"] = ids("mp_cds").groupby(level=0).first().reindex(base.index).fillna("")
        base["element_class"] = ""
        self.base = base
        self.asm = asm

    def compare(self):
        b = self.base
        b["coord_status"], b["coord_partner"] = "not_compared", ""
        report = []
        for ftype in sorted(set(b["feature_type"])):
            sub = b[b["feature_type"] == ftype]
            pipes = sub["pipelines"]
            if not ((pipes.str.contains("port")).any() and (pipes.str.contains("mp")).any()):
                continue
            both = pipes.str.contains("port") & pipes.str.contains("mp")
            b.loc[both[both].index, "coord_status"] = "match"
            port = sub[pipes == "port"].reset_index()
            mp = sub[pipes == "mp"].reset_index()
            for q, other, side in ((port, mp, "port"), (mp, port, "mp")):
                status = pd.Series("unique", index=q["feature_id"])
                partner = pd.Series("", index=q["feature_id"])
                if ftype == "CDS":
                    qs = q.assign(stop=np.where(q["strand"] == "+", q["end"], q["start"]))
                    os_ = other.assign(stop=np.where(other["strand"] == "+", other["end"], other["start"]))
                    m = qs.merge(os_[["contig", "strand", "stop", "feature_id"]], on=["contig", "strand", "stop"],
                                 suffixes=("", "_p")).drop_duplicates("feature_id")
                    status[m["feature_id"]] = "same_stop_diff_start"
                    partner[m["feature_id"]] = m["feature_id_p"].values
                rest = q[status.loc[q["feature_id"]].values == "unique"]
                o = other.assign(k=other["contig"] + "\t" + other["strand"]).sort_values(["k", "start"], kind="stable")
                ok, os_start, os_end, os_id = o["k"].values, o["start"].values, o["end"].values, o["feature_id"].values
                for k, g in rest.groupby(rest["contig"] + "\t" + rest["strand"]):
                    lo, hi = np.searchsorted(ok, k, "left"), np.searchsorted(ok, k, "right")
                    for fid, s, e in zip(g["feature_id"], g["start"], g["end"]):
                        i = lo + np.searchsorted(os_start[lo:hi], e, side="right")
                        best, bo = 0, None
                        for j in range(max(lo, i - 8), i):
                            ov = min(e, os_end[j]) - max(s, os_start[j]) + 1
                            if ov > best:
                                best, bo = ov, j
                        if bo is not None:
                            status[fid], partner[fid] = "overlap", os_id[bo]
                b.loc[status.index, "coord_status"] = status.values
                b.loc[partner.index, "coord_partner"] = partner.values
                if ftype == "CDS":
                    rep = q[["feature_id", "contig", "start", "end", "strand"]].copy()
                    rep["pipeline"] = side
                    rep["native_id"] = rep["feature_id"].map(b["port_id" if side == "port" else "mp_id"])
                    rep["status"] = status.loc[rep["feature_id"]].values
                    rep["partner"] = partner.loc[rep["feature_id"]].values
                    rep["partner_native_id"] = rep["partner"].map(b["mp_id" if side == "port" else "port_id"]).fillna("")
                    pb = b.reindex(rep["partner"])
                    rep["start_delta"] = (pb["start"].values - rep["start"].values)
                    rep["end_delta"] = (pb["end"].values - rep["end"].values)
                    report.append(rep)
        return concat(report)

    def contained(self, elements):
        if not hasattr(self, "_by_position"):
            b = self.base[self.base["feature_type"] != "element"].sort_values(["contig", "start"], kind="stable")
            self._by_position = (b["contig"].values, b["start"].values, b["end"].values, b.index.values)
        contigs, starts, ends, ids = self._by_position
        pairs = []
        for fid, contig, s, e in zip(elements["feature_id"], elements["contig"], elements["start"], elements["end"]):
            lo, hi = np.searchsorted(contigs, contig, "left"), np.searchsorted(contigs, contig, "right")
            window = starts[lo:hi]
            a, z = lo + np.searchsorted(window, s, "left"), lo + np.searchsorted(window, e, "right")
            pairs.extend((ids[i], fid) for i in a + np.flatnonzero(ends[a:z] <= e))
        return pd.DataFrame(pairs, columns=["feature_id", "element_id"])


def extras_json(df):
    if "raw_json" in df.columns:
        return df["raw_json"].fillna("").astype(str)
    cols = [c for c in df.columns if c not in STD + ["feature_id", "rank", "feature_type"]]
    if not cols or not len(df):
        return pd.Series("", index=df.index)
    return pd.Series(df[cols].to_json(orient="records", lines=True, force_ascii=False).splitlines(), index=df.index)


def text(s):
    return s.fillna("").astype(str)


def write_tsv(df, target, **kw):
    clean = df.copy()
    for c in clean.columns:
        if clean[c].dtype == object:
            clean[c] = clean[c].astype(str).str.replace(r"[\t\r\n]", " ", regex=True)
    clean.to_csv(target, sep="\t", index=False, quoting=csv.QUOTE_NONE, quotechar="\x01", **kw)


class Writer:
    def __init__(self, out, sample):
        self.sample = sample
        self.pq = pq.ParquetWriter(out / "annotations_long.parquet", LONG_SCHEMA, compression="zstd")
        self.tsv = gzip.open(out / "annotations_long.tsv.gz", "wt")
        self.header = True
        self.rows = defaultdict(int)

    def write(self, lane, df, chunk=500_000):
        for i in range(0, len(df), chunk):
            self.write_chunk(lane, df.iloc[i:i + chunk])

    def write_chunk(self, lane, df):
        out = pd.DataFrame({"sample": self.sample, "feature_id": text(df["feature_id"]),
                            "feature_type": text(df["feature_type"]), "lane": lane,
                            "rank": df["rank"].astype("int32"), "id": text(df["id"]), "desc": text(df["desc"]),
                            "conf": num(df["conf"]).astype("float64"), "key": text(df["key"]),
                            "extras": extras_json(df)})
        self.pq.write_table(pa.Table.from_pandas(out, schema=LONG_SCHEMA, preserve_index=False))
        write_tsv(out, self.tsv, header=self.header)
        self.header = False
        self.rows[lane] += len(out)

    def close(self):
        self.pq.close()
        self.tsv.close()


def rank_hits(lane, hits):
    direction = LANES.get(lane, (None,) * 9)[8]
    hits = hits.rename(columns={"rank": "source_rank"})
    hits["conf"] = num(hits["conf"])
    if "_rank" in hits.columns:
        hits["rank"] = hits.pop("_rank").astype(int)
        return hits
    if direction in (HIGH, LOW):
        hits = hits.sort_values(["key", "conf"], ascending=[True, direction == LOW], na_position="last", kind="stable")
    hits["rank"] = hits.groupby("key", sort=False).cumcount() + 1
    return hits


def cds_lanes(pf, mp, pgdb_tables, gpr_cache):
    if pf:
        p = pf.get("annotation::diamond_uniref50_results")
        if p:
            df = concat(read_tsv(x) for x in p)
            desc = concat(read_tsv(x) for x in pf.get("annotation::diamond_uniref50_descriptions"))
            names = dict(zip(desc["sseqid"], desc["description"])) if desc is not None else {}
            df["desc"] = df["sseqid"].map(names).fillna("").str.replace(r"^\S+\s+", "", regex=True)
            yield "diamond_uniref50", df.rename(columns={"qseqid": "key", "sseqid": "id", "bsr": "conf"}).assign(ns="port_cds")
        p = pf.get("annotation::kofamscan_results")
        if p:
            df = concat(pd.read_csv(x, dtype=str, keep_default_na=False) for x in p)
            d = concat(pd.read_csv(x, dtype=str, keep_default_na=False) for x in pf.get("annotation::kofamscan_descriptions"))
            names = dict(zip(d["KO"], d["definition"])) if d is not None else {}
            df = df.rename(columns={"gene_name": "key", "KO": "id", "score": "conf"})
            df["desc"] = df["id"].map(names).fillna("")
            yield "kofam", df.assign(ns="port_cds")
        p = pf.get("annotation::clean_predictions")
        if p:
            df = concat(read_tsv(x) for x in p)
            yield "clean", df.rename(columns={"Query ID": "key", "Predicted EC number": "id", "clean_score": "conf"}).assign(ns="port_cds", desc="")
        p = pf.get("annotation::deepec_predictions", "annotation::deepec_predictions_chunk")
        if p:
            df = concat(read_tsv(x) for x in p)
            ec = next((c for c in df.columns if "EC" in c), df.columns[1])
            df = df.rename(columns={df.columns[0]: "key", ec: "id"})
            df["id"] = df["id"].str.replace("EC:", "", regex=False)
            yield "deepec", df.assign(ns="port_cds", desc="", conf=np.nan)
        p = pf.get("annotation::ezpred_predictions")
        if p:
            df = concat(c[num(c["score"]) >= EZPRED_FLOOR] for x in p
                        for c in pd.read_csv(x, dtype=str, keep_default_na=False, chunksize=1_000_000))
            df = df.rename(columns={"sequence_id": "key", "ec_number": "id", "score": "conf"})
            yield "ezpred", df.assign(ns="port_cds", desc=df.get("head_kind", ""))
        for lane, dtype in (("gpr", "annotation::gpr_table"), ("gpr7", "annotation::gpr_table_7lane")):
            p = pf.get(dtype)
            if p:
                df = concat(pd.read_parquet(x) for x in p)
                gpr_cache[lane] = df
                yield lane, gpr_hits(df)
        src = gpr_cache.get("gpr7") if "gpr7" in gpr_cache else gpr_cache.get("gpr")
        if src is not None:
            for lane, channel in (("proteinbert", "pbert"), ("esm_c", "esmc")):
                ch = src[src["channel"].isin([channel, channel + "_transfer"])]
                if len(ch):
                    df = ch.rename(columns={"orf": "key", "mnxr": "id", "raw_score": "conf"}).astype({"id": str})
                    df["desc"] = (df["intermediate_id"].astype(str) + " " + df["intermediate_name"].astype(str)).str.strip()
                    yield lane, df.assign(ns="port_cds")
        pairs = embedding_pairs(pf)
        landmarks = next((p for p in pf.files.get("ref::label_transfer_landmarks_esmc", ()) if (p / "landmarks.parquet").is_file()), None)
        lookup = next((p for p in pf.files.get("ref::mnxr_lookup", ()) if p.is_file()), None)
        if pairs and (landmarks is None or lookup is None):
            print(f"esmc_transfer: embeddings found but no landmark set ({landmarks}) or mnxr_lookup ({lookup})", file=sys.stderr)
        elif pairs:
            yield "esmc_transfer", esmc_transfer(pairs, landmarks, lookup)
        p = pf.get("annotation::deeptfactor_results")
        if p:
            df = concat(read_tsv(x) for x in p)
            pred = next((c for c in df.columns if c.lower().startswith("prediction")), df.columns[1])
            df = df[df[pred].str.lower().isin(("true", "1", "tf"))]
            df = df.rename(columns={df.columns[0]: "key", next((c for c in df.columns if "score" in c.lower()), pred): "conf"})
            yield "deeptfactor", df.assign(ns="port_cds", id="TF", desc="")
        p = pf.get("annotation::amrfinderplus_results")
        if p:
            df = concat(read_tsv(x) for x in p)
            pick = lambda *names: next((c for c in names if c in df.columns), None)
            key, sym, name = pick("Protein id", "Protein identifier", "Protein_id"), pick("Element symbol", "Gene symbol"), pick("Element name", "Sequence name")
            ident = pick("% Identity to reference", "% Identity to reference sequence")
            cls = [c for c in ("Type", "Class", "Subclass") if c in df.columns]
            df["desc"] = df[name] + " [" + df[cls].agg("/".join, axis=1) + "]" if cls else df[name]
            yield "amrfinderplus", df.rename(columns={key: "key", sym: "id", ident: "conf"}).assign(ns="port_cds")
        blast6 = ["key", "id", "pident", "length", "mismatch", "gapopen", "qstart", "qend", "sstart", "send", "evalue", "conf", "desc"]
        for lane, dtype in (("bacmet", "annotation::bacmet_diamond_results"), ("megares", "annotation::megares_diamond_results"),
                            ("vfdb", "annotation::vfdb_diamond_results"), ("tcdb", "annotation::diamond_tcdb_results")):
            p = pf.get(dtype)
            if p:
                df = concat(pd.read_csv(x, sep="\t", header=None, names=blast6, dtype=str, keep_default_na=False) for x in p if Path(x).stat().st_size)
                if df is not None:
                    yield lane, df.assign(ns="port_cds")
        p = pf.get("annotation::pathofact_amr")
        if p:
            df = concat(read_tsv(x) for x in p)
            if df is not None and len(df):
                orf = next((c for c in df.columns if c.upper() in ("ORF", "ORF_ID")), df.columns[1])
                arg = next((c for c in df.columns if c.upper() in ("ARG", "ARG_PREDICTION")), df.columns[2])
                cats = [c for c in df.columns if "categor" in c.lower() or "class" in c.lower() or "mechanism" in c.lower()]
                df["desc"] = df[cats].agg("; ".join, axis=1) if cats else ""
                yield "pathofact", df.rename(columns={orf: "key", arg: "id"}).assign(ns="port_cds", conf=np.nan)
        p = pf.get("annotation::eggnog_results")
        if p:
            frames = []
            for x in p:
                lines = [l for l in open(x) if not l.startswith("##")]
                if lines:
                    lines[0] = lines[0].lstrip("#")
                    frames.append(pd.read_csv(io.StringIO("".join(lines)), sep="\t", dtype=str, keep_default_na=False))
            df = concat(frames)
            if df is not None:
                df["desc"] = (df.get("Preferred_name", "").replace("-", "") + ": " + df.get("Description", "")).str.strip(": ")
                yield "eggnog_mapper", df.rename(columns={"query": "key", "seed_ortholog": "id", "score": "conf"}).assign(ns="port_cds")
        p = pf.get("annotation::interproscan_results")
        if p:
            df = concat(pd.read_csv(x, dtype=str, keep_default_na=False) for x in p)
            d = concat(pd.read_csv(x, dtype=str, keep_default_na=False) for x in pf.get("annotation::interproscan_descriptions"))
            names = dict(zip(d["Name"], d["description"])) if d is not None else {}
            df["desc"] = df["Name"].map(names).fillna("") + np.where(df["Dbxref"] != "", " [" + df["Dbxref"] + "]", "")
            yield "interproscan", df.rename(columns={"orf": "key", "Name": "id", "score": "conf"}).assign(ns="port_cds")
    if mp:
        for path in mp.glob("blast_results/{s}.*.FASTout.parsed.txt") + mp.glob("blast_results/{s}.*.BLASTout.parsed.txt"):
            db = path.name[len(mp.sample) + 1:].split(".")[0]
            df = read_tsv(path).rename(columns={"#query": "key", "target": "id", "product": "desc", "bsr": "conf"})
            df["key"] = df["key"].map(mp_orf)
            yield (db if db in MP_DBS else f"mp_{db}"), df.assign(ns="mp_cds")
        ft = mp.path("results/annotation_table/{s}.functional_and_taxonomic_table.txt")
        if ft:
            df = read_tsv(ft)
            df["key"] = df["ORF_ID"].map(mp_orf)
            yield "mp_product", df.rename(columns={"target": "id", "product": "desc"}).assign(ns="mp_cds", conf=np.nan)[
                ["ns", "key", "id", "desc", "conf", "reference_db", "taxonomy", "lca_taxonomy"]]
            src = df.set_index("key")["reference_db"].to_dict()
            tax = mp.path("results/annotation_table/{s}.annotation_taxonomy.tsv")
            if tax:
                t = read_tsv(tax)
                t["key"] = t["orf_id"].map(mp_orf)
                t = t.drop_duplicates(["key", "reference_db", "lca_taxonomy"])
                t["primary"] = (t["reference_db"] == t["key"].map(src)).astype(int)
                t = t.sort_values(["key", "primary"], ascending=[True, False], kind="stable")
                lca = t.rename(columns={"lca_taxonomy": "id", "reference_db": "desc"})[["key", "id", "desc"]]
            else:
                lca = df.rename(columns={"lca_taxonomy": "id", "reference_db": "desc"})[["key", "id", "desc"]]
            yield "mp_lca", lca.assign(ns="mp_cds", conf=np.nan)
    for path in pgdb_tables:
        t = pd.read_parquet(path)
        for (pipeline, etype), g in t.groupby(["pipeline", "entity_type"]):
            lane = f"{pipeline}_pathologic" + ("_mag" if etype == "mag" else "")
            g = g[g["reaction"] != ""]
            keys = g["orf_id"].map(mp_orf) if pipeline == "mp" else g["orf_id"]
            desc = np.where(g["pathway"] != "", g["pathway"] + " (" + g["pathway_name"] + ")", "")
            df = pd.DataFrame({"ns": "mp_cds" if pipeline == "mp" else "port_cds", "key": keys.values, "id": g["reaction"].values,
                               "desc": desc, "conf": np.nan, "pathway": g["pathway"].values, "ec": g["ec"].values,
                               "product": g["product"].values, "enzrxn_name": g["enzrxn_name"].values,
                               "entity": g["entity"].values, "pgdb_id": g["pgdb_id"].values,
                               "represented_by": g["represented_by"].values})
            yield lane, df


def dim_columns(names, path):
    dims = sorted((c for c in names if c.startswith("dim_")), key=lambda c: int(c[4:]))
    if dims != [f"dim_{i}" for i in range(len(dims))]:
        sys.exit(f"{path} does not carry dim_0..dim_{len(dims) - 1} contiguously")
    return dims


def unit_rows(x):
    return x / np.clip(np.linalg.norm(x, axis=1, keepdims=True), 1e-9, None)


def embedding_pairs(pf):
    index = defaultdict(list)
    for p in pf.get("annotation::esm_c_index"):
        index[p.parent].append(p)
        index[p.name.rsplit("-", 1)[0]].append(p)
    pairs = []
    for emb in pf.get("annotation::esm_c_embeddings"):
        match = index.get(emb.parent) or index.get(emb.name.rsplit("-", 1)[0]) or []
        if len(match) != 1:
            sys.exit(f"{emb}: {len(match)} candidate ESM-C index files, cannot pair it by row")
        pairs.append((emb, match[0]))
    return pairs


def esmc_transfer(pairs, landmark_dir, lookup_path):
    table = Path(landmark_dir) / "landmarks.parquet"
    dims = dim_columns(pq.ParquetFile(table).schema_arrow.names, table)
    lm = pq.read_table(table, columns=["accession", "mnxr_list"] + dims).to_pandas()
    ref = unit_rows(lm[dims].to_numpy(dtype=np.float32))
    accession = lm["accession"].to_numpy()
    label_lists = [sorted(set(m for m in s.split(";") if m)) for s in lm["mnxr_list"].fillna("")]
    del lm
    vocab = np.asarray(sorted({m for ls in label_lists for m in ls}), dtype=object)
    vidx = {m: i for i, m in enumerate(vocab)}
    lab_ptr = np.zeros(len(label_lists) + 1, dtype=np.int64)
    np.cumsum([len(ls) for ls in label_lists], out=lab_ptr[1:])
    lab_idx = np.fromiter((vidx[m] for ls in label_lists for m in ls), dtype=np.int64, count=int(lab_ptr[-1]))
    ec = pq.read_table(lookup_path, columns=["id", "id_source", "mnxr"], filters=[("id_source", "==", "ec")]).to_pandas()
    ec = ec[ec["id"].str.match(EC4)]
    ec_of = ec.groupby("mnxr")["id"].agg(lambda s: ",".join(sorted(set(s)))).to_dict()

    k = min(ESMC_TRANSFER_K, len(ref) - 1)
    ids, frames, offset = [], [], 0
    for emb_path, idx_path in pairs:
        idx = pd.read_csv(idx_path, dtype=str, keep_default_na=False)
        orfs = idx["sequence_id" if "sequence_id" in idx.columns else "id"].to_numpy()
        source = pq.ParquetFile(emb_path)
        if source.metadata.num_rows != len(orfs):
            sys.exit(f"{idx_path} has {len(orfs)} rows and {emb_path} has {source.metadata.num_rows}; they pair by row")
        cols = dim_columns(source.schema_arrow.names, emb_path)
        if len(cols) != ref.shape[1]:
            sys.exit(f"{emb_path} is {len(cols)} dims and the ESM-C landmarks are {ref.shape[1]}")
        ids.append(orfs)
        start = offset
        for batch in source.iter_batches(batch_size=512, columns=cols):
            q = unit_rows(np.column_stack([c.to_numpy(zero_copy_only=False) for c in batch.columns]).astype(np.float32))
            sim = q @ ref.T
            top = np.argpartition(-sim, k, axis=1)[:, :k]
            cos = np.take_along_axis(sim, top, axis=1)
            order = np.argsort(-cos, axis=1, kind="stable")
            top, cos = np.take_along_axis(top, order, axis=1), np.take_along_axis(cos, order, axis=1)
            qi, nb = np.nonzero(cos >= ESMC_TRANSFER_FLOOR)
            li = top[qi, nb]
            cnt = lab_ptr[li + 1] - lab_ptr[li]
            rep = np.repeat(np.arange(len(li)), cnt)
            within = np.arange(int(cnt.sum())) - np.repeat(np.cumsum(cnt) - cnt, cnt)
            frames.append(pd.DataFrame({"q": start + qi[rep], "m": lab_idx[lab_ptr[li][rep] + within],
                                        "l": li[rep], "cos": cos[qi, nb][rep], "nb": nb[rep] + 1}))
            start += len(q)
        offset = start
    ids = np.concatenate(ids) if ids else np.array([], dtype=object)
    hits = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["q", "m", "l", "cos", "nb"])
    hits = hits.sort_values(["q", "m", "cos"], ascending=[True, True, False], kind="stable")
    support = hits.groupby(["q", "m"], sort=False).size().to_numpy()
    hits = hits.drop_duplicates(["q", "m"])
    mnxr = vocab[hits["m"].to_numpy()]
    print(f"esmc_transfer: {len(ids)} ORFs, {hits['q'].nunique()} with a landmark at cosine >= {ESMC_TRANSFER_FLOOR}", flush=True)
    return pd.DataFrame({"ns": "port_cds", "key": ids[hits["q"].to_numpy()], "id": mnxr,
                         "desc": pd.Series(mnxr).map(ec_of).fillna("").to_numpy(), "conf": hits["cos"].to_numpy(np.float64),
                         "landmark": accession[hits["l"].to_numpy()], "neighbour": hits["nb"].to_numpy(),
                         "landmarks": support})


def gpr_hits(df):
    g = df.rename(columns={"orf": "key", "mnxr": "id", "raw_score": "conf"}).astype({"id": str, "key": str})
    support = g.groupby(["key", "id"])["channel"].agg(lambda s: ",".join(sorted(set(s))))
    best = g.groupby(["key", "id"])["conf"].max()
    order = pd.DataFrame({"channels": support, "n": support.str.count(",") + 1, "best": best}).reset_index()
    order = order.sort_values(["key", "n", "best"], ascending=[True, False, False], kind="stable")
    order["_rank"] = order.groupby("key").cumcount() + 1
    g = g.merge(order[["key", "id", "_rank", "channels", "n"]], on=["key", "id"])
    g["desc"] = g["channel"] + ":" + g["intermediate_id"].astype(str) + " " + g["intermediate_name"].astype(str)
    g = g.sort_values(["key", "_rank", "conf"], ascending=[True, True, False], kind="stable").assign(ns="port_cds")
    g.attrs["wide"] = order[order["_rank"] == 1].rename(columns={"channels": "desc", "n": "conf"})[["key", "id", "desc", "conf"]]
    return g


def rna_lanes(pf, mp):
    if pf:
        for path in pf.get("taxonomy::rrna_gtdb"):
            df = read_tsv(path)
            df["id"] = df["lineage"].map(last_taxon)
            yield "rrna_gtdb", df.rename(columns={"gene": "key", "lineage": "desc", "best_identity": "conf"}).assign(ns="port_rrna")
        for path in pf.get("annotation::trna_gff"):
            rows = [("port_trna", a.get("ID", ""), a.get("product", ""), a.get("anticodon", ""), np.nan, c)
                    for c, t, s, e, st, _, a in gff_rows(path)]
            yield "aragorn", pd.DataFrame(rows, columns=STD + ["contig"])
    if mp:
        rows = []
        for path in mp.glob("results/rRNA/{s}.*.rRNA.stats.txt"):
            db = path.name[len(mp.sample) + 1:].split(".rRNA.stats")[0]
            for line in open(path):
                f = line.rstrip("\n").split("\t")
                if line.startswith("#") or len(f) < 7 or f[0] in ("", "sequence") or f[1] == "-":
                    continue
                m = re.search(r"([^:]+):(\d+)-(\d+)\(([+-])\)$", f[0].split("::")[-1])
                if not m:
                    continue
                key = f"{mp.contig(m.group(1))}:{m.group(2)}-{m.group(3)}({m.group(4)})"
                rows.append(("mp_rrna", key, last_taxon(f[6]), f[6], f[3], db, f[1], f[2], f[4], f[5]))
        if rows:
            yield "silva", pd.DataFrame(rows, columns=STD + ["db", "aln_start", "aln_end", "evalue", "bitscore"])
        t = mp_trna_table(mp)
        if t is not None:
            yield "mp_trna", t[STD + ["raw"]]


def contig_lanes(pf):
    if not pf:
        return
    for path in pf.get("taxonomy::contig_lcastar"):
        df = read_tsv(path)
        df["id"] = df["lineage"].map(last_taxon)
        yield "contig_lcastar", df.rename(columns={"contig": "key", "lineage": "desc", "p_value": "conf"})
    for path in pf.get("taxonomy::metabuli"):
        with open(path) as fh:
            header = fh.readline().lstrip("#").rstrip("\n").split("\t")
            rows = [f[:len(header)] for f in (line.rstrip("\n").split("\t") for line in fh) if f[0] == "1"]
        df = pd.DataFrame(rows, columns=header)
        df["lineage"] = df["lineage"].str.replace(r"(^|;)([a-z]{1,2})_\2__", r"\1\2__", regex=True)
        df["id"] = df["lineage"].str.replace(r";ss_[^;]*$", "", regex=True).map(last_taxon)
        yield "metabuli", df.rename(columns={"name": "key", "lineage": "desc", "score": "conf"})


def lane_presence(pf, mp, bins, pgdb_tables, counts):
    pgdb = defaultdict(set)
    for path in pgdb_tables:
        t = pd.read_parquet(path, columns=["pipeline", "entity_type", "entity"]).drop_duplicates()
        for pipeline, etype, entity in t.itertuples(index=False):
            pgdb[(pipeline, etype)].add(entity)
    rows, missing_bins = [], []
    for lane, info in LANES.items():
        pipeline = "port" if info[0] == "port" else "mp"
        hits, features = counts.get(lane, (0, 0))
        pathologic = lane.startswith(f"{pipeline}_pathologic")
        mag = lane.endswith("_mag")
        if (pf if pipeline == "port" else mp) is None:
            files, detail = [], f"no {'--port-run' if pipeline == 'port' else '--mp-out'} given"
        elif pipeline == "port":
            files = [p for d in SOURCES[lane] for p in pf.files.get(d, ()) if p.is_file()]
            detail = " or ".join(SOURCES[lane])
        else:
            files = [p for pattern in SOURCES[lane] for p in mp.glob(pattern)]
            detail = " or ".join(SOURCES[lane])
        files = list({(f.stat().st_dev, f.stat().st_ino): f for f in files}.values())
        etype = "mag" if mag else "community"
        have = pgdb.get((pipeline, etype), set())
        if mag and not bins:
            status, detail = "not_applicable", "no bins in --mag-map"
        elif hits and not features:
            status, detail = "UNMAPPED", f"{hits} hits, none matched a feature"
        elif hits:
            status = "ok"
            if mag and bins - have:
                gone = sorted(bins - have)
                missing_bins += [(pipeline, b) for b in gone]
                status, detail = "partial", f"{len(have & bins)} of {len(bins)} bins have a PGDB; missing {';'.join(gone)}"
        elif pathologic and files and not have:
            status, detail = "MISSING", f"{len(files)} archives, but no {pipeline} {etype} rows in --pgdb-table (empty PGDB, or no table)"
        elif files:
            status, detail = "EMPTY", f"{len(files)} source files hold no hits"
        else:
            status, detail = "MISSING", f"no file for {detail}"
        rows.append({"lane": lane, "pipeline": pipeline, "status": status, "hits": hits, "features": features,
                     "source_files": len(files), "detail": detail})
    return pd.DataFrame(rows), missing_bins


def wide_of(lane, hits, override=None):
    if override is not None:
        w = override.merge(hits[["key", "feature_id"]].drop_duplicates("key"), on="key")
    elif lane in JOINED_LANES:
        j = lambda s: ";".join(dict.fromkeys(x for x in s if x))
        w = hits.groupby("feature_id", sort=False).agg(id=("id", j), desc=("pathway", j), conf=("conf", "first")).reset_index()
    else:
        w = hits[hits["rank"] == 1].drop_duplicates("feature_id")
    return w.set_index("feature_id")[["id", "desc", "conf"]].add_prefix(f"{lane}_")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample")
    ap.add_argument("--assembly")
    ap.add_argument("--out-dir")
    ap.add_argument("--port-run", action="append", default=[])
    ap.add_argument("--mp-out")
    ap.add_argument("--pgdb-table", action="append", default=[])
    ap.add_argument("--mag-map")
    ap.add_argument("--write-legend")
    args = ap.parse_args()
    if args.write_legend:
        write_legend(args.write_legend)
        return
    if not (args.sample and args.assembly and args.out_dir):
        ap.error("--sample, --assembly and --out-dir are required")

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_legend(out / "lane_legend.tsv")
    asm = Assembly(args.assembly)
    mags = {}
    if args.mag_map:
        with open(args.mag_map) as fh:
            mags = dict(line.rstrip("\n").split("\t")[:2] for line in fh if "\t" in line)
    pf = PortFiles(args.port_run, args.assembly) if args.port_run else None
    mp = MPFiles(args.mp_out, args.sample) if args.mp_out else None
    notes = list(pf.notes) if pf else []

    frames, element_hits, self_hits = [], {}, {}
    if pf:
        frames += features_port(pf)
        bfeat, bhits = parse_bakta(pf, asm)
        if bfeat is not None:
            frames.append(bfeat)
            self_hits["bakta_ncrna"] = bhits[0]
            notes += [bhits[1]] if bhits[1] else []
        pfeat, phits = parse_promotech(pf)
        if pfeat is not None:
            frames.append(pfeat)
            self_hits["promotech"] = phits
        for lane, parsed in (("genomad", parse_genomad(pf, asm)), ("virsorter2", parse_virsorter2(pf)),
                             ("integronfinder", parse_integronfinder(pf)), ("mobileelementfinder", parse_mobileelementfinder(pf))):
            if parsed is not None:
                element_hits[lane] = parsed
        am, am_notes = parse_antismash(pf, asm)
        notes += am_notes
        if am is not None:
            element_hits["antismash"] = am
    if mp:
        frames += features_mp(mp)
    for lane, el in element_hits.items():
        frames.append(el.assign(feature_type="element", feature_name=el["element_class"])[FEATURE_COLS])

    feats = Features(frames, mags, asm)
    mismatches = feats.compare()
    if mismatches is not None:
        write_tsv(mismatches, out / "cds_coordinate_mismatches.tsv")
    base = feats.base
    classes = defaultdict(set)
    for lane, el in element_hits.items():
        for fid, cls in zip(el["key"].map(feats.index[lane]), el["element_class"]):
            classes[fid].add(cls)
    base["element_class"] = base.index.map(lambda f: ";".join(sorted(classes.get(f, ()))))

    writer = Writer(out, args.sample)
    wide_parts, summary, unmapped = [], [], {}

    def emit(lane, hits, ns=None):
        ns = ns or hits["ns"].iloc[0]
        override = hits.attrs.get("wide")
        hits = hits.copy()
        hits["key"] = hits["key"].astype(str)
        if ns == "port_cds":
            known = feats.index.get("port_cds", {})
            hits["key"] = hits["key"].str.replace(r"^.*~", "", regex=True)
            fix = ~hits["key"].isin(known.keys())
            hits.loc[fix, "key"] = hits.loc[fix, "key"].str.replace(r"-(\d+)$", r"_\1", regex=True)
        hits["feature_id"] = hits["key"].map(feats.index.get(ns, {}))
        miss = hits["feature_id"].isna()
        unmapped[lane] = int(miss.sum())
        hits.loc[miss, "feature_id"] = "unmapped:" + hits.loc[miss, "key"]
        hits["feature_type"] = hits["feature_id"].map(base["feature_type"]).fillna("unmapped")
        hits = rank_hits(lane, hits)
        writer.write(lane, hits)
        mapped = hits[~hits["feature_id"].str.startswith("unmapped:")]
        w = wide_of(lane, mapped, override)
        wide_parts.append(w)
        summary.append((lane, len(hits), mapped["feature_id"].nunique(), unmapped[lane]))
        return w

    for lane, hits in itertools.chain(cds_lanes(pf, mp, args.pgdb_table, {}), rna_lanes(pf, mp)):
        if hits is not None and len(hits):
            emit(lane, hits)
            print(f"{lane}\t{writer.rows[lane]} hits", flush=True)
        del hits
    for lane, hits in self_hits.items():
        if len(hits):
            emit(lane, hits)
            print(f"{lane}\t{writer.rows[lane]} hits", flush=True)

    element_lists = []
    for lane, el in element_hits.items():
        w = emit(lane, el.drop(columns=["contig", "start", "end", "strand"]).assign(ns=lane), ns=lane)
        els = base.loc[el["key"].map(feats.index[lane])].reset_index()
        inside = feats.contained(els)
        if len(inside):
            vals = inside.join(w, on="element_id")
            j = lambda s: ";".join(dict.fromkeys(str(x) for x in s if pd.notna(x) and x != ""))
            prop = vals.groupby("feature_id").agg({c: j for c in w.columns})
            prop[f"{lane}_conf"] = num(prop[f"{lane}_conf"].str.split(";").str[0])
            wide_parts.append(prop)
            element_lists.append(inside)
        print(f"{lane}\t{len(el)} elements, {len(inside)} contained features", flush=True)

    contig_wide = []
    for lane, hits in contig_lanes(pf):
        hits = hits.assign(ns="contig", key=hits["key"].astype(str))
        hits["feature_id"] = "contig:" + hits["key"]
        hits["feature_type"] = "contig"
        hits = rank_hits(lane, hits)
        writer.write(lane, hits)
        w = hits[hits["rank"] == 1].drop_duplicates("key").set_index("key")[["id", "desc", "conf"]].add_prefix(f"{lane}_")
        contig_wide.append(w)
        summary.append((lane, len(hits), hits["key"].nunique(), 0))
        print(f"{lane}\t{len(hits)} contig hits", flush=True)
    writer.close()

    base["elements"] = ""
    if element_lists:
        el = pd.concat(element_lists).groupby("feature_id")["element_id"].agg(";".join)
        base["elements"] = base.index.map(el).fillna("")
    firsts, fills, seen = [], [], set(base.columns)
    for part in wide_parts:
        new = [c for c in part.columns if c not in seen]
        seen.update(part.columns)
        firsts += [part[new].reindex(base.index)] if new else []
        fills += [part[[c for c in part.columns if c not in new]]] if len(new) < len(part.columns) else []
    contig_cols = [pd.DataFrame({c: base["contig"].map(w[c]) for c in w.columns}, index=base.index) for w in contig_wide]
    wide = pd.concat([base, *firsts, *contig_cols], axis=1)
    for part in fills:
        for c in part.columns:
            fill = part[c].reindex(wide.index)
            wide[c] = wide[c].where(wide[c].notna() & (wide[c].astype(str) != ""), fill)
    lane_order = [l for l in LANES if f"{l}_id" in wide.columns] + sorted(
        {c.rsplit("_", 1)[0] for c in wide.columns if c.endswith("_id") and c not in ("port_id", "mp_id")} - set(LANES))
    fixed = ["feature_type", "feature_name", "element_class", "contig", "start", "end", "strand", "length", "bin",
             "pipelines", "sources", "port_id", "mp_id", "coord_status", "coord_partner", "elements"]
    cols = fixed + [f"{l}_{x}" for l in lane_order for x in ("id", "desc", "conf") if f"{l}_{x}" in wide.columns]
    wide = wide[cols].reset_index().rename(columns={"index": "feature_id"})
    wide.insert(0, "sample", args.sample)
    for c in wide.columns:
        if c.endswith("_conf"):
            wide[c] = num(wide[c])
        elif wide[c].dtype == object:
            wide[c] = wide[c].fillna("").astype(str)
    wide.to_parquet(out / "annotations_wide.parquet", index=False, compression="zstd")
    write_tsv(wide, out / "annotations_wide.tsv.gz", compression="gzip")

    rows = [("features", ft, n) for ft, n in wide["feature_type"].value_counts().items()]
    rows += [("features_by_pipelines", f"{ft}|{p}", n) for (ft, p), n in wide.groupby(["feature_type", "pipelines"]).size().items()]
    rows += [("coord_status", f"{ft}|{s}", n) for (ft, s), n in wide.groupby(["feature_type", "coord_status"]).size().items()]
    rows += [("element_class", c, n) for c, n in wide.loc[wide["element_class"] != "", "element_class"].value_counts().items()]
    for lane, n, covered, miss in summary:
        rows += [("lane_hits", lane, n), ("lane_features", lane, covered), ("lane_unmapped_hits", lane, miss)]
    presence, missing_bins = lane_presence(pf, mp, set(mags.values()), args.pgdb_table,
                                           {lane: (n, covered) for lane, n, covered, _ in summary})
    presence.insert(0, "sample", args.sample)
    write_tsv(presence, out / "lane_presence.tsv")
    rows += [("lane_status", r.lane, f"{r.status}: {r.detail}" if r.status != "ok" else "ok") for r in presence.itertuples()]
    rows += [("pgdb_missing_bin", f"{p}|{b}", "") for p, b in missing_bins]
    rows += [("note", n, "") for n in notes]
    pd.DataFrame(rows, columns=["metric", "item", "value"]).to_csv(out / "summary.tsv", sep="\t", index=False)
    print(f"{len(wide)} features, {sum(writer.rows.values())} hits, {len(lane_order)} lanes -> {out}")
    for n in notes:
        print(f"NOTE {n}")
    for r in presence[presence["status"] != "ok"].itertuples():
        print(f"{r.status}\t{r.lane}\t{r.detail}", file=sys.stderr)
    failing = presence.loc[presence["status"].isin(FAILING), "lane"].tolist()
    if failing:
        sys.exit(f"{args.sample}: tables written, but lanes {', '.join(failing)} are missing or unmapped (lane_presence.tsv)")


if __name__ == "__main__":
    main()
