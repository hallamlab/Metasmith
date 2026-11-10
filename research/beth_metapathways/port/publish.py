#!/usr/bin/env python3
"""Build Beth's copper_enrichments publish tree from the port runs, the MetaPathways runs and the v1 tables.

    publish.py --out DIR [--part files|tables|logs ...] [--sample S ...]

The tree follows the engcyc layout as amended for this dataset (journal 889d37a9 in
agentic-commons seats/beth-davenport): <subject>/<product>/<stem>.<ext> per assembly or bin,
and pooled tables at the root of their subject. Unchanged files are hardlinked, so the tree
costs no extra space on scratch; files that carry the metasmith batch prefix are rewritten.
"""
import argparse
import hashlib
import os
import re
import tarfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

B = Path("/scratch/phyberos/beth_mp")
RUNS = B / "port/metasmith/runs"
TABLES = B / "tables/v1"
PARENT_KEYS = {
    "shortspades_cpe3_parent": "ZCTUtcoX ntMOjcyE 8aZeOJbC hry0wsb1 9JCtmmXA 61bdnq8M C9htVb7I",
    "operams_cpe3_parent": "S2d4bcXS En2PqajD uI0sXQoo txd7EpYX Gu3BEj2k wWPiMjhU sTKo4WlC",
}
ASSEMBLER = {"shortspades": ("metaSPAdes", "short"), "hybridspades": ("hybridSPAdes", "short+long"),
             "operams": ("OPERA-MS", "short+long")}

# metasmith result type -> (subject, product, extension or None to keep the source's)
PORT = {
    "sequences-orfs": ("assembly", "prodigal_orfs", None),
    "taxonomy-contig_lcastar": ("taxonomy_contigs", "contig_lcastar", None),
    "taxonomy-rrna_gtdb": ("taxonomy_contigs", "rrna_gtdb", None),
    "annotation-diamond_uniref50_results": ("annotation_orfs", "diamond_uniref50", None),
    "annotation-gpr_table": ("annotation_orfs", "gpr", None),
    "annotation-gpr_table_7lane": ("annotation_orfs", "gpr7", None),
    "annotation-eggnog_results": ("annotation_orfs", "eggnog_mapper", None),
    "annotation-interproscan_results": ("annotation_orfs", "interproscan", None),
    "annotation-deeptfactor_results": ("annotation_orfs", "deeptfactor", None),
    "annotation-diamond_tcdb_results": ("annotation_orfs", "tcdb", None),
    "annotation-rrna_gff": ("annotation_contigs", "barrnap_rrna", None),
    "annotation-trna_gff": ("annotation_contigs", "aragorn", None),
    "annotation-bakta_gff": ("annotation_contigs", "bakta_ncrna", "gff"),
    "annotation-promotech_predictions": ("annotation_contigs", "promotech", None),
    "annotation-antismash_json": ("annotation_contigs", "antismash", None),
    "annotation-amrfinderplus_results": ("amr", "amrfinderplus", None),
    "annotation-bacmet_diamond_results": ("amr", "bacmet", None),
    "annotation-megares_diamond_results": ("amr", "megares", None),
    "annotation-vfdb_diamond_results": ("amr", "vfdb", None),
    "annotation-pathofact_amr": ("amr", "pathofact_amr", None),
    "annotation-integronfinder_summary": ("mge", "integronfinder", None),
    "annotation-mobileelementfinder_results": ("mge", "mobileelementfinder", "tsv"),
    "annotation-virsorter2_scores": ("viral", "virsorter2_scores", None),
    "taxonomy-genomad_virus_summary": ("viral", "genomad_virus_summary", None),
    "annotation-pathway_table": ("models", "pathologic_pathways", None),
    "annotation-pgdb_archive": ("models", "pathologic_pgdb", None),
}
PORT_BIN = {
    "annotation-bin_pathway_table": ("models", "pathologic_pathways"),
    "annotation-bin_pgdb_archive": ("models", "pathologic_pgdb"),
}
BATCH_PREFIX = re.compile(r"[A-Za-z0-9_.-]+~")


def mp_files(S):
    """(subject, product, stem-suffix, source) for one MetaPathways run."""
    M = B / f"mp4/runs/{S}/{S}"
    if not M.is_dir():
        M = B / f"mp4/out/{S}"
    if not M.is_dir():
        return
    yield "assembly", "metapathways_orfs", ".faa", M / f"orf_prediction/{S}.faa"
    yield "assembly", "metapathways_gff", ".gff", M / f"orf_prediction/{S}.cds.gff"
    for db in ("swissprot", "uniref50", "uniref90", "cazy", "eggnog", "metacyc"):
        yield "annotation_orfs", "metapathways_fast", f".{db}.tsv", M / f"blast_results/{S}.{db}.FASTout.parsed.txt"
    yield "annotation_orfs", "metapathways_functional", ".tsv", M / f"results/annotation_table/{S}.functional_and_taxonomic_table.txt"
    yield "annotation_orfs", "metapathways_lca", ".tsv", M / f"results/annotation_table/{S}.annotation_taxonomy.tsv"
    for rset in ("SSU", "LSU"):
        yield ("annotation_contigs", "metapathways_rrna", f".{rset.lower()}.tsv",
               M / f"blast_results/{S}.rRNA.SILVA_138.1_{rset}Ref_NR99_tax_silva_trunc.BLASTout")
    yield "annotation_contigs", "metapathways_trna", ".tsv", M / f"results/tRNA/{S}.tRNA.results.txt"
    yield "models", "metapathways_pathways", ".tsv", M / f"results/pgdb/community/{S}_pwy.tsv"


def bin_stem(S, bin_id):
    m = re.search(r"_([a-z0-9]+)_bin_(\d+)$", bin_id)
    return f"{S}.{m.group(1)}.{int(m.group(2)):03d}", m.group(1)


def run_keys():
    keys = dict((S, k.split()) for S, k in PARENT_KEYS.items())
    with open(B / "jobs/tables/run_keys.tsv") as fh:
        next(fh)
        for line in fh:
            S, k = line.rstrip("\n").split("\t")
            keys[S] = k.split()
    return keys


class Tree:
    def __init__(self, out):
        self.out = out
        self.ledger = []

    def place(self, rel, src, stem, product, run, rewrite=False):
        dst = self.out / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            dst.unlink()
        if rewrite:
            with open(src, errors="surrogateescape") as fi, open(dst, "w", errors="surrogateescape") as fo:
                for line in fi:
                    fo.write(BATCH_PREFIX.sub("", line))
        else:
            os.link(src, dst)
        self.ledger.append((rel, stem, product, run, str(src)))

    def concat(self, rel, srcs, stem, product, run):
        """Join batch files under one header, batch prefix stripped."""
        dst = self.out / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        with open(dst, "w", errors="surrogateescape") as fo:
            for i, src in enumerate(srcs):
                with open(src, errors="surrogateescape") as fi:
                    header = fi.readline()
                    if i == 0:
                        fo.write(header)
                    for line in fi:
                        fo.write(BATCH_PREFIX.sub("", line))
        self.ledger.append((rel, stem, product, run, ";".join(map(str, srcs))))


def has_prefix(path):
    with open(path, errors="replace") as fh:
        return "~" in fh.read(2_000_000)


def publish_files(tree, S, keys):
    for k in keys:
        res = RUNS / k / "results"
        for tdir in sorted(p for p in res.iterdir() if p.is_dir() and p.name != "_metadata"):
            if tdir.name in PORT_BIN:
                continue
            if tdir.name not in PORT:
                raise SystemExit(f"{tdir}: result type not in the publish map")
            subject, product, ext = PORT[tdir.name]
            srcs = sorted(tdir.iterdir())
            ext = ext or "".join(srcs[0].suffixes[-1:]).lstrip(".")
            rel = f"{subject}/{product}/{S}.{ext}"
            if len(srcs) > 1:
                tree.concat(rel, srcs, S, product, k)
            elif srcs[0].suffix in (".tgz", ".json") or not has_prefix(srcs[0]):
                tree.place(rel, srcs[0], S, product, k)
            else:
                tree.place(rel, srcs[0], S, product, k, rewrite=True)
    for subject, product, suffix, src in mp_files(S) or ():
        if src.exists():
            tree.place(f"{subject}/{product}/{S}{suffix}", src, S, product, "metapathways")
    tree.place(f"assembly/contigs/{S}.fna", B / f"inputs/{S}.fasta", S, "contigs", "input")
    publish_exports(tree, S)
    publish_pgdbs(tree, S)
    publish_bins(tree, S)


def publish_exports(tree, S):
    """Per-assembly wide table as TSV, for readers without a parquet reader."""
    tree.place(f"integrated_annotations/annotations_wide/{S}.tsv.gz", TABLES / S / "annotations_wide.tsv.gz",
               S, "annotations_wide", "tables_v1")


def publish_pgdbs(tree, S):
    for pipe, product in (("port", "pathologic_pgdb"), ("mp", "metapathways_pgdb")):
        man = TABLES / S / f"pgdb_{pipe}/pgdb_manifest.tsv"
        if not man.exists():
            continue
        for r in pd.read_csv(man, sep="\t").itertuples():
            if r.status != "ok":
                continue
            src = Path(r.source)
            stem = S if r.entity == "community" else bin_stem(S, r.entity)[0]
            ext = "tar.bz2" if src.name.endswith(".tar.bz2") else "tgz"
            run = src.parts[src.parts.index("runs") + 1] if pipe == "port" else "metapathways"
            if r.entity != "community":
                tree.place(f"models/{product}/{stem}.{ext}", src, stem, product, run)
            if pipe == "port" and r.entity != "community":
                h = re.match(r"1-1-1\.([0-9a-f]+)-", src.name).group(1)
                for tbl in (src.parent.parent / "annotation-bin_pathway_table").glob(f"1-1-1.{h}-*"):
                    tree.place(f"models/pathologic_pathways/{stem}.tsv", tbl, stem, "pathologic_pathways", run)
            if pipe == "mp" and r.entity != "community":
                for tbl in src.parent.glob("*_pwy.tsv"):
                    tree.place(f"models/metapathways_pathways/{stem}.tsv", tbl, stem, "metapathways_pathways", run)
            if pipe == "mp" and r.entity == "community":
                tree.place(f"models/{product}/{stem}.{ext}", src, stem, product, run)


def publish_bins(tree, S):
    mag_map = B / f"mp4/mag_maps/{S}.tsv"
    if not mag_map.exists():
        return
    m = pd.read_csv(mag_map, sep="\t", header=None, names=["contig", "bin_id"])
    m[["bin", "binner"]] = [bin_stem(S, b) for b in m.bin_id]
    for binner, g in m.groupby("binner"):
        rel = Path(f"binning/{binner}_contig_to_bin/{S}.tsv")
        (tree.out / rel).parent.mkdir(parents=True, exist_ok=True)
        g[["contig", "bin"]].to_csv(tree.out / rel, sep="\t", index=False)
        tree.ledger.append((str(rel), S, f"{binner}_contig_to_bin", "metapathways", str(mag_map)))
    want = dict(zip(m.contig, m.bin))
    handles = {}
    name = None
    with open(B / f"inputs/{S}.fasta") as fh:
        for line in fh:
            if line.startswith(">"):
                name = want.get(line[1:].split()[0])
                if name and name not in handles:
                    binner = name.split(".")[1]
                    p = tree.out / f"binning/{binner}_bins/{name}.fa"
                    p.parent.mkdir(parents=True, exist_ok=True)
                    handles[name] = open(p, "w")
                    tree.ledger.append((str(p.relative_to(tree.out)), name, f"{binner}_bins", "metapathways",
                                        str(mag_map)))
            if name:
                handles[name].write(line)
    for h in handles.values():
        h.close()


def pooled_parquet(dst, srcs):
    """Stream per-assembly parquet files into one, adding a sample column and unifying schemas."""
    schemas = [pq.read_schema(p) for _, p in srcs]
    schema = pa.unify_schemas([pa.schema([pa.field("sample", pa.string())])] +
                              [s.remove_metadata() for s in schemas], promote_options="permissive")
    dst.parent.mkdir(parents=True, exist_ok=True)
    with pq.ParquetWriter(dst, schema, compression="zstd") as w:
        for S, p in srcs:
            for batch in pq.ParquetFile(p).iter_batches(batch_size=200_000):
                t = pa.Table.from_batches([batch])
                t = t.append_column("sample", pa.array([S] * len(t), pa.string())) if "sample" not in t.column_names else t
                cols = [t[f.name].cast(f.type) if f.name in t.column_names else pa.nulls(len(t), f.type)
                        for f in schema]
                w.write_table(pa.Table.from_arrays(cols, schema=schema))


def pooled_tsv(dst, srcs):
    frames = [pd.read_csv(p, sep="\t", low_memory=False).assign(sample=S) for S, p in srcs if p.exists()]
    dst.parent.mkdir(parents=True, exist_ok=True)
    df = pd.concat(frames, ignore_index=True)
    df[["sample"] + [c for c in df.columns if c != "sample"]].to_csv(dst, sep="\t", index=False)


def publish_tables(out, samples):
    T = {S: TABLES / S for S in samples}
    pooled_parquet(out / "integrated_annotations/annotations_wide.parquet",
                   [(S, T[S] / "annotations_wide.parquet") for S in samples])
    pooled_parquet(out / "integrated_annotations/annotations_long.parquet",
                   [(S, T[S] / "annotations_long.parquet") for S in samples])
    pooled_tsv(out / "integrated_annotations/annotation_coverage.tsv", [(S, T[S] / "lane_presence.tsv") for S in samples])
    (out / "integrated_annotations/annotation_sources.tsv").write_bytes((T[samples[0]] / "lane_legend.tsv").read_bytes())
    pooled_tsv(out / "assembly/assembly_summary.tsv", [(S, T[S] / "summary.tsv") for S in samples])
    pgdb = [(S, T[S] / f"pgdb_{p}/pgdb_genes.parquet") for S in samples for p in ("port", "mp")]
    pooled_parquet(out / "models/pgdb_genes.parquet", [(S, p) for S, p in pgdb if p.exists()])
    pooled_tsv(out / "models/pgdb_summary.tsv",
               [(S, T[S] / f"pgdb_{p}/pgdb_manifest.tsv") for S in samples for p in ("port", "mp")])
    bins = []
    for S in samples:
        mag_map = B / f"mp4/mag_maps/{S}.tsv"
        if mag_map.exists():
            m = pd.read_csv(mag_map, sep="\t", header=None, names=["contig", "bin_id"])
            for bin_id, g in m.groupby("bin_id"):
                stem, binner = bin_stem(S, bin_id)
                bins.append((stem, S, binner, bin_id, len(g)))
    pd.DataFrame(bins, columns=["bin", "sample", "binner", "source_bin_id", "n_contigs"]).to_csv(
        out / "binning/mags.tsv", sep="\t", index=False)
    rows = []
    for S in samples:
        a, rest = S.split("_", 1)
        rows.append((S, rest, "", ASSEMBLER[a][1], ASSEMBLER[a][0]))
    pd.DataFrame(rows, columns=["sample_id", "source_name", "accession", "sets", "assembler"]).to_csv(
        out / "samples.tsv", sep="\t", index=False)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


README = """# copper_enrichments: functional annotation of the cpe3 copper enrichment assemblies

Seventeen assemblies of the cpe3 mine-community enrichment series, annotated twice: by the
original MetaPathways pipeline and by its metasmith port. The five enrichment samples
(cpe3_r2a, cu1_5, cu6, cu7_8, cu9_11) were each assembled with metaSPAdes, hybridSPAdes and
OPERA-MS, and the pooled parent with metaSPAdes and OPERA-MS. `samples.tsv` lists them.

## Where to start

- `integrated_annotations/annotations_wide/<assembly>.tsv.gz`: one row per gene or feature,
  with every annotation source as columns. `annotation_sources.tsv` names the tool and
  reference database behind each column group; `annotation_coverage.tsv` counts hits per
  source per assembly. `annotations_wide.parquet` and `annotations_long.parquet` (one row
  per hit) pool all seventeen assemblies with a `sample` column.
- `models/pgdb_genes.parquet`: gene to reaction to pathway for every community and MAG
  pathway database, `pipeline` = mp (original) or port. The `*_pgdb` archives open in
  Pathway Tools; `*_pathways` hold each database's pathway list.
- The other folders hold each tool's own output, one file per assembly or bin.

## Layout

- `<subject>/<product>/<stem>.<ext>`: one tool output per assembly or bin. Subjects group
  outputs by what they describe: `assembly`, `taxonomy_contigs`, `annotation_orfs` (run on
  protein sequences), `annotation_contigs` (run on contig sequences), `amr` (resistance,
  virulence, toxins), `mge` (plasmids, integrons, insertion sequences, mobile genes),
  `viral`, `binning` and `models` (pathway databases).
- `<subject>/<table>.<ext>`: a table pooled over all assemblies.
- An assembly stem is `<assembler>_<sample>`. A bin stem is `<assembly>.<binner>.<NNN>`;
  `binning/mags.tsv` maps each to its original bin name.
- Contig IDs are the assembler's. Port ORF IDs are Prodigal's `<contig>_<n>`. Products
  named `metapathways_*` come from the original pipeline and use its own ORF IDs.
- `annotation_orfs/tcdb`, `amr/bacmet`, `amr/megares` and `amr/vfdb` are headerless BLAST6
  tables with columns qseqid, sseqid, pident, length, mismatch, gapopen, qstart, qend,
  sstart, send, evalue, bitscore, stitle.
- `_logs/ledger.tsv` lists every file with its size, sha256 and the run it came from;
  `_logs/runs/` holds each run's metadata, including tool container versions;
  `_logs/gaps.tsv` lists any assembly a product is missing for.

## Not yet included

DRAM, DRAM-v and mobileOG-db annotations will be added to `annotation_orfs/dram`,
`viral/dramv` and `mge/mobileog`. No metabolic models (CarveMe) were built. `mags.tsv` has
no completeness or contamination columns.

## Products

| Folder | Files |
|---|---|
"""


def write_readme(out):
    rows = [f"| `{d.relative_to(out)}` | {sum(1 for _ in d.iterdir())} |"
            for d in sorted(out.glob("*/*")) if d.is_dir() and not d.parts[-2].startswith("_")]
    (out / "README.md").write_text(README + "\n".join(rows) + "\n")


def publish_logs(out, keys, samples, ledger_rows):
    write_readme(out)
    logs = out / "_logs"
    (logs / "runs").mkdir(parents=True, exist_ok=True)
    for S in samples:
        for k in keys[S]:
            with tarfile.open(logs / f"runs/{k}.tar.gz", "w:gz") as tf:
                tf.add(RUNS / k / "results/_metadata", arcname=f"{k}/_metadata")
                tf.add(RUNS / k / "results/given.csv", arcname=f"{k}/given.csv")
    src_of = {r[0]: r for r in ledger_rows}
    rows = []
    for p in sorted(out.rglob("*")):
        rel = str(p.relative_to(out))
        if p.is_dir() or rel in ("_logs/ledger.tsv", "_logs/gaps.tsv"):
            continue
        r = src_of.get(rel, (rel, "", "", "", ""))
        rows.append((rel, r[1], r[2], r[3], r[4], p.stat().st_size, sha256(p)))
    pd.DataFrame(rows, columns=["path", "stem", "product", "run", "source", "bytes", "sha256"]).to_csv(
        logs / "ledger.tsv", sep="\t", index=False)
    gaps = []
    for d in sorted(p for p in out.glob("*/*") if p.is_dir()):
        if d.parts[-2] in ("binning", "_logs") or d.name in ("pathologic_pgdb", "metapathways_pgdb",
                                                              "pathologic_pathways", "metapathways_pathways"):
            continue
        have = {f.name.split(".")[0] for f in d.iterdir()}
        gaps += [(f"{d.parts[-2]}/{d.name}", S, "not_run") for S in samples if S not in have]
    pd.DataFrame(gaps, columns=["product", "stem", "reason"]).to_csv(logs / "gaps.tsv", sep="\t", index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--part", action="append", choices=["files", "exports", "tables", "logs"])
    ap.add_argument("--sample", action="append")
    args = ap.parse_args()
    keys = run_keys()
    samples = args.sample or sorted(keys)
    parts = args.part or ["files", "tables", "logs"]
    tree = Tree(args.out)
    ledger_tmp = args.out.parent / f".{args.out.name}.sources.tsv"
    if "files" in parts:
        for S in samples:
            print(f"files {S}", flush=True)
            publish_files(tree, S, keys[S])
        ledger_tmp.parent.mkdir(parents=True, exist_ok=True)
        old = pd.read_csv(ledger_tmp, sep="\t") if ledger_tmp.exists() else None
        new = pd.DataFrame(tree.ledger, columns=["path", "stem", "product", "run", "source"])
        (pd.concat([old, new]).drop_duplicates("path", keep="last") if old is not None else new).to_csv(
            ledger_tmp, sep="\t", index=False)
    if "exports" in parts:
        for S in samples:
            publish_exports(tree, S)
        ledger_tmp.parent.mkdir(parents=True, exist_ok=True)
        old = pd.read_csv(ledger_tmp, sep="\t")
        new = pd.DataFrame(tree.ledger, columns=["path", "stem", "product", "run", "source"])
        pd.concat([old, new]).drop_duplicates("path", keep="last").to_csv(ledger_tmp, sep="\t", index=False)
    if "tables" in parts:
        print("tables", flush=True)
        publish_tables(args.out, samples)
    if "logs" in parts:
        print("logs", flush=True)
        rows = list(pd.read_csv(ledger_tmp, sep="\t").fillna("").itertuples(index=False)) if ledger_tmp.exists() else []
        publish_logs(args.out, keys, sorted(keys), rows)


if __name__ == "__main__":
    main()
