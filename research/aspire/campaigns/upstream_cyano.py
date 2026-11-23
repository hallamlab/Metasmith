#!/usr/bin/env python3
"""Run upstream ASPIRE on the cyano_r1 reads, then diff its tables against the port's.

    python research/aspire/campaigns/upstream_cyano.py config
    CONTROL_ENV_DIR=$PWD/cache/aspire/t14_upstream/controller_env \\
        research/aspire/upstream/ASPIRE/run_asv_pipeline.sh cache/aspire/t14_upstream/aspire.yml
    python research/aspire/campaigns/upstream_cyano.py diff

`config` writes upstream's shipped YAML with cyano_r1's params.yml values, local copies of
the references the port read on sockeye, and every module after GENERAL_STATS switched off.
MitoMaster stays on, since this host has internet, so the diff shows what dropping it costs.

`diff` joins the two runs on ASV sequence, because each run numbers its ASVs by abundance.
It compares the filtered ASV table, the taxonomy and the curated table, and names the ASVs
that one side keeps and the other drops, with each one's mito and contaminant flags.
"""

import argparse
import csv
import gzip
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[3]
UPSTREAM = REPO / "research" / "aspire" / "upstream" / "ASPIRE"
CYANO = REPO / "research" / "aspire" / "campaigns" / "cyano_r1"
PIN = REPO / "data" / "aspire" / "cyano_r1"
T6 = REPO / "cache" / "aspire" / "t6_cyano"
KEEP_ON = {"mito", "filter_counts", "general_stats"}


def build_config(work: Path) -> dict:
    with open(UPSTREAM / "asv_pipeline_nextflow.yml") as f:
        cfg = yaml.safe_load(f)
    with open(CYANO / "params.yml") as f:
        p = yaml.safe_load(f)
    refs = work / "refs"
    out = work / "out"

    cfg["paths"] = {"input_dir": str(work / "reads"), "output_dir": str(out),
                    "runtime_dir": str(work / "runtime"), "keep_runtime_dir": True}
    cfg["resources"]["threads"] = 8
    cfg["fastp"] = {k: p["fastp"][k] for k in ("trim_front_r1", "trim_tail_r1", "trim_front_r2", "trim_tail_r2")}
    cfg["merge"] = dict(p["merge"])
    cfg["filter"] = {k: p["filter"][k] for k in ("max_ee", "min_len", "max_len")}
    cfg["unoise"]["min_size"] = p["unoise"]["min_size"]
    cfg["table_filter"] = dict(p["table_filter"])
    cfg["sina"].update(reference=str(refs / "silva.arb"), regions=p["sina"]["regions"],
                       trim_to=p["sina"]["trim_to"], threads=8)
    cfg["taxonomy"].update(ref_taxonomy=str(refs / "silva_tax.qza"),
                           ref_sequences=str(refs / "silva_seqs.qza"), threads=8)
    cfg["mito"].update(mito_fasta=str(refs / "refseq_mitochondrion.fasta"),
                       contaminant_fasta=str(refs / "contaminants.fasta"),
                       mito_db=None, biof_db=None, blast_threads=8,
                       min_pident=p["curate"]["min_pident"], min_percov=p["curate"]["min_percov"])
    cfg["filter_counts"].update(metadata=str(T6 / "study_metadata.tsv"), sample_id_col="sample",
                                group_col="culture", abundance_threshold=p["curate"]["abundance_threshold"],
                                min_consensus=p["curate"]["min_consensus"],
                                exclude_taxa=p["curate"]["exclude_taxa"],
                                min_group_size=p["analysis"]["min_level_size"])

    for name, section in cfg.items():
        if isinstance(section, dict) and "enabled" in section and name not in KEEP_ON:
            section["enabled"] = False
    cfg["spieceasi"].update(network_enabled=False, modules_enabled=False)
    return cfg


def cmd_config(args):
    cfg = build_config(args.work)
    for f in ("silva.arb", "silva_tax.qza", "silva_seqs.qza", "refseq_mitochondrion.fasta", "contaminants.fasta"):
        assert (args.work / "refs" / f).exists(), f"missing reference [{args.work / 'refs' / f}]"
    n = len(list((args.work / "reads").glob("*_R1.fastq.gz")))
    assert n == 18, f"expected 18 read pairs under {args.work / 'reads'}, found {n}"
    dest = args.work / "aspire.yml"
    with open(dest, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)
    print(dest)


def read_fasta(path: Path) -> dict[str, str]:
    opener = gzip.open if path.suffix == ".gz" else open
    seqs, name = {}, None
    with opener(path, "rt") as f:
        for line in f:
            line = line.strip()
            if line.startswith(">"):
                name = line[1:].split()[0].split(";")[0]
                seqs[name] = ""
            elif name:
                seqs[name] += line.upper()
    return seqs


def read_table(path: Path) -> tuple[list[str], dict[str, dict[str, float]]]:
    with open(path) as f:
        rows = list(csv.reader(f, delimiter="\t"))
    rows = [r for r in rows if r and not r[0].startswith("# Constructed")]
    header, body = rows[0], rows[1:]
    samples = [s for s in header[1:] if s not in ("taxonomy", "Taxon", "reason")]
    idx = [header.index(s) for s in samples]
    return samples, {r[0]: {s: float(r[i]) for s, i in zip(samples, idx)} for r in body}


def by_seq(table: dict[str, dict], seqs: dict[str, str], side: str) -> dict[str, dict]:
    missing = [a for a in table if a not in seqs]
    assert not missing, f"[{side}] {len(missing)} table ASVs have no sequence, e.g. {missing[:3]}"
    return {seqs[a]: dict(counts, _id=a) for a, counts in table.items()}


def compare(label: str, up: dict[str, dict], port: dict[str, dict], samples: list[str]):
    shared = up.keys() & port.keys()
    print(f"\n== {label}: upstream {len(up)} ASVs, port {len(port)}, shared {len(shared)}")
    unequal = []
    for s in shared:
        d = sum(abs(up[s].get(x, 0) - port[s].get(x, 0)) for x in samples)
        if d:
            unequal.append((d, up[s]["_id"], port[s]["_id"]))
    print(f"   shared ASVs with any per-sample count difference: {len(unequal)}")
    for d, u, p in sorted(unequal, reverse=True)[:10]:
        print(f"     {u} / {p}: |diff| summed over samples {d:g}")
    for side, mine, other in (("upstream only", up, port), ("port only", port, up)):
        only = sorted(mine.keys() - other.keys(), key=lambda q: -sum(mine[q].get(x, 0) for x in samples))
        total = sum(sum(mine[q].get(x, 0) for x in samples) for q in only)
        print(f"   {side}: {len(only)} ASVs, {total:g} reads")
        yield side, only, mine


def find_one(root: Path, pattern: str) -> Path:
    hits = sorted(root.rglob(pattern))
    assert hits, f"no [{pattern}] under {root}"
    return hits[0]


def cmd_diff(args):
    out = args.work / "out"
    port = args.port
    up_seqs = read_fasta(args.up_seqs or find_one(out, "ASVs_filtered.fasta.gz"))
    port_seqs = read_fasta(port / "asv_filtered_seqs.fasta.gz")

    up_filtered = find_one(out, args.up_filtered)
    samples, up_ft = read_table(up_filtered)
    _, port_ft = read_table(port / "asv_filtered_table.tsv")
    list(compare("filtered ASV table", by_seq(up_ft, up_seqs, "upstream"), by_seq(port_ft, port_seqs, "port"), samples))

    up_tax = read_taxonomy(find_one(out, "ASV_SILVA_tax.full-length.vsearch.tsv"))
    port_tax = read_taxonomy(port / "asv_taxonomy.tsv")
    port_by_seq = {s: a for a, s in port_seqs.items()}
    shared = [(u, port_by_seq[s]) for u, s in up_seqs.items() if s in port_by_seq and u in up_tax]
    differ = [(u, p) for u, p in shared if up_tax[u] != port_tax.get(p)]
    print(f"\n== taxonomy: {len(shared)} shared ASVs classified on both sides, {len(differ)} disagree")
    for u, p in differ[:10]:
        print(f"     {u} / {p}: {up_tax[u]} | {port_tax.get(p)}")

    s2, up_ct = read_table(find_one(out, args.up_curated))
    _, port_ct = read_table(port / "counts_clean.tsv")
    list(compare("curated table", by_seq(up_ct, up_seqs, "upstream"), by_seq(port_ct, port_seqs, "port"), s2))

    # The port has no MitoMaster, so an ASV only MitoMaster flags is one the port can keep.
    rel = {a: max(c[x] / sum(up_ft[b][x] for b in up_ft) * 100 for x in samples if sum(up_ft[b][x] for b in up_ft))
           for a, c in up_ft.items()}
    with open(find_one(out, "nontarget.master.tsv")) as f:
        only_mm = [r["Sequence_ID"] for r in csv.DictReader(f, delimiter="\t")
                   if r["MITOMASTER"] == "0" and r["BLAST_mito"] == "1"]
    print(f"\n== flagged by MitoMaster alone: {len(only_mm)}")
    for a in only_mm:
        if a not in up_seqs:
            print(f"     {a}: not an ASV; upstream's mito checker read a MitoMaster header row as one")
            continue
        print(f"     {a}: highest share in any sample {rel.get(a, 0):.3f}%, in port curated table: "
              f"{up_seqs.get(a) in {port_seqs[p] for p in port_ct}}")
    return 0


def read_taxonomy(path: Path) -> dict[str, str]:
    with open(path) as f:
        return {r["Feature ID"].split(";")[0]: r["Taxon"] for r in csv.DictReader(f, delimiter="\t")}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", type=Path, default=REPO / "cache" / "aspire" / "t14_upstream")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("config")
    d = sub.add_parser("diff")
    d.add_argument("--port", type=Path, default=REPO / "cache" / "aspire" / "t14_upstream" / "port",
                   help="asv_filtered_table.tsv, asv_filtered_seqs.fasta.gz and counts_clean.tsv from cyano_r1")
    d.add_argument("--up-seqs", type=Path)
    d.add_argument("--up-filtered", default="ASV_filtered.tsv")
    d.add_argument("--up-curated", default="ASV_target.tsv")
    args = ap.parse_args()
    return {"config": cmd_config, "diff": cmd_diff}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
