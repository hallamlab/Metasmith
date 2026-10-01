#!/usr/bin/env python3
"""ASPIRE over PRJNA801777 on sockeye: 18 V4 amplicon libraries from Anabaena and Microcystis cultures.

    python research/aspire/campaigns/cyano_r1/run_cyano.py list
    python research/aspire/campaigns/cyano_r1/run_cyano.py stage-reads
    python research/aspire/campaigns/cyano_r1/run_cyano.py stage-refs
    python research/aspire/campaigns/cyano_r1/run_cyano.py side-load-images
    python research/aspire/campaigns/cyano_r1/run_cyano.py check-refs
    python research/aspire/campaigns/cyano_r1/run_cyano.py run [--plan-only]
    python research/aspire/campaigns/cyano_r1/run_cyano.py status
    python research/aspire/campaigns/cyano_r1/run_cyano.py retrieve
    python research/aspire/campaigns/cyano_r1/run_cyano.py dag

Every download runs on the login node, since compute nodes have no internet. Connect to
sockeye through the awm ssh domain first; every ssh here rides that connection.

`stage-refs` needs the mock's contaminant FASTA locally (ASPIRE_MOCK_REFS), and
`side-load-images` needs the aspire image built locally (docker/aspire/dev.sh --build).
The study has no MAGs, so the ASV-MAG link is switched off.
"""

import csv
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from _campaign import (  # noqa: E402
    HOST, REFS, REPO, SILVA, Campaign, Sample, cmd_check_refs, main, ssh_once,
)
from metasmith.python_api import Duration, Resources, Size  # noqa: E402

ROOT = os.environ.get("ASPIRE_CYANO_ROOT", "/scratch/st-shallam-1/txyliu/aspire_cyano")
READS = f"{ROOT}/reads"
MOCK_REFS = Path(os.environ.get("ASPIRE_MOCK_REFS", REPO / "data" / "aspire" / "mock_references"))

TARGETS = ["aspire::read_fate", "aspire::sankey_outputs", "aspire::collectors_outputs",
           "aspire::diversity_outputs", "aspire::diversity_mito_outputs", "aspire::umap_plots",
           "aspire::bubble_plots", "aspire::upset_plots", "aspire::grouping_diagnostics_outputs",
           "aspire::clustermap_outputs", "aspire::network_outputs"]
SILVA_FILES = {
    "silva.arb.gz": "https://www.arb-silva.de/fileadmin/silva_databases/release_138_2/ARB_files/SILVA_138.2_SSURef_NR99_03_07_24_opt.arb.gz",
    "silva_seqs.qza": "https://data.qiime2.org/2024.10/common/silva-138-99-seqs.qza",
    "silva_tax.qza": "https://data.qiime2.org/2024.10/common/silva-138-99-tax.qza",
    "silva_nb_classifier.qza": "https://data.qiime2.org/classifiers/sklearn-1.4.2/silva/silva-138-99-nb-classifier.qza",
}
MITO_URL = "https://ftp.ncbi.nlm.nih.gov/refseq/release/mitochondrion/mitochondrion.1.1.genomic.fna.gz"
RESOURCE_OVERRIDES = {
    "sina_trim": Resources(cpus=16, memory=Size.GB(48), duration=Duration(hours=6)),
    "taxonomy": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=6)),
    "indicspecies": Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=4)),
    "spieceasi": Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
    "network_modules": Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=2)),
    "graph_network": Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=2)),
}


def samples():
    with open(HERE / "samples.tsv") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def campaign(_args=None) -> Campaign:
    return Campaign(
        name="cyano_r1", here=HERE, root=ROOT,
        samples=[Sample(s["sample"], f"{READS}/{s['sample']}_R1.fastq.gz",
                        f"{READS}/{s['sample']}_R2.fastq.gz") for s in samples()],
        study_sheet=(HERE / "study_metadata.tsv").read_text(),
        params=(HERE / "params.yml").read_text(),
        targets=TARGETS,
        mito_reference=f"{REFS}/refseq_mitochondrion.fasta",
        contaminant_reference=f"{REFS}/contaminants.fasta",
        switches_on={"spieceasi", "network_modules", "graph_network"},
        resource_overrides=RESOURCE_OVERRIDES,
    )


def cmd_list(_c, _args):
    for s in samples():
        print(f"{s['sample']:<6}{s['run_accession']:<14}{s['read_count']:>8} pairs")
    return 0


def cmd_stage_reads(_c, _args):
    lines = ["set -euo pipefail", f"mkdir -p {READS}", f"cd {READS}"]
    for s in samples():
        for mate, url, md5 in zip(("R1", "R2"), s["fastq_ftp"].split(";"), s["fastq_md5"].split(";")):
            out = f"{s['sample']}_{mate}.fastq.gz"
            lines.append(f'[ "$(md5sum < {out} 2>/dev/null | cut -d" " -f1)" = {md5} ] '
                         f'|| {{ curl -sSfL -o {out} https://{url}; '
                         f'[ "$(md5sum < {out} | cut -d" " -f1)" = {md5} ]; }}')
    lines.append("ls | wc -l")
    print(ssh_once(HOST, "\n".join(lines)).strip(), "files staged in", READS)
    return 0


def cmd_stage_refs(c, args):
    fetch = " && ".join(f"{{ [ -e {n.removesuffix('.gz')} ] || curl -sSfL -o {n} {u}; }}"
                        for n, u in SILVA_FILES.items())
    ssh_once(HOST, f"set -e; mkdir -p {SILVA}; cd {SILVA}; {fetch}; "
                   f"[ -e silva.arb ] || gunzip silva.arb.gz; ls -la")
    ssh_once(HOST, f"set -e; cd {REFS}; [ -s refseq_mitochondrion.fasta ] || "
                   f"curl -sSfL {MITO_URL} | gunzip > refseq_mitochondrion.fasta")
    subprocess.run(["scp", "-q", str(MOCK_REFS / "contaminants.fasta"), f"{HOST}:{REFS}/contaminants.fasta"],
                   check=True)
    return cmd_check_refs(c, args)


if __name__ == "__main__":
    raise SystemExit(main(__doc__, campaign, {
        "list": cmd_list, "stage-reads": cmd_stage_reads, "stage-refs": cmd_stage_refs,
    }))
