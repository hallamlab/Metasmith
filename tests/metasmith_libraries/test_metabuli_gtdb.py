import gzip
import subprocess
import sys

from conftest import MLIB

SCRIPT = MLIB / "resources" / "lib" / "metabuli_gtdb" / "select_reps.py"
COLUMNS = ["accession", "checkm2_completeness", "checkm2_contamination", "genome_size",
           "gtdb_representative", "gtdb_taxonomy", "ncbi_assembly_level"]


def _table(path, rows):
    with gzip.open(path, "wt") as fh:
        fh.write("\t".join(COLUMNS) + "\n")
        for r in rows:
            fh.write("\t".join(str(r[c]) for c in COLUMNS) + "\n")


def _row(acc, rep="t", level="Complete Genome", compl=95.0, contam=1.0):
    return dict(accession=acc, checkm2_completeness=compl, checkm2_contamination=contam,
                genome_size=1000, gtdb_representative=rep, gtdb_taxonomy="d__Bacteria",
                ncbi_assembly_level=level)


def test_the_r214_rule_and_archive_paths(tmp_path):
    bac, ar = tmp_path / "bac120.tsv.gz", tmp_path / "ar53.tsv.gz"
    _table(bac, [
        _row("RS_GCF_000020485.1"),
        _row("GB_GCA_012345678.2", level="Chromosome"),
        _row("RS_GCF_000000001.1", rep="f"),
        _row("RS_GCF_000000002.1", level="Scaffold"),
        _row("RS_GCF_000000003.1", compl=90.0),
        _row("RS_GCF_000000004.1", contam=5.0),
    ])
    _table(ar, [_row("GB_GCA_900000005.1")])
    subprocess.run([sys.executable, str(SCRIPT), "--metadata", str(bac), str(ar),
                    "--archive-root", "gtdb_genomes_reps_r232", "--out", str(tmp_path / "out")], check=True)

    assert (tmp_path / "out" / "members.txt").read_text().splitlines() == [
        "gtdb_genomes_reps_r232/database/GCF/000/020/485/GCF_000020485.1_genomic.fna.gz",
        "gtdb_genomes_reps_r232/database/GCA/012/345/678/GCA_012345678.2_genomic.fna.gz",
        "gtdb_genomes_reps_r232/database/GCA/900/000/005/GCA_900000005.1_genomic.fna.gz",
    ]
