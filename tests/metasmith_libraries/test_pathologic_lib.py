import sys

import pandas as pd

from conftest import MLIB

sys.path.insert(0, str(MLIB / "resources" / "lib"))
sys.path.insert(0, str(MLIB / "resources" / "lib" / "pathologic"))
import build_pf  # noqa: E402
import fabfos_evidence as fe  # noqa: E402
import gather  # noqa: E402
import pathway_table  # noqa: E402


def test_gather_keeps_clean_calls_at_the_floor(tmp_path):
    orfs = tmp_path / "orfs.faa"
    orfs.write_text(">c1_1 # x\nM\n>c2_1 # x\nM\n")
    clean = tmp_path / "clean.tsv"
    clean.write_text("Query ID\tPredicted EC number\tclean_score\n"
                     "c1_1\tEC:1.1.1.1\t0.9\nc2_1\t2.7.1.1\t0.001\nc9_1\t3.1.1.1\t0.9\n")
    out = tmp_path / "ann.parquet"
    sys.argv = ["gather", "--orfs", str(orfs), "--clean", str(clean), "--out", str(out)]
    gather.main()
    df = fe.read_parquet(out)
    assert df["orf_id"].tolist() == ["c1_1"] and df["value"].tolist() == ["1.1.1.1"]


def test_a_mag_pf_keeps_only_its_contigs(tmp_path):
    ann = tmp_path / "ann.parquet"
    fe.write_parquet(pd.DataFrame({"orf_id": ["c1_1", "c1_1", "c2_1"], "kind": ["EC"] * 3,
                                   "value": ["1.1.1.1", "1.1.1.2", "2.7.1.1"], "score": [0.9] * 3,
                                   "confidence": ["clean"] * 3}), ann)
    mag = tmp_path / "bin.1.fna"
    mag.write_text(">c1\nACGT\n")
    sys.argv = ["build_pf", "--annotations", str(ann), "--name", "1bin.1", "--bin-fasta", str(mag),
                "--out-dir", str(tmp_path / "ptin")]
    build_pf.main()
    pf = (tmp_path / "ptin" / "0.pf").read_text()
    assert pf == "ID\tc1_1\nNAME\tc1_1\nPRODUCT-TYPE\tP\nEC\t1.1.1.1\nEC\t1.1.1.2\n//\n"
    params = (tmp_path / "ptin" / "organism-params.dat").read_text()
    assert "ID\tP1bin1\n" in params and "NCBI-TAXON-ID\t131567\n" in params


def test_the_pathway_table_walks_reaction_to_orf(tmp_path):
    data = tmp_path / "pgdb" / "data"
    data.mkdir(parents=True)
    (data / "pathways.dat").write_text(
        "# header\nUNIQUE-ID - PWY-1\nCOMMON-NAME - glycolysis\nREACTION-LIST - RXN-1\n"
        "REACTION-LIST - RXN-2\n//\n")
    (data / "reactions.dat").write_text(
        "UNIQUE-ID - RXN-1\nEC-NUMBER - EC-2.7.1.1\nENZYMATIC-REACTION - ENZRXN-1\n//\n"
        "UNIQUE-ID - RXN-2\nEC-NUMBER - EC-5.3.1.9\n//\n")
    (data / "enzrxns.dat").write_text("UNIQUE-ID - ENZRXN-1\nENZYME - CPLX-1\n//\n")
    (data / "proteins.dat").write_text(
        "UNIQUE-ID - CPLX-1\nCOMPONENTS - MONO-1\n//\nUNIQUE-ID - MONO-1\nGENE - G-1\n//\n")
    (data / "genes.dat").write_text("UNIQUE-ID - G-1\nCOMMON-NAME - c1_1\n//\n")
    assert list(pathway_table.rows(tmp_path / "pgdb")) == [
        ("PWY-1", "glycolysis", "c1_1", "RXN-1", "2.7.1.1"),
        ("PWY-1", "glycolysis", "", "RXN-2", "5.3.1.9"),
    ]


def test_an_empty_export_is_an_empty_table(tmp_path):
    data = tmp_path / "pgdb" / "data"
    data.mkdir(parents=True)
    (data / "pathways.dat").write_text(
        "Error: The flat-file generation program didn't specify what kind of data to put in this file.\n")
    assert list(pathway_table.rows(tmp_path / "pgdb")) == []
