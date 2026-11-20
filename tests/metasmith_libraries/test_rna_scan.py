import sys

from conftest import MLIB

sys.path.insert(0, str(MLIB / "resources" / "lib" / "lcastar"))
sys.path.insert(0, str(MLIB / "resources" / "lib" / "rna_scan"))
import rrna  # noqa: E402
import trna  # noqa: E402

ECOLI = "d__Bacteria;p__Pseudomonadota;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Escherichia;s__Escherichia coli"
SALMO = "d__Bacteria;p__Pseudomonadota;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Salmonella;s__Salmonella enterica"


def _barrnap(path, rows):
    with open(path, "w") as fh:
        fh.write("##gff-version 3\n")
        for contig, start, end, strand, evalue, name in rows:
            fh.write(f"{contig}\tbarrnap:0.9\trRNA\t{start}\t{end}\t{evalue}\t{strand}\t.\t"
                     f"Name={name};product={name.replace('_', ' ')}\n")


class TestRrna:
    def test_overlapping_kingdom_calls_keep_the_best(self, tmp_path):
        bac, arc = tmp_path / "bac.gff", tmp_path / "arc.gff"
        _barrnap(bac, [("c1", 11, 40, "+", "1e-200", "16S_rRNA"), ("c1", 50, 60, "+", "1e-20", "5S_rRNA")])
        _barrnap(arc, [("c1", 15, 38, "+", "1e-50", "16S_rRNA"), ("c1", 15, 38, "-", "1e-60", "16S_rRNA")])
        asm = tmp_path / "asm.fna"
        asm.write_text(">c1 desc\n" + "A" * 10 + "ACGT" * 15 + "\n")
        gff, ssu = tmp_path / "out.gff", tmp_path / "ssu.fna"
        sys.argv = ["rrna", "merge", "--gff", str(bac), str(arc), "--assembly", str(asm),
                    "--out-gff", str(gff), "--out-ssu", str(ssu)]
        rrna.main()
        rows = [line.split("\t") for line in gff.read_text().splitlines()[1:]]
        assert [(r[3], r[4], r[6]) for r in rows] == [("11", "40", "+"), ("15", "38", "-"), ("50", "60", "+")]
        seqs = ssu.read_text().splitlines()
        assert len(seqs) == 4
        assert seqs[1] == ("ACGT" * 15)[:30]

    def test_classification_caps_the_rank_at_the_identity(self, tmp_path):
        gff = tmp_path / "rrna.gff"
        gff.write_text("##gff-version 3\n"
                       "c1\tbarrnap\trRNA\t1\t1500\t0\t+\t.\tID=rrna_1;product=16S ribosomal RNA\n"
                       "c2\tbarrnap\trRNA\t1\t1500\t0\t+\t.\tID=rrna_2;product=16S ribosomal RNA\n")
        blast = tmp_path / "hits.tsv"
        blast.write_text(
            f"rrna_1\tRS_1\t99.5\t1500\t0\t2700\tRS_1~x {ECOLI} [location=1..1500]\n"
            f"rrna_1\tRS_2\t97.0\t1500\t0\t2690\tRS_2~y {SALMO} [location=1..1500]\n"
            f"rrna_2\tRS_1\t95.0\t1500\t0\t2400\tRS_1~x {ECOLI} [location=1..1500]\n")
        out = tmp_path / "tax.tsv"
        sys.argv = ["rrna", "classify", "--gff", str(gff), "--blast", str(blast), "--out", str(out)]
        rrna.main()
        rows = {r.split("\t")[0]: r.split("\t") for r in out.read_text().splitlines()[1:]}
        assert rows["rrna_1"][3].endswith("f__Enterobacteriaceae")
        # 95% identity supports a genus but not a species.
        assert rows["rrna_2"][3].endswith("g__Escherichia") and rows["rrna_2"][4] == "genus"


class TestTrna:
    def test_aragorn_batch_rows(self, tmp_path):
        raw = tmp_path / "aragorn.txt"
        raw.write_text(">c1 desc\n2\n1   tRNA-Ala                [100,175]      34      (tgc)\n"
                       "2   tRNA-Ser               c[300,388]      35      (gct)\n>c2\n0\n")
        out = tmp_path / "trna.gff"
        sys.argv = ["trna", "aragorn", "--raw", str(raw), "--out", str(out)]
        trna.main()
        rows = [line.split("\t") for line in out.read_text().splitlines()[1:]]
        assert [(r[0], r[3], r[4], r[6]) for r in rows] == [("c1", "100", "175", "+"), ("c1", "300", "388", "-")]
        assert "product=tRNA-Ser;anticodon=GCT" in rows[1][8]

    def test_trnascan_brief_rows_skip_pseudogenes(self, tmp_path):
        raw = tmp_path / "trnascan.txt"
        raw.write_text("c1 \t1\t100\t175\tAla\tTGC\t0\t0\t70.1\t\n"
                       "c1 \t2\t388\t300\tSer\tGCT\t0\t0\t65.0\t\n"
                       "c2 \t1\t10\t80\tPseudo\tNNN\t0\t0\t20.0\tpseudo\n")
        out = tmp_path / "trna.gff"
        sys.argv = ["trna", "trnascan", "--raw", str(raw), "--out", str(out)]
        trna.main()
        rows = [line.split("\t") for line in out.read_text().splitlines()[1:]]
        assert [(r[0], r[3], r[4], r[6]) for r in rows] == [("c1", "100", "175", "+"), ("c1", "300", "388", "-")]
