import importlib.util
from pathlib import Path
import unittest

import pandas as pd


SCRIPT = Path(__file__).parents[1] / "processes" / "mito_decontam" / "mito_checker.py"
SPEC = importlib.util.spec_from_file_location("mito_checker", SCRIPT)
MITO_CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MITO_CHECKER)


class MitoCheckerTests(unittest.TestCase):
    def test_blast_only_asvs_are_retained_and_flagged(self):
        silva = pd.DataFrame({
            "Sequence_ID": ["ASV1"],
            "FL_Taxonomy": ["d__Bacteria"],
            "FL_Confidence": [1.0],
        })
        mito = pd.DataFrame({"qseqid": ["ASV28"], "sseqid": ["MITO_REF"], "pident": [100.0]})
        contaminant = pd.DataFrame({"qseqid": ["ASV29"], "sseqid": ["CONTAM_REF"], "pident": [99.0]})
        mitomaster = pd.DataFrame(columns=["Sequence_ID", "haplo"])

        master = MITO_CHECKER.build_master_table(
            silva, mito, contaminant, mitomaster, "mitochondria"
        ).set_index("Sequence_ID")

        self.assertEqual(set(master.index), {"ASV1", "ASV28", "ASV29"})
        self.assertEqual(master.loc["ASV28", "BLAST_mito"], 0)
        self.assertEqual(master.loc["ASV29", "BioFactorial"], 0)
        self.assertEqual(master.loc["ASV1", "BLAST_mito"], 1)


if __name__ == "__main__":
    unittest.main()
