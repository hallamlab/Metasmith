from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

nx = None
try:
    import networkx as nx
except ImportError:  # pragma: no cover - exercised only outside the module env.
    nx = None


SCRIPT = Path(__file__).parents[1] / "processes" / "asv_mag_network" / "asv_mag_network.py"
SPEC = importlib.util.spec_from_file_location("asv_mag_network", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
try:
    SPEC.loader.exec_module(MODULE)
except ImportError as exc:  # pragma: no cover - exercised only outside the module env.
    MODULE = None
    MODULE_IMPORT_ERROR = exc
else:
    MODULE_IMPORT_ERROR = None


class AsvMagNetworkTest(unittest.TestCase):
    def test_suffix_after_double_underscore_mag_id_mode(self) -> None:
        if MODULE is None:
            self.skipTest("networkx/seaborn dependencies are supplied by the module env")
        self.assertEqual(
            MODULE.standardize_mag_id("100m__xPG_SAGs__AB-746_B09_AB-901.fasta.gz", "suffix_after_double_underscore"),
            "AB-746_B09_AB-901",
        )
        self.assertEqual(
            MODULE.standardize_mag_id("xPG_SAGs__100m__AB-746_B09_AB-901", "suffix_after_double_underscore"),
            "AB-746_B09_AB-901",
        )

    @unittest.skipIf(nx is None or MODULE is None, "networkx/seaborn dependencies are supplied by the module env")
    def test_smoke_exports_paper_and_heterogeneous_networks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            graph_path = root / "network.graphml"
            graph = nx.Graph()
            graph.add_edge("ASV1", "ASV2", weight=0.42)
            nx.write_graphml(graph, graph_path)

            node_features = root / "node_features.csv"
            pd.DataFrame(
                [
                    {"Taxon": "ASV1", "degree": 1},
                    {"Taxon": "ASV2", "degree": 1},
                ]
            ).to_csv(node_features, index=False)

            taxonomy = root / "taxonomy.tsv"
            pd.DataFrame(
                [
                    {"Feature ID": "ASV1", "Taxon": "d__Bacteria;p__Proteobacteria;g__Nitrosomonas"},
                    {"Feature ID": "ASV2", "Taxon": "d__Bacteria;p__Bacteroidota;g__Flavobacterium"},
                ]
            ).to_csv(taxonomy, sep="\t", index=False)

            pairing = root / "pairing.tsv"
            pd.DataFrame(
                [
                    {
                        "ASV_ID": "ASV1",
                        "genome_id": "MAG_A",
                        "pairing_status": "paired_unique",
                        "link_pident": 100.0,
                        "link_qcov": 100.0,
                        "mag_phylum": "Proteobacteria",
                        "mag_genus": "Nitrosomonas",
                    },
                    {
                        "ASV_ID": "ASV2",
                        "genome_id": "MAG_B",
                        "pairing_status": "paired_unique",
                        "link_pident": 100.0,
                        "link_qcov": 100.0,
                        "mag_phylum": "Firmicutes",
                        "mag_genus": "Bacillus",
                    },
                ]
            ).to_csv(pairing, sep="\t", index=False)

            outdir = root / "out"
            old_argv = sys.argv
            try:
                sys.argv = [
                    "asv_mag_network.py",
                    "--graph",
                    str(graph_path),
                    "--node-features",
                    str(node_features),
                    "--asv-mag-pairing",
                    str(pairing),
                    "--taxonomy",
                    str(taxonomy),
                    "--outdir",
                    str(outdir),
                    "--prefix",
                    "smoke",
                    "--asv-taxonomy-source",
                    "ncbi",
                    "--mag-taxonomy-source",
                    "ncbi",
                ]
                MODULE.main()
            finally:
                sys.argv = old_argv

            validation = pd.read_csv(outdir / "validation" / "smoke_taxonomy_validation.tsv", sep="\t")
            accepted = validation.set_index("ASV_ID")["accepted_paper_pair"].to_dict()
            self.assertTrue(bool(accepted["ASV1"]))
            self.assertFalse(bool(accepted["ASV2"]))
            self.assertEqual(
                validation.set_index("ASV_ID").loc["ASV2", "taxonomy_validation_status"],
                "taxonomy_rejected",
            )
            self.assertTrue((outdir / "network" / "smoke_paper.graphml").exists())
            self.assertTrue((outdir / "network" / "smoke_heterogeneous.graphml").exists())
            hetero_nodes = pd.read_csv(outdir / "network" / "smoke_heterogeneous_nodes.tsv", sep="\t")
            self.assertIn("MAG_A", set(hetero_nodes["id"].astype(str)))
            self.assertNotIn("MAG_B", set(hetero_nodes["id"].astype(str)))


if __name__ == "__main__":
    unittest.main()
