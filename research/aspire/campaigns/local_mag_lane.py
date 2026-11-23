#!/usr/bin/env python3
"""Run the ASPIRE MAG lane locally in docker, on the AB48 MAGs and two ASV sets.

    python research/aspire/campaigns/local_mag_lane.py

barrnap runs on each 95% centroid bin of the AB48 dedup run, and collect_mags lays them out as
the linker's genome directory. The link runs twice: on the July DADA2 ASVs of AB48 Enrichment5,
whose Sodalinema ASV must pair with bin 1-15, and on cyano_r1's filtered ASVs, whose pairing is
what the MAG network, the anchors and the master summary join to cyano_r1's network.

Inputs: the cyanoverse AB48 chunk (assembly, bins/quality, bins/cluster_table.tsv), the July
sequences under --cache/july, and under --cache/cyano cyano_r1's filtered ASVs and SpiecEasi
graphs from sockeye. The rest of cyano_r1 comes from its pin and cache/aspire/t6_cyano.
"""

import argparse
import csv
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from local_run import MLIB, REPO, _transform  # noqa: E402

from metasmith.models.direct_run import RunTransform  # noqa: E402
from metasmith.models.solver import Endpoint  # noqa: E402
from metasmith.python_api import DataInstanceLibrary  # noqa: E402

AB48 = Path("/home/tony/agentic_workspace/projects/cyanoverse/ab48/data/revio")
T6 = REPO / "cache" / "aspire" / "t6_cyano"
PIN = REPO / "data" / "aspire" / "cyano_r1"
TYPE_FILES = ("env.yml", "lib.yml", "aspire.yml", "amplicon.yml", "sequences.yml", "binning_local.yml")


def product(transform: Path, out: Path, dtype_props: frozenset) -> Path:
    for group in _transform(transform).model.produces:
        for dep in group:
            if frozenset(dep.properties) == dtype_props:
                hits = list(out.glob(f"*-{Endpoint(properties=set(dep.properties)).key}*"))
                assert len(hits) == 1, f"[{transform.stem}] product not found once in {out}: {hits}"
                return hits[0]
    raise KeyError(f"[{transform.stem}] has no such product")


def pinned(dtype: str) -> Path:
    (child,) = (PIN / dtype.replace("::", "-")).iterdir()
    return child


class Lane:
    def __init__(self, work: Path, home: Path):
        self.work, self.home = work, home
        self.lib = DataInstanceLibrary(work / "inputs.xgdb")
        self.lib.Purge()
        for t in TYPE_FILES:
            self.lib.AddTypeLibrary(MLIB / "data_types" / t)
        for env in ("aspire", "barrnap"):
            self.lib.AddItem(MLIB / "resources" / "env" / f"{env}.env", f"env::{env}.env")
        self.lib.AddItem(MLIB / "resources" / "lib" / "aspire", "lib::aspire")

    def run(self, rel: str, name: str, inputs: list[tuple[str, Path]], cpus=4):
        transform = (MLIB / "transforms" / rel).resolve()
        self.lib.Save()
        out = self.work / name
        shutil.rmtree(out, ignore_errors=True)
        os.environ["METASMITH_WORK_ROOT"] = str(out)
        tooling = list(self._tooling(transform))
        result = RunTransform(transform, self.lib, tooling + [(k, str(v)) for k, v in inputs],
                              work_dir=out, agent_home=self.home, cpus=cpus, memory=8)
        assert result.success, f"[{name}] failed; see {out}"
        print(f"[{name}] ok", flush=True)
        return transform, out

    def _tooling(self, transform: Path):
        inst = _transform(transform)
        for name in inst.BindableNames():
            props = frozenset(inst._dep_names[name].properties)
            for path, dtype in self.lib.manifest.items():
                if dtype.split("::")[0] in ("env", "lib") and frozenset(self.lib.GetType(dtype).properties) == props:
                    yield name, str(path)

    def out(self, transform: Path, out: Path, dtype: str) -> Path:
        return product(transform, out, frozenset(self.lib.GetType(dtype).properties))


def centroids() -> list[str]:
    with open(AB48 / "bins" / "cluster_table.tsv") as f:
        return sorted(r["bin_id"] for r in csv.DictReader(f, delimiter="\t") if r["is_centroid_95"] == "1")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cache", type=Path, default=REPO / "cache" / "aspire" / "t10_mags")
    ap.add_argument("--home", type=Path, default=Path(os.environ.get("AGENT_HOME", Path.home() / "msm.M0mGIjKq")))
    ap.add_argument("--reuse-barrnap", action="store_true", help="keep GFFs an earlier run wrote")
    args = ap.parse_args()
    work = args.cache / "runs"
    work.mkdir(parents=True, exist_ok=True)
    lane = Lane(work, args.home)
    L = lane.lib

    asm = L.AddItem(AB48 / "assembly" / "ABC-240403_KD.fna", "sequences::assembly")
    table = L.AddItem(AB48 / "bins" / "cluster_table.tsv", "binning_local::cluster_table", parents={asm})
    bins, gffs = [], []
    for b in centroids():
        path = AB48 / "bins" / "quality" / f"{b}.fna"
        bins.append(L.AddItem(path, "binning_local::quality_bin_fasta", parents={asm}))
        barrnap = MLIB / "transforms" / "metagenomics" / "binning" / "barrnap.py"
        out = work / "barrnap" / b
        if not (args.reuse_barrnap and out.is_dir()):
            lane.run("metagenomics/binning/barrnap.py", f"barrnap/{b}", [("bin", path)], cpus=2)
        gff = lane.out(barrnap, out, "binning_local::quality_bin_rrna_gff")
        gffs.append(gff)
        L.AddItem(gff, "binning_local::quality_bin_rrna_gff", parents={bins[-1]})

    inputs = [("asm", AB48 / "assembly" / "ABC-240403_KD.fna"),
              ("table", AB48 / "bins" / "cluster_table.tsv")]
    inputs += [("bins", AB48 / "bins" / "quality" / f"{b}.fna") for b in centroids()]
    inputs += [("gffs", g) for g in gffs]
    t, out = lane.run("aspire/collect_mags.py", "collect_mags", inputs, cpus=1)
    mags = lane.out(t, out, "aspire::mag_collection")
    L.AddItem(mags, "aspire::mag_collection")

    study = L.AddItem(T6 / "study_metadata.tsv", "aspire::study_metadata")
    L.AddValue("policy_asv_mag_link.txt", "on", "aspire::asv_mag_link_on", parents={study})
    on = next(p for p, d in L.manifest.items() if d == "aspire::asv_mag_link_on")

    july = next((args.cache / "july").glob("*/asv_seqs/*.fasta"))
    july_gz = work / "july_asvs.fasta.gz"
    os.system(f"gzip -c {july} > {july_gz}")
    links = {}
    for tag, seqs in (("july", july_gz), ("cyano", args.cache / "cyano" / "asv_filtered_seqs.fasta.gz")):
        L.AddItem(seqs, "aspire::asv_filtered_seqs", parents={study})
        t, out = lane.run("aspire/asv_mag_link.py", f"asv_mag_link_{tag}",
                          [("study", T6 / "study_metadata.tsv"), ("policy", on),
                           ("fseqs", seqs), ("mags", mags)], cpus=8)
        links[tag] = (lane.out(t, out, "aspire::asv_mag_pairing"), lane.out(t, out, "aspire::asv_mag_outputs"))

    pairing, magl = links["cyano"]
    c = args.cache / "cyano"
    net = pinned("aspire::network_outputs")
    common = [("study", T6 / "study_metadata.tsv")]
    for path, dtype in ((pairing, "aspire::asv_mag_pairing"), (magl, "aspire::asv_mag_outputs"),
                        (c / "network_all.graphml", "aspire::network_graph_all"),
                        (c / "node_features.csv", "aspire::network_node_features"),
                        (T6 / "asv_taxonomy.tsv", "amplicon::asv_taxonomy"),
                        (T6 / "analysis_counts.tsv", "aspire::analysis_counts"),
                        (T6 / "analysis_metadata.tsv", "aspire::analysis_metadata"),
                        (T6 / "analysis_asv_meta.tsv", "aspire::analysis_asv_meta"),
                        (net, "aspire::network_outputs"),
                        (net / "spieceasi_modules_all.tsv", "aspire::network_modules_all"),
                        (pinned("aspire::clustermap_outputs"), "aspire::clustermap_outputs"),
                        (pinned("aspire::indicspecies_results"), "aspire::indicspecies_results")):
        L.AddItem(path, dtype, parents={study})

    lane.run("aspire/asv_mag_network.py", "asv_mag_network", common + [
        ("graph", c / "network_all.graphml"), ("nf", c / "node_features.csv"),
        ("tax", T6 / "asv_taxonomy.tsv"), ("counts", T6 / "analysis_counts.tsv"),
        ("pairing", pairing), ("magl", magl)])
    t, out = lane.run("aspire/module_mag_anchors.py", "module_mag_anchors", common + [
        ("policy", on), ("mall", net / "spieceasi_modules_all.tsv"), ("nf", c / "node_features.csv"),
        ("tax", T6 / "asv_taxonomy.tsv"), ("counts", T6 / "analysis_counts.tsv"),
        ("md", T6 / "analysis_metadata.tsv"), ("pairing", pairing), ("net", net)])
    anchors = lane.out(t, out, "aspire::module_asv_anchor_table")
    L.AddItem(anchors, "aspire::module_asv_anchor_table", parents={study})
    lane.run("aspire/master_summary.py", "master_summary", common + [
        ("am", T6 / "analysis_asv_meta.tsv"), ("counts", T6 / "analysis_counts.tsv"),
        ("cmaps", pinned("aspire::clustermap_outputs")),
        ("indic", pinned("aspire::indicspecies_results")), ("net", net),
        ("anchors", anchors), ("magl", magl)])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
