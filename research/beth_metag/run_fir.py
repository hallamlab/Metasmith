#!/usr/bin/env python3
"""Beth's CPE3 copper-mine enrichments (TCAG DAV36689 HiFi): binning, taxonomy and 4-lane GPR on fir.

    python research/beth_metag/run_fir.py dag [reads|assembly]
    python research/beth_metag/run_fir.py run [reads|assembly] [--plan-only]

reads     HiFi BAM -> fastq -> hifiasm-meta, per sample.
assembly  Beth's own per-sample assemblies, with the HiFi reads for coverage.

Both modes then bin with MetaBAT2, SemiBin2 and COMEBin, score each binner with CheckM2, classify
each binner's bins with GTDB-Tk, pool and dereplicate the passing bins with skani, and build the
gene-attributed GPR table from the four annotation lanes: KOFAMSCAN, CLEAN, DIAMOND UniRef50 and
ProteinBERT label transfer.

The BAMs are copies under ROOT/reads, one per sample, named for the sample: every TCAG folder
names its file deduplicated.ccs.bam, and a leaf id pinned on the file name alone would serve
one sample's reads to all six.
"""

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))

from _driver import FIR_ACCOUNT, fir_agent, pin_external_leaf_ids  # noqa: E402
from metasmith.models.dag_draw import Label  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, DataInstanceLibrary, Runtime, Source, TransformInstanceLibrary,
)

MLIB = REPO / "src" / "metasmith_libraries"
ROOT = os.environ.get("BETH_METAG_ROOT", "/scratch/phyberos/beth_metag")
AGENT_HOME = f"{ROOT}/agent_home"
READS = f"{ROOT}/reads"
REFS = f"{ROOT}/refs"
ASSEMBLIES = f"{ROOT}/assemblies"
CONTAINER = "docker://quay.io/hallamlab/metasmith:0.24.2"
WORK = REPO / "cache" / "beth_metag"
RESULTS = HERE / "results"

SAMPLES = ["cpe3_parent", "cpe3_r2a", "cpe3_r2a_cu1_5", "cpe3_r2a_cu6", "cpe3_r2a_cu7_8",
           "cpe3_r2a_cu9_11"]

# The three GPR references have consumers and no producer in the library. They are the pinned
# copies under data/fabfos/processed/, staged to REFS. Every other database (GTDB, CheckM2,
# KOfam, UniRef50) is left to its download transform here and swapped for fir's copy at run time.
GPR_REFS = {
    "ref::mnxr_lookup": f"{REFS}/mnxr_lookup/mnxr_lookup.parquet",
    "ref::label_transfer_landmarks": f"{REFS}/label_transfer_landmarks",
    "ref::lane_calibration": f"{REFS}/lane_calibration",
}

BINNERS = ("metabat2", "semibin2", "comebin")


def targets(mode: str) -> list:
    # flye, miniasm and hifiasm also assemble long reads, so in reads mode an unpinned
    # assembly-derived target is answered from whichever the planner likes, and it ran all
    # three. Each is pinned to hifiasm-meta's assembly at index 0. A given cannot be a target,
    # so assembly mode has nothing to pin to and relies on the given being the cheaper route.
    t = ["sequences::hifiasm_meta_assembly"] if mode == "reads" else []

    def on_asm(dtype):
        return {"type": dtype, "parents": [0]} if mode == "reads" else dtype

    t.append("sequences::read_qc_stats")
    for d in ("sequences::assembly_stats", "sequences::assembly_per_contig_coverage",
              "sequences::assembly_per_bp_coverage"):
        t.append(on_asm(d))
    bins = {}
    for b in BINNERS:
        bins[b] = len(t)
        t.append(on_asm(f"sequences::{b}_bin_fasta"))
        t.append(on_asm(f"binning::{b}_contig_to_bin_table"))
    # Every binner's bin_fasta is a sequences::putative_genome, so an unpinned checkm_stats or
    # gtdbtk is satisfied by one binner alone and the aggregator, which needs all three, starves.
    for b in BINNERS:
        for d in ("taxonomy::checkm_stats", "taxonomy::gtdbtk"):
            t.append({"type": d, "parents": [bins[b]]})
    t += [on_asm("binning_local::quality_bin_fasta"), on_asm("binning_local::cluster_table")]
    orfs = len(t)
    t += [on_asm("sequences::orfs"), on_asm("sequences::gff")]
    # The lane outputs are named because gpr_4lane consumes them: as intermediates the collect
    # phase drops them, and none is recoverable without rerunning its lane.
    t += [{"type": d, "parents": [orfs]} for d in (
        "annotation::gpr_table", "annotation::kofamscan_results",
        "annotation::kofamscan_descriptions", "annotation::clean_predictions",
        "annotation::diamond_uniref50_results", "annotation::diamond_uniref50_descriptions",
        "annotation::proteinbert_embeddings")]
    return t


def build_inputs(mode: str) -> tuple[DataInstanceLibrary, DataInstanceLibrary]:
    work = WORK / mode
    work.mkdir(parents=True, exist_ok=True)
    types = ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml", "taxonomy.yml",
             "binning.yml", "binning_local.yml")

    lib = DataInstanceLibrary(work / "inputs.xgdb")
    lib.Purge()
    for tl in types:
        lib.AddTypeLibrary(MLIB / "data_types" / tl)
    for s in SAMPLES:
        meta = lib.AddValue(f"{s}.read_metadata.json",
                            {"sample": s, "parity": "single", "length_class": "long"},
                            "sequences::read_metadata")
        lib.AddItem(f"{READS}/{s}.hifi.bam", "sequences::pacbio_hifi_bam", parents={meta})
        if mode == "assembly":
            lib.AddItem(f"{ASSEMBLIES}/{s}.contigs.fna", "sequences::assembly", parents={meta})
    pin_external_leaf_ids(lib)
    lib.Save()

    refs = DataInstanceLibrary(work / "refs.xgdb")
    refs.Purge()
    refs.AddTypeLibrary(MLIB / "data_types" / "ref.yml")
    for dtype, path in GPR_REFS.items():
        refs.AddItem(path, dtype)
    pin_external_leaf_ids(refs)
    refs.Save()
    return lib, refs


TRANSFORMS = [MLIB / "transforms" / n
              for n in ("logistics", "assembly", "metagenomics", "functionalAnnotation", "fabfos")]


def plan(smith, mode: str):
    inputs, refs = build_inputs(mode)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[refs] + [DataInstanceLibrary.Load(MLIB / "resources" / n) for n in ("env", "lib")],
        transforms=[TransformInstanceLibrary.Load(p) for p in TRANSFORMS],
        targets=targets(mode),
    )
    if not task.ok:
        sys.exit(f"{mode} did not solve: dropped {sorted(task.plan.dropped_targets)}")
    print(f"{mode}: {len(task.plan.steps)} steps over {len(SAMPLES)} samples, key={task.GetKey()}")
    for step in task.plan.steps:
        uses = sorted({u.dtype_name for u in step.uses if not u.dtype_name.startswith(("env::", "lib::"))})
        print(f"  {step.order:>3} {step.transform.name:<28} <- {', '.join(uses)}")
    return task


def dry_agent(mode: str) -> Agent:
    return Agent(home=Source.FromLocal(WORK / mode / "dryrun_home"), runtime=Runtime.APPTAINER)


def agent() -> Agent:
    return fir_agent(agent_home=AGENT_HOME, container=CONTAINER)


def cmd_dag(args):
    RESULTS.mkdir(exist_ok=True)
    for mode in modes(args):
        task = plan(dry_agent(mode), mode)
        dag = task.plan.BuildDAG(given_root=False, target_sink=False)
        # BuildDAG names a transform node "<order> <name>"; DagRenderer has no public relabel.
        for step in task.plan.steps:
            root = Path(step.transform_library.location).name
            dag._labels[f"{step.order} {step.transform.name}"] = Label(name=step.transform.name, namespace=root)
        for fmt in ("svg", "png"):
            print(dag.render(RESULTS / f"beth_metag_dag_{mode}", fmt))
    return 0


def cmd_run(args):
    for mode in modes(args):
        plan(dry_agent(mode) if args.plan_only else agent(), mode)
    if args.plan_only:
        return 0
    sys.exit(f"Not launched. Stage the BAMs and refs under {ROOT}, point the databases at fir's "
             f"copies, and clear the run with Tony first: it is a multi-day job on {FIR_ACCOUNT}.")


def modes(args) -> list[str]:
    return [args.mode] if args.mode else ["reads", "assembly"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("dag", "run"):
        p = sub.add_parser(name)
        p.add_argument("mode", nargs="?", choices=["reads", "assembly"])
        if name == "run":
            p.add_argument("--plan-only", action="store_true")
    args = ap.parse_args()
    return {"dag": cmd_dag, "run": cmd_run}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
