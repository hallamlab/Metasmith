#!/usr/bin/env python3
"""The MetaPathways transforms on fir, from a given assembly and its quality MAGs.

  smoke   OMD genome ENG000030922, given as the assembly and as its one quality MAG
  sg8w14  Spanish Lakes SG8W14 (MEGAHIT, 154 MB) with three COMEBin MAGs

Subcommands: setup [--deploy], import --sample S, run --sample S [--trna T] [--dag]
[--stage-only|--launch] [--clear], status.

Without --stage-only or --launch, `run` solves against a local dry-run home and stages nothing.
Remote modes run inside submit_driver.sbatch on a compute node (sync_pilot.sh puts the checkout there).
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "research" / "metasmith_benchmark" / "drivers"))
CACHE_DIR = Path(os.environ.get("MP_PILOT_CACHE_DIR", HERE / ".cache"))

import _common as c  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, DataInstanceLibrary, Runtime, Source, SshSource, TargetBuilder, TransformInstanceLibrary,
)

PILOT = Path("/scratch/phyberos/engcyc/mp_pilot")
HOME = PILOT / "metasmith"
REFS = PILOT / "refs"
SL = Path("/project/rpp-shallam/phyberos/spanish_lakes/metagenomics")

SAMPLES = {
    # The MAG is a hardlink of the genome: one path cannot be both a given and its own child.
    "smoke": (Path("/scratch/phyberos/engcyc/batches/pilot00003/ENG000030922.fa"),
              [PILOT / "inputs" / "ENG000030922.mag.fna"]),
    "sg8w14": (SL / "assembly" / "fna" / "SG8W14.fna",
               [SL / "binning" / "fna_comebin" / f"SG8W14.comebin.{b}.fa" for b in ("041", "072", "018")]),
}

# The Metabuli database on fir is GTDB214.1+humanT2T. gtdb232 is a 634 GB tarball nobody has staged.
GLOBAL_REFS = {
    "ref::uniref50_diamond_db": c.DB_PATHS["ref::uniref50_diamond_db"],
    "ref::kofamscan_profiles": c.DB_PATHS["ref::kofamscan_profiles"],
    "ref::kofamscan_ko_list": c.DB_PATHS["ref::kofamscan_ko_list"],
    "ref::metabuli_ref": Path("/home/phyberos/project-rpp/lib/metabuli/gtdb"),
    "ref::gtdb_metadata": REFS / "gtdb_metadata",
    "ref::gtdb_rrna": REFS / "gtdb_rrna",
    "ref::ncbi_taxdump": REFS / "ncbi_taxdump",
    "ref::mnxr_lookup": Path("/scratch/phyberos/fabfos_refs/processed/mnxr_lookup/mnxr_lookup.parquet"),
    "ref::label_transfer_landmarks": Path("/scratch/phyberos/fabfos_refs/processed/label_transfer_landmarks/landmarks"),
}
MEDIUM_TSV = REPO / "research" / "metasmith_libraries" / "carveme_m8_medium.tsv"
MEDIUM_NAME = "M8"

TYPE_LIBS = [c.MLIB / "data_types" / t for t in (
    "sequences.yml", "ref.yml", "annotation.yml", "taxonomy.yml", "binning.yml", "binning_local.yml",
    "modelling.yml", "env.yml", "lib.yml")]

# The licensed Pathway Tools image cannot be pulled. It is adopted into the store under the name
# the engine derives from env::pathologic.env's container URI, and the store stays mode 700.
LOCAL_IMAGES = {
    "docker..quay.io_txyliu_pathologic..latest.sif": Path("/scratch/phyberos/mp4/private/pathologic.sif"),
}
SETUP_COMMANDS = [*c.SETUP_COMMANDS, f"export APPTAINER_CACHEDIR={HOME / 'container_images'}"]
AGENT_IMAGE = os.environ.get("MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.24.2")

TRNA = {"aragorn": "trna_aragorn.py", "trnascan": "trna_trnascan.py"}
SCALED = {
    "diamond_uniref50": (16, 64, 12),
    "pathologic": (2, 32, 24),
}


def get_agent(remote):
    if not remote:
        return Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home"), runtime=Runtime.APPTAINER)
    source = Source.FromLocal(HOME) if c.ON_HOST else SshSource(host=c.HPC_HOST, path=HOME).AsSource()
    return Agent(home=source, container=AGENT_IMAGE, runtime=Runtime.APPTAINER, setup_commands=SETUP_COMMANDS)


def declare_sample(smith, sample, ensure):
    asm_path, mags = SAMPLES[sample]
    givens = smith.PoolGivens()
    tags = ["engcyc", "mp_pilot", sample]
    asm = c.add_file(givens, f"mp_pilot/{sample}/assembly", asm_path, "sequences::assembly", tags=tags)
    for mag in mags:
        c.add_file(givens, f"mp_pilot/{sample}/{mag.stem}", mag, "binning_local::quality_bin_fasta",
                   parents=[asm], tags=tags)
    return c.cite(givens, CACHE_DIR / sample / "inputs.xgdb", TYPE_LIBS, ensure)


def declare_globals(smith, ensure):
    givens = smith.PoolGivens()
    c.declare_refs(givens, GLOBAL_REFS, tags=["mp_pilot"])
    c.add_value(givens, "ref/modelling::media", MEDIUM_TSV.read_text(), "modelling::media", tags=["reference"])
    c.add_value(givens, "ref/modelling::medium_name", MEDIUM_NAME, "modelling::medium_name", tags=["reference"])
    return c.cite(givens, CACHE_DIR / "globals.xgdb", TYPE_LIBS, ensure)


def build_transforms(trna):
    def load(name, masked=(), keep=None):
        lib = TransformInstanceLibrary.Load((c.MLIB / "transforms" / name).resolve())
        if keep is not None:
            return lib.AsView({Path(p) for p in keep})
        return lib.AsView({Path(p) for p in masked}, invert=True)

    # The losing tRNA scanner leaves, so trna_gff has one producer. The OMD lane stays out of the search.
    other = {p for k, p in TRNA.items() if k != trna}
    return [
        load("metagenomics", {"binning/das_tool.py", "taxonomy/gtdbtk_de_novo.py"}),
        load("functionalAnnotation", other | {"metapathways_pgdb.py"}),
        load("fabfos"),
        load("logistics"),
        load("metabolicModelling", keep={"prodigal_from_quality_bin.py", "carveme_from_orfs.py", "memote_score.py"}),
    ]


def build_targets(with_gem):
    t = TargetBuilder()
    orfs = t.Add("sequences::orfs")
    for dtype in ("annotation::diamond_uniref50_results", "annotation::ec_consensus",
                  "annotation::pgdb_archive", "annotation::pathway_table"):
        t.Add(dtype, parents=[orfs])
    for dtype in ("annotation::rrna_gff", "taxonomy::rrna_gtdb", "annotation::trna_gff",
                  "taxonomy::contig_lcastar", "annotation::bin_pgdb_archive", "annotation::bin_pathway_table"):
        t.Add(dtype)
    if with_gem:
        gem = t.Add("modelling::carveme_model")
        t.Add("modelling::memote_score", parents=[gem])
    return t


def expected_counts(sample):
    return {"sequences::assembly": 1, "binning_local::quality_bin_fasta": len(SAMPLES[sample][1])}


def cmd_setup(args):
    store = HOME / "container_images"
    # The `.verified` stamp stops the engine re-pulling an image it finds without one, which fails here.
    links = "; ".join(f'([ -e "{store}/{name}" ] || ln "{src}" "{store}/{name}") && touch "{store}/{name}.verified"'
                      for name, src in LOCAL_IMAGES.items())
    print(c.host_sh(f'mkdir -p "{store}" && chmod 700 "{store}" && {links} && ls -la "{store}"'))
    if args.deploy:
        source = Source.FromLocal(HOME) if c.ON_HOST else SshSource(host=c.HPC_HOST, path=HOME).AsSource()
        Agent(home=source, container=AGENT_IMAGE, runtime=Runtime.APPTAINER,
              setup_commands=[SETUP_COMMANDS[0], SETUP_COMMANDS[-1]]).Deploy(assertive=True)
    return 0


def cmd_run(args):
    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch
    smith = get_agent(remote)
    ensure = importing or not remote
    inputs = declare_sample(smith, args.sample, ensure)
    refs = declare_globals(smith, ensure)
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds {args.sample}'s givens and the pilot references")
        return 0

    t0 = time.time()
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::assembly")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"), refs],
        transforms=build_transforms(args.trna),
        targets=build_targets(args.gem),
    )
    print(f"solved in {time.time() - t0:.1f}s", flush=True)
    c.check_plan(task, expected_counts(args.sample))
    c.print_plan(task, width=34)
    if args.dag:
        c.write_dag(task, f"mp_pilot_{args.sample}", CACHE_DIR / args.sample)
    if not remote:
        print(f"key={task.GetKey()} (dry run; nothing staged or submitted)")
        return 0
    c.stage_and_run(smith, task, CACHE_DIR / args.sample, args.tag or f"mp_pilot_{args.sample}_{args.trna}",
                    stage_only=args.stage_only, params=dict(executor=dict(queueSize=200), process=dict(tries=3)),
                    scaled=SCALED, gpus=c.FIR_GPU, on_exist="clear" if args.clear else "update")
    return 0


def cmd_status(args):
    smith = get_agent(True)
    for keys_file in sorted(CACHE_DIR.glob("*/task_keys.json")):
        for name, key in json.loads(keys_file.read_text()).items():
            print(f"\n{name} ({key}):")
            try:
                smith.CheckWorkflow(key)
            except Exception as e:
                print(f"  {type(e).__name__}: {e}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("setup", help="adopt the licensed image into the home's store")
    p.add_argument("--deploy", action="store_true")
    p.set_defaults(fn=cmd_setup)
    for name in ("import", "run"):
        p = sub.add_parser(name)
        p.add_argument("--sample", required=True, choices=list(SAMPLES))
        p.set_defaults(fn=cmd_run, stage_only=False, launch=False, dag=False, clear=False, tag=None,
                       trna="aragorn", gem=False)
        if name == "run":
            p.add_argument("--trna", choices=list(TRNA), default="aragorn")
            p.add_argument("--gem", action="store_true", help="add CarveMe and MEMOTE per quality MAG")
            p.add_argument("--dag", action="store_true")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--launch", action="store_true")
            p.add_argument("--tag")
            p.add_argument("--clear", action="store_true", help="restage from scratch; deletes the run's logs")
    sub.add_parser("status").set_defaults(fn=cmd_status)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
