#!/usr/bin/env python3
"""The metasmith port of MetaPathways on Beth Davenport's 17 CPE3 assemblies, one workflow per assembly.

Each workflow takes one assembly as sequences::assembly and its eligible UniteM MAGs (mags.tsv,
written by build_mags.py) as binning_local::quality_bin_fasta.

Subcommands: setup [--deploy], import --sample S, run --sample S [--dag]
[--stage-only|--materialise|--launch] [--clear], status.

Without a mode, `run` solves against a local dry-run home and stages nothing. --materialise stages
and pulls every tool image into the home's store; run it from the workstation, whose agent shell is a
login node. --launch runs inside submit_driver.sbatch on a compute node (sync_port.sh puts the
checkout there).
"""

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "research" / "metasmith_benchmark" / "drivers"))
CACHE_DIR = Path(os.environ.get("BETH_PORT_CACHE_DIR", HERE / ".cache"))

import _common as c  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, DataInstanceLibrary, Runtime, Source, SshSource, TargetBuilder, TransformInstanceLibrary,
)

ROOT = Path("/scratch/phyberos/beth_mp")
PORT = ROOT / "port"
HOME = PORT / "metasmith"
INPUTS = ROOT / "inputs"
PILOT_REFS = Path("/scratch/phyberos/engcyc/mp_pilot/refs")
MAGS_TSV = HERE / "mags.tsv"


def load_samples():
    mags = defaultdict(list)
    with MAGS_TSV.open() as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            mags[r["sample"]].append(Path(r["path"]))
    return {s: (INPUTS / f"{s}.fasta", sorted(m)) for s, m in mags.items()}


SAMPLES = load_samples()

GLOBAL_REFS = {
    "ref::uniref50_diamond_db": c.DB_PATHS["ref::uniref50_diamond_db"],
    "ref::kofamscan_profiles": c.DB_PATHS["ref::kofamscan_profiles"],
    "ref::kofamscan_ko_list": c.DB_PATHS["ref::kofamscan_ko_list"],
    "ref::metabuli_ref": Path("/home/phyberos/project-rpp/lib/metabuli/gtdb"),
    "ref::gtdb_metadata": PILOT_REFS / "gtdb_metadata",
    "ref::gtdb_rrna": PILOT_REFS / "gtdb_rrna",
    "ref::ncbi_taxdump": PILOT_REFS / "ncbi_taxdump",
    "ref::mnxr_lookup": Path("/scratch/phyberos/fabfos_refs/processed/mnxr_lookup/mnxr_lookup.parquet"),
    "ref::label_transfer_landmarks": Path("/scratch/phyberos/fabfos_refs/processed/label_transfer_landmarks/landmarks"),
}

TYPE_LIBS = [c.MLIB / "data_types" / t for t in (
    "sequences.yml", "ref.yml", "annotation.yml", "taxonomy.yml", "binning.yml", "binning_local.yml",
    "modelling.yml", "env.yml", "lib.yml")]

# The licensed Pathway Tools image cannot be pulled. It is hardlinked into the store under the name
# the engine derives from env::pathologic.env's container URI, and the store stays mode 700.
LOCAL_IMAGES = {
    "docker..quay.io_txyliu_pathologic..27.0.sif": Path("/scratch/phyberos/engcyc/licensed/pathologic.sif"),
}
SETUP_COMMANDS = [*c.SETUP_COMMANDS, f"export APPTAINER_CACHEDIR={HOME / 'container_images'}"]
AGENT_IMAGE = os.environ.get("MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.24.2")

SCALED = {
    "diamond_uniref50": (16, 64, 12),
    "pathologic": (2, 32, 24),
}


def home_source():
    return Source.FromLocal(HOME) if c.ON_HOST else SshSource(host=c.HPC_HOST, path=HOME).AsSource()


def get_agent(remote):
    if not remote:
        return Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home"), runtime=Runtime.APPTAINER)
    return Agent(home=home_source(), container=AGENT_IMAGE, runtime=Runtime.APPTAINER, setup_commands=SETUP_COMMANDS)


def declare_sample(smith, sample, ensure):
    asm_path, mags = SAMPLES[sample]
    givens = smith.PoolGivens()
    tags = ["beth_mp_port", sample]
    asm = c.add_file(givens, f"beth_mp_port/{sample}/assembly", asm_path, "sequences::assembly", tags=tags)
    for mag in mags:
        c.add_file(givens, c.given_name(f"beth_mp_port/{sample}/{mag.stem}", asm_path), mag,
                   "binning_local::quality_bin_fasta", parents=[asm], tags=tags)
    return c.cite(givens, CACHE_DIR / sample / "inputs.xgdb", TYPE_LIBS, ensure)


def declare_globals(smith, ensure):
    givens = smith.PoolGivens()
    c.declare_refs(givens, GLOBAL_REFS, tags=["beth_mp_port"])
    return c.cite(givens, CACHE_DIR / "globals.xgdb", TYPE_LIBS, ensure)


def build_transforms():
    def load(name, masked=(), keep=None):
        lib = TransformInstanceLibrary.Load((c.MLIB / "transforms" / name).resolve())
        if keep is not None:
            return lib.AsView({Path(p) for p in keep})
        return lib.AsView({Path(p) for p in masked}, invert=True)

    return [
        load("metagenomics", {"binning/das_tool.py", "taxonomy/gtdbtk_de_novo.py"}),
        load("functionalAnnotation"),
        load("fabfos"),
        load("logistics"),
        load("metabolicModelling", keep={"prodigal_from_quality_bin.py"}),
    ]


def build_targets():
    t = TargetBuilder()
    orfs = t.Add("sequences::orfs")
    for dtype in ("annotation::diamond_uniref50_results", "annotation::pgdb_archive",
                  "annotation::pathway_table"):
        t.Add(dtype, parents=[orfs])
    for dtype in ("annotation::rrna_gff", "taxonomy::rrna_gtdb", "annotation::trna_gff",
                  "taxonomy::contig_lcastar", "annotation::bin_pgdb_archive", "annotation::bin_pathway_table"):
        t.Add(dtype)
    return t


def cmd_setup(args):
    store = HOME / "container_images"
    # The `.verified` stamp stops the engine re-pulling an image it finds without one, which fails here.
    links = "; ".join(f'([ -e "{store}/{name}" ] || ln "{src}" "{store}/{name}") && touch "{store}/{name}.verified"'
                      for name, src in LOCAL_IMAGES.items())
    print(c.host_sh(f'mkdir -p "{store}" && chmod 700 "{store}" && {links} && ls -la "{store}"'))
    if args.deploy:
        Agent(home=home_source(), container=AGENT_IMAGE, runtime=Runtime.APPTAINER,
              setup_commands=[SETUP_COMMANDS[0], SETUP_COMMANDS[-1]]).Deploy(assertive=True)
    return 0


def cmd_run(args):
    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.materialise or args.launch
    smith = get_agent(remote)
    ensure = importing or not remote
    inputs = declare_sample(smith, args.sample, ensure)
    refs = declare_globals(smith, ensure)
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds {args.sample}'s givens and the port references")
        return 0

    t0 = time.time()
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::assembly")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"), refs],
        transforms=build_transforms(),
        targets=build_targets(),
    )
    print(f"solved in {time.time() - t0:.1f}s", flush=True)
    c.check_plan(task, {"sequences::assembly": 1, "binning_local::quality_bin_fasta": len(SAMPLES[args.sample][1])})
    c.print_plan(task, width=34)
    if args.dag:
        c.write_dag(task, f"beth_mp_port_{args.sample}", CACHE_DIR / args.sample)
    if not remote:
        print(f"key={task.GetKey()} (dry run; nothing staged or submitted)")
        return 0
    c.stage_and_run(smith, task, CACHE_DIR / args.sample, args.tag or f"beth_mp_port_{args.sample}",
                    stage_only=args.stage_only, materialise=args.materialise,
                    params=dict(executor=dict(queueSize=200), process=dict(tries=3)),
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
    p = sub.add_parser("setup", help="hardlink the licensed image into the home's store")
    p.add_argument("--deploy", action="store_true")
    p.set_defaults(fn=cmd_setup)
    for name in ("import", "run"):
        p = sub.add_parser(name)
        p.add_argument("--sample", required=True, choices=sorted(SAMPLES))
        p.set_defaults(fn=cmd_run, stage_only=False, materialise=False, launch=False, dag=False, clear=False, tag=None)
        if name == "run":
            p.add_argument("--dag", action="store_true")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--materialise", action="store_true")
            mode.add_argument("--launch", action="store_true")
            p.add_argument("--tag")
            p.add_argument("--clear", action="store_true", help="restage from scratch; deletes the run's logs")
    sub.add_parser("status").set_defaults(fn=cmd_status)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
