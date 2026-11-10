#!/usr/bin/env python3
"""The metasmith port of MetaPathways on Beth Davenport's 17 CPE3 assemblies, one workflow per assembly.

Each workflow takes one assembly as sequences::assembly and its eligible UniteM MAGs (mags.tsv,
written by build_mags.py) as binning_local::quality_bin_fasta. --group picks one of several target
sets per assembly (mp is the MetaPathways port itself); they share the home's task cache, so a lane two
groups need runs once.

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
from metasmith.agents.agent_lock import AgentBusyError  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, DataInstanceLibrary, Runtime, Source, SshSource, TargetBuilder, TransformInstanceLibrary,
)

ROOT = Path("/scratch/phyberos/beth_mp")
PORT = ROOT / "port"
HOME = PORT / "metasmith"
INPUTS = ROOT / "inputs"
PILOT_REFS = Path("/scratch/phyberos/engcyc/mp_pilot/refs")
# fir's /project/6004975 has no twin on another cluster. A ref's cache identity includes its path,
# so moving it re-runs the steps that read it.
FIR_PROJECT = Path(os.environ.get("BETH_FIR_PROJECT", "/project/6004975/phyberos"))
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

# For the extra annotation groups; existing fir copies only, so no plan carries a download.
EXTRA_REFS = {
    "ref::genomad": Path("/scratch/phyberos/databases/genomad"),
    # CAUTION built under a /db bind, which virsorter2.py also uses. VirSorter2 hashes its env path.
    "annotation::virsorter2_db": Path("/scratch/phyberos/refs/virsorter2_2.2.4"),
    "annotation::bakta_db": FIR_PROJECT / "databases" / "bakta",
    "ref::antismash": FIR_PROJECT / "databases" / "antismash",
    "annotation::eggnog_data": Path("/scratch/phyberos/databases/eggnog"),
    "ref::interproscan_data": FIR_PROJECT / "lib" / "interproscan" / "interproscan_data.tgz",
    "annotation::amrfinderplus_db": Path("/scratch/phyberos/databases/amrfinderplus/2026-05-15.1"),
    "annotation::bacmet_diamond_db": Path("/scratch/phyberos/databases/bacmet/bacmet.dmnd"),
    "annotation::megares_diamond_db": Path("/scratch/phyberos/databases/megares/megares.dmnd"),
    "annotation::vfdb_diamond_db": Path("/scratch/phyberos/databases/vfdb/vfdb.dmnd"),
    "annotation::tcdb_diamond_db": Path("/scratch/phyberos/databases/tcdb/tcdb.dmnd"),
    "annotation::pathofact_db": Path("/scratch/phyberos/databases/pathofact_db"),
    # Copied from the workstation DVC cache (data/fabfos/processed). The leaf names differ from
    # ref::label_transfer_landmarks's `landmarks`, which nextflow would otherwise stage over.
    "ref::esm_c_600m_weights": PORT / "refs" / "esm_c_weights" / "esmc_600m.tgz",
    "ref::label_transfer_landmarks_esmc": PORT / "refs" / "label_transfer_landmarks_esmc" / "landmarks_esmc",
    "ref::ezpred_model": PORT / "refs" / "ezpred_model" / "EZpred",
}

TYPE_LIBS = [c.MLIB / "data_types" / t for t in (
    "sequences.yml", "ref.yml", "annotation.yml", "taxonomy.yml", "binning.yml", "binning_local.yml",
    "modelling.yml", "env.yml", "lib.yml")]

# The licensed Pathway Tools image cannot be pulled. It is hardlinked into the store under the name
# the engine derives from env::pathologic.env's container URI, and the store stays mode 700.
LOCAL_IMAGES = {
    "docker..quay.io_txyliu_pathologic..27.0.sif": Path("/scratch/phyberos/engcyc/licensed/pathologic.sif"),
}
# Pulls unpack in node-local tmp, never on Lustre. A login node's /tmp is tmpfs, charged unreclaimably to
# the 16 GiB per-user cgroup, so a login-node pull uses its local-disk /var/tmp. Deploy bakes only the
# first and last lines.
APPTAINER_TMP = "${SLURM_TMPDIR:-/var/tmp}/phyberos_apt.beth_port"
SETUP_COMMANDS = [*c.SETUP_COMMANDS,
                  f'export APPTAINER_TMPDIR="{APPTAINER_TMP}" SINGULARITY_TMPDIR="{APPTAINER_TMP}";'
                  f' mkdir -p "{APPTAINER_TMP}"',
                  f"export APPTAINER_CACHEDIR={HOME / 'container_images'}"]
AGENT_IMAGE = os.environ.get("MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.24.2")

# gpr_7lane's dense landmark x MNXR matrix is ~11 GiB, past its declared 8 GB. A Slurm OOM reaches
# nextflow as exit 1, not 137, so the memory ladder never retries it.
# Promotech pinned cu6 at its full 64 GB grant.
SCALED = {
    "diamond_uniref50": (16, 64, 12),
    "pathologic": (2, 32, 24),
    "gpr_7lane": (4, 64, 1),
    "promotech": (4, 96, 4),
}
# The two parents are 0.81 and 0.96 Gbp against cu6's 49 Mbp, up to ~31x its ORFs. Each entry is a
# single per-assembly task (or a merge over every chunk) whose cu6 wall or MaxRSS, scaled that far,
# passes its declared first grant. Memory has to be right first time, since an OOM is not retried.
PARENT_SCALED = {
    **SCALED,
    "antismash": (32, 160, 12),
    "clean": (4, 64, 20),
    "promotech": (8, 192, 12),
    # 932k parent proteins: per-protein accumulators hold ~18 GB, and the final stack doubles them.
    "esm_c": (4, 80, 12),
    "ezpred": (4, 64, 6),
    "gpr_4lane": (4, 128, 6),
    "gpr_7lane": (4, 192, 6),
    "bakta_noncoding": (32, 96, 12),
    "virsorter2": (8, 32, 16),
    "genomad": (16, 96, 12),
    "pathofact": (8, 96, 12),
    "mobileelementfinder": (4, 32, 6),
    # Measured, not scaled: shortspades_cpe3_parent's single ORF batch ran out of memory at 8 GB.
    "amrfinderplus": (8, 64, 6),
    "ptools_annotation_gather": (1, 64, 2),
    "pathologic": (2, 64, 24),
    "prodigal": (4, 64, 6),
    **{m: (2, 64, 4) for m in ("merge_diamond_uniref50", "merge_proteinbert", "merge_interproscan",
                               "merge_eggnog_mapper", "merge_deepec", "merge_kofamscan", "merge_deeptfactor")},
}

# Tasks that finished with exit 0 were read as "terminated by the external system" while Lustre lagged
# under a neighbouring copy, and a failed resubmission after that wedged the run with nothing queued.
# Nextflow's default wait for .exitcode is 270 s.
#
# The preset retries only on 125/137/140/143 and ignores any other failure, and an ignored chunk
# lets its merge run on the rest. A container that failed to mount on a bad node took three of
# shortspades_cpe3_parent's 187 DIAMOND chunks with exit 1, and the merge completed without them.
# Here any failure retries until the last attempt.
EXTRA_CONFIG = """
executor {
    $slurm {
        exitReadTimeout = '30 min'
    }
}
process {
    errorStrategy = { task.exitStatus != 0 && task.attempt < params.process.tries ? 'retry' : 'ignore' }
}
"""


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


def declare_extra_refs(smith, ensure):
    givens = smith.PoolGivens()
    c.declare_refs(givens, EXTRA_REFS, tags=["beth_mp_port"])
    return c.cite(givens, CACHE_DIR / "extra_refs.xgdb", TYPE_LIBS, ensure)


def load_lib(name, masked=(), keep=None):
    lib = TransformInstanceLibrary.Load((c.MLIB / "transforms" / name).resolve())
    if keep is not None:
        return lib.AsView({Path(p) for p in keep})
    return lib.AsView({Path(p) for p in masked}, invert=True)


def mp_transforms():
    return [
        load_lib("metagenomics", {"binning/das_tool.py", "taxonomy/gtdbtk_de_novo.py"}),
        load_lib("functionalAnnotation"),
        load_lib("fabfos"),
        load_lib("logistics"),
        load_lib("metabolicModelling", keep={"prodigal_from_quality_bin.py"}),
    ]


# No per-bin prodigal, so every ORF target resolves to the assembly's ORFs, and logistics holds only
# the splitters, so a reference that fails to match cannot be replaced by a download.
def annotation_transforms():
    return [
        load_lib("metagenomics", {"binning/das_tool.py", "taxonomy/gtdbtk_de_novo.py"}),
        load_lib("functionalAnnotation"),
        load_lib("fabfos"),
        load_lib("logistics", keep={"chunkOrfsForAnnotation.py", "splitOrfsForAmr.py", "splitContigsForAmr.py"}),
    ]


def mp_targets():
    t = TargetBuilder()
    orfs = t.Add("sequences::orfs")
    for dtype in ("annotation::diamond_uniref50_results", "annotation::pgdb_archive",
                  "annotation::pathway_table"):
        t.Add(dtype, parents=[orfs])
    for dtype in ("annotation::rrna_gff", "taxonomy::rrna_gtdb", "annotation::trna_gff",
                  "taxonomy::contig_lcastar", "annotation::bin_pgdb_archive", "annotation::bin_pathway_table"):
        t.Add(dtype)
    return t


def flat_targets(*dtypes):
    def build():
        t = TargetBuilder()
        for dtype in dtypes:
            t.Add(dtype)
        return t
    return build


GROUPS = {
    "mp": (mp_targets, mp_transforms, True),
    "gpr": (flat_targets("annotation::gpr_table"), annotation_transforms, False),
    "gpr7": (flat_targets("annotation::gpr_table", "annotation::gpr_table_7lane"), annotation_transforms, False),
    "mge": (flat_targets("taxonomy::genomad_virus_summary", "annotation::virsorter2_scores",
                         "annotation::integronfinder_summary", "annotation::mobileelementfinder_results"),
            annotation_transforms, False),
    "noncoding": (flat_targets("annotation::bakta_gff", "annotation::promotech_predictions",
                               "annotation::deeptfactor_results"), annotation_transforms, False),
    "amr_a": (flat_targets("annotation::amrfinderplus_results", "annotation::bacmet_diamond_results",
                           "annotation::megares_diamond_results"), annotation_transforms, False),
    "amr_b": (flat_targets("annotation::vfdb_diamond_results", "annotation::diamond_tcdb_results",
                           "annotation::pathofact_amr"), annotation_transforms, False),
    "bgc_func": (flat_targets("annotation::antismash_json", "annotation::eggnog_results",
                              "annotation::interproscan_results"), annotation_transforms, False),
}


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
    extra = declare_extra_refs(smith, ensure) if importing or args.group != "mp" else None
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds {args.sample}'s givens and the port references")
        return 0

    targets, transforms, with_bins = GROUPS[args.group]
    expected = {"sequences::assembly": 1}
    if with_bins:
        expected["binning_local::quality_bin_fasta"] = len(SAMPLES[args.sample][1])

    t0 = time.time()
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::assembly")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"), refs, *([extra] if extra else [])],
        transforms=transforms(),
        targets=targets(),
    )
    print(f"solved in {time.time() - t0:.1f}s", flush=True)
    c.check_plan(task, expected)
    c.print_plan(task, width=34)
    tag = args.tag or "_".join(["beth_mp_port", args.sample, *([args.group] if args.group != "mp" else [])])
    if args.dag:
        c.write_dag(task, tag, CACHE_DIR / args.sample)
    if not remote:
        print(f"key={task.GetKey()} (dry run; nothing staged or submitted)")
        return 0
    parent = args.sample.endswith("_parent")
    extra_config = CACHE_DIR / "port_extra.config"
    extra_config.write_text(EXTRA_CONFIG)
    # The home stages one workflow at a time and refuses a second rather than queueing it.
    for attempt in range(40):
        try:
            c.stage_and_run(smith, task, CACHE_DIR / args.sample, tag,
                            stage_only=args.stage_only, materialise=args.materialise,
                            params=dict(executor=dict(queueSize=120 if parent else 200), process=dict(tries=3)),
                            scaled=PARENT_SCALED if parent else SCALED, gpus=c.FIR_GPU,
                            on_exist="clear" if args.clear else "update", extra_config=extra_config)
            break
        except AgentBusyError as e:
            print(f"retrying in 60 s: {e}", flush=True)
            time.sleep(60)
    else:
        raise RuntimeError("the agent home stayed busy for 40 minutes")
    if args.materialise:
        c.host_sh('rm -rf /var/tmp/phyberos_apt.beth_port')
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
        p.set_defaults(fn=cmd_run, stage_only=False, materialise=False, launch=False, dag=False, clear=False, tag=None,
                       group="mp")
        if name == "run":
            p.add_argument("--group", choices=list(GROUPS), default="mp")
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
