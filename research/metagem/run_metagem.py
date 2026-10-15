#!/usr/bin/env python3
"""Assembly, MetaWRAP binning and per-MAG CarveMe/MEMOTE reconstruction over the
five metaGEM studies (Zorrilla et al. 2021, NAR 49(21):e126) on fir.

Modelled on research/cami/run_cami_metag.py's Pratama arm (paired two-file reads,
not CAMI's single interleaved file) -- see that file's build_inputs_pratama for the
exemplar this driver's read-registration chain is copied from. Three things are
DELIBERATELY different here, and each is explained where it happens rather than
just in this docstring:

1.  NO shared study root. build_inputs_pratama parents every run's read_metadata
    under one viromics::contig_study node so its cross-sample viral clustering can
    pool every run into one solver sample-view. metaGEM has no cross-run pooling
    step at all -- it reconstructs one genome-scale model per MAG per run -- so
    parenting reads under a shared root here would silently collapse 491 runs into
    one solver sample-view instead of 491, for no benefit. See build_inputs's
    docstring.

2.  One study (korem2015) is SINGLE-END, not paired. metaGEM itself keeps a
    separate Snakemake rule for this case; this driver mirrors that with a
    SEPARATE solver task rather than a Snakemake rule -- `cmd_run` splits the
    corpus by read shape (`split_by_layout`) and calls `GenerateWorkflow` once
    per shape (`_run_lane`), because solving both shapes in one call silently
    drops one of them (see build_globals's docstring, "CORRECTION" paragraph,
    for the reproduced mechanism and why the earlier single-call fix was
    incomplete). korem2015 ALSO gets a different binning path than the other
    four studies -- MetaBAT2+SemiBin2+COMEBin+DAS Tool rather than MetaWRAP,
    which hard-asserts paired reads -- but reaches the SAME reconstruction
    and scoring targets and STAYS IN the model comparison: metaGEM published
    a full model set for this study, and its own repository splits binning
    method by read shape the same way this driver now does. See
    enumerate_runs, build_inputs and build_targets's "REVERSED" note.

3.  The reconstruction target is metaGEM's own step: CarveMe run directly on
    per-bin predicted proteins (carveme_from_orfs.py, wave 1), not the GPR-draft
    -> gapfill route this library also carries for the KBase-parity lane. See
    build_targets.

Every solve checks `Counter(inst.dtype_name for inst in task.plan.given)`
against each lane's expected per-type leaf counts before printing "Plan OK"
and refuses to proceed on a mismatch (see _check_given_leaf_counts) -- the
bug this round fixed reported `dropped=[]` and `ok=True` while silently
planning 48 of 491 runs out of existence, and neither field nor a step count
can see that class of defect; only counting what actually reached the plan
can.

Subcommands: list-samples, check-dbs, setup, run [--dry-run], status.
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

os.environ["PATH"] = f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}"

from metasmith.python_api import (  # noqa: E402
    Agent, Source, SshSource,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Runtime,
    Resources, Size, Duration,
)

ROOT = Path(__file__).resolve().parent
MLIB = Path(os.environ.get(
    "MSM_LIB", str(Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries")))
CACHE_DIR = Path(os.environ.get("MSM_CACHE_DIR", ROOT / ".cache"))

HPC_HOST      = os.environ.get("MSM_HPC_HOST", "fir")
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "rrg-shallam-ab")
# Rendered into the run's launcher (start.sh, and start.slurm.sh under
# METASMITH_DRIVER_SLURM) after the cd into the run workspace -- AND run, one command at a
# time, in the persistent login-node shell that `Agent._run_setup` opens for every
# StageWorkflow and RunWorkflow. Two contexts, different working directories, so every
# line here has to be correct in both.
#
# WARNING never `exit` from one of these. An `exit 1` guard here KILLED the orchestrating
# shell, and the caller saw `TimeoutError: [agent setup command] produced no output for
# 300s` -- a hang that names no cause. Report and continue; a missing relay fails loudly
# one step later either way. For the same reason every line must emit something or return
# promptly, and anything backgrounded needs all three descriptors redirected plus `setsid`,
# or the shell never sees the command finish.
#
# The relay line is what makes the ENGINE'S OWN compute-node driver route usable with the
# APPTAINER runtime. `METASMITH_DRIVER_SLURM=1` sbatches the driver onto an allocated node,
# which is the right place for it -- a fir login node's 16 GiB per-user cgroup fills with
# page cache from our own staging I/O and then SIGKILLs any JVM that claims a heap floor.
# But the relay is NODE-BOUND: its socket lives in that node's own /tmp and is discovered
# by hostname, symlinked into the agent home as `relay/<hostname>`. Nothing in the engine's
# Slurm branch starts one, and `runner.py`'s stage and run paths connect THROUGH it whenever
# `needs_relay`, which is true for APPTAINER.
#
# CAUTION the `[ ! -e ]` guard is load-bearing rather than defensive: starting a second
# relay on a node that already has one replaces the symlink the first one's driver is using.
# The agent home is found by walking UP from $PWD looking for relay/msm_relay, so the same
# line resolves it from the agent home (the login-node shell) and from runs/<key> (the
# launcher) without either context being named here.
SETUP_COMMANDS = [
    "module load apptainer",
    'MSM_RELAY_HOME="$PWD"; for _ in 1 2 3 4; do'
    ' [ -x "$MSM_RELAY_HOME/relay/msm_relay" ] && break;'
    ' MSM_RELAY_HOME="$(dirname "$MSM_RELAY_HOME")"; done',
    '[ -x "$MSM_RELAY_HOME/relay/msm_relay" ] && [ ! -e "$MSM_RELAY_HOME/relay/$(hostname)" ]'
    ' && ( cd "$MSM_RELAY_HOME" && setsid ./relay/msm_relay start'
    ' >"/tmp/msm_relay_$(hostname).log" 2>&1 </dev/null & ); true',
    'for _ in 1 2 3 4 5 6 7 8 9 10; do'
    ' [ -e "$MSM_RELAY_HOME/relay/$(hostname)" ] && break; sleep 1; done;'
    ' echo "relay on $(hostname):'
    ' $([ -e "$MSM_RELAY_HOME/relay/$(hostname)" ] && echo present || echo MISSING)"',
]

METAGEM_ROOT = Path(os.environ.get("METAGEM_ROOT", "/scratch/phyberos/metagem"))
HPC_MSM_HOME = Path(os.environ.get("MSM_AGENT_HOME", str(METAGEM_ROOT / "metasmith")))

# The tracked manifest a standing data-acquisition agent owns and is actively
# writing (935 rows: header + 934 files, 491 runs across the 5 studies) --
# read-only from here, exactly as research/cami/run_cami_metag.py treats its own
# samples.tsv. Runs are recovered from `relpath`, NOT trusted to a directory
# glob under METAGEM_ROOT: the acquisition job has not populated a `cluster/`
# tree yet at the time this driver was written, and a glob that finds nothing
# would silently plan zero runs rather than fail loudly. See enumerate_runs.
MANIFEST_TSV = Path(os.environ.get("METAGEM_MANIFEST_TSV", str(ROOT / "manifest.tsv")))

# metaGEM's own medium table (74 BiGG-namespaced compounds, built from metaGEM's
# media_db.tsv) and the row it selects -- both STUDY-WIDE constants, the same for
# every run in every study, so they are registered ONCE with no parent at all
# (see build_inputs), never per-run.
MEDIUM_TSV  = Path(os.environ.get(
    "METAGEM_MEDIUM_TSV",
    str(ROOT.parent / "metasmith_libraries" / "carveme_m8_medium.tsv")))
MEDIUM_NAME = os.environ.get("METAGEM_MEDIUM_NAME", "M8")

# The private CPLEX 22.2.0.0 Python runtime (modelling::cplex_installation),
# registered directly at its real path the way research/cami's DB_PATHS
# registers reference databases -- NEVER DEFERRED (StageWorkflow refuses a
# deferred input rather than staging a hole). Found live via a single `ssh fir`
# probe this session (not on scratch's usual /home/phyberos/project-rpp/lib --
# that guess MISSED; the real location is durable project space, not scratch).
# NOT independently verified to actually import cplex/docplex from THIS driver
# (the ssh connection dropped before a second probe could confirm the
# directory's contents; check-dbs re-checks existence, not importability).
# --with-cplex stays off by default until that import is confirmed.
# resources/lib/modelling/stage_cplex_installation.sh's node-local-scratch copy
# (a beforeScript, same shape as research/cami/run_cami_metag.py's COMEBin GPU
# arm) is a further optimization against repeated small Lustre reads, NOT
# wired in here -- correctness only needs the item to resolve to a directory
# holding cplex+docplex, which this path already does.
CPLEX_ROOT = Path(os.environ.get(
    "CPLEX_ROOT", "/home/phyberos/projects/rpp-shallam/phyberos/cplex/cplex_runtime"))

AGENT_IMAGE = os.environ.get(
    "MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.22.1")

CONTAINERS = [
    "bbtools", "megahit", "samtools", "minimap2", "bedtools",
    "metawrap", "checkm", "pprodigal", "carveme", "memote",
]

# Named per-process overrides for RunWorkflow. metawrap was missing here
# entirely in the previous revision, which left it on the SLURM preset's
# defaults (4 cpus, 16 GB, 6 h) -- metawrap.py's own `mem = max(int(mem_gb *
# 0.85), 4)` turns 16 GB into mem=13, and `quick = mem < CHECKM_FULL_TREE_GB`
# (40) is then True for every sample regardless of its actual size, silently
# running bin_refinement's CheckM step against the REDUCED reference tree for
# the whole campaign. That is not a slow-but-correct run: the transform's own
# comment says completeness/contamination shift enough near the 50%/10%
# thresholds this campaign's MAG recovery is filtered on to move bins across
# them, in a lane whose entire point is parity with a published method that
# used the full tree.
#
# 64 GB clears the 40 GB floor with real margin (mem = int(64*0.85) = 54),
# matching bbduk's own memory ask below rather than inventing a new number.
# 16 cpus and an 8 h wall are sized off this repo's own measurement
# (README.md: 84,992 contigs / 10M read pairs, three hours wall on sixteen
# cores, refinement two of those) with roughly 2.5x headroom for larger
# samples across the campaign's bigger studies (karlsson2013, sunagawa2015);
# process.tries=4 with the SLURM preset's time-doubling retry (slurm.nf) gives
# a second attempt at 16 h if one sample runs long, rather than a hard 6 h
# wall three times too short even for the measured baseline.
RESOURCE_OVERRIDES = {
    "bbduk":    Resources(memory=Size.GB(64),  cpus=16),
    "megahit":  Resources(memory=Size.GB(128), cpus=32, duration=Duration(hours=12)),
    "metawrap": Resources(memory=Size.GB(64),  cpus=16, duration=Duration(hours=8)),
    # COMEBin is part of the single-end (korem2015) lane's three-binner path
    # (build_targets, layout="single") -- CPU-only here, matching this
    # driver's own no-GPU shape (make_slurm_config carries no GPU block).
    # 48 cores / 96 GB / 3d is BORROWED VERBATIM from
    # research/cami/run_cami_metag.py's own settled CPU default for the same
    # tool (make_slurm_config's `comebin_device="cpu"` body), not measured on
    # korem2015 -- that driver's own comment records the reasoning (Amdahl-
    # limited training, a marine sample ranging 6h28m-11h44m at 96 cores, a 3d
    # wall as a cheap hedge against the slow tail) and a known deadlock risk
    # in the Leiden sweep at high core counts that did not reproduce at 16 or
    # 96 cores there. korem2015's own per-sample contig/read counts have not
    # been checked against that range -- if a run times out or a
    # `cluster_res` file never appears, that is the first thing to revisit,
    # not a driver bug.
    "comebin":  Resources(memory=Size.GB(96), cpus=48, duration=Duration(days=3)),
}


def ssh_cmd(cmd, timeout=180, check=True):
    r = subprocess.run(["ssh", HPC_HOST, cmd], capture_output=True, text=True,
                       timeout=timeout)
    if check and r.returncode != 0:
        print(f"ssh stderr: {r.stderr}", file=sys.stderr)
        raise RuntimeError(f"ssh command failed: {cmd}")
    return r.stdout.strip(), r.returncode


def get_agent():
    return Agent(
        home=SshSource(host=HPC_HOST, path=HPC_MSM_HOME).AsSource(),
        container=AGENT_IMAGE,
        runtime=Runtime.APPTAINER,
        setup_commands=SETUP_COMMANDS,
    )


def enumerate_runs():
    """(dataset, run_accession, layout, paths) for every run in manifest.tsv.

    Grouped from `relpath` (`<BioProject>/<run>/<run>[_N].fastq.gz`) by (dataset,
    run) rather than trusted to a directory glob under METAGEM_ROOT -- the same
    reasoning research/cami's enumerate_samples gives for reading its own tracked
    manifest instead of globbing a cluster path whose subtrees do not all nest the
    same way, and doubly true here because the acquisition job has not populated
    METAGEM_ROOT with anything yet at the time of writing.

    `layout` is "single" or "paired", read off the file COUNT per run group, not
    off the dataset name -- korem2015 is the one single-end study (48 runs, 1 file
    each; the other four studies are 2 files/run, confirmed against every row:
    443 two-file groups + 48 one-file groups = 491, matching manifest.tsv and the
    module README exactly). Any other count raises, rather than silently guessing
    a layout for a shape nobody has seen.
    """
    rows = list(csv.DictReader(MANIFEST_TSV.open(), delimiter="\t"))
    groups = defaultdict(list)
    for r in rows:
        parts = r["relpath"].split("/")
        assert len(parts) == 3, f"unexpected relpath shape: {r['relpath']}"
        run = parts[1]
        groups[(r["dataset"], run)].append(r["relpath"])

    runs = []
    for (dataset, run), relpaths in sorted(groups.items()):
        relpaths = sorted(relpaths)
        # METAGEM_ROOT / DATASET / relpath, not METAGEM_ROOT / relpath. The fetcher writes
        # DEST="$ROOT/$DATASET/$RELPATH" (fetch_array.sbatch), so every read sits one level
        # deeper than the manifest relpath alone implies. Verified on fir for all four fetched
        # studies: nothing exists at the flat path, everything at the dataset-prefixed one.
        # CAUTION this is invisible at plan time. RegisterItem/_stable_id hashes the path
        # STRING and never stats it -- which is deliberate, since the reads are on the cluster
        # and this driver runs on a workstation -- so a wrong-but-consistent path plans clean,
        # reports correct given-leaf counts, and only fails when Nextflow tries to stage a read.
        paths = tuple(METAGEM_ROOT / dataset / p for p in relpaths)
        if len(paths) == 1:
            runs.append((dataset, run, "single", paths))
        elif len(paths) == 2:
            runs.append((dataset, run, "paired", paths))
        else:
            raise AssertionError(
                f"{dataset}/{run}: expected 1 (single-end) or 2 (paired) files, "
                f"got {len(paths)}: {relpaths}")
    return runs


def select(runs, args):
    if getattr(args, "study", None):
        wanted = set(args.study)
        picked = [r for r in runs if r[0] in wanted]
        missing = wanted - {r[0] for r in picked}
        if missing:
            print(f"ERROR: study/studies not found: {sorted(missing)}", file=sys.stderr)
            sys.exit(1)
        runs = picked
    if getattr(args, "sample", None):
        wanted = set(args.sample)
        picked = [r for r in runs if f"{r[0]}_{r[1]}" in wanted]
        missing = wanted - {f"{r[0]}_{r[1]}" for r in picked}
        if missing:
            print(f"ERROR: sample(s) not found: {sorted(missing)}", file=sys.stderr)
            sys.exit(1)
        runs = picked
    if getattr(args, "limit", None):
        runs = runs[: args.limit]
    return runs


def split_by_layout(runs):
    """Partition selected runs into (paired, single) -- the grouping this driver
    now solves as two SEPARATE tasks, one GenerateWorkflow call each.

    By SHAPE, not by study name, even though today exactly one study
    (korem2015) is single-end: the failure this fixes is a solver-group
    collision between two READ SHAPES sharing one `resources=` case (see
    build_globals's docstring), not between two studies. Grouping on the axis
    the bug actually lives on is what keeps this correct if the manifest ever
    grows a second single-end study -- grouping by study name would then need
    a second special case, grouping by layout does not.
    """
    paired = [r for r in runs if r[2] == "paired"]
    single = [r for r in runs if r[2] == "single"]
    return paired, single


def _stable_id(corpus: str, *parts: str) -> str:
    """A leaf id that survives a re-plan, which AddItem's does not.

    Copied verbatim in spirit from research/cami/run_cami_metag.py's own
    `_stable_id` -- see that function's docstring for the full mechanism
    (AddItem mints a fresh uuid4 for any path it cannot stat, which is every
    input here since the reads live on the cluster and this driver runs on a
    workstation; RegisterItem's instance_id is what pins the plan key across
    re-plans so Nextflow's -resume actually fires). `corpus` is "metagem" here,
    not a hardcoded literal, for the same collision-avoidance reason the CAMI
    driver gives.
    """
    from metasmith.caching.keys import multihash_key
    return multihash_key("\x00".join((corpus,) + parts).encode("utf-8")).hex()


def build_inputs(runs):
    """Register every run's reads through the library's paired- or single-end
    chain. Study-wide globals (medium table/name, CPLEX) are a SEPARATE small
    library -- see build_globals and its docstring for why combining the two
    into one `resources=[...]` argument is actively wrong here, not just untidy.

    `runs` must be a SINGLE layout (all paired or all single) -- the caller is
    `cmd_run`, which splits by `split_by_layout` and calls this, `build_targets`
    and `GenerateWorkflow` once per shape. See the module docstring and
    build_globals's docstring for the reproduced defect that follows from
    solving two shapes in one call: CollectSolverInputs classifies a solver
    group's "unique case" by the endpoint set across the WHOLE group, so mixing
    shapes into one `inputs` library (even split across `resources=` the way
    build_globals no longer needs to worry about, since each call is now
    homogeneous) lets the solver silently pick whichever shape's producer chain
    is cheaper for its one merged case and drop the other shape's chain out of
    the plan with `dropped_targets` staying empty. The assert below is the
    guard against that regressing silently.

    NO shared study root. build_inputs_pratama in research/cami/run_cami_metag.py
    parents every run's read_metadata under one viromics::contig_study node
    because its cross-sample viral clustering needs that lineage edge to pool
    calls across runs (`DataInstanceLibrary.AsSamples` masks each sample to
    `{path} | ancestors | siblings`, and a shared parent makes every run every
    other run's sibling, collapsing AsSamples to ONE view spanning all of them --
    see that function's docstring for the measurement this rests on). metaGEM has
    no such pooling step: it reconstructs one genome-scale model per MAG per run,
    full stop. Registering read_metadata with NO parents at all (matching
    research/cami/run_cami_metag.py's plain CAMI arm, not its Pratama arm) is
    what keeps AsSamples at one solver sample-view PER RUN -- 491, not 1. THE
    NEXT READER: do not "fix" this back to a shared root. It looks like the
    Pratama shape and is not; there is nothing here for a shared root to buy.

    Namespaced sample id `f"{dataset}_{run}"`: run accessions are unique within
    every study in the current manifest (checked directly -- no accession
    recurs across datasets), but namespacing anyway is cheap and the manifest is
    a live document a standing acquisition agent is still writing, so an
    accession collision across studies could appear later without warning.
    """
    layouts = {layout for _, _, layout, _ in runs}
    assert len(layouts) <= 1, (
        f"build_inputs got a mixed-layout run list ({sorted(layouts)}); solve "
        "each read shape as its own task -- see this function's docstring"
    )

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(CACHE_DIR / f"metagem_inputs_{next(iter(layouts), 'empty')}.xgdb")
    inputs.Purge()

    for tl in ["sequences.yml", "alignment.yml", "binning.yml", "binning_local.yml",
               "modelling.yml", "taxonomy.yml", "env.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    for dataset, run, layout, paths in runs:
        sid = f"{dataset}_{run}"
        parity = "single" if layout == "single" else "paired"
        meta_value = json.dumps({"parity": parity, "length_class": "short"})
        meta_name = f"{sid}_read_metadata.json"
        (inputs.location / meta_name).write_text(meta_value)
        meta = inputs.RegisterItem(
            meta_name, "sequences::read_metadata",
            instance_id=_stable_id("metagem", "read_metadata", sid, meta_value),
        )

        if layout == "single":
            (reads,) = paths
            # short_reads_se, not the bare short_reads bbduk itself requires --
            # bbduk's own requirement is the supertype (see that transform), so
            # the concrete parity-carrying subtype is what documents intent at
            # the registration site. The metadata JSON's own "parity": "single"
            # above is the one bbduk actually reads at runtime (see this
            # driver's module docstring point 2, and bbduk.py's own assert on
            # {single, paired}) -- the type and the JSON value are two
            # independent things that both have to say the same thing, the
            # exact trap research/cami/run_cami_metag.py's short_reads_pe vs.
            # "paired" carries too.
            inputs.RegisterItem(
                reads, "sequences::short_reads_se", parents={meta},
                instance_id=_stable_id("metagem", "short_reads_se", sid, str(reads)),
            )
        else:
            fwd, rev = paths
            pair_name = f"{sid}_read_pair.txt"
            (inputs.location / pair_name).write_text(run)
            pair = inputs.RegisterItem(
                pair_name, "sequences::read_pair", parents={meta},
                instance_id=_stable_id("metagem", "read_pair", sid, run),
            )
            inputs.RegisterItem(
                fwd, "sequences::zipped_forward_short_reads", parents={pair},
                instance_id=_stable_id("metagem", "zipped_forward_short_reads", sid, str(fwd)),
            )
            inputs.RegisterItem(
                rev, "sequences::zipped_reverse_short_reads", parents={pair},
                instance_id=_stable_id("metagem", "zipped_reverse_short_reads", sid, str(rev)),
            )

    inputs.Save()
    return inputs


def _solver(args):
    """Resolve the reconstruction lane, honouring the deprecated --with-cplex alias.

    --with-cplex predates --solver and meant "add the CPLEX lane BESIDE the open
    one", which is exactly --solver both.
    """
    if getattr(args, "with_cplex", False):
        return "both"
    return getattr(args, "solver", "open")


def build_globals(solver="open"):
    """The study-wide medium table/name (and, opt-in, the CPLEX installation)
    as their OWN small library -- registered exactly like
    research/cami/run_cami_metag.py's DB_PATHS/STAGED_REFS (a real path or a
    small written file, no parent, instance_id-pinned), but deliberately kept
    OUT of `build_inputs`'s per-run library, and passed to GenerateWorkflow as
    its own `resources=` entry rather than folded into `inputs`.

    This split is NOT tidiness -- it fixes a real, reproduced defect. Spec.SolveViews
    builds each solver "given" GROUP as `[one_sample_view] + resources`, and
    CollectSolverInputs classifies a group's "unique case" by the SET of type
    endpoints across THAT WHOLE GROUP, resources included (see
    src/metasmith/agents/spec.py `SolveViews` and
    src/metasmith/models/workflow/plan.py `CollectSolverInputs`). Passing the
    FULL per-run `inputs` library (every run's reads, every layout) as a
    resource means every sample's group carries every OTHER sample's registered
    types too -- so a korem2015 (single-end) sample's group and a bissett_base
    (paired) sample's group end up with the IDENTICAL aggregate endpoint set,
    both structurally "containing" short_reads_se AND the read_pair/zipped/
    interleave chain regardless of which one that particular sample actually
    needs. The solver then treats the whole heterogeneous corpus as ONE
    unique case (confirmed: the log always says "as [1] unique case", even
    for a deliberately mixed run set) and picks whichever producer chain is
    CHEAPEST to reach `sequences::short_reads` for that one case -- the direct
    short_reads_se registration, zero extra steps -- and `interleave_zipped_short_reads`
    silently DROPS OUT OF THE PLAN ENTIRELY, with dropped_targets staying empty
    the whole time (no target failure, since `sequences::short_reads` is still
    satisfied for the chosen case). A paired-only corpus never exposes this
    (there is only one valid producer chain, so which "case" wins never
    matters); mixing korem2015's single-end runs in is what surfaces it.
    Reproduced directly: swapping `resources=[..., inputs]` for
    `resources=[..., globals]` on an otherwise-identical mixed 3-paired +
    3-single-end sample set changes the compiled step list from
    [seqkit_reads, bbduk, megahit] to [interleave_zipped_short_reads,
    seqkit_reads, bbduk, megahit] -- exactly the missing step, and nothing else
    moves.

    media/medium_name (and cplex) have to be SOME resource, though -- they have
    no parent and are not reachable from any read_metadata sample view, so
    `AsSamples` never yields them. A minimal library holding only these
    parentless globals avoids importing any per-run type into every group's
    endpoint set, which is what keeps single-end and paired samples counted as
    genuinely different structural cases.

    CORRECTION (repair round 2): this split is necessary but was NOT
    sufficient, and shipping it alone was itself the round-1 defect. An
    adversarial review reproduced the mirror image: `resources=[..., globals]`
    does stop the interleave step from dropping, but the SAME "whole corpus
    treated as one solver case" mechanism above still applies to `samples=`
    itself when every run's `sequences::read_metadata` sample-view is solved
    in one `GenerateWorkflow` call regardless of shape -- and with globals used
    instead of the full inputs library, the case that loses flips from
    korem2015 (single-end, 48 runs) to whichever OTHER shape shares the run
    with it (measured: a 3-paired+3-single-end run reproduces this as
    `korem2015: 0 fastqs staged`). `dropped_targets` stays empty either way,
    because the requested TARGET (`sequences::short_reads`, or downstream of
    it) is still satisfied by the winning case. Splitting `globals_lib` out of
    `inputs` fixes ONE specific misattribution (which types leak into a
    group's endpoint set via `resources=`); it does nothing about mixing two
    read shapes into one `samples=` list in the first place. The actual fix is
    in `cmd_run`/`_run_lane`: split by `split_by_layout` and call
    `GenerateWorkflow` once per shape, so no group's endpoint set is ever
    built from more than one shape's registered types, on either axis. This
    function's split is still correct and still needed for the reason above --
    it is just not, by itself, the reason both shapes now solve.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    globals_lib = DataInstanceLibrary(CACHE_DIR / "metagem_globals.xgdb")
    globals_lib.Purge()
    globals_lib.AddTypeLibrary(MLIB / "data_types" / "modelling.yml")

    # The medium table lives IN THE REPO, not on the cluster, so it must NOT be
    # registered as an external absolute path. StageWorkflow binds an external input's
    # parent directory into the remote container, and that directory does not exist on
    # fir -- staging fails with "external input folder(s) must be bound into the remote
    # container but do not exist on the remote host". Copying the content into the
    # library and registering the relative name is what medium_name.txt below already
    # does, and it is correct for any small repo-resident constant.
    # CAUTION do NOT "fix" this by passing verify_external_paths=False. That turns a
    # clear staging error into an obscure runtime one: the bind is created pointing at a
    # path that does not exist on the far side, and the tool fails inside the container
    # with a missing input instead.
    # The instance_id stays keyed on the ORIGINAL path string, so a content-identical
    # fix does not move the plan key.
    media_file = "media.tsv"
    (globals_lib.location / media_file).write_text(MEDIUM_TSV.read_text())
    globals_lib.RegisterItem(
        media_file, "modelling::media",
        instance_id=_stable_id("metagem", "media", str(MEDIUM_TSV)),
    )
    medium_name_file = "medium_name.txt"
    (globals_lib.location / medium_name_file).write_text(MEDIUM_NAME)
    globals_lib.RegisterItem(
        medium_name_file, "modelling::medium_name",
        instance_id=_stable_id("metagem", "medium_name", MEDIUM_NAME),
    )
    if solver in ("cplex", "both"):
        globals_lib.RegisterItem(
            CPLEX_ROOT, "modelling::cplex_installation",
            instance_id=_stable_id("metagem", "cplex_installation", str(CPLEX_ROOT)),
        )

    globals_lib.Save()
    return globals_lib


def _assembly_without(*masked: str):
    """The assembly library with the other assembler hidden from the planner.

    Same mechanism and the same reason as research/cami/run_cami_metag.py's own
    `_assembly_without`: metabat2/semibin2/comebin/metawrap/prodigal all require
    the BARE `sequences::assembly` supertype, which a target pin never reaches,
    so hiding the file is what actually settles that interior slot onto MEGAHIT.
    """
    lib = TransformInstanceLibrary.Load(MLIB / "transforms" / "assembly")
    return lib.AsView({Path(f"{m}.py") for m in masked}, invert=True)


def build_transforms_for():
    """Both lanes reach reconstruction now (see build_targets), so both need
    every library -- metabolicModelling included. round-2 briefly dropped it
    for the single-end lane when that lane stopped at assembly; it no longer
    does (see build_targets's "REVERSED" note), so there is nothing left to
    conditionally exclude. An unreached transform is simply never picked, so
    carrying the full set for both lanes costs nothing but is no longer even
    the point -- both lanes reach every one of these libraries for real.
    """
    return [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
        _assembly_without("spades"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metabolicModelling"),
    ]


def build_targets(solver="open", layout="paired"):
    """MEGAHIT, then this lane's binning path, then CarveMe-from-bin-ORFs +
    MEMOTE per MAG. Both `layout` values now reach reconstruction and scoring
    -- see the "REVERSED" paragraph below for why round 2's assembly-only
    stop for `layout="single"` was wrong, not merely incomplete.

    MEGAHIT, not metaSPAdes, for BOTH layouts: metaGEM's own Snakemake
    workflow assembles with MEGAHIT by default, and this is the
    metaGEM-PARITY lane -- see `_assembly_without`, which is what actually
    settles the ambiguous interior `sequences::assembly` slot every
    binner/prodigal call carries.

    `layout="paired"` (four of five studies): MetaWRAP as the sole binner,
    not the three-binner ensemble research/cami/run_cami_metag.py's "variant"
    target set runs -- this driver is not comparing binners on the paired
    side, and MetaWRAP's own bin_refinement already IS a three-tool
    (CONCOCT/MetaBAT2/MaxBin2) consolidation, close enough to metaGEM's own
    DASTool-consolidated binning to answer the same question without
    doubling the plan.

    `layout="single"` (korem2015): MetaBAT2 + SemiBin2 + COMEBin, called
    DIRECTLY off one alignment (no MetaWRAP -- `metagenomics/binning/
    metawrap.py` hard-asserts `parity == "paired"` and there is no relaxing
    that; the underlying `metawrap binning` module genuinely takes two read
    files), consolidated by DAS Tool
    (`metagenomics/binning/das_tool.py`).

    REVERSED from round 2, on evidence round 2 did not have: metaGEM
    published a full model set for korem2015 (MAGs.tar.gz, MAGs_protein.tar.gz,
    GEMs.tar.gz, a MEMOTE table -- the same shape it ships for every paired
    study), so excluding it from the model comparison was a parity gap, not a
    match, and round 2's target set stopped short of it. metaGEM's own
    repository explains how: its main Snakefile is paired-only throughout,
    but it carries a SEPARATE workflow for single-end samples
    (`workflow/rules/Snakefile_single_end.smk.py`, `maxbin_single.smk`,
    `metabat_single.smk`) that calls MetaBAT2, MaxBin2 and CONCOCT DIRECTLY
    off a per-sample alignment, then refines, then reconstructs and scores --
    structurally different from its paired path (which maps cross-sample into
    the same three binners) by metaGEM's OWN design, not by an omission in
    either codebase. Running a three-binner-plus-consolidator path on
    korem2015 while the paired lane uses MetaWRAP is therefore not the
    inconsistency round 2 took it for -- it mirrors the split metaGEM itself
    made, which is what a faithful port is supposed to do. This library's own
    three-binner set is MetaBAT2/SemiBin2/COMEBin + DAS Tool rather than
    metaGEM's MetaBAT2/MaxBin2/CONCOCT + a Snakemake refine rule (this
    library has no MaxBin2/CONCOCT transform and no bespoke refine rule to
    port), so the TOOLS differ from metaGEM's single-end path while the
    STRUCTURE -- direct multi-binner calls off one alignment, then a real
    consolidation step, then reconstruction -- matches it. That tool
    substitution is the same kind of deviation the paired lane already makes
    (MetaWRAP's refinement standing in for metaGEM's DASTool-consolidated
    binning), stated the same way: as a result-table row, not silently.

    Refinement IS reachable on this lane, checked rather than assumed:
    `das_tool.py` requires `sequences::assembly`, `sequences::orfs` and the
    three binners' own `binning::*_contig_to_bin_table` outputs -- no raw
    reads and no parity gate anywhere in its requirement set, so nothing
    about the single-end shape blocks it. None of metabat2.py/semibin2.py/
    comebin.py gate on parity either; all three key off `alignment::bam`,
    itself parity-agnostic (`assembly_stats.py` maps with plain minimap2
    regardless of shape -- confirmed directly, since a BAM was already in the
    single-end lane's plan before this target set grew to need one for
    binning).

    The CarveMe step requires `sequences::bin_orfs`, NOT the ambiguous
    `sequences::orfs` -- see `prodigal_from_bin.py` and `carveme_from_orfs.py`'s
    own comments. Pinning `sequences::bin_orfs` as its own target (rather than
    just as an interior requirement of the carveme_model target below) is
    necessary for the same "lineage matching is ancestral, ambiguous interior
    slots need a masked library or an explicit target" reason as everywhere
    else in this project. `prodigal_from_bin.py` requires the bare
    `sequences::bin_fasta` supertype, which every binner's own bin fasta type
    (metawrap_/metabat2_/semibin2_/comebin_/das_tool_bin_fasta) descends from
    (data_types/sequences.yml) -- so pinning bin_orfs's parent to whichever
    concrete bin fasta is THIS lane's consolidated output (metawrap's or DAS
    Tool's) is what disambiguates the producer, exactly as `taxonomy::
    checkm_stats`'s parent does (checkm.py requires the bare
    `sequences::putative_genome` ancestor of `bin_fasta`, satisfied by
    either). Each lane names exactly one bin fasta type as a target, so there
    is no cross-lane ambiguity to mask.

    `modelling::memote_score` is the only MEMOTE product target needed: MEMOTE's
    own transform (memote_score.py) emits `memote_report`, `memote_results` AND
    `memote_score` from ONE test run, so naming the others too would buy nothing
    and cost nothing but a slot -- the same reasoning
    research/cami/run_cami_metag.py's build_targets gives for not naming
    amber_bin_metrics beside amber_results.

    No AMBER, no cami_read_truth: these are real SRA/ENA runs with no simulated
    ground truth to score bins against, the same reasoning
    build_targets_pratama gives.

    `with_cplex` adds the CPLEX parity variant (carveme_from_orfs_cplex.py) and
    its own MEMOTE score AS A SECOND, SEPARATE pair of targets parented to the
    SAME bin_orfs, not to the standard carveme_model -- pinning memote_score to
    a specific parent model each time is what disambiguates which of the two
    sibling metabolic_model producers (carveme_model vs. carveme_model_cplex,
    genuinely different property VALUES, not a superset relationship -- see
    modelling.yml) a given memote_score instance scores, the identical
    disambiguation research/cami/run_cami_metag.py's per-binner amber targets
    rely on. Off by default: see CPLEX_ROOT's own comment for why the real path
    is unverified from this workstation. Applies identically to both layouts --
    the CPLEX reconstruction sits downstream of bin_orfs, which by this point
    is already layout-agnostic.
    """
    t = TargetBuilder()
    asm = t.Add("sequences::megahit_assembly")
    t.Add("sequences::read_qc_stats")
    for dtype in ("sequences::orfs", "sequences::gff", "sequences::assembly_stats",
                  "sequences::assembly_per_contig_coverage", "alignment::bam"):
        t.Add(dtype, parents=[asm])

    if layout == "paired":
        bin_fasta = t.Add("sequences::metawrap_bin_fasta", parents=[asm])
        t.Add("binning::metawrap_contig_to_bin_table", parents=[asm])
    else:
        assert layout == "single", f"unknown layout: {layout}"
        for b in ("metabat2", "semibin2", "comebin"):
            t.Add(f"sequences::{b}_bin_fasta", parents=[asm])
            t.Add(f"binning::{b}_contig_to_bin_table", parents=[asm])
        bin_fasta = t.Add("sequences::das_tool_bin_fasta", parents=[asm])
        t.Add("binning::das_tool_contig_to_bin_table", parents=[asm])

    t.Add("taxonomy::checkm_stats", parents=[bin_fasta])
    bin_orfs = t.Add("sequences::bin_orfs", parents=[bin_fasta])

    # CAUTION `solver` SELECTS the reconstruction lane; it does not merely add one.
    # Measured 2026-09-12 on li2019/SRR7664615_bin.3.s -- 684 proteins, a 570-reaction
    # draft, and the SMALLEST bin in that sample's set, so a lower bound on cost:
    #
    #   open (SCIP)  draft 10m01s   gap fill 1h49m11s and STILL RUNNING when killed
    #   cplex        draft  2m25s   gap fill    32m00s, 1.89 MB model written
    #
    # So the open solver does not finish gap filling on the cheapest input in the
    # corpus, against a transform duration of 2 h. Leaving it in the launch plan
    # means 443 (or 48) tasks that hit their wall, get retried, and are then
    # swallowed by the run's ignore strategy -- the arm reports complete and
    # produces no models at all. `solver="cplex"` is therefore a FEASIBILITY
    # requirement here, not a parity preference; it also happens to be closer to
    # metaGEM, which used CPLEX 12.8 (unobtainable -- IBM withdrew every release
    # before 20.1 in March 2021, so 22.2.0 is the deviation to state).
    #
    # "both" keeps the original two-lane solver comparison, which is now ANSWERED
    # by the measurement above and costs a guaranteed timeout per bin to re-run.
    if solver in ("open", "both"):
        model = t.Add("modelling::carveme_model", parents=[bin_orfs])
        t.Add("modelling::memote_score", parents=[model])

    if solver in ("cplex", "both"):
        cplex_model = t.Add("modelling::carveme_model_cplex", parents=[bin_orfs])
        t.Add("modelling::memote_score", parents=[cplex_model])

    return t


def make_slurm_config():
    """The base SLURM preset plus the cached-twin array exemption.

    Copied verbatim in spirit from research/cami/run_cami_metag.py's own
    `make_slurm_config` -- see that function's comment for the full mechanism.
    It is NOT cosmetic: every cacheable step compiles to a `<name>_cached`
    sibling process that declares `executor 'local'` inline, and the preset's
    own `process.array` exemption is keyed by label and never reaches it, so
    Nextflow refuses at compile time ("Executor 'local' does not support job
    arrays") before a single task is submitted -- while metasmith still prints
    `run completed` with zero outputs. This driver's own COMEBin usage
    (single-end lane) stays CPU-only and goes through the ordinary
    `resource_overrides=` mechanism (RESOURCE_OVERRIDES, plain cpus/memory/
    duration) rather than research/cami's raw `clusterOptions` GPU block --
    no GPU-bound step needs the MIG-slice handling that block exists for, so
    the cached-twin exemption is the whole appended body here.
    """
    smith = get_agent()
    base = Path(smith.GetNxfConfigPresets()["slurm"]).read_text()
    # `array = 0` is the whole appended body, and it is NOT optional: without it
    # Nextflow aborts on a local-executor process asking for a job array.
    #
    # A `scratch = false` exemption used to sit here too, to stop the twin's product
    # round-tripping through node-local SLURM_TMPDIR and back through
    # `nxf_fs_copy`'s dereferencing `cp -fRL`. It is REMOVED: the task contract is
    # fixed, and bending it for every cached twin to accommodate one product's
    # defect is the wrong layer. The one product that failed was vConTACT3's
    # database, which ships upstream mmseqs build scratch containing 404 dangling
    # symlinks; `logistics/downloadVcontact3DB.py` now prunes that scratch and
    # asserts no dangling symlink survives, so the product is valid inside the
    # standard contract. Audited exposure before removing this: 0 of 32 shards in
    # this agent home and 0 of 4,823 in the CAMI home hold a dangling symlink.
    text = base + "\n" + "\n".join(
        ["", "process {", "    withName: '.*_cached' {", "        array = 0",
         "    }", "}", ""])
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out = CACHE_DIR / "fir_slurm_metagem.config"
    out.write_text(text)
    return out


def _report_plan_failure(task):
    print("ERROR: workflow generation failed", file=sys.stderr)
    for h in getattr(task.plan, "hints", []) or []:
        print(f"  [{h.kind}] target={getattr(h, 'target', '?')}: {getattr(h, 'message', '')}")
        for c in getattr(h, "chain", []) or []:
            print(f"      chain: {c}")
        for c in getattr(h, "near_misses", []) or []:
            print(f"      near-miss: {c}")
    sys.exit(1)


def _expected_leaf_counts(runs):
    """Per-run leaf dtypes `build_inputs` registers for these runs, at their
    expected count. Mirrors build_inputs's own layout branch exactly (same
    `if layout == "single"` split) so the two cannot drift apart silently --
    this is deliberately NOT read back off `inputs` itself, because the point
    is to check the SOLVED PLAN against an independent expectation, not
    against the thing that produced it.
    """
    counts = Counter()
    for _, _, layout, _ in runs:
        counts["sequences::read_metadata"] += 1
        if layout == "single":
            counts["sequences::short_reads_se"] += 1
        else:
            counts["sequences::read_pair"] += 1
            counts["sequences::zipped_forward_short_reads"] += 1
            counts["sequences::zipped_reverse_short_reads"] += 1
    return counts


def _check_given_leaf_counts(task, expected, label):
    """Refuse to proceed unless every expected leaf type reached the SOLVED
    plan at its expected count.

    This is the direct check for the defect class the wave-2 corpus-mixing bug
    belongs to, not a duplicate of `dropped_targets`: `WorkflowPlan.Generate`
    (src/metasmith/models/workflow/plan.py) filters `_given` down to
    `used_endpoints` -- ancestors of whatever the solver's chosen case actually
    consumed -- so a given item with no consuming step in the winning case is
    silently absent from `task.plan.given` while `task.ok` stays True and
    `dropped_targets` stays empty, because the TARGET was still satisfied by
    some other given item. A plan step count cannot see this either: the step
    list looks identical whether 491 or 443 samples fed it. Counting the
    leaves that actually reached the plan is the only place this class of
    silent drop is visible at all.
    """
    counts = Counter(inst.dtype_name for inst in task.plan.given)
    problems = [
        f"  {dtype}: expected {want}, got {counts.get(dtype, 0)} "
        f"({counts.get(dtype, 0) - want:+d})"
        for dtype, want in expected.items() if counts.get(dtype, 0) != want
    ]
    if problems:
        print(f"ERROR: given-leaf count mismatch for {label} -- the plan "
              f"reports ok but silently dropped input(s):", file=sys.stderr)
        for p in problems:
            print(p, file=sys.stderr)
        sys.exit(1)
    print(f"given-leaf counts OK for {label}: " +
          ", ".join(f"{k}={counts[k]}" for k in expected))


def cmd_list_samples(args):
    runs = select(enumerate_runs(), args)
    by_study = defaultdict(list)
    for dataset, run, layout, paths in runs:
        by_study[dataset].append((run, layout))
    for dataset, rows in sorted(by_study.items()):
        layouts = {layout for _, layout in rows}
        print(f"{dataset:14s} {len(rows):4d} run(s)  layout={sorted(layouts)}")
    print(f"\n{len(runs)} run(s) total")
    for dataset, run, layout, paths in runs:
        print(f"  {dataset}_{run:14s} {layout:7s} {paths[0]}")
    return 0


def cmd_check_dbs(args):
    checks = {"metagem root": METAGEM_ROOT, "agent home": HPC_MSM_HOME,
              "medium table (local)": MEDIUM_TSV, "cplex installation": CPLEX_ROOT}
    local_checks = {k: v for k, v in checks.items() if "local" in k}
    remote_checks = {k: v for k, v in checks.items() if "local" not in k}
    for name, path in local_checks.items():
        print(f"{'OK  ' if path.exists() else 'MISS'} {name} -> {path}")
    probe = "; ".join(
        f'test -e "{p}" && echo "OK   {t} -> {p}" || echo "MISS {t} -> {p}"'
        for t, p in remote_checks.items())
    out, _ = ssh_cmd(probe)
    print(out)
    missing = [ln for ln in out.splitlines() if ln.startswith("MISS")]
    if not MEDIUM_TSV.exists():
        missing.append("medium table")
    if missing:
        print(f"\n{len(missing)} path(s) missing.", file=sys.stderr)
        return 1
    return 0


def cmd_setup(args):
    smith = get_agent()
    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    logistics = TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics")
    wl = {Path(f"{n}.env") for n in CONTAINERS}
    samples = [s for s in containers.AsSamples("env::env") if s._mask.intersection(wl)]
    missing = wl - {p for s in samples for p in s._mask}
    if missing:
        print(f"ERROR: envs not in {MLIB}/resources/env: {sorted(missing)}", file=sys.stderr)
        sys.exit(1)
    print(f"containers to pull: {len(samples)}")

    targets = TargetBuilder()
    targets.Add("env::pulled_container")
    task = smith.GenerateWorkflow(samples=samples, resources=[],
                                  transforms=[logistics], targets=targets)
    if not task.ok or not task.plan.steps:
        _report_plan_failure(task)
    print(f"pull plan OK -- {len(task.plan.steps)} steps, key={task.GetKey()}")
    if not args.run:
        print("(render-only; pass --run to deploy + pull)")
        return 0
    smith.Deploy(assertive=True)
    smith.StageWorkflow(task, on_exist="update")
    smith.RunWorkflow(task, config_file=smith.GetNxfConfigPresets()["local"],
                      params=dict(executor=dict(queueSize=4)),
                      resource_overrides={"all": Resources(memory=Size.GB(2), cpus=2)})
    return 0


def _run_lane(args, layout, lane_runs, containers, resource_lib, globals_lib):
    """Plan (and, unless --dry-run/--stage-only, stage + submit) ONE read
    shape's task. Called once for "paired" and once for "single" by cmd_run --
    see the module docstring and split_by_layout for why the corpus cannot be
    one task, and build_targets's "REVERSED" note for why BOTH layouts now
    carry the same reconstruction/scoring targets, just via different binning
    paths.
    """
    inputs = build_inputs(lane_runs)
    targets = build_targets(solver=_solver(args), layout=layout)
    expected = _expected_leaf_counts(lane_runs)
    # Consumed by carveme_from_orfs[_cplex].py on BOTH layouts now -- neither
    # lane stops short of reconstruction any more, so this is unconditional.
    expected["modelling::media"] = 1
    expected["modelling::medium_name"] = 1
    if _solver(args) in ("cplex", "both"):
        expected["modelling::cplex_installation"] = 1

    smith = (Agent(home=Source.FromLocal(CACHE_DIR / f"dryrun_home_{layout}"), runtime=Runtime.APPTAINER)
             if args.dry_run else get_agent())

    binner_desc = ("MetaWRAP+CheckM2" if layout == "paired" else
                   "MetaBAT2+SemiBin2+COMEBin+DAS Tool+CheckM2")
    print(f"\n=== {layout} lane: {len(lane_runs)} run(s) "
          f"(MEGAHIT+{binner_desc}+CarveMe+MEMOTE) ===")
    sample_views = list(inputs.AsSamples("sequences::read_metadata"))
    # Expected to equal len(lane_runs), NOT 1 -- no shared study root parents
    # any run's read_metadata to any other. See build_inputs's docstring.
    print(f"AsSamples: {len(lane_runs)} run(s) -> {len(sample_views)} solver sample-view(s)")
    task = smith.GenerateWorkflow(
        samples=sample_views,
        resources=[containers, resource_lib, globals_lib],
        transforms=build_transforms_for(),
        targets=targets,
    )
    if not task.ok:
        _report_plan_failure(task)

    _check_given_leaf_counts(task, expected, f"{layout} lane")

    steps = task.plan.steps
    print(f"Plan OK -- {len(steps)} steps across {len(lane_runs)} runs, key={task.GetKey()}")
    for s in steps:
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {Path(s.transform._path).stem:28s} -> {prods}")
    if task.plan.dropped_targets:
        print(f"\ndropped: {sorted(task.plan.dropped_targets)}")
        for h in (task.plan.hints or []):
            print(f"  hint: {getattr(h, 'kind', '?')} target={getattr(h, 'target', '?')}"
                  f" msg={getattr(h, 'message', '')}")

    if args.dry_run:
        print(f"\n({layout} lane dry-run; nothing staged or submitted)")
        return 0

    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    tag = f"{args.tag}_{layout}" if args.tag else f"metagem_{layout}_{len(lane_runs)}runs"
    keys[tag] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))

    print(f"Staging {layout} lane to {HPC_HOST}...")
    smith.StageWorkflow(task, on_exist=args.on_exist, verify_external_paths=False)
    if args.stage_only:
        print(f"\n({layout} lane stage-only; staged as {task.GetKey()})")
        return 0

    config = make_slurm_config()
    print(f"Submitting {layout} lane to SLURM (config: {config})...")
    smith.RunWorkflow(
        task=task, config_file=config,
        params=dict(slurmAccount=SLURM_ACCOUNT,
                    executor=dict(queueSize=500),
                    process=dict(tries=4, array=25)),
        resource_overrides=RESOURCE_OVERRIDES,
    )
    print(f"Submitted {layout} lane: {task.GetKey()}")
    return 0


def cmd_run(args):
    runs = select(enumerate_runs(), args)
    if not runs:
        print("ERROR: no runs found; run list-samples", file=sys.stderr)
        return 1
    print(f"{len(runs)} run(s) selected: "
          f"{', '.join(f'{d}_{r}' for d, r, *_ in runs)}"
          if len(runs) <= 20 else
          f"{len(runs)} run(s) selected across "
          f"{len(set(d for d, *_ in runs))} stud(ies)")

    paired_runs, single_runs = split_by_layout(runs)
    print(f"split by read shape: {len(paired_runs)} paired, {len(single_runs)} single-end")
    if single_runs:
        # See build_targets's "REVERSED" note: korem2015 (single-end) STAYS IN
        # the model comparison -- metaGEM published a full model set for it --
        # but reaches CarveMe/MEMOTE through a different binning path
        # (MetaBAT2+SemiBin2+COMEBin+DAS Tool, not MetaWRAP, which hard-
        # asserts paired reads) than the four paired studies. This mirrors a
        # split metaGEM's own repository makes for the same reason, so it is
        # declared here as a stated deviation, not a silent one.
        print(f"NOTE: {len(single_runs)} single-end run(s) (korem2015) will be planned "
              f"through a DIFFERENT binning path than the paired lanes -- MetaBAT2 + "
              f"SemiBin2 + COMEBin + DAS Tool consolidation, not MetaWRAP (which "
              f"requires paired reads) -- reaching the same CarveMe/MEMOTE reconstruction "
              f"and staying IN the model comparison. metaGEM itself splits its binning "
              f"method by read shape the same way; see build_targets's docstring.")

    globals_lib = build_globals(solver=_solver(args))
    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    # resources/lib, not just resources/env: prodigal_from_bin.py,
    # carveme_from_orfs[_cplex].py and memote_score.py all require `lib::modelling`
    # helper scripts. Missing this is THE first thing to check on an all-drop
    # solve -- research/cami/run_cami_metag.py's own comment on this exact
    # mistake cost a wave-1 agent an hour: the chain becomes structurally
    # unsatisfiable and the planner blames an unrelated target instead of naming
    # the missing resource library.
    resource_lib = DataInstanceLibrary.Load(MLIB / "resources" / "lib")

    rc = 0
    for layout, lane_runs in (("paired", paired_runs), ("single", single_runs)):
        if not lane_runs:
            continue
        rc = _run_lane(args, layout, lane_runs, containers, resource_lib, globals_lib) or rc
    return rc


def cmd_status(args):
    keys_file = CACHE_DIR / "task_keys.json"
    if not keys_file.exists():
        print("no workflows submitted")
        return 0
    smith = get_agent()
    for name, key in json.loads(keys_file.read_text()).items():
        print(f"\n{name} ({key}):")
        try:
            smith.CheckWorkflow(key)
        except Exception as e:
            print(f"  {type(e).__name__}: {e}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("list-samples")
    p.add_argument("--study", nargs="*",
                   help="korem2015, karlsson2013, li2019, sunagawa2015, bissett_base")
    p.add_argument("--sample", nargs="*")
    p.add_argument("--limit", type=int)
    p.set_defaults(fn=cmd_list_samples)

    sub.add_parser("check-dbs").set_defaults(fn=cmd_check_dbs)

    p = sub.add_parser("setup", help="pull the container images onto the cluster")
    p.add_argument("--run", action="store_true")
    p.set_defaults(fn=cmd_setup)

    p = sub.add_parser("run")
    p.add_argument("--study", nargs="*")
    p.add_argument("--sample", nargs="*")
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stage-only", action="store_true")
    p.add_argument("--on-exist", default="update", choices=["update", "clear"])
    p.add_argument("--solver", default="open", choices=["open", "cplex", "both"],
                   help="which reconstruction lane to plan. 'cplex' is what the "
                        "campaign launches: the open solver does not finish gap "
                        "filling even on the corpus's smallest bin -- see "
                        "build_targets's CAUTION for the measurement. 'both' keeps "
                        "the original two-solver comparison and pays a guaranteed "
                        "timeout per bin for it. Anything but 'open' needs "
                        "modelling::cplex_installation to be a real directory on "
                        "fir; run check-dbs first.")
    p.add_argument("--with-cplex", action="store_true",
                   help="deprecated alias for --solver both.")
    p.add_argument("--tag")
    p.set_defaults(fn=cmd_run)

    sub.add_parser("status").set_defaults(fn=cmd_status)

    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
