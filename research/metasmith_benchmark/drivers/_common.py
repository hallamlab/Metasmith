"""What the benchmark drivers share: agent homes, pool givens, fir's Slurm config and plan checks.

A driver runs in one of two places. On the workstation it solves against a local dry-run
home, whose pool it fills itself. Inside a Slurm job on fir (BENCH_ON_HOST=1) the agent
home is a local path, so imports, staging and submission never cross ssh.
"""

import csv
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

from metasmith.python_api import Agent, Gpu, Size, Source, SshSource, Runtime, TransformInstanceLibrary

HERE = Path(__file__).resolve().parent
BENCH = HERE.parent
REPO = HERE.parents[2]
LIBRARY = BENCH / "library"
DAG_DIR = BENCH / "page" / "dags"
MLIB = Path(os.environ.get("MSM_LIB", str(REPO / "src" / "metasmith_libraries")))

HPC_HOST = os.environ.get("MSM_HPC_HOST", "fir")
# Moved off rrg-shallam-ab on 2026-09-21 (Tony's call) because the RAC's fair share is spent. At the
# account level rrg-shallam-ab_cpu reads EffectvUsage 1.000000 against 3,584,000 shares and LevelFS 3.31,
# while rpp-shallam_cpu holds 495,000 shares against a RawUsage 245x smaller -- LevelFS 111.95, and
# `sreport` finds ZERO cpu-hours on it in the eight days to 2026-09-21. This account is also already where
# E4's CPLEX runtime lives (see e4_metagem.py's CPLEX_ROOT), so it is not a foreign allocation to this work.
#
# NOT MEASURED, and worth knowing before trusting it: `sbatch --test-only` for a single 2-cpu/1 h job
# predicted the SAME start time under both accounts, because one small job finds backfill either way. The
# throttle this is meant to lift is aggregate priority across a whole queue, which --test-only does not
# model. The evidence is the fair-share arithmetic; the confirmation is watching real start latency.
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "rpp-shallam")
# CAUTION both RACs are CPU-only: a GPU step under rrg-shallam-ab or rpp-shallam is rejected, and there is
# no rpp-shallam_gpu association at all, so GPU steps keep billing def-shallam_gpu.
SLURM_GPU_ACCOUNT = os.environ.get("MSM_SLURM_GPU_ACCOUNT", "def-shallam_gpu")
FIR_GPU = Gpu(memory=Size.GB(40), type="nvidia_h100_80gb_hbm3_3g.40gb", flag="--gres=gpu:")
AGENT_IMAGE = os.environ.get("MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.22.1")
ON_HOST = os.environ.get("BENCH_ON_HOST") == "1"
FIR_MEM_MB_PER_CPU = 4000
CHECKM2_DB = Path(os.environ.get(
    "CHECKM2_DB", "/scratch/phyberos/wave2_b3_nfcore/checkm2_db/CheckM2_database/uniref100.KO.1.dmnd"))
# Nodes whose Lustre client failed our tasks with Errno 108, EIO or 0-second starts in R1.
# A driver job's own --exclude does not reach the grid tasks nextflow submits.
FIR_BAD_NODES = ("fc30372,fc30557,fc30559,fc30560,fc30564,fc30567,fc30568,fc30570,fc30604,"
                 "fc30608,fc30609,fc30622,fc30623,fc30628,fc30640")

# E2's two arms and E5's CAMI pilot share the CAMI home, E3 and E5's Pratama pilot share
# Pratama's. Each home pays its own reference staging and needs its own dev overlay push.
# CAUTION a launch against the wrong home still submits and prints a key. It shows in the
# relay path the launch prints, and in nextflow ignoring -resume on a project that has run.
HOMES = {
    "cami": Path("/scratch/phyberos/cami/metasmith"),
    "pratama": Path("/scratch/phyberos/pratama2026/metasmith"),
    "metagem": Path("/scratch/phyberos/metagem/metasmith"),
}

# Each line runs twice: in the agent's persistent shell from the home, and in the run's
# launcher from runs/<key>. The relay is node-bound (its socket sits in the node's /tmp), so
# every node that drives a run starts its own, found by walking up to relay/msm_relay.
# WARNING never `exit` from a line here. It kills the agent's shell, and the caller sees
# only a 300 s timeout.
# CAUTION the `[ ! -e ]` guard keeps a second relay from replacing a running one's symlink.
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

CAMI_SAMPLES_TSV = REPO / "research" / "cami" / "samples.tsv"
PRATAMA_RUNS_TSV = REPO / "research" / "pratama2026" / "runs.tsv"
METAGEM_MANIFEST_TSV = REPO / "research" / "metagem" / "manifest.tsv"
METAGEM_ROOT = Path(os.environ.get("METAGEM_ROOT", "/scratch/phyberos/metagem"))
# e1_nfcore/stage_split_reads.py writes these pairs. E1's sheet and E2's short arm both read them.
CAMI_SPLIT_ROOT = Path(os.environ.get("CAMI_SPLIT_ROOT", "/scratch/phyberos/cami_nfcore_split_reads"))

DB_ROOT = Path("/home/phyberos/project-rpp/lib")
DB_PATHS = {
    "ref::uniref50_diamond_db": DB_ROOT / "diamond" / "uniref50.dmnd",
    # CAUTION the unpacked directory, not profiles.tgz beside it. kofamscan fails every
    # chunk instantly on the tarball, which reads as a tool failure.
    "ref::kofamscan_profiles": DB_ROOT / "kofamscan" / "profiles",
    "ref::kofamscan_ko_list": DB_ROOT / "kofamscan" / "ko_list.tsv",
}

# Staged unpacked on fir, so no plan carries a download step for them. A compute-node
# driver cannot finish a large download: it stalls without failing.
STAGED_REFS = {
    "ref::genomad": Path("/scratch/phyberos/databases/genomad"),
    # CAUTION the release directory itself: gtdbtk wants markers/, masks/ and taxonomy/
    # directly under GTDBTK_DATA_PATH, one level shallower than downloadGtdbDB produces.
    "ref::gtdb": Path("/scratch/phyberos/staging/gtdb/release232"),
    # Copied out of the Pratama task cache, which garbage collection may clear.
    "ref::vibrant_db": Path("/scratch/phyberos/refs/vibrant_1.2.1"),
    "ref::checkv_db": Path("/scratch/phyberos/refs/checkv_db"),
    # The Aug23 build (GTDB r214), which iphop.env's 1.3.3 requires. A Jun25 build needs iPHoP >= 1.4.1.
    "ref::iphop_db": Path("/scratch/phyberos/viromics_refs/iphop_db/Aug_2023_pub_rw"),
}
# GTDB's genome trees as one image, which the bench library's GTDB-Tk binds into ref::gtdb.
GTDB_GENOMES_IMAGE = {
    "bench::gtdb_genomes_image": Path("/scratch/phyberos/staging/gtdb/release232_skani_genomes.sqfs"),
}
# MetaPop 0.0.60's conda prefix as one image (drivers/refs/build_metapop_env.sh), bound at /opt/metapop.
METAPOP_ENV_IMAGE = {
    "bench::metapop_env_image": Path("/scratch/phyberos/refs/metapop_0.0.60_env.sqfs"),
}

# Outside the ref:: namespace, so only libraries that load annotation.yml can declare them.
STAGED_REFS_PRATAMA = {
    # CAUTION verify DRAM.config by content. prepare_databases writes an all-null config
    # first and logs per-database failures without failing.
    "annotation::dram_db": Path(os.environ.get("DRAM_DB_ROOT", "/scratch/phyberos/refs/dram_1.5.0")),
    # Pruned of upstream mmseqs scratch, whose dangling symlinks fail nextflow's `cp -fRL`
    # unstage and silently drop every consumer.
    "ref::vcontact3_db": Path(os.environ.get("VCONTACT3_DB_ROOT", "/scratch/phyberos/refs/vcontact3_v230")),
    # CAUTION built under a /db bind. VirSorter2 hashes its env path, so a copy at any other
    # bind point does not match.
    "annotation::virsorter2_db": Path(os.environ.get("VIRSORTER2_DB_ROOT", "/scratch/phyberos/refs/virsorter2_2.2.4")),
}

# The recovery comparison's shapes are set by their consumers: one pooled multi-fasta for
# `skani dist --qi`, and one flat directory of per-genome fasta for a non-recursive glob.
PRATAMA_PUBLISHED = {
    "pratama::published_votus": Path(os.environ.get(
        "PRATAMA_PUBLISHED_VOTUS",
        "/scratch/phyberos/pratama2026/zenodo_17897233_unpacked/votu/FINAL_groundwater-votu-5k.fasta")),
    "pratama::published_mags": Path(os.environ.get(
        "PRATAMA_PUBLISHED_MAGS", "/scratch/phyberos/pratama2026/zenodo_17897233_unpacked/mags")),
}


def get_agent(corpus):
    home = HOMES[corpus]
    source = Source.FromLocal(home) if ON_HOST else SshSource(host=HPC_HOST, path=home).AsSource()
    return Agent(home=source, container=AGENT_IMAGE, runtime=Runtime.APPTAINER,
                 setup_commands=SETUP_COMMANDS)


def agent_for(corpus, remote, dryrun_home):
    if remote:
        return get_agent(corpus)
    return Agent(home=Source.FromLocal(dryrun_home), runtime=Runtime.APPTAINER)


def host_sh(cmd, timeout=180):
    """Run a shell command where the data is: in place inside a fir job, over ssh otherwise."""
    argv = ["bash", "-c", cmd] if ON_HOST else ["ssh", HPC_HOST, cmd]
    r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        sys.exit(f"host command failed ({r.returncode}): {r.stderr.strip()}")
    return r.stdout


def given_name(base, declared):
    """A pool name that moves when what it declares moves.

    The pool matches a given by name alone. A name kept across a new path or new content
    would cite the old entry and serve the old entry's cached products.
    """
    return f"{base}@{hashlib.sha256(str(declared).encode()).hexdigest()[:12]}"


def add_file(givens, base, path, dtype, **kw):
    return givens.Add(path, dtype, name=given_name(base, path), **kw)


def add_value(givens, base, content, dtype, **kw):
    text = content if isinstance(content, str) else json.dumps(content, sort_keys=True)
    return givens.Value(given_name(base, text), text, dtype, **kw)


def declare_refs(givens, refs, tags=()):
    for dtype, path in refs.items():
        add_file(givens, f"ref/{dtype}", path, dtype, tags=["reference", *tags])


def cite(givens, location, type_libs, ensure):
    """The given library for what was declared. `ensure` imports whatever the pool lacks."""
    Path(location).parent.mkdir(parents=True, exist_ok=True)
    return givens.Build(location, type_library_paths=type_libs, ensure=ensure)


def pratama_globals(smith, cache_dir, ensure, with_zenodo_comparison=False):
    """The Pratama references, cited as their own resource library.

    Kept apart from the per-sample givens: a resource library holding sample types joins
    every sample's solver group, and samples of different shapes then solve as one case.
    """
    givens = smith.PoolGivens()
    for refs in (DB_PATHS, STAGED_REFS, STAGED_REFS_PRATAMA, GTDB_GENOMES_IMAGE):
        declare_refs(givens, refs)
    if with_zenodo_comparison:
        declare_refs(givens, PRATAMA_PUBLISHED)
    types = [MLIB / "data_types" / t for t in ("ref.yml", "env.yml", "annotation.yml", "pratama.yml")]
    types.append(LIBRARY / "data_types" / "bench.yml")
    return cite(givens, cache_dir / "pratama_globals.xgdb", types, ensure)


def cami_rows():
    return list(csv.DictReader(CAMI_SAMPLES_TSV.open(), delimiter="\t"))


def cami_split_pair(dataset, sample):
    return tuple(CAMI_SPLIT_ROOT / dataset / f"{sample}_R{mate}.fastq.gz" for mate in (1, 2))


def pratama_rows():
    return list(csv.DictReader(PRATAMA_RUNS_TSV.open(), delimiter="\t"))


def metagem_runs():
    """(dataset, run, layout, paths) per run in the metaGEM manifest."""
    groups = defaultdict(list)
    for r in csv.DictReader(METAGEM_MANIFEST_TSV.open(), delimiter="\t"):
        groups[(r["dataset"], r["relpath"].split("/")[1])].append(r["relpath"])
    runs = []
    for (dataset, run), relpaths in sorted(groups.items()):
        # The fetcher writes under <root>/<dataset>/<relpath>, one level below what relpath implies.
        paths = tuple(METAGEM_ROOT / dataset / p for p in sorted(relpaths))
        assert len(paths) in (1, 2), f"{dataset}/{run}: {len(paths)} files"
        runs.append((dataset, run, "single" if len(paths) == 1 else "paired", paths))
    return runs


def assembly_without(*masked):
    """The assembly library minus the named assemblers.

    Binners and Prodigal require the bare `sequences::assembly`, which no target pin
    reaches, so the other assembler has to be hidden.
    """
    lib = TransformInstanceLibrary.Load(MLIB / "transforms" / "assembly")
    return lib.AsView({Path(f"{m}.py") for m in masked}, invert=True)


def check_plan(task, expected_givens):
    """Exit unless the plan solved and carries every given at its declared count.

    `task.ok` with no dropped targets still passes a plan whose solver case consumed only
    some of the samples. Counting `plan.given` is where that drop shows.
    """
    if not task.ok:
        print(f"dropped: {sorted(task.plan.dropped_targets)}", file=sys.stderr)
        print("ERROR: workflow generation failed", file=sys.stderr)
        for h in getattr(task.plan, "hints", []) or []:
            print(f"  [{h.kind}] target={getattr(h, 'target', '?')}: {getattr(h, 'message', '')}")
            for line in (getattr(h, "chain", []) or []) + (getattr(h, "near_misses", []) or []):
                print(f"      {line}")
        sys.exit(1)
    got = Counter(i.dtype_name for i in task.plan.given)
    problems = [f"  {dtype}: declared {want}, plan.given has {got.get(dtype, 0)}"
                for dtype, want in expected_givens.items() if got.get(dtype, 0) != want]
    if problems:
        sys.exit("ERROR: given count mismatch:\n" + "\n".join(problems))


def print_plan(task, width=30):
    print(f"Plan OK -- {len(task.plan.steps)} steps, key={task.GetKey()}")
    for s in task.plan.steps:
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {Path(s.transform._path).stem:{width}s} -> {prods}")


def write_dag(task, stem, cache_dir):
    DAG_DIR.mkdir(parents=True, exist_ok=True)
    rendered = Path(str(task.plan.RenderDAG(str(cache_dir / stem), format="svg")))
    out = DAG_DIR / f"{stem}.dag.svg"
    out.write_bytes(rendered.read_bytes())
    print(f"dag: {out}")


# Steps whose products run to tens of GB. From node-local scratch a product lands on Lustre
# twice: in the work dir nextflow unstages it to, and in the cache shard promotion copies it
# into. Run in the work dir, it is written once and the shard hard-links it
# (promote._link_dir). A selector matches the whole process name, so a pinned transform needs its
# own entry.
# bowtie2_binning_bam stays on node-local scratch: in place it read its index from Lustre, and in two
# Lustre incidents 26 tasks exited 0 with 0.00% alignment.
#
# vcontact3_pratama is here for a different reason: it writes vcontact3_out and a
# `*.profile.pkl.gz` resume checkpoint, and on node-local scratch every retry gets a fresh work
# dir, so the tool's own checkpoint never engages and each of the 24 h-clamped rungs (6+12+24+24 =
# 66 h of possible burn per task) restarts from zero. In place, a same-directory relaunch lets the
# checkpoint pick up where it left off. CAUTION this puts its intermediates on Lustre, and
# therefore on the project inode/byte quota, which is under pressure -- at N=70,785 genomes that
# footprint is small next to the wall-clock saved, but it is a quota cost this step did not have
# before. assembly_stats_pratama is deliberately NOT here despite the same clamp pressure: its
# ~45 GB SAM/BAM intermediates are better off the quota, on node-local scratch.
IN_PLACE_STEPS = (
    "fastp", "bbduk_pratama", "megahit", "spades_pratama",
    "assembly_stats", "porechop_abi", "chopper", "minimap2_binning_bam",
    "vcontact3_pratama",
)

# No task may exceed 24 h of walltime. Assembly and bin refinement are the only sanctioned
# exceptions, because both are single-threaded-tail monoliths with no chunking available: hybrid
# metaSPAdes' six observed tasks ran 7:40 to 15:35 and pinned 384 GiB, and MetaWRAP refinement runs
# CheckM inside itself.
#
# The ceiling is enforced as a params entry, NOT as a constant in the rendered config, because
# codegen runs in the agent process where a driver-side constant never arrives, and because it has to
# be read inside a directive CLOSURE -- the config is parsed before the -params-file merge.
# `Resources.AsNextflowFormat` renders `[scaled, ceiling].min()` around every duration.
MAX_TASK_DURATION = "24h"

# The sanctioned exceptions, as a transform name -> its own ceiling. A `withName` block overrides the
# global one for that selector only. Keep this list short and justified: every entry is a task that
# can occupy a node for longer than a day, and the 7.0-day submit-time cap still applies to the
# rung it reaches, so base x 2^(tries-1) must stay under 168 h.
LONG_RUNNING_STEPS = {
    "spades_pratama": "36h",
    "spades_hybrid_pratama": "36h",
    "metawrap_refine_pratama": "36h",
    "spades": "36h",
}

# The memory half of the same rule, and it needs its own ceiling because nothing else caps memory:
# `Resources.AsNextflowFormat` clamps duration against params and leaves memory doubling unbounded.
# 192 GB is `spades_pratama`'s own declaration, which is the envelope this campaign measured its
# largest short-read assembly inside; a step asking past it is asking for a grant no result here has
# ever needed. Unbounded doubling is how assembly_stats_pratama reached a 512 GB rung while failing
# on walltime at every grant from 64 GB up -- four attempts that tell you memory was never the
# constraint, and a ladder that answered by asking for eight times more of it.
MAX_TASK_MEMORY_GB = 192

# Sanctioned exceptions to the memory ceiling, same shape and same discipline as LONG_RUNNING_STEPS.
# Hybrid metaSPAdes is the one measured case: six tasks at MaxRSS 238-402 GB, so its declared 384 GB
# is the observation rather than a guess. It is listed for the day something scales it -- it is not
# in any driver's SCALED today, so its resources come from its own declaration and this dict is
# currently unused. Keep it that way: an entry here is a claim that a measurement justifies it.
LARGE_MEMORY_STEPS = {
    "spades_hybrid_pratama": 384,
}


def make_slurm_config(smith, cache_dir, scaled=None, comebin_cpus=48, comebin_time="3d"):
    """fir's Slurm preset plus process selectors.

    `scaled` maps a transform name to (cpus, GB, hours) for its first attempt, and each retry
    doubles memory and time, as a transform's own declaration does. A flat `withName` value would
    replace that doubling, so a task that needs more than its first grant would fail identically on
    every retry.

    COMEBin gets a quarter node at 4 GB per core. Its training is Amdahl-limited, and ten
    marine samples at 96 cores took 6.5 to 11.7 h, so 3 d covers the slow tail at 48.
    The `_cached` twins declare the local executor, which refuses a job array, and the
    preset's label-keyed exemption does not reach them. Without `array = 0` nextflow aborts
    before submitting anything, and metasmith still reports the run complete. On node-local
    scratch their `ln -f` from the shard crosses devices and falls back to a copy, so a relaunch
    wrote every hit to Lustre again. In the work dir the link succeeds.
    """
    base = Path(smith.GetNxfConfigPresets()["slurm"]).read_text()
    mem_gb = comebin_cpus * FIR_MEM_MB_PER_CPU // 1000
    # comebin_time is clamped against LONG_RUNNING_STEPS the same way the `scaled` loop below clamps
    # itself, and for the same reason: a `withName` time is a LITERAL, outside the params clamp that
    # `Resources.AsNextflowFormat` wraps every declared duration in, so an unclamped literal here is a
    # silent hole in the 24 h rule. COMEBin is not in LONG_RUNNING_STEPS -- it was never made a
    # sanctioned exception, only defaulted to 3d -- so absent this clamp it ran the 24 h rule was
    # supposed to bind on. This makes COMEBin 24 h unless someone adds it to LONG_RUNNING_STEPS
    # deliberately; the point of that dict is that an exception is listed there on purpose, auditably,
    # and COMEBin never was.
    comebin_cap = LONG_RUNNING_STEPS.get("comebin", MAX_TASK_DURATION)
    # A literal, not params.process.clusterOptionsExtra: config reads params before the -params-file merge.
    text = base + "\n" + "\n".join([
        "", "process {",
        f'    clusterOptions = "--nodes=1 --ntasks=1 --account={SLURM_ACCOUNT} --exclude={FIR_BAD_NODES}"',
        "}", "",
        "process {", "    withName: '.*__comebin' {",
        f"        cpus = {comebin_cpus}",
        f"        memory = '{mem_gb} GB'",
        f"        time = {{ [('{comebin_time}' as Duration), ('{comebin_cap}' as Duration)].min() }}",
        f'        clusterOptions = "--nodes=1 --ntasks=1 --account={SLURM_ACCOUNT} --exclude={FIR_BAD_NODES}"',
        "    }", "}", "",
        "process {", "    withName: '.*_cached' {", "        array = 0", "        scratch = false", "    }", "}", ""])
    for name in IN_PLACE_STEPS:
        text += "\n".join([
            "process {", f"    withName: '.*__{name}' {{", "        scratch = false", "    }", "}", ""])
    # A `withName` time is a LITERAL and therefore outside the params clamp that
    # `Resources.AsNextflowFormat` wraps every declared duration in -- so each one clamps itself here,
    # or it silently becomes a hole in the 24 h rule. megahit at a 12 h base reached 96 h on rung 4
    # this way without ever being refused, because 96 h is under fir's 7 d cap and nothing else looked.
    for name, (cpus, gb, hours) in (scaled or {}).items():
        cap = LONG_RUNNING_STEPS.get(name, MAX_TASK_DURATION)
        mem_cap = LARGE_MEMORY_STEPS.get(name, MAX_TASK_MEMORY_GB)
        text += "\n".join([
            "process {", f"    withName: '.*__{name}' {{", f"        cpus = {cpus}",
            f"        memory = {{ [{gb}.GB * (2 ** (task.attempt - 1)), {mem_cap}.GB].min() }}",
            f"        time = {{ [{hours}.h * (2 ** (task.attempt - 1)), ('{cap}' as Duration)].min() }}",
            "    }", "}", ""])
    # The sanctioned exceptions to the 24 h rule. Deliberately FLAT rather than a doubling ladder: the
    # whole point of an exception is a fixed, auditable ceiling, and for a step that is already the
    # exception "it missed, so give it twice as long" is the behaviour the rule exists to stop. A step
    # that cannot assemble or refine inside its ceiling needs a different shape, not another rung.
    for name, cap in LONG_RUNNING_STEPS.items():
        if name in (scaled or {}):
            continue
        text += "\n".join([
            "process {", f"    withName: '.*__{name}' {{", f"        time = '{cap}'", "    }", "}", ""])
    cache_dir.mkdir(parents=True, exist_ok=True)
    out = cache_dir / "fir_slurm.config"
    out.write_text(text)
    return out


def stage_and_run(smith, task, cache_dir, tag, *, stage_only, params, scaled=None, materialise=False, gpus=None,
                  on_exist="update", extra_config=None):
    """Stage the plan, then run it, or with `materialise` fetch every image it needs and stop.

    CAUTION `on_exist="update"` re-sends context but KEEPS a transform bundle the run directory already
    holds. A plan key ignores protocol source (solver model only), so a fix that changes only a protocol
    re-materialises onto the same key and silently keeps the old bundle: wave 5 staged wave-4 copies of
    smetana, deepvirfinder and metapop_study that way. Pass `on_exist="clear"` to force a full restage.
    """
    keys_file = cache_dir / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[tag] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))

    smith.StageWorkflow(task, on_exist=on_exist, verify_external_paths=False)
    if materialise:
        report = smith.MaterialiseImages(task)
        print(f"images for {tag} ({task.GetKey()}): {report['fetched']} fetched, "
              f"{report['already_present']} already present, unknown steps {report['unknown']}", flush=True)
        return
    if stage_only:
        print(f"staged {tag} as {task.GetKey()}")
        return
    # fir refuses ANY job over 7.0 days AT SUBMIT TIME, whatever the partition -- reproduced with
    # `sbatch --test-only`, and note `cpubase_bycore_b6` advertises 28 days yet is still refused.
    # The retry ladder doubles duration, so a base of B asks B * 2^(attempts-1) on its last rung; at
    # tries=4 a 48 h base asked 192 h and 384 h, both rejected. An ignored SUBMISSION failure then
    # decrements nextflow's running-task counter with no matching increment (runs ended at
    # runningCount -6 and -7), so the monitor never sees the queue drain and the run wedges.
    # `Resources.AsNextflowFormat` caps the rendered time against this ceiling.
    # CAUTION it must be a params entry read inside a CLOSURE, never interpolated into the config:
    # the config is parsed BEFORE the -params-file merge, which is why clusterOptionsExtra had to be
    # a literal (see make_slurm_config). A closure is evaluated per task, after the merge -- proven
    # in production by `errorStrategy`, which read tries=4 from the params file rather than the
    # config's own default of 2.
    # 24 h, not 7 days. The 7-day value only kept the ladder submittable; the policy is that no task
    # occupies a node for more than a day, with LONG_RUNNING_STEPS the sanctioned exceptions.
    #
    # CAUTION this key reaches nextflow as `process.max.duration`, NOT `process.max_duration`:
    # `RunWorkflow(params=<dict>)` splits every underscored key into nested maps. The clamp closure in
    # `Resources.AsNextflowFormat` reads both spellings for that reason. It previously read only the
    # flat one, so this ceiling was set and never armed -- which is how E3's vConTACT3 ladder asked
    # 8 days against fir's 7-day cap, had the submission refused, and had the failure swallowed by the
    # ignore path. Verify with `grep max <run>/workflow.params.yml` after staging, not by reading this.
    params = dict(params)
    params["process"] = dict(params.get("process") or {}, max_duration=MAX_TASK_DURATION)
    config = make_slurm_config(smith, cache_dir, scaled)
    if extra_config is not None:
        # Appended last, so its literals win. `array` and `submitRateLimit` read params outside a
        # closure in slurm.nf, and only a literal in the file reaches them.
        config.write_text(config.read_text() + "\n" + Path(extra_config).read_text())
    smith.RunWorkflow(task=task, config_file=config, gpus=gpus,
                      params=dict(slurmAccount=SLURM_ACCOUNT, slurmGpuAccount=SLURM_GPU_ACCOUNT, **params))
    print(f"submitted {tag}: {task.GetKey()}", flush=True)
    if ON_HOST:
        wait_for_run(smith, task.GetKey())


def wait_for_run(smith, key, poll_s=60):
    """Hold the driver job open until the run exits, and cancel the run on USR1 or TERM.

    The launcher backgrounds the run and returns, and a Slurm job that ends kills every
    process in it. submit_driver.sbatch forwards Slurm's pre-wall USR1 here.
    CancelWorkflow lets nextflow cancel its grid jobs and keep the tasks that finished.
    """
    import signal
    import time
    from metasmith.constants import AgentPaths

    workspace = AgentPaths.to_task(key, root=smith.home.GetPath()).parent.parent
    pgid = int((workspace / AgentPaths.RUN_PGID_FILE).read_text().strip())

    def cancel(signum, _frame):
        print(f"signal {signum}: cancelling {key}", flush=True)
        print(json.dumps(smith.CancelWorkflow(key), indent=2, default=str), flush=True)

    for sig in (signal.SIGUSR1, signal.SIGTERM):
        signal.signal(sig, cancel)
    print(f"waiting on run {key} (pgid {pgid}) in {workspace}", flush=True)
    while True:
        try:
            os.kill(pgid, 0)
        except ProcessLookupError:
            break
        time.sleep(poll_s)
    print(f"run {key} exited", flush=True)
