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
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "rrg-shallam-ab")
# CAUTION the RAC is CPU-only: a GPU step under rrg-shallam-ab is rejected, so GPU steps bill def-shallam_gpu.
SLURM_GPU_ACCOUNT = os.environ.get("MSM_SLURM_GPU_ACCOUNT", "def-shallam_gpu")
FIR_GPU = Gpu(memory=Size.GB(40), type="nvidia_h100_80gb_hbm3_3g.40gb", flag="--gres=gpu:")
AGENT_IMAGE = os.environ.get("MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.22.1")
ON_HOST = os.environ.get("BENCH_ON_HOST") == "1"
FIR_MEM_MB_PER_CPU = 4000

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
# (promote._link_dir).
IN_PLACE_STEPS = (
    "fastp", "bbduk_pratama", "megahit", "spades_pratama", "bowtie2_binning_bam",
    "assembly_stats", "porechop_abi", "chopper", "minimap2_binning_bam", "metawrap_pratama",
)


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
    text = base + "\n" + "\n".join([
        "", "process {", "    withName: '.*__comebin' {",
        f"        cpus = {comebin_cpus}",
        f"        memory = '{mem_gb} GB'",
        f"        time = '{comebin_time}'",
        f'        clusterOptions = "--nodes=1 --ntasks=1 --account={SLURM_ACCOUNT}"',
        "    }", "}", "",
        "process {", "    withName: '.*_cached' {", "        array = 0", "        scratch = false", "    }", "}", ""])
    for name in IN_PLACE_STEPS:
        text += "\n".join([
            "process {", f"    withName: '.*__{name}' {{", "        scratch = false", "    }", "}", ""])
    for name, (cpus, gb, hours) in (scaled or {}).items():
        text += "\n".join([
            "process {", f"    withName: '.*__{name}' {{", f"        cpus = {cpus}",
            f"        memory = {{ {gb}.GB * (2 ** (task.attempt - 1)) }}",
            f"        time = {{ {hours}.h * (2 ** (task.attempt - 1)) }}",
            "    }", "}", ""])
    cache_dir.mkdir(parents=True, exist_ok=True)
    out = cache_dir / "fir_slurm.config"
    out.write_text(text)
    return out


def stage_and_run(smith, task, cache_dir, tag, *, stage_only, params, scaled=None, materialise=False, gpus=None):
    """Stage the plan, then run it, or with `materialise` fetch every image it needs and stop."""
    keys_file = cache_dir / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[tag] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))

    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)
    if materialise:
        report = smith.MaterialiseImages(task)
        print(f"images for {tag} ({task.GetKey()}): {report['fetched']} fetched, "
              f"{report['already_present']} already present, unknown steps {report['unknown']}", flush=True)
        return
    if stage_only:
        print(f"staged {tag} as {task.GetKey()}")
        return
    smith.RunWorkflow(task=task, config_file=make_slurm_config(smith, cache_dir, scaled), gpus=gpus,
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
