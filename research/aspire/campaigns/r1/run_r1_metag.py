"""run_r1_metag.py — GMCF_3495 (Steven Chen) metagenomics run r1 on fir.

One driver, one DAG, four product families, 34 samples:

  reads ─ interleave ─ seqkit_reads ────────────────────────────► read QC stats
                     └ bbduk ─ megahit ─┬─ prodigal ─┬─ diamond_uniref50 ─► annotation
                                        │            ├─ kofamscan ────────► annotation
                                        │            ├─ eggnog_mapper ────► annotation
                                        │            └─ proteinbert ──────► ORF embeddings
                                        ├─ assembly_stats ─ bam + coverages
                                        └─ {metabat2, semibin2, comebin} ─┬─ checkm2
                                                                          └─ aggregator ─ skani_dedup
        short_reads ─┬─ kraken2 + bracken ────────────────────────────────► read taxonomy
                     └─ centrifuger ────────────────────────────────────► read taxonomy

Two taxonomy products are deliberately NOT in this DAG, because their reference
databases are too large to keep on the cluster and both now live as on-demand
Arbutus services (arbutus-infra/dev/scripts/{metabuli,gtdbtk}-submit.sh):

  * contig taxonomy — metabuli against GTDB r232 (~744 GB on disk)
  * bin taxonomy    — GTDB-Tk against GTDB r232 (~110 GB)

Both are driven after this run by the campaign driver, over products this DAG
produces (megahit assemblies; quality bins from the aggregator). That is also
what makes this a SINGLE submission: with both big databases off-cluster,
nothing in the target set waits on a staged database, so there are no waves.

GTDB release: every GTDB-based tool here is on r232 — centrifuger's on-cluster
index, and both Arbutus services. kraken2/bracken is NCBI and is deliberately
NOT reconciled onto r232: it is the non-GTDB second opinion.

Adapted from the spanish-lakes river drivers (`projects/spanish-lakes/river/scripts/
run_w{1,2,3}_*_river.py`), which split the same chain across four separately-submitted
waves against fir. Here it is a single plan, because this dataset is ~430 GiB
rather than 100+ samples and there is no reason to stage the waves apart.

Site notes:
  * `module load apptainer` alone. The `module load gcc/9.4.0` that must precede
    it on Sockeye is a Sockeye quirk and is wrong here.
  * SLURM allocation is `rrg-shallam-ab` (`def-shallam_gpu` for the GPU partition).
  * Reference DBs are the lab's own nested library at /home/phyberos/project-rpp/lib/,
    except eggnog, which must be the UNCOMPRESSED copy on scratch (the lib copy is
    a single eggnog.db.gz).
  * fir's centrifuger index is a real DIRECTORY and the transform discovers the
    `-x` prefix from its contents, so no DB_PROBE_SUFFIX entry is needed. It is
    the `r232+refseq_hvfpc` hybrid — a superset of bare r232, not plain r232.
  * Compute nodes have no outbound network: containers must be prefetched and every
    reference DB pre-staged, so the planner never inserts a `download*` step. Run
    `setup --run` once before `run`.

HISTORICAL, as of the monorepo migration: this RAN against pinned run scopes rather
than the deployed `msm` environment -- metasmith 0.19.1 at
projects/metasmith/steven-c-metag and the ExecWithEnv-ported libraries at
projects/metasmith-libraries/steven-c-metag. Neither path exists now; `MLIB` below
defaults into this repo instead, and the engine is whatever it ships. Re-running is
a re-plan, not a replay -- see ../README.md. The old warning about
never prepending a metasmith source checkout (its LiveShell used an fd-5 control
trampoline that hung over plain ssh) no longer applies on this line — 0.19.x uses
the in-band MARKER mechanism — but verify it with a trivial remote call before
trusting a long run to it.

Usage:
  python run_r1_metag.py list-samples                  # 34 samples + sizes from samples.tsv
  python run_r1_metag.py list-samples --from-cluster   # re-derive from what is on fir
  python run_r1_metag.py check-dbs                     # ssh-verify every declared input path
  python run_r1_metag.py stage-reads                   # print the Globus batch (add --yes to submit)
  python run_r1_metag.py setup                         # render the container-pull plan
  python run_r1_metag.py setup --run                   # deploy agent + prefetch containers
  python run_r1_metag.py run --dry-run                 # plan + render the DAG, submit nothing
  python run_r1_metag.py run                           # stage + submit to SLURM
  python run_r1_metag.py status
"""
import os
import re
import sys
import json
import argparse
import subprocess
from pathlib import Path

# Put the env bin (graphviz `dot`) on PATH so RenderDAG works — the plan render
# shells out to `dot` and a real run hits the same call before staging.
os.environ["PATH"] = f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}"

from metasmith.python_api import (  # noqa: E402
    Agent, Source, SshSource,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Runtime,
    Resources, Size, Duration,
)

ROOT = Path(__file__).resolve().parent
# The transform library. This used to name the `metasmith-libraries/lung-microbiome`
# worktree, because every transform there is ported to ExecWithEnv().ifContainerDo()
# and metasmith 0.20.x statically rejects the old ExecWithContainer -- it is in
# _FORBIDDEN_CALLS. That worktree is archived; the monorepo's library carries the
# same port (154 transforms on ExecWithEnv, zero on ExecWithContainer), so the
# default now points there. `MSM_LIB` still overrides it.
MLIB      = Path(os.environ.get(
    "MSM_LIB",
    str(Path(__file__).resolve().parents[4] / "src" / "metasmith_libraries")))
# Overridable because a dry run is NOT read-only: build_inputs() calls Purge()
# on r1_inputs.xgdb inside this directory, which is version-controlled and also
# holds the approved DAG. Point it at scratch for any exploratory planning.
CACHE_DIR = Path(os.environ.get("MSM_CACHE_DIR", ROOT / ".cache"))
SAMPLES_TSV = ROOT / "samples.tsv"

# ── fir site config ──────────────────────────────────────────────────────────
HPC_HOST       = os.environ.get("MSM_HPC_HOST", "fir")
SLURM_ACCOUNT  = os.environ.get("MSM_SLURM_ACCOUNT", "rrg-shallam-ab")
GPU_ACCOUNT    = os.environ.get("MSM_GPU_ACCOUNT", "def-shallam_gpu")
SETUP_COMMANDS = ["module load apptainer"]

HPC_USER      = os.environ.get("MSM_HPC_USER", "phyberos")
HPC_SCRATCH   = Path(f"/scratch/{HPC_USER}")
# A RUN-PRIVATE agent home, not the lab's shared /scratch/phyberos/metasmith.
#
# That shared home carries a dev overlay at dev/metasmith which Agent.Deploy
# binds over site-packages/metasmith inside the task container whenever it
# exists — so whatever version sits there is what actually executes, regardless
# of the client. It currently holds 0.18.7 (injected 2026-07-03), which cannot
# run this DAG: it predates gpu_args on Agent, and predates the caching system
# this run exists to pilot.
#
# Overwriting it would silently move every other metasmith run on fir onto an
# unreleased 0.19.1 — the same hazard as repointing ~/lib/locals/metasmith
# locally. So this run gets its own home and its own container store, and the
# shared one is left exactly as found.
HPC_MSM_HOME  = Path(os.environ.get(
    "MSM_AGENT_HOME", str(HPC_SCRATCH / "gmcf3495" / "metasmith")))
HPC_READS_DIR = Path(os.environ.get("MSM_READS_DIR", str(HPC_SCRATCH / "gmcf3495" / "reads")))

# ── reference DBs, pre-staged on fir ─────────────────────────────────────────
# The lab's nested reference library. Not the flat Sockeye clone.
DB_ROOT = Path("/home/phyberos/project-rpp/lib")
DB_PATHS = {
    "ref::uniref50_diamond_db": DB_ROOT / "diamond" / "uniref50.dmnd",
    "ref::kofamscan_profiles":  DB_ROOT / "kofamscan" / "profiles.tgz",
    "ref::kofamscan_ko_list":   DB_ROOT / "kofamscan" / "ko_list.tsv",
    # UNCOMPRESSED, and therefore on scratch — lib/eggnog holds only eggnog.db.gz.
    # Same path the spanish-lakes w2 ORF driver uses.
    "annotation::eggnog_data":  Path("/scratch/phyberos/databases/eggnog"),
    # NCBI, not GTDB. The deliberate non-GTDB second opinion; do not "reconcile"
    # this onto r232.
    "ref::kraken2_db":          DB_ROOT / "kraken2_2026",
    # A DIRECTORY, not a prefix: the transform runs `ls <dir>/*.1.cfr` and derives
    # the `-x` prefix itself, so there is no DB_PROBE_SUFFIX entry for it. The
    # index inside is cfr_gtdb_r232+refseq_hvfpc — a SUPERSET of bare r232.
    "ref::centrifuger_db":      DB_ROOT / "centrifuger_r232",
    # No ref::metabuli_ref, and no ref::gtdb. Both databases are off-cluster
    # Arbutus services now; see the module docstring.
}

# `check-dbs` stats these paths verbatim except where a dtype names a prefix
# rather than a real file — then probe one member instead. Empty on fir: every
# path above is a real file or directory.
DB_PROBE_SUFFIX = {}

# ── Globus (raw reads still live on the chinook guest collection) ─────────────
GLOBUS_SRC_EP   = "2602486c-1e0f-47a0-be15-eec1b0ff0f96"   # chinook guest collection
GLOBUS_SRC_ROOT = "/Received_raw_data/GMCF_3495"
GLOBUS_DST_EP   = "8dec4129-9ab4-451d-a45f-5b4b8471f7a3"   # fir (Alliance DTN)

# ── tool environments this DAG needs prefetched before any job runs ───────────
# Names are resources/env/<name>.env in MLIB. metabuli is NOT here: its contig
# taxonomy left the cluster, and the Arbutus service carries its own image on the
# reference volume. If metabuli ever comes back on-cluster, this list and
# build_targets() both need it — they are separate facts.
R1_CONTAINERS = [
    "seqkit", "bbtools", "megahit", "samtools", "minimap2", "bedtools",
    "pprodigal", "diamond", "kofamscan", "eggnog-mapper",
    "proteinbert", "polars",
    "kraken2", "bracken", "centrifuger", "python_for_data_science",
    "metabat2", "semibin", "comebin", "checkm", "skani",
]


# ── helpers ──────────────────────────────────────────────────────────────────
def ssh_cmd(cmd, timeout=180, check=True):
    result = subprocess.run(
        ["ssh", HPC_HOST, cmd], capture_output=True, text=True, timeout=timeout,
    )
    if check and result.returncode != 0:
        print(f"ssh stderr: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"ssh command failed: {cmd}")
    return result.stdout.strip(), result.returncode


# The run is on 0.20.2, whose artifacts are not published: conda still serves
# 0.20.1 and quay has no 0.20.2 tag. The tag metasmith computes for this
# checkout ("metasmith:0.20.2", since an editable install leaves BUILD_HASH
# empty) therefore does not exist either.
#
# Pinning the 0.20.1 CI build is not a compromise here, because the image
# supplies exactly one thing -- the conda environment -- and 0.20.2's
# envs/base.yml is byte-identical to 0.20.1's. 0.20.2 is two commits past
# 0.20.1: a version bump and a RELEASE_PROTOCOL.md edit, no code. Verified in
# the image: flask 3.1.3, coolname 2.2.0 (the two packages 0.20.x adds over
# 0.19.1), python 3.12.13, nextflow 26.04.1 matching the pin.
#
# The code that actually runs is this scope's own source, bound over
# site-packages/metasmith by the dev overlay. Nothing is pushed to the registry.
AGENT_IMAGE = os.environ.get(
    "MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.20.1-bf54d6f")


def get_agent():
    home = SshSource(host=HPC_HOST, path=HPC_MSM_HOME).AsSource()
    return Agent(
        home=home,
        container=AGENT_IMAGE,
        runtime=Runtime.APPTAINER,
        setup_commands=SETUP_COMMANDS,
    )


def _sid_sort_key(sid):
    m = re.fullmatch(r"S(\d+)", sid)
    return (0, int(m.group(1)), "") if m else (1, 0, sid)


def read_samples_tsv():
    """[(sample_id, globus_src_r1, globus_src_r2, bytes_r1, bytes_r2), ...]."""
    rows = []
    for line in SAMPLES_TSV.read_text().splitlines():
        if not line.strip() or line.startswith(("#", "sample_id")):
            continue
        sid, r1, r2, b1, b2 = line.split("\t")
        rows.append((sid, r1, r2, int(b1), int(b2)))
    return sorted(rows, key=lambda r: _sid_sort_key(r[0]))


def remote_reads(sid):
    """Flat on-cluster names the Globus batch lands the per-sample dirs into."""
    return HPC_READS_DIR / f"{sid}_R1.fastq.gz", HPC_READS_DIR / f"{sid}_R2.fastq.gz"


def enumerate_samples(from_cluster=False):
    """[(sample_id, r1_on_cluster, r2_on_cluster), ...].

    Default source is the committed samples.tsv snapshot (taken from the Globus
    listing), so the DAG can be planned before the reads finish landing. Pass
    `from_cluster` to enumerate what is actually on fir instead.
    """
    if from_cluster:
        out, _ = ssh_cmd(f"ls {HPC_READS_DIR}/*_R1.fastq.gz 2>/dev/null || true")
        pairs = []
        for r1 in sorted(p.strip() for p in out.splitlines() if p.strip()):
            sid = Path(r1).name[: -len("_R1.fastq.gz")]
            pairs.append((sid, Path(r1), Path(r1[: -len("_R1.fastq.gz")] + "_R2.fastq.gz")))
        return sorted(pairs, key=lambda r: _sid_sort_key(r[0]))
    return [(sid, *remote_reads(sid)) for sid, *_ in read_samples_tsv()]


def select(samples, args):
    wanted = set(args.sample or [])
    if getattr(args, "samples_file", None):
        for raw in Path(args.samples_file).read_text().splitlines():
            s = raw.strip()
            if s and not s.startswith("#"):
                wanted.add(s)
    if wanted:
        picked = [s for s in samples if s[0] in wanted]
        missing = wanted - {s[0] for s in picked}
        if missing:
            print(f"ERROR: requested sample(s) not found: {sorted(missing)}", file=sys.stderr)
            sys.exit(1)
        samples = picked
    if getattr(args, "exclude", None):
        samples = [s for s in samples if s[0] not in set(args.exclude)]
    return samples


# ── the step-1 products, supplied as givens ──────────────────────────────────
# Run KMQ5eomS interleaved all 34 libraries and the products were verified
# against an independent read count: truth/<sid>.truth column 4 equals
# truth/<sid>.verify column 2 for all 34, every one OK. Because those 34 counts
# are mutually distinct, the agreement is also a proof of sample attribution --
# a product carrying the wrong library's reads could not match its own sample's
# expected count. That is the one thing the 0.19.1 lineage bug could have
# falsified, and it did not.
#
# So step 1 is not re-run. The 34 products are hardlinked out of the task-cache
# shard into a stable directory (each link verified same-inode, same-size) and
# enter the plan as givens of type sequences::short_reads, parented to the same
# read_pair as the raw reads they came from.
#
# What this deliberately does NOT reuse is anything downstream of step 1. Every
# bbduk task in run j60YFVIo read the SAME interleaved product -- 34 tasks, all
# reporting `Input: 896 reads`, which is NTC, the smallest library -- because
# 0.19.1 replayed only the first member of a cached batch to every consumer.
# Those results are discarded, and the fix (ba76d63, 68cc8c6) is why this run
# is on 0.20.1.
#
# Set MSM_NO_INTERLEAVED=1 to plan from the raw reads instead and re-derive
# step 1 from scratch.
INTERLEAVED_DIR = Path(os.environ.get(
    "MSM_INTERLEAVED_DIR", "/scratch/phyberos/gmcf3495/interleaved_backup"))
USE_INTERLEAVED = not os.environ.get("MSM_NO_INTERLEAVED")


# ── plan construction ────────────────────────────────────────────────────────
def build_inputs(samples):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(CACHE_DIR / "r1_inputs.xgdb")
    inputs.Purge()

    for tl in ["sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
               "taxonomy.yml", "binning.yml", "binning_local.yml", "env.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    for sid, r1, r2 in samples:
        meta = inputs.AddValue(
            f"{sid}_read_metadata.json",
            {"parity": "paired", "length_class": "short"},
            "sequences::read_metadata",
        )
        pair = inputs.AddValue(
            f"{sid}_read_pair.json",
            {"atomic": "a pair of read files", "sample": sid},
            "sequences::read_pair",
            parents={meta},
        )
        inputs.AddItem(r1, "sequences::zipped_forward_short_reads", parents={pair})
        inputs.AddItem(r2, "sequences::zipped_reverse_short_reads", parents={pair})

        # The step-1 product, supplied rather than recomputed. See INTERLEAVED_DIR.
        if USE_INTERLEAVED:
            inputs.AddItem(INTERLEAVED_DIR / f"{sid}.interleaved.fq.gz",
                           "sequences::short_reads", parents={pair})

    # Pre-staged reference DBs → the planner skips every download* transform
    # (each is login-node-pinned and none can run on a compute node anyway).
    for dtype, path in DB_PATHS.items():
        inputs.AddItem(path, dtype)

    _apply_leaf_pins(inputs)
    inputs.Save()
    return inputs


LEAF_PINS = Path(os.environ.get("MSM_LEAF_PINS", ROOT / "leaf_ids_r1_0201.json"))


def cmd_dump_pins(args):
    """Mint every given once and record the ids, so restaging is reproducible.

    Measured on 0.20.1 with this input set: two builds back to back agreed on
    0 of 176 identities. Every given is on fir, so `_mint_leaf_id` cannot read
    the bytes to derive an id from them and takes the random arm for all of
    them -- the `1e20...` here is a blake3 multihash of random material, not of
    content, which is exactly why it differs each time. A step's cache_key
    folds in its inputs' identities, so without a pin file no plan key is
    reproducible and nothing can ever be resumed.

    Sizes come from fir in a single batched stat rather than being omitted.
    The guard in _apply_leaf_pins refuses to pin a path whose size changed --
    a pinned id on changed content is a silent false cache hit, which is worse
    than a miss. Recording sizes only for locally-visible paths would leave
    that guard measuring nothing for the 176 inputs that actually matter.
    """
    samples = select(enumerate_samples(from_cluster=False), args)
    prev = os.environ.get("MSM_NO_LEAF_PINS")
    os.environ["MSM_NO_LEAF_PINS"] = "1"       # mint fresh; do not read an old file
    try:
        inputs = build_inputs(samples)
    finally:
        if prev is None: os.environ.pop("MSM_NO_LEAF_PINS", None)
        else: os.environ["MSM_NO_LEAF_PINS"] = prev

    meta = {str(p): dict(m) for p, m in inputs.instance_meta.items()}
    remote = sorted(p for p in meta if p.startswith("/"))
    sizes = {}
    if remote:
        print(f"stat'ing {len(remote)} path(s) on {HPC_HOST}...")
        script = "\n".join(f'printf "%s\\t%s\\n" "{p}" "$(stat -c %s "{p}" 2>/dev/null || echo -1)"'
                           for p in remote)
        out, _ = ssh_cmd(script, timeout=600)
        for line in out.splitlines():
            if "\t" in line:
                path, sz = line.rsplit("\t", 1)
                sizes[path] = int(sz)
        bad = [p for p, s in sizes.items() if s < 0]
        if bad:
            print(f"WARNING: {len(bad)} path(s) not stat-able on {HPC_HOST}:", file=sys.stderr)
            for p in bad[:10]:
                print(f"  {p}", file=sys.stderr)

    given = {}
    for path, m in meta.items():
        rec = {"instance_id": m.get("instance_id")}
        if path in sizes and sizes[path] >= 0:
            rec["size"] = sizes[path]
        if m.get("type"):
            rec["type"] = m["type"]
        given[path] = rec

    out_file = Path(args.out) if args.out else LEAF_PINS
    out_file.write_text(json.dumps({
        "_comment": ("Leaf instance_ids minted once under metasmith 0.20.1 so that "
                     "restaging reproduces the same plan key. Regenerate with "
                     "`run_r1_metag.py dump-pins` if the given set changes."),
        "given": given,
    }, indent=2))
    n_sized = sum(1 for r in given.values() if "size" in r)
    print(f"wrote {len(given)} pins to {out_file} ({n_sized} with a size guard)")
    return 0


def _apply_leaf_pins(inputs):
    """Pin leaf instance_ids to the ones run KMQ5eomS minted.

    A step's cache_key folds in the identities of its inputs, so if the givens
    get new identities every time the plan is staged, nothing upstream can ever
    be reused -- and this run banked 411 GB against KMQ5eomS's identities.
    Measured: two stagings four minutes apart, with no edits between them,
    agreed on 21 of 163 identities.

    Metasmith already tries to prevent this. `_mint_leaf_id` derives the id from
    blake3(content) + library-relative path, which is stable across runs and
    hosts, and only falls back to uuid4+time_ns when the file is not a readable
    regular file at mint time. Two things put us on the fallback path for 142 of
    the 163 givens:

      * The reads and reference databases live on fir and are named by absolute
        path, while the driver mints on this machine, where those paths do not
        resolve. 68 reads + 6 references.
      * `AddValue` calls `AddItem` -- which mints -- and only then writes the
        file, so a value's id is minted while its own file does not yet exist.
        That one is order-dependent and would take the random path even on fir.
        68 read_metadata/read_pair values.

    The 21 that were stable are exactly the `env` entries, which are
    library-relative and readable locally. That is the feature working, and it
    is what makes the diagnosis certain rather than plausible.

    HAZARD: a pinned id asserts "this path still holds the bytes it held then."
    Pinning a path whose content changed would manufacture a false cache hit,
    which is worse than a miss because it is silent. Verifying content would
    mean re-reading 431 GiB, so the check here is size, which catches a
    replaced or truncated file without the read. Set MSM_NO_LEAF_PINS=1 to
    stage without pins and recompute from scratch.

    The size check runs ON fir, in one batched stat, and that is the whole
    point of it. It used to `Path(path).stat()` locally, which for every input
    that matters -- all 108 absolute paths are on fir -- hit an `is_file()`
    that is False on this host and fell through to "no change detected".
    Demonstrated by tampering with a recorded size and watching the plan sail
    through unchanged. It was reporting a clean guard over an empty set.
    Set MSM_SKIP_PIN_VERIFY=1 to skip the round trip; it prints that it did.
    """
    if os.environ.get("MSM_NO_LEAF_PINS"):
        print("leaf pins DISABLED (MSM_NO_LEAF_PINS set) — upstream steps will miss")
        return
    if not LEAF_PINS.exists():
        print(f"no leaf pin file at {LEAF_PINS}; ids will be freshly minted")
        return

    pins = json.loads(LEAF_PINS.read_text())["given"]

    # Batched remote stat for every pinned absolute path carrying a size.
    remote_sizes = {}
    to_check = sorted(p for p in inputs.instance_meta
                      if str(p).startswith("/") and (pins.get(str(p)) or {}).get("size"))
    if not to_check:
        pass
    elif os.environ.get("MSM_SKIP_PIN_VERIFY"):
        print(f"pin size-verify SKIPPED for {len(to_check)} path(s) (MSM_SKIP_PIN_VERIFY set)")
    else:
        script = "\n".join(f'printf "%s\\t%s\\n" "{p}" "$(stat -c %s "{p}" 2>/dev/null || echo -1)"'
                           for p in to_check)
        try:
            out, _ = ssh_cmd(script, timeout=600)
            for line in out.splitlines():
                if "\t" in line:
                    pth, sz = line.rsplit("\t", 1)
                    remote_sizes[pth] = int(sz)
        except Exception as e:
            raise SystemExit(
                f"REFUSING to pin: could not verify input sizes on {HPC_HOST} ({e}).\n"
                "A pin applied without verification is an unchecked assertion that\n"
                "every input still holds the bytes it held when minted. Fix the\n"
                "connection, or accept the recompute with MSM_NO_LEAF_PINS=1, or\n"
                "bypass deliberately with MSM_SKIP_PIN_VERIFY=1.")

    applied, missing, resized = 0, [], []
    for path in list(inputs.instance_meta):
        rec = pins.get(str(path))
        if rec is None:
            missing.append(str(path))
            continue
        want = rec.get("size")
        if want is not None and want > 0 and str(path) in remote_sizes:
            got = remote_sizes[str(path)]
            if got < 0:
                resized.append(f"{path}: GONE on {HPC_HOST} (recorded {want})")
                continue
            if got != want:
                resized.append(f"{path}: {got} != recorded {want}")
                continue
        inputs.instance_meta[path]["instance_id"] = rec["instance_id"]
        applied += 1

    if resized:
        raise SystemExit(
            "REFUSING to pin: %d input(s) changed size or vanished.\n  %s\n"
            "A pinned id on changed content is a silent false cache hit. Either\n"
            "restore the inputs, regenerate the pins with `dump-pins`, or stage\n"
            "with MSM_NO_LEAF_PINS=1 and accept the recompute."
            % (len(resized), "\n  ".join(resized))
        )
    if remote_sizes:
        print(f"pin size-verify: {len(remote_sizes)} path(s) checked on {HPC_HOST}, all match")
    print(f"leaf pins: {applied} applied from {LEAF_PINS.name}"
          + (f", {len(missing)} unpinned (new inputs)" if missing else ""))
    for m in missing[:5]:
        print(f"  unpinned: {m}")
    return applied


def build_transforms():
    return [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "assembly"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
    ]


def build_targets(with_gtdbtk=False, with_dedup=True):
    t = TargetBuilder()

    # assembly
    t.Add("sequences::read_qc_stats")
    t.Add("sequences::megahit_assembly")
    t.Add("sequences::orfs")
    t.Add("sequences::gff")
    t.Add("sequences::assembly_stats")
    t.Add("sequences::assembly_per_contig_coverage")
    t.Add("sequences::assembly_per_bp_coverage")
    t.Add("alignment::bam")

    # functional annotation (on the predicted ORFs)
    t.Add("annotation::diamond_uniref50_results")
    t.Add("annotation::kofamscan_results")
    t.Add("annotation::eggnog_results")
    t.Add("annotation::proteinbert_embeddings")

    # taxonomy — READS ONLY. The two read classifiers are deliberate: they emit
    # the same 6-column kraken-style report, so centrifuger (GTDB r232+hvfpc) is
    # a like-for-like second opinion on kraken2 (NCBI) rather than a different
    # measurement to reconcile.
    #
    # Contig taxonomy (metabuli) is NOT here: its r232 database is ~744 GB and
    # runs as an Arbutus service against the assemblies this DAG produces. That
    # omission is what collapses this run to one submission — see the docstring.
    t.Add("taxonomy::kraken2_report")
    t.Add("taxonomy::bracken_species")
    t.Add("taxonomy::centrifuger_kreport")
    t.Add("taxonomy::centrifuger_summary")

    # bins — one branch per binner. The distinct TargetSpec parents are what
    # force a separate checkm2/gtdbtk instance per binner; without them the
    # planner picks ONE binner's bins to satisfy each and the other two go
    # unqualified.
    t.Add("binning::metabat2_contig_to_bin_table")
    t.Add("binning::semibin2_contig_to_bin_table")
    t.Add("binning::comebin_contig_to_bin_table")
    mb = t.Add("sequences::metabat2_bin_fasta")
    sb = t.Add("sequences::semibin2_bin_fasta")
    cb = t.Add("sequences::comebin_bin_fasta")
    for parent in (mb, sb, cb):
        t.Add("taxonomy::checkm_stats", parents=[parent])
        if with_gtdbtk:
            t.Add("taxonomy::gtdbtk", parents=[parent])
    if with_dedup:
        # aggregator (quality bins across binners) → cross-sample skani dedup
        t.Add("binning_local::cluster_table")
    return t


def make_slurm_config(comebin_device="cpu", comebin_threads=64,
                      comebin_mem="48 GB", comebin_time="8h"):
    """Upstream slurm.nf + a per-transform override for comebin.

    comebin ships a CUDA-built container. On CPU its torch falls back cleanly
    under `apptainer --nv` (a benign "no nv files" warning) and honours
    `-t {cpus}` on every stage, so `cpus` below sets both the SLURM
    cpus-per-task and comebin's own thread count. This block is appended AFTER
    resources.nf loads, so it wins over the 8-cpu/32 GB/4 h defaults.

    Default is CPU: on fir the low-priority GPU queue stalled comebin 24 h+
    while CPU nodes scheduled in seconds, and a 64-thread CPU run finished a
    73k-contig assembly in ~12 min at 5.2 GB peak RSS.
    """
    smith = get_agent()
    base = Path(smith.GetNxfConfigPresets()["slurm"]).read_text()
    if comebin_device == "gpu":
        body = [
            f'        clusterOptions = "--nodes=1 --ntasks=1 --account={GPU_ACCOUNT} --gpus=1"',
            "        beforeScript = 'export APPTAINERENV_CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES'",
        ]
    else:
        body = [
            f"        cpus = {comebin_threads}",
            f"        memory = '{comebin_mem}'",
            f"        time = '{comebin_time}'",
            f'        clusterOptions = "--nodes=1 --ntasks=1 --account={SLURM_ACCOUNT}"',
        ]
    # Deliberately NO megahit block here. megahit's memory does have to escalate
    # on retry -- S27 (1.33e9 reads) and S25 (1.47e9) were both killed with exit
    # -9 extracting solid 21-mers at ReqMem=64G -- but it already does, and not
    # from this file. The base slurm preset's process block is
    #     memory = { task.attempt==1 ? params.process.memory : 2*params.process.memory }
    # and `resource_overrides` (see cmd_run) emits its own
    # `withName: '.*__megahit'` with `2**(task.attempt-1) * 64.GB`, giving
    # 64 -> 128 -> 256 -> 512 GB across the 4 tries. That override block is
    # appended AFTER this config loads and uses the same selector, so a `memory`
    # written here would be silently shadowed while the neighbouring `time` and
    # `clusterOptions` still applied -- a half-effective edit, the worst kind.
    # Keep the ladder in one place: resource_overrides.
    # Scheduler concurrency, set on the EFFECTIVE directives rather than on
    # `params`. `RunWorkflow(params=...)` writes workflow.params.yml and passes
    # it as `-params-file`, but this config arrives as `-config`, and for these
    # two settings the config wins -- so the driver's params have never taken
    # effect. Measured on QkqCNJOo: params.yml asked for queueSize 500 / array
    # 25 / tries 4, and the run came up with `capacity: 100` and array leaders
    # spaced exactly 100 apart, both matching the preset's defaults instead.
    #
    # The reason only *some* settings are affected is evaluation order. The
    # preset does `executor { queueSize = params.executor.queueSize }` and
    # `array = params.process.array` -- eager assignments, resolved as the file
    # is parsed, so a later params override cannot reach back and change them.
    # By contrast `errorStrategy` and `memory` are closures that read `params`
    # when the task runs, which is why `tries` CAN be set through params and is
    # left there. Set the eager ones directly; a params-only fix is silently
    # half-effective, the same trap as the megahit memory block above.
    #
    # Why the values matter: nextflow will not submit a job array unless the
    # WHOLE array fits in the monitor's free capacity at once. With capacity
    # 100 and array 100, a single running task makes every full array
    # permanently unsubmittable -- that is the 19-hour stall on QkqCNJOo, where
    # the four annotation processes built 767 tasks each and only the tail
    # array of 67 ever went out, and it recurred verbatim afterwards because
    # the fix had gone into params. 500/25 leaves twenty arrays in flight and
    # keeps a straggler from blocking the queue.
    #
    # Verify from the run, not from intent: read the "Creating task monitor
    # ... capacity:" line in nxf.log, and check the array-leader index spacing
    # in the submission-queue dump. Those two are the ground truth.
    text = base + "\n" + "\n".join(
        ["", "process {",
         "    withName: '.*__comebin' {", *body, "    }",
         "}", "",
         "executor { queueSize = 500 }",
         "process { array = 25 }",
         ""]
    )
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out = CACHE_DIR / f"fir_slurm_r1_{comebin_device}comebin.config"
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


# ── commands ─────────────────────────────────────────────────────────────────
def cmd_list_samples(args):
    if args.from_cluster:
        samples = enumerate_samples(from_cluster=True)
        print(f"{len(samples)} sample pairs on {HPC_HOST}:{HPC_READS_DIR}")
        for sid, r1, _ in samples:
            print(f"  {sid:6s}  {r1.name}")
        return
    rows = read_samples_tsv()
    total = sum(b1 + b2 for *_, b1, b2 in rows)
    print(f"{len(rows)} samples in {SAMPLES_TSV.name}  ({total / 1024**3:.1f} GiB raw)")
    print(f"{'sample':8s} {'R1+R2':>10s}   note")
    for sid, _, _, b1, b2 in rows:
        gib = (b1 + b2) / 1024**3
        note = ""
        if sid == "NTC":
            note = "negative control — exclude with `--exclude NTC` unless you want it profiled"
        elif gib < 0.05:
            note = "under 50 MB"
        print(f"{sid:8s} {gib:9.2f}G   {note}")


def cmd_check_dbs(args):
    checks = {"reads dir": HPC_READS_DIR,
              "agent home": HPC_MSM_HOME,
              "container store": HPC_MSM_HOME / "container_images",
              **DB_PATHS}
    probe = "; ".join(
        f'test -e "{p}{DB_PROBE_SUFFIX.get(t, "")}" '
        f'&& echo "OK   {t} -> {p}" || echo "MISS {t} -> {p}"'
        for t, p in checks.items()
    )
    out, _ = ssh_cmd(probe)
    print(out)
    missing = [ln for ln in out.splitlines() if ln.startswith("MISS")]
    if missing:
        print(f"\n{len(missing)} path(s) missing — the run will fail at the step that needs them.",
              file=sys.stderr)
        return 1
    return 0


def cmd_stage_reads(args):
    """Globus chinook → fir, flattening the per-sample `*_L7_ds.<hash>/` dirs.

    Only the L007 (full-lane) fastqs move; the tiny `_L001_` files in the sibling
    `*_ds.<hash>/` dirs are the sequencer's QC subsample, not data.
    """
    rows = read_samples_tsv()
    if args.sample:
        rows = [r for r in rows if r[0] in set(args.sample)]
    lines = []
    total = 0
    for sid, src_r1, src_r2, b1, b2 in rows:
        dst_r1, dst_r2 = remote_reads(sid)
        lines.append(f"{GLOBUS_SRC_ROOT}/{src_r1} {dst_r1}")
        lines.append(f"{GLOBUS_SRC_ROOT}/{src_r2} {dst_r2}")
        total += b1 + b2
    batch = "\n".join(lines) + "\n"
    print(f"{len(rows)} samples / {len(lines)} files / {total / 1024**3:.1f} GiB")
    print(f"  src {GLOBUS_SRC_EP}:{GLOBUS_SRC_ROOT}")
    print(f"  dst {GLOBUS_DST_EP}:{HPC_READS_DIR}")
    batch_file = CACHE_DIR / "globus_batch.txt"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    batch_file.write_text(batch)
    print(f"  batch written: {batch_file}")
    if not args.yes:
        print("\n(no --yes; nothing submitted)")
        return 0
    cmd = ["globus", "transfer", "--batch", str(batch_file),
           "--label", "gmcf3495-r1-reads", "--sync-level", "checksum",
           "--verify-checksum", GLOBUS_SRC_EP, GLOBUS_DST_EP]
    print("+ " + " ".join(cmd))
    return subprocess.run(cmd).returncode


def cmd_setup(args):
    """W0 — deploy the agent and prefetch every container into the store.

    Compute nodes have no outbound network, so a container that is not in
    <agent_home>/container_images at submit time is a job that dies on pull.
    """
    smith = get_agent()
    # resources/env, not resources/containers: the env migration replaced the OCI
    # instance library with one .env file per tool, each declaring `container:`
    # (used by APPTAINER, which is what we run) and/or `conda:`.
    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    logistics = TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics")

    wl = {Path(f"{n}.env") for n in R1_CONTAINERS}
    # AsSamples matches subtypes, so env::env catches every env::<tool>.env.
    samples = [s for s in containers.AsSamples("env::env")
               if s._mask.intersection(wl)]
    missing = wl - {p for s in samples for p in s._mask}
    if missing:
        print(f"ERROR: envs not in {MLIB}/resources/env: {sorted(missing)}",
              file=sys.stderr)
        sys.exit(1)
    print(f"containers to pull: {len(samples)}")

    targets = TargetBuilder()
    targets.Add("env::pulled_container")
    task = smith.GenerateWorkflow(samples=samples, resources=[],
                                  transforms=[logistics], targets=targets)
    if not task.ok or not task.plan.steps:
        _report_plan_failure(task)
    print(f"pull plan OK — {len(task.plan.steps)} steps, key={task.GetKey()}")

    if not args.run:
        print("(render-only; pass --run to deploy + pull)")
        return 0

    smith.Deploy(assertive=True)
    ssh_cmd(f"mkdir -p {HPC_READS_DIR}")
    smith.StageWorkflow(task, on_exist="update")
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["local"],
        params=dict(executor=dict(queueSize=4)),
        resource_overrides={"all": Resources(memory=Size.GB(2), cpus=2)},
    )
    print("setup done — now `run --dry-run`, then `run`")
    return 0


def cmd_run(args):
    if args.with_gtdbtk and "ref::gtdb" not in DB_PATHS:
        # Without a pre-staged entry the planner happily satisfies ref::gtdb with
        # downloadGtdbDB — a ~110 GB wget pinned to the login node. Refuse rather
        # than let that into the plan by accident.
        print("ERROR: --with-gtdbtk needs a staged GTDB-Tk release tree, and this run\n"
              "  deliberately has none — bin taxonomy runs off-cluster against the\n"
              "  Arbutus GTDB-Tk r232 service (arbutus-infra/dev/scripts/gtdbtk-submit.sh),\n"
              "  driven over the aggregator's quality bins after this DAG finishes.\n"
              "  To run it on-cluster anyway: stage a release tree (gtdbtk_r232_data.tar.gz\n"
              "  is on the chinook Globus collection at /Resources/GTDB/) and add to DB_PATHS:\n"
              '      "ref::gtdb": DB_ROOT / "gtdb" / "release232",',
              file=sys.stderr)
        sys.exit(1)

    samples = select(enumerate_samples(from_cluster=args.from_cluster), args)
    print(f"Selected {len(samples)} sample(s): {[s[0] for s in samples]}")

    inputs = build_inputs(samples)
    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    targets = build_targets(with_gtdbtk=args.with_gtdbtk,
                            with_dedup=not args.no_dedup)

    if args.dry_run:
        smith = Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home"),
                      runtime=Runtime.APPTAINER)
    else:
        smith = get_agent()

    print("Planning workflow...")
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[containers, inputs],
        transforms=build_transforms(),
        targets=targets,
    )
    if not task.ok:
        _report_plan_failure(task)

    steps = task.plan.steps
    print(f"Plan OK — {len(steps)} steps across {len(samples)} samples, key={task.GetKey()}")
    for s in steps:
        name = Path(s.transform._path).stem
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {name:28s} -> {prods}")

    # NOT "r1_dag": that is the approved reference DAG, tracked in git, and the
    # thing every rendered plan is compared against. Rendering over it destroys
    # the comparison -- which happened once, on the first real staging, because
    # only the dry-run path had been given a scratch cache dir.
    dag_base = args.dag_out or (CACHE_DIR / "r1_dag_current")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        # `env` joins the blacklist for the same reason as `lib`/`containers`:
        # all 21 are leaf givens feeding one step each, so they add a node per
        # tool and say nothing about data flow -- which is what the DAG is read
        # for. They are still planned and staged; only the render drops them.
        task.plan.RenderDAG(str(dag_base), format="svg",
                            blacklist_namespaces={"lib", "containers", "env"})
        print(f"DAG rendered: {dag_base}.svg")
    except Exception as e:
        print(f"DAG render skipped ({type(e).__name__}: {e}); .dot still at {dag_base}")

    if args.dry_run:
        print("\n(dry-run; nothing staged or submitted)")
        return 0

    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[args.tag or f"r1_metag_{len(samples)}samples"] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))

    print(f"Staging workflow to {HPC_HOST}...")
    smith.StageWorkflow(task, on_exist=args.on_exist, verify_external_paths=False)

    if args.stage_only:
        # The cache keys only exist once the workflow is staged -- they are
        # written into <run_dir>/workflow.step_N.meta, not into the plan. So a
        # resubmission that intends to reuse banked work has no way to check it
        # will, unless staging can stop here. Compare the staged keys against
        # the cache, THEN run again without this flag: the plan is deterministic
        # so the same run dir is reused, and on_exist=update resends the data
        # libraries alongside it, which is what keeps task.yml agreeing with
        # what is actually on disk (see the --on-exist comment).
        print(f"\n(stage-only; staged as {task.GetKey()}, nothing submitted)")
        return 0

    config = make_slurm_config(comebin_device=args.comebin_device,
                               comebin_time=args.comebin_time)
    print(f"Submitting to SLURM (config: {config})...")
    # bbduk and megahit both auto-detect NODE ram, which blows past the cgroup —
    # the transforms pin -Xmx/--memory to the allocation, so these floors plus
    # nextflow's 2^(attempt-1) doubling give 64 → 128 → 256 → 512 GB over 4 tries.
    smith.RunWorkflow(
        task=task,
        config_file=config,
        # `array` and `queueSize` are NOT set here -- they are written onto the
        # effective directives in make_slurm_config, because this params-file is
        # shadowed by the -config file for both of them. Setting them in this
        # dict looks like it works and does nothing; that cost QkqCNJOo a second
        # 19-hour stall after the first one was "fixed". See the long note in
        # make_slurm_config for the evaluation-order reason.
        #
        # `tries` stays here and does work: errorStrategy is a closure that
        # reads params.process.tries when the task runs, so a params value is
        # still live at that point.
        params=dict(
            slurmAccount=SLURM_ACCOUNT,
            process=dict(tries=4),
        ),
        # THREAD COUNTS ARE SIZED FOR A SMALLER MACHINE THAN FIR. The libraries
        # declare 8 cpus for the heaviest steps, which is what they should
        # declare -- they have to run anywhere. fir's cpularge_ nodes are 192
        # cores and 6 TB, so an 8-thread task leaves the node at 12.5% CPU while
        # its walltime burns. That is not a theoretical waste: it is what made
        # centrifuger miss its wall. Measured on the 07-27 retry, the three
        # largest libraries were consuming their compressed input at 1.3-1.8
        # MB/s with `-t 8` saturated (530-680% of 800%), projecting to 4.8h,
        # 6.6h and 7.5h against a 6h wall -- while node fb21808 sat at
        # CPUAlloc=24 of 192. So raise the threads rather than only the clock.
        #
        # `cpus` reaches the tool, not just the scheduler: nextflow's runtime
        # `task.cpus` is echoed into .command.metadata and parsed back into
        # context.params, which centrifuger.py turns into `-t`. Verified
        # end-to-end against the live task, which showed `-t 8`.
        #
        # All of this goes in resource_overrides rather than in the transforms
        # because an override is never folded into the cache key -- it merges
        # per-directive at RunWorkflow, emitting only the fields set here. The
        # same edit inside a transform would re-key every step in its group and
        # discard the banked work. Note `cpus` is flat while memory and time
        # double per attempt.
        resource_overrides={
            "bbduk":   Resources(memory=Size.GB(64), cpus=16),
            "megahit": Resources(memory=Size.GB(64), cpus=32),
            # 8h base (-> 16h, 32h on retry) with 4x the threads. The previous
            # 6h was sized against the 8-thread rate and was still short of the
            # 7.5h the worst library actually projected to.
            "centrifuger": Resources(duration=Duration(hours=8), cpus=32),
        },
    )
    print(f"Submitted: {task.GetKey()}")
    return 0


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
            print(f"  Error: {e}")
    return 0


def main():
    p = argparse.ArgumentParser(description="GMCF_3495 r1: reads → assembly + bins + annotation + taxonomy")
    sub = p.add_subparsers(dest="command", required=True)

    p_ls = sub.add_parser("list-samples")
    p_ls.add_argument("--from-cluster", action="store_true",
                      help="enumerate what is on fir instead of samples.tsv")
    p_ls.set_defaults(func=cmd_list_samples)

    p_db = sub.add_parser("check-dbs", help="ssh-verify every declared input path")
    p_db.set_defaults(func=cmd_check_dbs)

    p_sr = sub.add_parser("stage-reads", help="Globus chinook → fir (flattened)")
    p_sr.add_argument("--sample", action="append", default=None)
    p_sr.add_argument("--yes", action="store_true", help="actually submit the transfer")
    p_sr.set_defaults(func=cmd_stage_reads)

    p_su = sub.add_parser("setup", help="W0: deploy agent + prefetch containers")
    p_su.add_argument("--run", action="store_true")
    p_su.set_defaults(func=cmd_setup)

    p_run = sub.add_parser("run", help="plan, render the DAG, stage, submit")
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--from-cluster", action="store_true",
                       help="enumerate samples from fir instead of samples.tsv")
    p_run.add_argument("--sample", action="append", default=None)
    p_run.add_argument("--samples-file", default=None)
    p_run.add_argument("--exclude", action="append", default=None,
                       help="sample IDs to drop (e.g. --exclude NTC)")
    p_run.add_argument("--with-gtdbtk", action="store_true",
                       help="add per-bin GTDB-Tk targets. OFF by default — needs a GTDB-Tk "
                            "release tree staged and a `ref::gtdb` entry in DB_PATHS; bins "
                            "otherwise ship with CheckM2 quality but no taxonomy.")
    p_run.add_argument("--no-dedup", action="store_true",
                       help="drop the cross-sample skani cluster_table target")
    p_run.add_argument("--comebin-device", choices=["cpu", "gpu"], default="cpu")
    # CPU comebin scales with contig count -- measured on QkqCNJOo: 37k contigs
    # 0.67h, 52k 1.45h, 121k 3.1h, 152k 4.2h. The 8h default killed the two
    # largest assemblies (1.28M and 755k contigs) at 7h59m.
    p_run.add_argument("--comebin-time", default="8h")
    p_run.add_argument("--stage-only", action="store_true",
                       help="stage the workflow but do not submit; lets the "
                            "staged cache keys be checked against the cache first")
    p_run.add_argument("--dag-out", default=None, help="path stem for the rendered DAG")
    p_run.add_argument("--tag", default=None)
    # NOT update_workflow, which is broken for any run whose given-inputs
    # library carries parent references. That mode resends the transforms and
    # recompiles, but skips resending the data libraries -- while still
    # rewriting task.yml, which names them by key. The keys are content hashes,
    # and a library whose instances have parents embeds its OWN key in
    # _metadata/index.yml as "<key>@<file>". So the hash is taken over content
    # containing a previous value of itself: it has no fixed point and comes out
    # different on every staging. Manifest and payload then disagree in exactly
    # one direction, and the next Load dies with "could not find data library
    # [<id>]". Verified by diffing three generations of this run's reads
    # library -- byte-identical except for the self-reference.
    #
    # `update` resends everything into the same directory, so the manifest and
    # the payload move together. It leaves the superseded key directory behind,
    # which is a little disk and not a correctness problem. `clear` would also
    # work but rm -rf's the whole run dir, nxf_work and results included.
    p_run.add_argument("--on-exist",
                       choices=["skip", "error", "clear", "update",
                                "update_workflow", "update_data"],
                       default="update")
    p_run.set_defaults(func=cmd_run)

    p_dp = sub.add_parser("dump-pins",
                          help="mint every given once and record ids+sizes for reproducible staging")
    p_dp.add_argument("--out", help=f"output file (default {LEAF_PINS.name})")
    p_dp.add_argument("--sample", action="append", default=None)
    p_dp.add_argument("--exclude", action="append", default=None,
                      help="sample IDs to drop (e.g. --exclude NTC)")
    p_dp.set_defaults(func=cmd_dump_pins)

    p_st = sub.add_parser("status")
    p_st.set_defaults(func=cmd_status)

    args = p.parse_args()
    raise SystemExit(args.func(args) or 0)


if __name__ == "__main__":
    main()
