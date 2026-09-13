#!/usr/bin/env python3
"""Assembly, binning and functional annotation over CAMI or Pratama 2026 samples on fir.

Two corpora, selected by --corpus, share this one driver.

CAMI reads arrive interleaved in one anonymous_reads.fq.gz per sample, which the library
takes directly: short_reads_pe extends the short_reads that bbduk requires, so nothing
deinterleaves. The metadata parity must still read "paired" -- bbduk asserts on
{single, paired} and turns "paired" into its int=t flag.

Pratama arrives as two files (_1.fastq.gz/_2.fastq.gz) per run, so it goes through the
library's read_pair -> zipped_forward/reverse_short_reads -> interleave_zipped_short_reads
chain instead, and additionally carries a viral survey (research/viromics's own template)
pinned to MEGAHIT rather than metaSPAdes. That survey's cross-sample tools pool under one
shared viromics::contig_study root -- see build_inputs_pratama's docstring for what that
does to sample enumeration, because it is not what CAMI's per-sample shape would suggest.

Subcommands: list-samples, check-dbs, setup, run [--dry-run], status.
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

os.environ["PATH"] = f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}"

from metasmith.python_api import (  # noqa: E402
    Agent, Source, SshSource,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Runtime, DEFERRED,
    Resources, Size, Duration,
)

ROOT = Path(__file__).resolve().parent
MLIB = Path(os.environ.get(
    "MSM_LIB", str(Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries")))
CACHE_DIR = Path(os.environ.get("MSM_CACHE_DIR", ROOT / ".cache"))

HPC_HOST      = os.environ.get("MSM_HPC_HOST", "fir")
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "rrg-shallam-ab")
GPU_ACCOUNT   = os.environ.get("MSM_GPU_ACCOUNT", "def-shallam_gpu")
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

CAMI_ROOT    = Path(os.environ.get("CAMI_ROOT", "/scratch/phyberos/cami"))
HPC_MSM_HOME = Path(os.environ.get("MSM_AGENT_HOME", str(CAMI_ROOT / "metasmith")))
# The tracked manifest (229 rows across six datasets), not a directory glob: the
# subtrees do not share a shape -- marine nests under
# .../simulation_short_read/*/reads/, strain under .../short_read/*/reads/,
# gastrooral directly under .../*/reads/ -- so no single glob reaches all of
# them. CAMI_READS_GLOB is kept only as an override, for if the manifest ever
# goes stale relative to what is actually unpacked on the cluster.
CAMI_SAMPLES_TSV = Path(os.environ.get("CAMI_SAMPLES_TSV", str(ROOT / "samples.tsv")))
READS_GLOB = os.environ.get("CAMI_READS_GLOB")

PRATAMA_ROOT = Path(os.environ.get("PRATAMA_ROOT", "/scratch/phyberos/pratama2026"))
# The tracked metadata table, not a directory listing -- library_layout and the run/sample
# split live only here, and the fetch that populates PRATAMA_ROOT is a separate, ongoing job.
PRATAMA_RUNS_TSV = Path(os.environ.get(
    "PRATAMA_RUNS_TSV", str(ROOT.parent / "pratama2026" / "runs.tsv")))

DB_ROOT = Path("/home/phyberos/project-rpp/lib")
DB_PATHS = {
    "ref::uniref50_diamond_db": DB_ROOT / "diamond" / "uniref50.dmnd",
    # The unpacked directory, not the tarball beside it. kofamscan asserts on the
    # staged path being a directory and fails every chunk instantly with a message
    # naming both producers, so 135 chunks x 4 retries cost nothing but read as a
    # tool failure rather than a wiring mistake.
    "ref::kofamscan_profiles":  DB_ROOT / "kofamscan" / "profiles",
    "ref::kofamscan_ko_list":   DB_ROOT / "kofamscan" / "ko_list.tsv",
}

# References already staged UNPACKED on the cluster, given as inputs so the plan does
# not carry a step to fetch what we have. Each entry deletes a download step.
#
# Only geNomad qualifies today, and the shortfall is worth writing down because the
# directory listing looks like a bigger win than it is. Checked 2026-09-11: gtdb,
# virsorter2 and dram exist under project-rpp/lib as TARBALLS only
# (gtdb_genomes_reps_r232.tar.gz, virsorter2_data.tar.gz, dram_data.tar.gz). Their
# consumers want an unpacked database directory, and nothing here unpacks a given
# tarball, so pointing a `ref::` at one fails the way kofamscan fails when handed
# profiles.tgz instead of profiles/ -- instantly, on every chunk, reading like a tool
# failure rather than a wiring mistake. Re-downloading stays the status quo for those
# until an unpack transform exists or someone unpacks them by hand.
#
# iphop, vibrant, vcontact3 and checkv are not on the cluster in any form.
STAGED_REFS = {
    "ref::genomad": Path("/scratch/phyberos/databases/genomad"),
    # GTDB r232's full data package, 15 GB, unpacked. Staged rather than downloaded for
    # three independent reasons, any one of which would be enough:
    #   1. downloadGtdbDB's FULL_PKG url is DEAD. GTDB renamed the archive in release232
    #      from gtdbtk_data.tar.gz to gtdbtk_r232_data.tar.gz; the old name returns 404 and
    #      the new one 200, both checked directly. REPS_PKG is unaffected (200).
    #   2. That transform also pulls the ~179 GB representative-genome tarball, and the
    #      only consumer here, taxonomy/gtdbtk.py, runs `classify_wf --skip_ani_screen`,
    #      which touches neither the skani sketches nor the reps. The staged tree has
    #      neither and does not need them.
    #   3. It is a `local`-labelled step, so it runs on a login node, where the per-user
    #      cgroup and the local executor's capacity are the binding constraints.
    # CAUTION the path is the release directory ITSELF, not a parent containing it,
    # because the consumer exports GTDBTK_DATA_PATH=/ref and gtdbtk expects markers/,
    # masks/, msa/, pplacer/ and taxonomy/ directly beneath it. downloadGtdbDB's own
    # product is nested one level deeper (it asserts <out>/release232/skani/database),
    # so the produced and staged shapes DISAGREE -- a latent defect in that transform,
    # which has never run against a consumer on any host. Satisfy the consumer.
    "ref::gtdb": Path("/scratch/phyberos/staging/gtdb/release232"),
}

# Staged references OUTSIDE the `ref::` namespace, and PRATAMA-ONLY. Kept separate from
# STAGED_REFS for two reasons, both measured 2026-09-12:
#
#  1. Every registration site loads its OWN type-library list, and only the two Pratama
#     builders load annotation.yml. Putting an `annotation::` entry in STAGED_REFS makes
#     build_globals and build_globals_long_read fail to plan at all -- the traceback reads
#     `AssertionError: namespace [annotation] not found`, which names the library rather
#     than the dict, so it does not point at the dict that caused it.
#  2. Pratama is the only arm with a DRAM consumer. Registering an unconsumed given in the
#     CAMI builders risks moving their launch baseline keys, and waves 3 and 4 depend on
#     those keys not moving for the life of the run.
STAGED_REFS_PRATAMA = {
    # DRAM 1.5.0's prepared database tree, so `downloadDramDB` leaves the plan.
    #
    # That step carries labels=["local"], so it runs wherever the DRIVER runs -- and the
    # driver moved to a compute node, where bulk egress does not work. Measured from
    # fc30220: a zenodo HEAD returns http 200 in 0.61 s, a github tarball moves at
    # 0.14 MB/s, and ftp.genome.jp STALLS at 19 MB and 0 B/s over two windows, against
    # ~550 MB/s from a login node. So small requests succeed and large transfers HANG:
    # the step neither errors nor progresses, and a driver whose only submitted step is
    # this one looks exactly like a driver that is working.
    #
    # Same remedy as ref::gtdb -- stage it, drop the step. The staged tree is built by
    # /scratch/phyberos/stage_dram.sh, which runs THIS image's own
    # `DRAM-setup.py prepare_databases` with the identical --select_db set, binding the
    # output at /db, so DRAM writes its config entries `/db`-prefixed natively. That is
    # the shape the consumer needs: `dramv` binds this directory at /db and passes
    # `--config_loc /db/DRAM.config`.
    #
    # CAUTION test the config's CONTENT, never its presence, and never the exit code.
    # DRAM's downloaders log a per-database failure and carry on, and prepare_databases
    # copies a valid all-null CONFIG as its FIRST action, so a DRAM.config whose entries
    # are null is the signature of a step that ran and achieved nothing. The five
    # *_form / *_database sheets are the half that `DRAM-v.py distill` cannot run without,
    # and their absence surfaces one step after its cause.
    "annotation::dram_db": Path(os.environ.get(
        "DRAM_DB_ROOT", "/scratch/phyberos/refs/dram_1.5.0")),

    # vConTACT3's v230 database, so `downloadVcontact3DB` leaves the plan. Same
    # compute-node-egress reason as DRAM above: that step is labels=["local"] and a
    # ~3 GB fetch cannot complete where the driver now runs.
    #
    # There is a second reason specific to this database, and it is why the staged copy
    # is not simply the cache shard. The published release ships upstream mmseqs BUILD
    # SCRATCH -- one `*/*.mmseq_tmp/<run-id>/` per source database -- carrying 404
    # DANGLING symlinks, which was 5,888 of the product's 6,925 inodes. A directory
    # product holding a dangling symlink cannot survive the standard task contract:
    # nextflow's unstage copies with `nxf_fs_copy`'s `cp -fRL`, `-L` dereferences, the
    # copy fails, and retry-then-ignore swallows it -- so the step reported complete
    # with no product and vcontact3 plus everything past it dropped silently out of the
    # run. `logistics/downloadVcontact3DB.py` now prunes that scratch and asserts no
    # dangling symlink survives; /scratch/phyberos/stage_vcontact3.sbatch applied the
    # identical prune to the copy we already had, verified 0 dangling and >=1 version
    # manifest before promoting, and took the tree from 6,925 inodes / 5.1 GB to
    # 1,057 / 3.2 GB.
    #
    # CAUTION nothing reads the pruned scratch -- checked with a python walk over all 17
    # modules of vcontact3's own package, NOT with grep, because `grep -rl 'def '`
    # returns 0 of 17 files inside that image and so a zero from grep there is not
    # evidence. The tmp paths the tool builds at run time are `*.updated.mmseq_clu_tmp`
    # under its own out_dir and `tempfile.TemporaryDirectory` scratch.
    "ref::vcontact3_db": Path(os.environ.get(
        "VCONTACT3_DB_ROOT", "/scratch/phyberos/refs/vcontact3_v230")),
}

AGENT_IMAGE = os.environ.get(
    "MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.22.1")

# 768000 MB / 192 cores on every fir cpubase_* partition, measured with
# `sinfo -o "%P %c %m"` 2026-09-12 -- see make_slurm_config's comment for why this
# replaced a flat memory literal that drifted out of sync with the cpu count it was
# meant to track.
FIR_MEM_MB_PER_CPU = 4000

# Every container `setup --run` pre-pulls. CAUTION this list is NOT derived from the
# plans, so it drifts silently, and the way it fails is the worst available: a compute-node
# driver moves 0.14 MB/s externally, so a step whose image is absent from the cache HANGS
# instead of failing -- measured, it sat for eleven minutes looking like a driver that was
# working. It was missing das_tool (CAMI, step 16) and ten of Pratama's twenty-one.
#
# The authoritative per-arm list is a STAGED RUN's own `workflow.env.json`, which names
# every env and its container URI for the shape that was actually staged. Re-derive from it
# rather than from a grep over transform sources:
#   grep -oE '"container":"[^"]+"' <run>/workflow.env.json | sort -u
# and check each against the cache, whose naming is the URI with :// -> .. , / -> _ , : -> ..
# (`/scratch/phyberos/imgcheck.sh` does both for all three arms).
#
# Read only by cmd_setup, so it is key-neutral: it is a pull list, not a plan input.
CONTAINERS = [
    # shared across all three arms
    "seqkit", "bbtools", "megahit", "samtools", "minimap2", "bedtools",
    "pprodigal", "checkm", "polars", "skani",
    # binning ensemble + scoring (CAMI short read, CAMI long read, Pratama)
    "metabat2", "semibin", "comebin", "amber", "das_tool", "metawrap",
    # Pratama viromics lane
    "vibrant", "virsorter2", "vcontact3", "checkv", "genomad", "dram",
    "mmseqs2", "cctyper", "blast",
    # metaGEM reconstruction lane
    "carveme", "memote",
    # severed lanes, kept so re-enabling one needs no pull round -- see the taxonomy
    # and cellular-annotation decisions in the plan's run log
    "diamond", "kofamscan", "python_for_data_science",
]


# Substrings of a transport-level failure on the shared awm ssh ControlMaster --
# NOT the cluster being down. Four-plus driver launches (Pratama, CAMI short read,
# CAMI long read, the orchestrator's own checks) share one ControlMaster on this
# workstation, and its MaxSessions cap (openssh default 10) is what actually
# refuses a session under concurrency; fir itself was up and healthy throughout
# every occurrence measured so far. It clears on its own -- attempt 4 of 5 at a
# 6 s backoff cleared it once -- which is why ssh_cmd retries these rather than
# raising on the first hit. Deliberately narrow: an auth failure or a host held
# for operator approval is not this condition and must fail fast, never retry,
# because a retry path must ride the existing master and never re-establish a
# fresh direct connection -- that fires a new Duo/MFA attempt, and ten
# consecutive failures locked this account once (2026-07-02).
_SSH_TRANSPORT_RETRY_SUBSTRINGS = (
    "session open refused",
    "mux_client_request_session",
    "connection reset",
    "connection timed out",
    "broken pipe",
    "kex_exchange_identification",
)


def ssh_cmd(cmd, timeout=180, check=True, retries=5, backoff=6):
    last = None
    for attempt in range(1, retries + 1):
        r = subprocess.run(["ssh", HPC_HOST, cmd], capture_output=True, text=True,
                           timeout=timeout)
        transport_failure = (
            r.returncode == 255
            and any(p in r.stderr.lower() for p in _SSH_TRANSPORT_RETRY_SUBSTRINGS)
        )
        if not transport_failure:
            if check and r.returncode != 0:
                print(f"ssh stderr: {r.stderr}", file=sys.stderr)
                raise RuntimeError(
                    f"ssh command returned non-zero (exit {r.returncode}): {cmd}")
            return r.stdout.strip(), r.returncode
        last = r
        if attempt < retries:
            time.sleep(backoff)
    print(f"ssh stderr: {last.stderr}", file=sys.stderr)
    raise RuntimeError(
        f"ssh transport failed after {retries} attempts -- looks like a saturated "
        f"shared ControlMaster, not a failed command: {cmd}")


# One agent home for the whole campaign, both corpora, all four runs -- NOT one
# per corpus. The cache lives at <home>/task_cache, and the campaign's headline
# number is the reuse between batch 1 and batch 2, so a second home would put the
# two halves of that measurement in two trees that cannot see each other. Three
# more things are per-home and would each have to be done twice: the relay, the
# Deploy, and the dev overlay that binds the pinned engine over the published
# image (research/cami/ops/push_dev_overlay.sh pushes to exactly one home).
#
# The override exists because the home is the operator's call, not because two
# homes are a supported shape here. If you do split them, push the overlay to
# both or every task in the second one dies on the collapsed dispatch API.
HPC_MSM_HOME_PRATAMA = Path(os.environ.get(
    "MSM_AGENT_HOME_PRATAMA", str(HPC_MSM_HOME)))

# Same override pattern as HPC_MSM_HOME_PRATAMA, and the same default (share
# unless the operator opts out): added for the long-read arm, which is its own
# separate launch (`run --with-long-read`), not a variant folded into the
# existing CAMI corpus= "cami" invocation. Without this, `get_agent("cami_long")`
# would have had to fall back to the SAME two-way cami-vs-pratama dispatch below,
# and a corpus string that is neither literal "cami" nor "pratama" would have
# silently landed on HPC_MSM_HOME_PRATAMA -- colliding the long-read launch with
# a concurrently-running Pratama launch rather than with CAMI short read, which
# is a worse collision than the one this env var exists to avoid. See the wave
# report for why the default (full sharing) was judged sufficient rather than
# forcing a split.
HPC_MSM_HOME_CAMI_LONG = Path(os.environ.get(
    "MSM_AGENT_HOME_CAMI_LONG", str(HPC_MSM_HOME)))

_AGENT_HOMES = {
    "cami": HPC_MSM_HOME,
    "cami_long": HPC_MSM_HOME_CAMI_LONG,
    "pratama": HPC_MSM_HOME_PRATAMA,
}


def get_agent(corpus="cami"):
    home = _AGENT_HOMES.get(corpus, HPC_MSM_HOME_PRATAMA)
    return Agent(
        home=SshSource(host=HPC_HOST, path=home).AsSource(),
        container=AGENT_IMAGE,
        runtime=Runtime.APPTAINER,
        setup_commands=SETUP_COMMANDS,
    )


# Every short-read dataset the core/variant arms run against: CAMI II's six plus
# CAMI III's `toy_humangut`. 249 samples.
#
# CAMI 2+3 IN ONE RUN is the principal's directive (2026-09-12). `toy_humangut`'s
# short-read half was previously excluded here, and that exclusion was mine rather
# than a requirement -- it was introduced because build_samples.py discovers those
# rows only so the long-read arm can pair them with `toy_humangut_long` under one
# read_metadata, and I did not want the corpus to grow as a side effect of that
# pairing landing. Keeping it out meant 20 samples that are on disk and scoreable
# sat in neither arm, and the comparison covered CAMI II only.
#
# CAUTION this allowlist is DUPLICATED, by design, in two nf-core scripts:
# `nfcore/build_samplesheet.py` and `nfcore/stage_split_reads.py`. All three must
# change together. If they diverge, the reference arm and the parity arm cover
# DIFFERENT corpora while both reporting a sample count that looks right -- and no
# check in this campaign sees it, because the join between a generated table and
# the scripts that read it is not a property of any plan. The check that does see
# it is diffing the two arms' emitted sample-id lists against each other.
_CORE_SHORT_READ_DATASETS = {
    "marine", "strain", "toy_mousegut", "toy_hmp_airskinurogenital",
    "toy_hmp_gastrooral", "plant_associated",
    "toy_humangut",
}


def enumerate_samples():
    """(sample_id, remote reads path) for every CAMI core/variant short-read sample.

    Read from the tracked manifest research/cami/samples.tsv (229 rows across the
    six datasets in `_CORE_SHORT_READ_DATASETS`) rather than a directory glob -- a
    glob matching only marine's own nesting silently limited every downstream
    driver to 10 of the 229 samples, with no error, because the other five
    subtrees nest reads differently and a glob that finds nothing is not a glob
    that fails. `sample_id` is unique only WITHIN a dataset ("sample_0" recurs in
    several), so the id returned here is namespaced with the dataset -- without
    that, two different datasets' samples collide on one `_stable_id`.

    Every row here has has_truth=1 (spot-checked on fir across the strain and
    gastrooral subtrees too), so cami_contig_truth's bridge reaches every
    sample. Only 110 of 229 have has_binning_gs=1 -- 119 samples have no
    gold-standard bin file at all, which is why AMBER scores through the
    read-truth bridge rather than through binning_gs.tsv directly.

    samples.tsv now also carries long-read and CAMI III rows (`read_type` column);
    this function deliberately excludes all of them -- see
    enumerate_long_read_samples and enumerate_paired_toy_humangut_samples for
    those.

    Set CAMI_READS_GLOB to fall back to the old single-glob enumeration instead,
    for the (documented, not expected) case where the manifest has gone stale
    relative to what is actually unpacked on the cluster.
    """
    if READS_GLOB:
        out, _ = ssh_cmd(f"ls {READS_GLOB} 2>/dev/null || true")
        samples = []
        for line in sorted(p.strip() for p in out.splitlines() if p.strip()):
            # .../<timestamp>_sample_N/reads/anonymous_reads.fq.gz -> sample_N
            stem = Path(line).parent.parent.name
            m = re.search(r"(sample_\d+)$", stem)
            sid = m.group(1) if m else stem
            samples.append((sid, Path(line)))
        return samples

    rows = list(csv.DictReader(CAMI_SAMPLES_TSV.open(), delimiter="\t"))
    return [(f"{r['dataset']}_{r['sample_id']}", Path(r["reads_path"]))
            for r in rows if r["dataset"] in _CORE_SHORT_READ_DATASETS]


def enumerate_paired_toy_humangut_samples():
    """(sample_num, remote short-reads path, remote long-reads path, remote long
    truth path or None) for every toy_humangut sample_id present in BOTH the
    `toy_humangut` (short) and `toy_humangut_long` (long) rows of samples.tsv.

    toy_humangut is CAMI III's one community CAMISIM sequenced both ways, and
    pairing its two halves under a SHARED read_metadata is what the long-read
    arm's ancestry-separation test actually needs: a fully independent
    long-read-only sample can never exercise it, because metabat2, comebin,
    prodigal and metawrap's bare `sequences::assembly` interior requirement (and
    assembly_stats.py's bare `sequences::reads` one) only has a second candidate
    to confuse it with when both a short and a long assembly descend from the
    SAME sample. See build_targets_long_read's docstring for what the test is
    and build_inputs_long_read for how the shared parent is registered.

    `long truth path` is read_type long's own `has_truth`, never assumed --
    CAMI III's toy_humangut_long ships `reads_mapping.tsv.gz` (checked
    2026-09-12); the other four CAMI II long-read subtrees this table also now
    carries do too, per samples.tsv's own has_truth column, which contradicts
    this campaign's recorded assumption that they ship none. See this module's
    docstring and the wave report for that discrepancy -- it is left
    unresolved here on purpose: build_targets_long_read still does not add
    AMBER for the CAMI II long-read lane, because that is a scope decision for
    the orchestrator, not something to flip unilaterally off one honest column.
    """
    rows = list(csv.DictReader(CAMI_SAMPLES_TSV.open(), delimiter="\t"))
    short = {r["sample_id"]: r for r in rows if r["dataset"] == "toy_humangut"}
    long_ = {r["sample_id"]: r for r in rows if r["dataset"] == "toy_humangut_long"}
    out = []
    for sid in sorted(set(short) & set(long_),
                      key=lambda s: int(re.search(r"\d+", s).group())):
        s, lg = short[sid], long_[sid]
        long_reads = Path(lg["reads_path"])
        truth = long_reads.parent / "reads_mapping.tsv.gz" if int(lg["has_truth"]) else None
        out.append((sid, Path(s["reads_path"]), long_reads, truth))
    return out


def enumerate_long_read_samples():
    """(sample_id, remote long-reads path, remote truth path or None, dataset) for
    every long-read row in samples.tsv (`read_type == "long"`), independent of
    whether a short-read pair exists.

    Present for completeness/reporting (`list-samples --corpus cami
    --long-read`) and for a possible future long-read-only arm; the long-read
    arm actually wired into `run` today only uses the toy_humangut pairing from
    enumerate_paired_toy_humangut_samples, for the reason given there.
    """
    rows = list(csv.DictReader(CAMI_SAMPLES_TSV.open(), delimiter="\t"))
    out = []
    for r in rows:
        if r.get("read_type") != "long":
            continue
        sid = f"{r['dataset']}_{r['sample_id']}"
        reads = Path(r["reads_path"])
        truth = reads.parent / "reads_mapping.tsv.gz" if int(r["has_truth"]) else None
        out.append((sid, reads, truth, r["dataset"]))
    return out


def select(samples, args):
    if getattr(args, "sample", None):
        wanted = set(args.sample)
        picked = [s for s in samples if s[0] in wanted]
        missing = wanted - {s[0] for s in picked}
        if missing:
            print(f"ERROR: sample(s) not found: {sorted(missing)}", file=sys.stderr)
            sys.exit(1)
        return picked
    if getattr(args, "limit", None):
        return samples[: args.limit]
    return samples


def _stable_id(corpus: str, *parts: str) -> str:
    """A leaf id that survives a re-plan, which AddItem's does not.

    `AddItem` mints a leaf id by stat-ing the file, and returns a fresh uuid4 when
    it cannot -- which is every input here, because the reads and the reference
    databases live on the cluster and this driver runs on a workstation. The plan
    key hashes the given instances' ids, so two identical submissions minutes apart
    plan to different keys, Nextflow sees a project it has never run, and `-resume`
    is discarded. Measured: `daqL9cFU` then `WwhBaN3k` from back-to-back dry runs.

    RegisterItem is the sanctioned way to supply the id instead. Metasmith still
    re-derives the honest identity at staging time on the host that owns the file,
    so pinning it here only fixes what the key is hashed over.

    `corpus` is the caller's namespace ("cami" or "pratama"), not a hardcoded
    literal -- two corpora hashing the same parts under the same prefix could
    collide on one id.
    """
    from metasmith.caching.keys import multihash_key
    return multihash_key("\x00".join((corpus,) + parts).encode("utf-8")).hex()


def build_inputs(samples):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(CACHE_DIR / "cami_inputs.xgdb")
    inputs.Purge()

    for tl in ["sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
               "taxonomy.yml", "binning.yml", "binning_local.yml", "env.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    for sid, reads in samples:
        # "paired", not "interleaved": bbduk asserts on {single, paired}.
        meta_value = json.dumps({"parity": "paired", "length_class": "short"})
        meta_name = f"{sid}_read_metadata.json"
        (inputs.location / meta_name).write_text(meta_value)
        meta = inputs.RegisterItem(
            meta_name, "sequences::read_metadata",
            # Over the content, so editing the metadata does retire the old plan.
            instance_id=_stable_id("cami", "read_metadata", sid, meta_value),
        )
        inputs.RegisterItem(
            reads, "sequences::short_reads_pe", parents={meta},
            instance_id=_stable_id("cami", "short_reads_pe", sid, str(reads)),
        )
        # CAMISIM's per-read truth, sitting beside the reads. NOT
        # binning_gs.tsv: that keys on the CAMI-provided gold-standard-assembly's
        # own contig ids, which our own megahit assembly does not share, so it
        # cannot score our bins directly. See cami_contig_truth.py.
        truth = reads.parent / "reads_mapping.tsv.gz"
        inputs.RegisterItem(
            truth, "binning::cami_read_truth", parents={meta},
            instance_id=_stable_id("cami", "cami_read_truth", sid, str(truth)),
        )

    for dtype, path in DB_PATHS.items():
        inputs.RegisterItem(path, dtype, instance_id=_stable_id("cami", "ref", dtype, str(path)))
    for dtype, path in STAGED_REFS.items():
        inputs.RegisterItem(path, dtype, instance_id=_stable_id("cami", "ref", dtype, str(path)))

    inputs.Save()
    return inputs


def build_globals():
    """Study-wide references ONLY (DB_PATHS, STAGED_REFS), for `resources=`
    INSTEAD OF the full per-sample `inputs` library -- see
    build_globals_long_read's docstring for the mechanism this guards
    against. Latent here today, same as build_inputs_pratama's: every
    core/variant sample carries the identical short_reads_pe + read_metadata
    + cami_read_truth shape, so `resources=[..., inputs]` has never yet
    unioned away a second shape the way it did for the metaGEM driver. Fixed
    anyway rather than left for the day this corpus stops being one shape.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    lib = DataInstanceLibrary(CACHE_DIR / "cami_globals.xgdb")
    lib.Purge()
    for tl in ["ref.yml", "env.yml"]:
        lib.AddTypeLibrary(MLIB / "data_types" / tl)
    for dtype, path in DB_PATHS.items():
        lib.RegisterItem(path, dtype, instance_id=_stable_id("cami", "ref", dtype, str(path)))
    for dtype, path in STAGED_REFS.items():
        lib.RegisterItem(path, dtype, instance_id=_stable_id("cami", "ref", dtype, str(path)))
    lib.Save()
    return lib


def build_targets(with_dedup=True, variant="core"):
    """The batch-1 core, or the batch-2 variant, from one definition.

    core    metaSPAdes under the JGI protocol, MetaWRAP for binning and refinement,
            CheckM2, and an AMBER score. No functional annotation: DIAMOND and
            KOfamScan were 85% of the task-hours and 96% of the tasks on the last
            ten-sample run and tell a binning benchmark nothing.
    variant the same spine with the assembler and binners swapped -- MEGAHIT, the
            three binners under the aggregator, skANI dereplication.

    NAME THE ASSEMBLER, NEVER `sequences::assembly`. Both assemblers satisfy the
    supertype, so an unpinned slot lets the planner answer targets from different
    assemblers and assemble every sample twice. Pinning the named targets is
    necessary and NOT sufficient: metabat2, comebin, prodigal and metawrap all
    require the bare supertype themselves, and lineage matching is ancestral, so
    "descends from this assembly" cannot be told from "descends from that one".
    `_assembly_without()` masks the other assembler out of the planner's view,
    which is what actually settles those interior slots.
    """
    assembler = "spades" if variant == "core" else "megahit"
    t = TargetBuilder()
    asm = t.Add(f"sequences::{assembler}_assembly")
    t.Add("sequences::read_qc_stats")
    for dtype in ("sequences::orfs",
                  "sequences::gff",
                  "sequences::assembly_stats",
                  "sequences::assembly_per_contig_coverage",
                  "alignment::bam"):
        t.Add(dtype, parents=[asm])

    if variant == "core":
        binners = ("metawrap",)
    else:
        # DAS Tool rides the same four-target pattern as a plain binner even
        # though it is a consolidator: its products are a bin fasta and a
        # contig2bin table like any other, and its requirement on the other
        # three tables is satisfied by targets this same list already names.
        # Adding it here is the whole change -- the planner works out that it
        # must run after the three and before checkm and amber.
        binners = ("metabat2", "semibin2", "comebin")

    bins = [t.Add(f"sequences::{b}_bin_fasta", parents=[asm]) for b in binners]
    tables = [t.Add(f"binning::{b}_contig_to_bin_table", parents=[asm]) for b in binners]

    # WHICH BIN SETS GET EVALUATED, and this is a scientific decision rather than a
    # plumbing one: when a consolidator runs, the MAGs are ITS output. The individual
    # binners' bin sets are intermediates -- they exist to be consolidated, and a
    # completeness/contamination or AMBER number computed on one of them is not a
    # statement about any genome this pipeline reports.
    #
    #   core variant    -> metawrap consolidates INTERNALLY, so its own bin set IS the
    #                      MAG set. It is evaluated.
    #   other variants  -> das_tool consolidates the three binners, so DAS Tool's bin
    #                      set is the MAG set and the three are intermediates.
    #
    # Nothing is lost by not evaluating them here. Their bin fastas and contig2bin
    # tables are still named as targets below -- das_tool REQUIRES the three tables, so
    # they are produced and collected either way -- which means a per-binner AMBER or
    # CheckM2 pass remains available post hoc at any time, from artifacts the run keeps.
    # `research/cami/score_reference_amber.py` already scores a two-column table
    # out-of-plan in a format identical to the in-plan scorer's.
    #
    # CAUTION do not "restore symmetry" by adding these back because nf-core/mag is
    # configured with `post_binning_input = 'both'`. That setting exists so the pinned
    # binners appear in downstream tables at all; the REPORTED comparison is DAS Tool's
    # refined set against nf-core's `DASTool` rows, and nf-core publishes its per-binner
    # contig_to_bin map unconditionally (mag.nf:467, storeDir) whether or not it QCs them.
    if variant == "core":
        # One amber target per binner, pinned to that binner's own table, exactly as
        # checkm_stats is pinned per bin set. Pinned to the assembly instead, the
        # planner satisfies the slot once and scores ONE binner -- a solve that
        # succeeds and silently answers a fraction of the question. amber emits both
        # its products in one step, so naming amber_bin_metrics too would buy nothing
        # and cost a slot.
        for b in bins:
            t.Add("taxonomy::checkm_stats", parents=[b])
        for tb in tables:
            t.Add("binning::amber_results", parents=[tb])
    if variant != "core":
        # DAS Tool consolidates the three binners above. It cannot be scored by
        # amber.py itself -- three measurements, in order:
        #   - an amber slot pinned to the DAS Tool table is dropped at solve
        #     time ("requested but not included in plan -- check if group_by
        #     dependency can be satisfied"); amber's group_by IS its table.
        #   - an UNPINNED fourth amber slot solves, but binds to MetaWRAP's
        #     table and drags a fifth binner into the variant run, leaving DAS
        #     Tool unscored -- a solve that succeeds and answers the wrong
        #     question.
        #   - masking metawrap.py out of the metagenomics library so DAS Tool is
        #     the only table left for that slot makes the solve fail outright,
        #     which is what proves amber cannot consume this table rather than
        #     merely preferring another. DAS Tool's own table pools the three
        #     binner tables, and amber requires its table to descend from the
        #     assembly; lineage is ancestral, so the pooled table re-qualifies
        #     its own producers.
        # amber_das_tool.py (metagenomics/binning/) is what actually scores it:
        # it requires the CONCRETE das_tool_contig_to_bin_table type rather than
        # the shared binning::contig_to_bin_table supertype amber.py fans out
        # over, so there is exactly one producer and no ambiguous slot to pin --
        # naming the product directly needs no parent, unlike the per-binner
        # amber targets above. Its products (das_tool_amber_results /
        # das_tool_amber_bin_metrics) are their own types, so this cannot
        # collide with or displace the three per-binner amber targets.
        # Naming the table keeps DAS Tool explicit rather than incidental (the
        # aggregator pulls it in either way) and costs no step.
        t.Add("binning::das_tool_contig_to_bin_table", parents=[asm])
        t.Add("binning::das_tool_amber_results")
        # The refined bin set DAS Tool actually selects, published and
        # CheckM2-scored like the three raw binners' sets above -- otherwise
        # it is built (das_tool.py already runs for the table above) and
        # thrown away, and the reference pipeline this campaign compares
        # against is pinned to score refined bins only. No collision with the
        # three raw-bin checkm_stats targets above: das_tool_bin_fasta shares
        # no ancestry with metabat2_bin_fasta/semibin2_bin_fasta/
        # comebin_bin_fasta (das_tool.py takes each binner's TABLE and the
        # assembly, not their bin FASTAs, and cuts its own bins straight out
        # of the assembly -- see das_tool.py's own comment), so this is a
        # fourth, distinct fan-out slot for checkm.py's existing
        # sequences::putative_genome supertype match, not a fifth ambiguous
        # producer of one already-claimed slot the way das_tool's pooled
        # TABLE is for amber.py.
        das_tool_bin = t.Add("sequences::das_tool_bin_fasta", parents=[asm])
        t.Add("taxonomy::checkm_stats", parents=[das_tool_bin])
    if with_dedup and variant != "core":
        t.Add("binning_local::cluster_table", parents=[asm])
    return t


def _assembly_without(*masked: str):
    """The assembly library with the other assembler hidden from the planner.

    A target pin fixes the targets. It does not reach an interior slot that no
    target names, and metabat2, comebin, prodigal and metawrap all require the
    bare `sequences::assembly`. Hiding the file is what settles those.
    """
    from pathlib import Path as _P
    lib = TransformInstanceLibrary.Load(MLIB / "transforms" / "assembly")
    return lib.AsView({_P(f"{m}.py") for m in masked}, invert=True)


def build_transforms_for(variant="core"):
    other = "megahit" if variant == "core" else "spades"
    return [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
        _assembly_without(other),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
    ]


# -- Long-read arm ------------------------------------------------------------
#
# One flye_raw_assembly branch alongside the existing short-read spine, both
# requested against toy_humangut's short+long pairing (enumerate_paired_toy_
# humangut_samples) so the two assemblies share a common read_metadata ancestor.
# That shared parentage is the whole point: it is what actually tests whether
# the planner needs `_assembly_without()` to keep flye_raw separate from the
# short-read assembler, the way spades needs it kept separate from megahit --
# see build_targets_long_read's docstring for the prediction and
# run_cami_metag.py's module docstring / the wave report for which way solving
# it went.

def enumerate_long_only_sample(dataset="marine_long", sample_id="sample_0"):
    """One long-read-only sample (no short-read pair), added purely so
    `list-samples --long-read` and this arm's corpus are not limited to
    toy_humangut alone -- see the call site in cmd_run's --with-long-read
    branch.

    This does NOT give the long-read arm's solve shape heterogeneity, and an
    earlier revision of this docstring claimed the opposite. It cannot:
    build_inputs_long_read registers every sample, paired or long-only, under
    the identical "long_reads + its own read_metadata [+ cami_read_truth]"
    shape -- it never registers a paired sample's short_reads_pe at all (see
    that function's own docstring for why). So every sample this arm plans
    against presents the SAME shape to the solver regardless of this
    function, "solving plan for [21] samples as [1] unique case" is the
    CORRECT log line, and mixing this sample in exercises no case-collapse
    check. `_assert_given_counts` (see cmd_run) is the check that actually
    catches a collapse, because it counts registered instances directly
    rather than reading the solver's own "unique case" bookkeeping, which is
    right either way here and so proves nothing about it.
    """
    rows = list(csv.DictReader(CAMI_SAMPLES_TSV.open(), delimiter="\t"))
    for r in rows:
        if r["dataset"] == dataset and r["sample_id"] == sample_id:
            reads = Path(r["reads_path"])
            truth = reads.parent / "reads_mapping.tsv.gz" if int(r["has_truth"]) else None
            return (f"{dataset}_{sample_id}", reads, truth)
    raise ValueError(f"no such long-read sample tracked in samples.tsv: {dataset}/{sample_id}")


def build_globals_long_read():
    """Study-wide references ONLY (DB_PATHS, STAGED_REFS) -- never per-sample
    reads or metadata -- in a library of its own, to be passed as a
    `resources=` entry INSTEAD OF the full per-sample `inputs` library.

    This split exists because of a real, confirmed bug: `Spec.SolveViews`
    builds each solver "case" as `[sample_view] + resource_views` (see
    src/metasmith/agents/spec.py), and `CollectSolverInputs`
    (src/metasmith/models/workflow/plan.py) unions each group's endpoint types
    to decide whether two groups are the "same case". A resource view of the
    WHOLE per-sample `inputs` library contributes the union of every type
    registered ANYWHERE in it -- every sample's read type included -- to EVERY
    group, identically. Once that union is a superset of whatever actually
    differs between two genuinely different sample shapes, every group's
    endpoint set comes out equal and the solver silently collapses them to one
    case, taking the cheapest path to the union and dropping the other shape's
    steps with `dropped_targets` staying EMPTY and the plan reporting OK. CAMI
    core/variant have never tripped this because every one of their samples
    shares one shape; the long-read arm is the first corpus in this driver
    with two, which is exactly why it needs this split and they do not (yet --
    see the wave report for the same latent risk in build_inputs/
    build_inputs_pratama, left unfixed there since it is not live today and
    both arms are pinned reference plans this wave).
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    lib = DataInstanceLibrary(CACHE_DIR / "cami_long_read_globals.xgdb")
    lib.Purge()
    for tl in ["ref.yml", "env.yml"]:
        lib.AddTypeLibrary(MLIB / "data_types" / tl)
    for dtype, path in DB_PATHS.items():
        lib.RegisterItem(path, dtype, instance_id=_stable_id("cami_long", "ref", dtype, str(path)))
    for dtype, path in STAGED_REFS.items():
        lib.RegisterItem(path, dtype, instance_id=_stable_id("cami_long", "ref", dtype, str(path)))
    lib.Save()
    return lib


def build_inputs_long_read(samples):
    """Register every given long-read sample -- normally the full corpus from
    enumerate_long_read_samples(), or any --sample/--limit-selected subset of
    it -- EACH under its own independent read_metadata. `samples` is a list of
    (sample_id, remote long-reads path, remote truth path or None, dataset)
    tuples, matching enumerate_long_read_samples()'s return shape exactly;
    `dataset` is accepted but unused here (registration does not care which
    CAMI II subtree or CAMI III a sample came from -- see below for why mixing
    them is safe). No `sequences::short_reads_pe` is registered anywhere in
    this library, and that is deliberate, not an oversight.

    Was scoped to just the CAMI III toy_humangut pairing (20 samples) plus one
    marine_long sample in an earlier revision, which left 151 of the
    campaign's 152 CAMI II long-read samples structurally unreachable by this
    arm -- a scope gap, not a defect, but one that defeated the point of
    reversing the "CAMI II long-read cannot be AMBER-scored" decision. That
    scoping existed to test one thing (does ancestry alone separate flye_raw
    from a short-read assembler, no mask, when both shared one meta?), which
    ran and is settled; nothing here still depends on the toy_humangut pairing
    specifically. See build_targets_long_read and the wave report for the
    verification that mixing CAMI II's read-type-only samples with CAMI III's
    (which happen to also have a short-read sibling elsewhere, never
    registered here) is still one shape at the full 172-sample corpus size --
    every sample gets its own independent meta with ONLY long_reads under it,
    so nothing about a sample's dataset of origin varies what gets registered.

    This used to also register a pair's short_reads_pe under the SAME meta as
    its long_reads, specifically to test whether the planner separates
    spades/megahit from flye_raw by ancestry alone without
    `_assembly_without()`. That test ran and the ASSEMBLY CHOICE resolved
    correctly in every configuration tried -- spades_assembly and
    flye_raw_assembly, named as distinct concrete types, never bound to the
    wrong one. But two OTHER transforms have a bare interior requirement
    parented only to `sequences::read_metadata`, not to a specific assembly's
    own reads, and solving with both lineages under one shared meta got BOTH
    of them wrong:

      - assembly_stats.py's bare `sequences::reads` requirement. Solved with a
        shared meta, one run bound `short_reads_pe` to the flye_raw_assembly
        branch (minimap2 aligning SHORT reads against a LONG-read assembly)
        and never bound `long_reads` to it at all in that run.
      - cami_contig_truth.py's bare `binning::cami_read_truth` requirement.
        With both a short and a long truth table registered under one meta,
        BOTH got bound to the single truth slot of one `cami_contig_truth`
        step simultaneously, for both lineages' steps.

    Both failure shapes are silent and exit-zero-shaped -- exactly what "check
    products, not exit codes" exists to catch -- not solver errors: `task.ok`
    was True, `dropped_targets` was empty, and the step list looked identical
    to a correct one. See the wave report for the full step-by-step evidence
    (`s.uses` on the actual WorkflowStep objects, not just step names/counts,
    which is what surfaced this).

    The fix is this: give every read type its own meta, with nothing shared,
    so no bare-parented-to-meta dependency can ever see another lineage's
    sibling at all. That closes the ambiguity by construction. The cost is
    that a short-read assembly and this arm's flye_raw_assembly can no longer
    be jointly requested in ONE solve -- confirmed separately: doing so, even
    with two independent metas and no ambiguous dependency at all, fails
    outright (a target reachable from one sample-view and a second target
    reachable only from an unrelated sample-view cannot both be satisfied in
    the same solve). This arm never needed that anyway -- it is a long-read
    arm, not a joint short+long one.

    No `binning::cami_read_truth` is registered for a sample unless one is
    genuinely on disk (has_truth read honestly per row -- see
    build_samples.py); `truth` may be None for a caller that has not checked.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(CACHE_DIR / "cami_long_read_inputs.xgdb")
    inputs.Purge()

    for tl in ["sequences.yml", "alignment.yml", "binning.yml", "binning_local.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    def _register(sid, long_reads, truth):
        meta_value = json.dumps({"parity": "single", "length_class": "long"})
        meta_name = f"{sid}_read_metadata.json"
        (inputs.location / meta_name).write_text(meta_value)
        meta = inputs.RegisterItem(
            meta_name, "sequences::read_metadata",
            instance_id=_stable_id("cami_long", "read_metadata", sid, meta_value),
        )
        inputs.RegisterItem(
            long_reads, "sequences::long_reads", parents={meta},
            instance_id=_stable_id("cami_long", "long_reads", sid, str(long_reads)),
        )
        if truth is not None:
            inputs.RegisterItem(
                truth, "binning::cami_read_truth", parents={meta},
                instance_id=_stable_id("cami_long", "cami_read_truth", sid, str(truth)),
            )

    for sid, long_reads, truth, _dataset in samples:
        _register(sid, long_reads, truth)

    inputs.Save()
    return inputs


def build_targets_long_read():
    """The raw-Flye long-read arm, on its own: flye_raw_assembly,
    assembly_stats (which also yields alignment::bam), gff and
    assembly_per_contig_coverage (matching build_targets' short-read
    treatment), a three-binner tail (metabat2/semibin2/comebin), checkm per
    raw bin set, AMBER scoring each of the three per-binner tables, DAS Tool
    consolidating them into a published, CheckM2-scored refined bin set, and
    AMBER scoring DAS Tool's own pooled table too. Verified at N=1, N=2 and
    the full N=21 corpus -- 17 steps, `dropped_targets` empty, `task.ok`
    True, every step correctly scoped to a single flye_raw_assembly lineage
    with no cross-contamination (checked by dumping `s.uses`, not by step
    count or position). That last part depends on
    `build_transforms_for_long_read` masking `hifi/hifiasm_meta.py` as well
    as `flye.py` -- see its own comment for a second, worse-shaped case of
    this same bug: left unmasked, hifiasm_meta.py silently pulled in a
    second flye_raw_assembly and, from there, a table amber_das_tool could
    bind DAS Tool's slot to instead of the assembly this arm actually wants
    scored.

    DO NOT ALSO NAME `sequences::orfs` AS AN EXPLICIT TARGET HERE (gff is
    fine, and is named above), even though this is no longer the hard
    failure an earlier revision of this docstring described. That revision
    measured (at N=1, before amber.py was narrowed to
    `raw_contig_to_bin_table`) that naming
    `orfs` dropped one target and re-routed a second per-binner AMBER slot
    onto DAS Tool's pooled table -- and, before that, an even earlier
    revision overstated it as "the ENTIRE plan fails outright, every target
    dead-ends", which was never true. Re-measured now, with amber.py's fix in
    place: naming `orfs` (parented to `asm_lr`) solves cleanly, `ok=True`,
    `dropped_targets` empty, and all four AMBER-family steps still bind their
    correct, distinct tables. It is still not free, though: it adds a second,
    unrelated `prodigal` step bound to `sequences::isolate_assembly` by way
    of `logistics/getNcbiAssembly.py` -- the `ncbi::genome_name` hint chain
    already flagged elsewhere in this module as reporting noise, not a
    failure cause. That branch is orphaned (nothing here consumes an
    isolate_assembly-derived anything), so naming `orfs` costs a wasted step
    for no benefit rather than breaking the plan. Leaving it untargeted does
    not lose the data either way -- das_tool.py requires `sequences::orfs`
    internally (parented to its own asm, prodigal's own output format), so
    DAS Tool alone still pulls prodigal into the plan, see step 3 in the
    verified step list -- but it is never PUBLISHED this way: nothing here
    names `sequences::orfs` as an output, so a caller wanting the gene calls
    as their own artifact will not get them back.

    Always the three-binner tail, never MetaWRAP: MetaWRAP requires
    `sequences::clean_short_reads`, which this arm's inputs never register
    (see build_inputs_long_read), so it is a short-read-only binner. This is
    also why this function takes no `variant` parameter -- there is no
    short-read assembler choice left to make in a pure long-read arm.

    NAME `sequences::flye_raw_assembly`, never the shared supertype -- for the
    same reason build_targets names spades/megahit. Confirmed by solving that
    this concrete-type naming is sufficient on its own for the ASSEMBLY
    choice (no `_assembly_without()` mask needed between flye_raw and a
    short-read assembler, because they descend from different immediate
    ancestors) -- but see build_inputs_long_read's docstring for why that
    result alone was not enough to make a shared read_metadata safe, and why
    this arm's inputs give the long lineage no shared metadata to be ambiguous
    about at all.

    Requesting `binning::amber_results` / `binning::das_tool_amber_results` is
    what pulls cami_contig_truth in automatically -- neither
    `binning::contig_gold_standard_table` nor cami_contig_truth is named as an
    explicit target here, matching build_targets, which never names it either
    for the short-read lineage.
    """
    t = TargetBuilder()
    asm_lr = t.Add("sequences::flye_raw_assembly")
    t.Add("sequences::assembly_stats", parents=[asm_lr])
    # gff and assembly_per_contig_coverage published here too, matching
    # build_targets' short-read treatment -- both used to be orphaned in this
    # arm (produced as manifest side-products of prodigal/assembly_stats but
    # never named, so never collected) while the short-read arm always named
    # them. sequences::orfs stays deliberately unnamed; see this function's
    # own docstring for why.
    t.Add("sequences::gff", parents=[asm_lr])
    t.Add("sequences::assembly_per_contig_coverage", parents=[asm_lr])
    binners = ("metabat2", "semibin2", "comebin")
    bins = [t.Add(f"sequences::{b}_bin_fasta", parents=[asm_lr]) for b in binners]
    tables = [t.Add(f"binning::{b}_contig_to_bin_table", parents=[asm_lr]) for b in binners]
    # No per-binner checkm_stats or amber targets: das_tool consolidates these three, so
    # they are intermediates and DAS Tool's bin set is the MAG set. See build_targets'
    # note on which bin sets get evaluated; the tables above are still named, so a
    # per-binner pass remains available post hoc.
    t.Add("binning::das_tool_contig_to_bin_table", parents=[asm_lr])
    t.Add("binning::das_tool_amber_results")
    # Refined bins, published and CheckM2-scored -- see build_targets' identical
    # addition for why this does not collide with the three raw-bin checkm
    # targets above.
    das_tool_bin_lr = t.Add("sequences::das_tool_bin_fasta", parents=[asm_lr])
    t.Add("taxonomy::checkm_stats", parents=[das_tool_bin_lr])
    return t


def build_transforms_for_long_read():
    return [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
        # flye.py (the clean-reads variant, fed by filtlong) masked out: it and
        # flye_raw.py share long_reads as their common ancestor (via
        # filtlong's clean_long_reads for flye.py), which is the "same reads,
        # two assemblers" case build_transforms_for's own _assembly_without
        # call guards against for spades/megahit. spades.py and megahit.py
        # need no masking here -- nothing in this arm's inputs ever registers
        # short_reads_pe/short_reads, so they are simply unreachable, not
        # ambiguous.
        #
        # hifi/hifiasm_meta.py masked out for the identical reason: it requires
        # bare `sequences::long_reads` with no parent pin at all (not even
        # read_qc_stats), so it is a second, unpinned producer over this arm's
        # only registered read type. Left unmasked it silently pulls in a whole
        # SECOND assembly-to-DAS-Tool branch off `hifiasm_meta_assembly` -- 24
        # steps instead of 17, metabat2/semibin2/comebin/das_tool all rerun --
        # and das_tool.py's own bare `asm` requirement (needed so it can run
        # against EITHER assembly) is what then lets `amber_das_tool` bind to
        # the WRONG one: das_tool_contig_to_bin_table gets two producers, one
        # per assembly, and the flye_raw-descended one -- the one this arm
        # actually wants scored -- comes out orphaned instead. Confirmed by
        # dumping `s.uses`: the other hifi/*.py transforms (hifiasm.py,
        # miniasm.py, filtlong_targeted.py, pbBam2fastq.py) all require a
        # narrower type this arm's inputs never register
        # (100x_long_reads/miniasm_gfa/pacbio_hifi_bam), so they are
        # unreachable rather than ambiguous and need no masking.
        _assembly_without("flye", "hifi/hifiasm_meta"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
    ]


# -- Pratama 2026 ------------------------------------------------------------
#
# Reads arrive as two files per run rather than CAMI's one interleaved file, so
# sample enumeration below reads research/pratama2026/runs.tsv for the
# accession/layout truth and then checks the cluster for what is actually on
# disk, rather than trusting either alone: the tsv does not know what a
# still-running fetch job has landed, and a directory listing does not know
# which runs are short-read at all.

def enumerate_pratama_runs():
    """(run_accession, dataset, remote fwd path, remote rev path) for every PAIRED
    run with BOTH mates present on disk right now, plus a report of what was
    skipped and why.

    Filters out the 6 SINGLE (MinION long-read) runs -- this target set is
    short-read only -- and any PAIRED run missing one or both mates, which a
    still-running fetch job can do at any moment. Reported rather than silently
    dropped, because an incomplete download otherwise reads as a smaller corpus
    rather than as a fetch still in flight.

    The unit of work is the sequencing RUN, not the well or sample: several runs
    here are replicates of one well and filter fraction, and per-run is kept
    deliberately. Pooling across replicates would throw away the structure the
    paper's own comparison needs, and the cross-sample viral tools below pool
    across every run anyway -- collapsing replicates here would only lose
    information, not gain anything downstream.

    HARD-DEPENDS ON THE CLUSTER BEING REACHABLE, including for `--dry-run`: mate
    presence is a fetch-in-progress fact that has to be measured on fir right now
    via `ssh_cmd`, not assumed from a static manifest, so there is no offline path
    through this function at all. A saturated shared ControlMaster surfaces here
    as a traceback out of `ssh_cmd` before dry-run even reaches the planner --
    that is a transport hiccup, not evidence the Pratama corpus or the cluster is
    broken. See `ssh_cmd`'s own retry/backoff for the same reason it exists.
    """
    rows = list(csv.DictReader(PRATAMA_RUNS_TSV.open(), delimiter="\t"))
    paired = [r for r in rows if r["library_layout"] == "PAIRED"]
    n_single = len(rows) - len(paired)

    tokens = " ".join(f"{r['dataset']}:{r['run_accession']}" for r in paired)
    cmd = (
        f'ROOT={PRATAMA_ROOT}; for e in {tokens}; do '
        'd=${e%%:*}; r=${e##*:}; '
        'f1="$ROOT/$d/$r/${r}_1.fastq.gz"; f2="$ROOT/$d/$r/${r}_2.fastq.gz"; '
        's1=0; s2=0; [ -s "$f1" ] && s1=1; [ -s "$f2" ] && s2=1; '
        'echo "$r $s1 $s2"; done'
    )
    out, _ = ssh_cmd(cmd, timeout=120)
    have = {}
    for line in out.splitlines():
        run, s1, s2 = line.split()
        have[run] = (s1 == "1", s2 == "1")

    kept, n_missing, n_partial = [], 0, 0
    for r in paired:
        run = r["run_accession"]
        s1, s2 = have.get(run, (False, False))
        if s1 and s2:
            base = PRATAMA_ROOT / r["dataset"] / run / run
            kept.append((run, r["dataset"],
                        Path(f"{base}_1.fastq.gz"), Path(f"{base}_2.fastq.gz")))
        elif s1 or s2:
            n_partial += 1
        else:
            n_missing += 1

    report = (f"{len(rows)} runs in runs.tsv: {n_single} SINGLE (MinION, excluded), "
             f"{len(paired)} PAIRED of which {len(kept)} have both mates on disk, "
             f"{n_partial} have one mate (fetch in progress), {n_missing} have neither")
    return kept, report


# Pratama's published products, for the recovery comparison. They arrive as zips; these are
# the unpacked, permanent locations. The SHAPE of each is dictated by the transform that
# consumes it, not by the archive's layout:
#   published_votus  a SINGLE multi-fasta. pratama_votu_recovery passes it straight to
#                    `skani dist --qi -q <file>`, where -q takes one multi-fasta and --qi
#                    scores each contig inside it as its own genome, so the catalogue has to
#                    stay pooled exactly as published. 257,252 records, verified by header count.
#   published_mags   a FLAT DIRECTORY of per-genome fasta. pratama_mag_recovery globs
#                    `<dir>/*.fasta`, one level and not recursive, so the archive's three
#                    part-directories are merged into one (425 each, 1275 total, zero filename
#                    collisions across parts).
#
# CAUTION registered with an explicit stable instance_id, and that is the whole point. These
# two were previously `AddItem(DEFERRED, ...)` with no id, and AddItem mints a fresh uuid4 for
# any path it cannot stat -- which a DEFERRED path never can. Measured: five consecutive dry
# runs of --with-zenodo-comparison produced five different plan keys in one untouched tree, so
# any interruption of a real run discarded the entire cache. A DEFERRED input also carries no
# data at all, so the two recovery steps could not have produced a comparison either way.
# Override a path with the env var to point at your own unpack location.
PRATAMA_PUBLISHED = {
    "pratama::published_votus": Path(os.environ.get(
        "PRATAMA_PUBLISHED_VOTUS",
        "/scratch/phyberos/pratama2026/zenodo_17897233_unpacked/votu/FINAL_groundwater-votu-5k.fasta")),
    "pratama::published_mags": Path(os.environ.get(
        "PRATAMA_PUBLISHED_MAGS",
        "/scratch/phyberos/pratama2026/zenodo_17897233_unpacked/mags")),
}


def _register_pratama_published(lib):
    """Register the published products with ids identical across every call site.

    Both `build_inputs_pratama` and `build_globals_pratama` register these, and the two must
    agree: a differing instance_id for one path would present the solver with two distinct
    given items of the same type. Deriving the id here rather than at each call site makes
    agreement structural instead of a convention someone has to remember.
    """
    for dtype, path in PRATAMA_PUBLISHED.items():
        lib.RegisterItem(path, dtype,
                         instance_id=_stable_id("pratama_published", "ref", dtype, str(path)))


def build_inputs_pratama(runs, with_gpr_panel=False, with_zenodo_comparison=False):
    """Register every run's reads through the library's paired-reads chain, plus
    one shared viromics::contig_study root the viral survey's cross-sample tools
    pool under.

    `sequences::read_pair` -> `zipped_forward/reverse_short_reads` ->
    `interleave_zipped_short_reads` -> `sequences::short_reads` is the chain
    bbduk needs; CAMI's single interleaved file skips it entirely, which is why
    that driver never needed it.

    Parenting every run's read_metadata under `study` is not optional set
    dressing: `merge_candidate_calls` (the viral lane's pooling step) requires
    its own `read_pair` slot with `parents={study}`, so nothing merges unless
    the lineage is actually there. The consequence, measured directly rather
    than assumed: `DataInstanceLibrary.AsSamples("sequences::read_metadata")`
    masks each sample to `{path} | ancestors | siblings`, and once every run's
    ancestors set is `{study}`, every run is also every other run's sibling --
    so AsSamples yields exactly ONE view spanning all runs, not one per run.
    That collapse is the expected shape here, not a bug: `WorkflowPlan.Generate`
    pools items into its `given_map` by structural type/lineage endpoint
    regardless of how many given-groups they arrived through, so a collecting
    transform (the merge) still gathers every run's calls, and a regular
    transform (QC, assembly, binning) still gets one instance per run at
    Nextflow-compile time. What actually changes is the solver's own
    bookkeeping: it plans this as "1 sample, 1 unique case" rather than the "N
    samples" CAMI's unshared runs give it, because it is validating that ONE
    chain of types exists, not enumerating N physical chains. See the driver's
    module docstring and the campaign report for the measurement this rests on.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(CACHE_DIR / "pratama_inputs.xgdb")
    inputs.Purge()

    for tl in ["sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
               "taxonomy.yml", "binning.yml", "binning_local.yml", "env.yml",
               "viromics.yml", "pratama.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    study_value = json.dumps({"logistics": "contig study"})
    (inputs.location / "contig_study.json").write_text(study_value)
    study = inputs.RegisterItem(
        "contig_study.json", "viromics::contig_study",
        instance_id=_stable_id("pratama", "contig_study", study_value),
    )

    for run, dataset, fwd, rev in runs:
        meta_value = json.dumps({"parity": "paired", "length_class": "short"})
        meta_name = f"{run}_read_metadata.json"
        (inputs.location / meta_name).write_text(meta_value)
        meta = inputs.RegisterItem(
            meta_name, "sequences::read_metadata", parents={study},
            instance_id=_stable_id("pratama", "read_metadata", run, meta_value),
        )
        pair_value = run
        pair_name = f"{run}_read_pair.txt"
        (inputs.location / pair_name).write_text(pair_value)
        pair = inputs.RegisterItem(
            pair_name, "sequences::read_pair", parents={meta},
            instance_id=_stable_id("pratama", "read_pair", run, pair_value),
        )
        inputs.RegisterItem(
            fwd, "sequences::zipped_forward_short_reads", parents={pair},
            instance_id=_stable_id("pratama", "zipped_forward_short_reads", run, str(fwd)),
        )
        inputs.RegisterItem(
            rev, "sequences::zipped_reverse_short_reads", parents={pair},
            instance_id=_stable_id("pratama", "zipped_reverse_short_reads", run, str(rev)),
        )

    for dtype, path in DB_PATHS.items():
        inputs.RegisterItem(path, dtype, instance_id=_stable_id("pratama", "ref", dtype, str(path)))
    for dtype, path in STAGED_REFS.items():
        inputs.RegisterItem(path, dtype, instance_id=_stable_id("pratama", "ref", dtype, str(path)))
    for dtype, path in STAGED_REFS_PRATAMA.items():
        inputs.RegisterItem(path, dtype, instance_id=_stable_id("pratama", "ref", dtype, str(path)))

    # annotation::gpr_table's two study-wide references, registered only when the
    # panel is asked for. DEFERRED because no location for either was sourced on
    # fir, which is exactly why the panel is off by default: StageWorkflow calls
    # RefuseIfDeferred and names both rows rather than staging a hole, so this
    # cannot be discovered three days into a run.
    #
    # The coupling runs the other way too. Register them with the panel OFF and
    # they are parentless deferred inputs that belong to no sample, which the
    # solver never sees; register neither with the panel ON and gpr_table
    # dead-ends with no producer, and per the planner's own reporting quirk that
    # one unsatisfiable chain poisons every other target in the same solve. Both
    # switch together or neither does.
    if with_gpr_panel:
        inputs.AddItem(DEFERRED, "ref::mnxr_lookup")
        inputs.AddItem(DEFERRED, "ref::label_transfer_landmarks")

    # T17's ground truth, same coupling as the GPR panel above and for the same
    # reason: DEFERRED because neither is sourced yet. The Zenodo record is on fir
    # (/scratch/phyberos/pratama2026/zenodo_17897233/) but still as the three
    # Filtered_dereplicated_genomes_part{1,2,3}.zip and Groundwater-votu-5k.fasta.zip
    # themselves -- pratama_mag_recovery.py and pratama_votu_recovery.py both want
    # them unzipped (the three MAG parts pooled into one directory of one FASTA per
    # MAG; the vOTU zip unpacked to a directory or a single multi-fasta). Nobody has
    # done that yet, and this driver does not reach onto the cluster to do it either
    # -- see the campaign report. Registering only when asked mirrors the GPR
    # panel's reasoning exactly: on with nothing sourced is a DEFERRED the solver can
    # still plan around; off is silent and correct until someone unzips the archive
    # and this driver's `DB_PATHS` grows a `PRATAMA_ZENODO_*`-style override pointing
    # a real RegisterItem at the unzipped location instead of this DEFERRED stand-in.
    if with_zenodo_comparison:
        _register_pratama_published(inputs)

    inputs.Save()
    return inputs


def build_globals_pratama(with_gpr_panel=False, with_zenodo_comparison=False):
    """Study-wide references ONLY (DB_PATHS, STAGED_REFS, and the two
    DEFERRED pairs when their flag is on), for `resources=` INSTEAD OF the
    full per-sample `inputs` library -- see build_globals_long_read's
    docstring for the mechanism this guards against.

    The two DEFERRED pairs have to be carried here rather than dropped: they
    are parentless (see build_inputs_pratama), so `AsSamples` never reaches
    them either way, and they were ONLY ever visible to the solver by riding
    along inside the `resources=[..., inputs]` list this function replaces.
    Registering them under a fresh corpus namespace ("pratama_globals", not
    "pratama") is fine -- DEFERRED items have no path to hash and no sample
    lineage to collide with; the type name alone identifies the slot.

    Latent otherwise, same as build_globals's docstring: every Pratama run
    carries the identical read_pair + read_metadata + zipped_forward/reverse
    shape today, parented under the one shared `contig_study` root, so
    `resources=[..., inputs]` has not yet unioned away a second run shape.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    lib = DataInstanceLibrary(CACHE_DIR / "pratama_globals.xgdb")
    lib.Purge()
    # annotation.yml is load-bearing for STAGED_REFS_PRATAMA's dram_db entry; without it
    # RegisterItem asserts `namespace [annotation] not found` and NO arm plans.
    for tl in ["ref.yml", "env.yml", "annotation.yml", "pratama.yml"]:
        lib.AddTypeLibrary(MLIB / "data_types" / tl)
    for dtype, path in DB_PATHS.items():
        lib.RegisterItem(path, dtype, instance_id=_stable_id("pratama_globals", "ref", dtype, str(path)))
    for dtype, path in STAGED_REFS.items():
        lib.RegisterItem(path, dtype, instance_id=_stable_id("pratama_globals", "ref", dtype, str(path)))
    for dtype, path in STAGED_REFS_PRATAMA.items():
        lib.RegisterItem(path, dtype, instance_id=_stable_id("pratama_globals", "ref", dtype, str(path)))
    if with_gpr_panel:
        lib.AddItem(DEFERRED, "ref::mnxr_lookup")
        lib.AddItem(DEFERRED, "ref::label_transfer_landmarks")
    if with_zenodo_comparison:
        _register_pratama_published(lib)
    lib.Save()
    return lib


def build_targets_pratama(with_gpr_panel=False, with_zenodo_comparison=False,
                          with_host_prediction=False, with_cellular_annotation=False):
    """MEGAHIT + MetaWRAP + CheckM2, plus the viral survey.

    The viral block WAS a blind copy of research/viromics/viromics_survey_from_paired_reads.py's
    TARGETS[2:] (2026-09-11) with the assembler re-pointed -- that driver pins
    `sequences::megahit_assembly` as target 0 and masks spades out; this campaign's run 2
    is the reverse. Checked against Pratama's own Virus_bioinformatics.md
    (data/docs/pratama2026/Groundwater_virome/Workflows/) for the first time in this
    revision (research/pratama2026/reproduction_map.md is the row-by-row account) and
    trimmed of two targets that were never part of Pratama's workflow at all --
    `annotation::kofamscan_descriptions` (Antonio's KEGG-Mapper substitute; Pratama's
    AMG calls are DRAM-v's, already covered by `annotation::dramv_distill` below) and
    `taxonomy::metabuli` (answers no row in either workflow). Everything else survives
    the check: geNomad, VirSorter2, VIBRANT, MMseqs2, CheckV, vConTACT3, iPHoP and the
    CRISPR spacer BLAST are all really Pratama's own tools too, not just Antonio's.

    No AMBER and no cami_contig_truth: Pratama is real data with no simulated ground
    truth to score bins against.

    THE ASSEMBLY CONFLICT described here in earlier revisions -- the library's one spades
    transform running the DOE JGI protocol against Pratama's own untuned metaSPAdes call --
    no longer applies to this arm. Wave 1 flips run 2 from metaSPAdes to MEGAHIT: `asm` now
    names `sequences::megahit_assembly`, masked in via `_assembly_without("spades")` in
    `build_transforms_for_pratama` below, and the metaSPAdes-vs-JGI-protocol gap is moot
    because this arm no longer runs metaSPAdes at all.

    A SECOND, narrower gap in the same spot: Pratama's virus identification runs on
    contigs from metaSPAdes AND a second MEGAHIT assembly of the same reads ("different
    assemblers can yield complementary viral contigs"), then calls on both feed the same
    curation. `merge_candidate_calls.py` pins its callers to ONE `sequences::assembly`
    ancestor per contig study, so pooling both assemblers' calls into one frozen set
    would need that transform's model changed, not a target-set change -- left as a
    reported gap, not attempted here.

    `annotation::gpr_table` is OFF by default, and that is a campaign decision rather
    than a technical one. It pulls the whole chosen-4 panel in behind it -- KOfamScan,
    CLEAN, DIAMOND UniRef50 and ProteinBERT, each chunked and merged -- which is six
    steps and, measured on the last ten-sample run, 85% of the task-hours and 96% of
    the tasks. Batch 1 is a citable core pipeline and deliberately carries no
    functional annotation lane; the panel also goes beyond what Pratama published,
    which used DRAM. It is also the only thing in this target set that needs
    `ref::mnxr_lookup` and `ref::label_transfer_landmarks`, neither of which has a
    sourced path on fir, so turning it on requires finding those two files first --
    StageWorkflow refuses a deferred input rather than staging a hole.

    `with_zenodo_comparison` is OFF for the identical reason and by the identical
    mechanism: `pratama::mag_recovery_table` and `pratama::votu_recovery_table` are
    T17's own comparison against Pratama's Zenodo products, and both transforms
    (viromics/pratama_votu_recovery.py, metagenomics/binning/pratama_mag_recovery.py)
    are themselves marked DESIGNED, NOT YET RUN. The MAG side costs nothing extra to
    reach: `viromics::host_prediction_genome` above already pulls the three-binner
    (MetaBAT2/SemiBin2/COMEBin) + aggregator + skani_dedup + GTDB-Tk de novo + iPHoP
    chain in behind it for host prediction, which is exactly what
    `binning::derep_mag_ref` needs -- so run 2 already runs a SECOND, undocumented
    binning ensemble beside its own MetaWRAP lane just to answer host_prediction_genome,
    and pinning `pratama::mag_recovery_table` to that same assembly is one more cheap
    step (derep_mag_reference.py) on top of work already being done. See the campaign
    report for why that second ensemble -- not MetaWRAP -- is what actually gets
    compared to Pratama's 1275 published MAGs.
    """
    t = TargetBuilder()
    asm = t.Add("sequences::megahit_assembly")
    frozen = t.Add("viromics::dereplicated_candidate_virus")

    # This plan has TWO producers of `sequences::orfs` -- prodigal.py on the whole
    # assembly and prodigal_gv.py on `frozen`, the viral set -- and NOTHING PINS
    # WHICH ONE WINS. Both descend from the same assembly, so ancestral matching
    # cannot tell them apart even in principle.
    #
    # CAUTION this comment previously asserted "the annotation chain binds
    # prodigal_gv's ... Verified correct". That was true when written and is NOT
    # true now. On 2026-09-12 an UNRELATED change -- pointing `ref::gtdb` at a
    # staged tree, which removed a download step -- flipped the chain onto
    # prodigal's whole-assembly ORFs. Step count stayed 41 both ways and one step
    # name differed. So do not read the current binding as a defect and flip it
    # back: the binding is not pinned in either direction, and it moves on changes
    # that have nothing to do with ORFs.
    #
    # The stakes of the flip are an order of magnitude, which is why the expensive
    # consumer below is now opt-in. One Pratama sample measured 672,121 ORFs from
    # 401,906 contigs on the whole assembly; the chunker cuts at 5,000, so that is
    # ~135 chunks per sample and ~8,900 tasks at 66 samples, each declaring 8 cpus
    # and 64 GB over KOfam + Pfam + dbCAN. Bound to the viral set instead it is a
    # small fraction of that. Chunk size is NOT the lever -- it trades task count
    # for per-task runtime and leaves total core-hours unchanged.
    #
    # The durable fix is a type fence giving viral ORFs their own type, the same
    # shape as `raw_contig_to_bin_table` in binning.yml. Out of scope mid-launch.
    #
    # cross-sample tools, pooled on the frozen set (viromics TARGETS[2:9] minus
    # kofamscan_descriptions -- see the docstring)
    cross_sample = ["viromics::contig_length_table", "viromics::precluster_table",
                    "viromics::votu_cluster_table", "viromics::checkv_contamination",
                    "viromics::vcontact3_network", "viromics::spacer_host_links"]
    # host_prediction_genome is OPT-IN, and dropping it is the single largest cost
    # saving in this arm. Naming it pulls `gtdbtk_de_novo` (32 cpus, 240 GB, a 48 h
    # wall over 189,805 taxa) plus iphop_add_to_db/iphop_predict and their ~0.5 TB
    # database in behind it, and acceptance criterion 2 asks for a vOTU catalogue,
    # MAGs and AMG calls -- host prediction is not among them.
    #
    # CAUTION, and this is why the line below exists: host_prediction_genome was
    # ALSO the only thing reaching the MAG chain. It pulls the three-binner
    # ensemble + aggregator + skani_dedup in behind it, and `aggregator` HARD
    # REQUIRES taxonomy::gtdbtk per binner, so that chain is where criterion 2's
    # MAG comparison actually comes from -- NOT the MetaWRAP lane. Dropping host
    # prediction without naming the dereplication output directly would silently
    # delete the thing being compared to Pratama's published MAGs.
    if with_host_prediction:
        cross_sample.append("viromics::host_prediction_genome")
    for dtype in cross_sample:
        t.Add(dtype, parents=[frozen])
    # The MAG chain, named directly rather than inherited from host prediction.
    t.Add("binning_local::cluster_table", parents=[asm])
    if with_zenodo_comparison:
        t.Add("pratama::votu_recovery_table", parents=[frozen])
    # per-sample viral work that nothing above reaches (viromics TARGETS[10], [12]).
    # dramv_distill is DRAM-v on the viral contigs and IS criterion 2's AMG calls --
    # always on.
    per_sample = ["annotation::dramv_distill"]
    # dram_annotations is the CELLULAR annotation lane and is OPT-IN, for the same
    # reason host prediction is: it answers no acceptance criterion and it is now the
    # largest single cost in this arm. Criterion 2 asks for a vOTU catalogue, MAGs and
    # AMG calls; the AMG calls are DRAM-v's, above. Measured cost when it binds the
    # whole assembly's ORFs: ~8,900 tasks at 8 cpus and 64 GB, on the order of 36,000
    # to 71,000 core-hours -- twenty to forty times the de-novo tree this arm already
    # dropped on exactly this argument. Add it back as a follow-on run against an
    # assembly that already exists; nothing about it needs to be in the first pass.
    if with_cellular_annotation:
        per_sample.append("annotation::dram_annotations")
    if with_gpr_panel:
        per_sample.append("annotation::gpr_table")
    for dtype in per_sample:
        t.Add(dtype, parents=[asm])

    # run 2's own core lane: metaSPAdes + MetaWRAP + CheckM2, mirroring
    # build_targets(variant="core") minus AMBER/cami_read_truth.
    t.Add("sequences::read_qc_stats")
    for dtype in ("sequences::orfs", "sequences::gff", "sequences::assembly_stats",
                  "sequences::assembly_per_contig_coverage", "alignment::bam"):
        t.Add(dtype, parents=[asm])
    mw_bin = t.Add("sequences::metawrap_bin_fasta", parents=[asm])
    t.Add("binning::metawrap_contig_to_bin_table", parents=[asm])
    t.Add("taxonomy::checkm_stats", parents=[mw_bin])

    # T17's MAG-side comparison. Parented to `asm`, not `frozen`: derep_mag_ref
    # descends from the aggregator/skani_dedup chain off the SAME assembly, not from
    # the frozen viral set, and lineage matching is ancestral -- pinning this to
    # `frozen` would ask the solver to find a MAG reference descended from a viral
    # FASTA, which it is not.
    if with_zenodo_comparison:
        t.Add("pratama::mag_recovery_table", parents=[asm])
    return t


def build_transforms_for_pratama():
    return [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
        _assembly_without("spades"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "viromics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "fabfos"),
    ]


def make_slurm_config(comebin_device="cpu", comebin_time="3d", comebin_cpus=48):
    smith = get_agent()
    base = Path(smith.GetNxfConfigPresets()["slurm"]).read_text()
    if comebin_device == "gpu":
        # A MIG slice, not `--gpus=1`. COMEBin's contrastive net is small -- 20 GB is
        # ample -- and a whole H100 queues far longer than a slice does. The slice's
        # CUDA_VISIBLE_DEVICES is a `MIG-<uuid>` handle rather than an index, which is
        # why it has to be forwarded verbatim and cannot be re-derived in the container.
        #
        # 16 CPUs, not 64. Only coverage and the Leiden sweep are CPU-bound, and the
        # sweep is where every large CPU-only run deadlocked: cluster.py forks a Pool
        # from a parent that k-means already left threaded, so a smaller pool is a
        # smaller target. See research/aspire/campaigns/r1/gapfill/STATE.md.
        body = [
            "        cpus = 16",
            "        memory = '48 GB'",
            f"        time = '{comebin_time}'",
            f'        clusterOptions = "--nodes=1 --ntasks=1 --account={GPU_ACCOUNT}'
            ' --gres=gpu:nvidia_h100_80gb_hbm3_2g.20gb:1"',
            "        beforeScript = 'export APPTAINERENV_CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES'",
        ]
    else:
        # A quarter (at the 48-cpu default) of a fir compute node: 2x AMD EPYC 9655
        # (Zen 5), 192 cores, 768000 MB per node -- measured directly with
        # `sinfo -o "%P %c %m"` on fir 2026-09-12 against every cpubase_* partition,
        # because the prior comment's "96 GB is free headroom by entitlement" was
        # never checked against that command and was wrong: entitlement is exactly
        # 768000/192 = 4000 MB/core, so a flat 96 GB is headroom only up to 24 cpus,
        # and at the 96 cpus this used to hardcode it is 288 GB short of entitlement,
        # not over it. That gap is why two of ten tasks were OOM-killed -- measured
        # RSS ran 72-96 GiB, i.e. already pinned against the flat cap.
        #
        # Memory is DERIVED from comebin_cpus below, not a second literal, so the
        # two can no longer drift apart the way they just did. At the 48-cpu default
        # that is 48 * 4000 MB = 192 GB -- comfortably above the 72-96 GiB observed
        # at double the cores, since comebin's memory floor is set mostly by the
        # contig/k-mer data rather than by thread count.
        #
        # CAUTION: cpus is not a pure resource knob here. comebin.py reads
        # context.params.get('cpus', 8) and passes it straight to
        # `run_comebin.sh -t {threads}`, which is COMEBin's own thread count for its
        # torch contrastive-learning step -- the container's hardcoded single-thread
        # OpenMP setting does not throttle that. So this value simultaneously sizes
        # the Slurm allocation AND how many threads COMEBin itself spins up.
        #
        # 48 rather than 96 because comebin's training is Amdahl-limited and the
        # allocation, not the wall, is the scarce thing. Halving the cores costs well
        # under double the wall and saves real core-hours. Calibration from a sibling
        # project: 73k contigs at 64 threads finished in 12 min with a 5.2 GB peak,
        # and the Leiden sweep itself finished in 16 m at 16 cores against 7 m 43 s at
        # 96 -- its training is Amdahl-limited, which is the same reason 48 beats 96
        # here rather than just costing less.
        #
        # 3d, not 24h, and this is the load-bearing setting. Ten marine samples at 96
        # cores ranged 6 h 28 m to over 11 h 44 m -- a 1.8x spread driven by community
        # complexity, not by anything the driver controls. Halve the cores and the slow
        # tail lands near 20 h, which is too close to a 24 h wall to bet a full re-run
        # on. Under the original 8 h wall this step reached 191, 195 and 28 epochs of
        # 200 across three attempts and never once finished. The 3d band reaches 380 of
        # fir's 519 by-core nodes against 432 at 24h, which is a cheap hedge.
        #
        # The r1 fork-after-threads deadlock in the Leiden sweep has not reproduced
        # here: the sweep finished in 16 m at 16 cores and 7 m 43 s at 96.
        mem_gb = comebin_cpus * FIR_MEM_MB_PER_CPU // 1000
        body = [
            f"        cpus = {comebin_cpus}",
            f"        memory = '{mem_gb} GB'",
            f"        time = '{comebin_time}'",
            f'        clusterOptions = "--nodes=1 --ntasks=1 --account={SLURM_ACCOUNT}"',
        ]
    # Two appended blocks, and the second is a workaround rather than a preference.
    #
    # queueSize and array themselves go through params, not raw config: the preset
    # reads params.process.array and exempts its `xlocalx` label, and a top-level
    # `process { array = N }` overrides that exemption.
    #
    # That exemption is not enough on its own. Every cacheable step compiles to a
    # sibling `<name>_cached` process that declares `executor 'local'` inline and
    # carries no label, so the preset's label-keyed exemption never reaches it while
    # the global array directive does. Nextflow then refuses at process-construction
    # time with "Executor 'local' does not support job arrays", which aborts the whole
    # run before a single task is submitted -- and metasmith still prints `run
    # completed` with zero outputs. Exempting them by name is the narrow fix; the
    # broad one belongs in nextflow_config/slurm.nf.
    #
    # DO NOT add `scratch = false` here. It was tried and REMOVED, and the reason is
    # worth the space because the symptom is convincing.
    #
    # The preset sets `scratch` to the node-local SLURM_TMPDIR for every process, so a
    # twin materialises the cached product into node-local scratch and Nextflow copies
    # it back with `nxf_fs_copy`'s `cp -fRL`. `-L` DEREFERENCES, so a product tree
    # carrying a dangling symlink fails with hundreds of `cannot stat` lines naming the
    # DESTINATION path. `nxf_unstage`'s status becomes the task's, so the twin is retried
    # and then swallowed by the ignore strategy: the product is silently absent, every
    # consumer drops out, and the run still reports completed.
    #
    # Turning scratch off does stop that, and it halves the twin's I/O -- but it bends
    # the execution contract for EVERY cached twin to accommodate ONE product's defect,
    # and the contract is not the thing to bend. Exactly one product ever failed:
    # vConTACT3's database, which ships upstream mmseqs build scratch containing 404
    # dangling symlinks. `logistics/downloadVcontact3DB.py` now prunes that scratch in the
    # protocol that builds the product and asserts no dangling symlink survives, so the
    # product is valid inside the standard contract.
    #
    # Exposure audited before removing the exemption: 1 shard of 4,865 across all three
    # agent homes held a dangling symlink, and it was that database. If a future reference
    # product fails its unstage the same way, fix THAT product's protocol -- the check is
    # `find <shard>/out -xtype l | wc -l`.
    text = base + "\n" + "\n".join(
        ["", "process {", "    withName: '.*__comebin' {", *body, "    }", "}", "",
         "process {", "    withName: '.*_cached' {", "        array = 0",
         "    }", "}", ""])
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out = CACHE_DIR / f"fir_slurm_cami_{comebin_device}comebin.config"
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


def _assert_given_counts(task, expected: dict):
    """Refuse to proceed unless every registered read leaf appears in
    `task.plan.given` at exactly the count it was registered.

    `task.ok` and an empty `dropped_targets` are not enough: the metaGEM
    driver reported both while `CollectSolverInputs` had silently planned a
    whole study's reads out of existence -- 491 metadata nodes coexisted with
    443 of each read type and zero single-end reads, no error raised
    anywhere. The solver's own "solving plan for [N] samples as [K] unique
    cases" log line does not catch this either -- see
    enumerate_long_only_sample's docstring for a case where that line is
    correct either way and so proves nothing.
    `Counter(i.dtype_name for i in task.plan.given)` does, because it counts
    the actual given instances the plan carries rather than the solver's
    bookkeeping about them: any driver that registers per-sample reads should
    call this right after `GenerateWorkflow` with the count it just
    registered for each read-bearing type.
    """
    from collections import Counter
    got = Counter(i.dtype_name for i in task.plan.given)
    problems = [
        f"{dtype_name}: expected {want}, plan.given has {got.get(dtype_name, 0)}"
        for dtype_name, want in expected.items() if got.get(dtype_name, 0) != want
    ]
    if problems:
        print("ERROR: given-instance count mismatch -- refusing to proceed "
              "(a plan can report ok=True and dropped_targets=[] while still "
              "having silently planned reads out of existence):", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        sys.exit(1)


def cmd_list_samples(args):
    if args.corpus == "pratama":
        kept, report = enumerate_pratama_runs()
        print(report)
        # An incomplete corpus does not launch, and this is not tidiness. Run 2's
        # cross-sample viral tools cluster across EVERY run to define the vOTUs, so a
        # run that lands later is a re-run of that clustering rather than an append,
        # and a partial corpus quietly defines a different vOTU set than the one the
        # comparison is about. A dry run and a stage are still allowed: neither spends
        # anything, and both are how the wait gets used.
        n_expected = sum(1 for r in csv.DictReader(PRATAMA_RUNS_TSV.open(), delimiter="\t")
                         if r["library_layout"] == "PAIRED")
        if len(kept) < n_expected and not (args.dry_run or args.stage_only or args.allow_partial):
            print(f"ERROR: {len(kept)} of {n_expected} paired runs have both mates on disk; "
                  f"the fetch is still in flight.\n"
                  f"  Waiting is the default because the viral clustering pools across every "
                  f"run, so the missing ones cannot be added later without redoing it.\n"
                  f"  --dry-run and --stage-only work now; --allow-partial launches a "
                  f"deliberately partial corpus.", file=sys.stderr)
            return 1
        for run, dataset, fwd, rev in kept:
            print(f"{run:14s} {dataset:10s} {fwd}")
        return 0
    if getattr(args, "long_read", False):
        for sid, short_reads, long_reads, truth in enumerate_paired_toy_humangut_samples():
            print(f"{sid:24s} paired  short={short_reads} long={long_reads} "
                  f"has_truth={1 if truth else 0}")
        for sid, reads, truth, dataset in enumerate_long_read_samples():
            if dataset in ("toy_humangut_long",):
                continue  # already listed above, paired
            print(f"{sid:24s} long-only  reads={reads} has_truth={1 if truth else 0}")
        return 0
    for sid, reads in enumerate_samples():
        print(f"{sid:12s} {reads}")
    return 0


def cmd_check_dbs(args):
    checks = {"cami root": CAMI_ROOT, "agent home": HPC_MSM_HOME,
              "container store": HPC_MSM_HOME / "container_images", **DB_PATHS}
    probe = "; ".join(
        f'test -e "{p}" && echo "OK   {t} -> {p}" || echo "MISS {t} -> {p}"'
        for t, p in checks.items())
    out, _ = ssh_cmd(probe)
    print(out)
    missing = [ln for ln in out.splitlines() if ln.startswith("MISS")]
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


def cmd_run(args):
    if args.corpus == "pratama":
        if args.variant != "core":
            print("ERROR: --corpus pratama only implements --variant core "
                  "(MEGAHIT + MetaWRAP + CheckM2 + viral survey); "
                  "the three-binner variant was never asked for and is "
                  "not wired up.", file=sys.stderr)
            return 1
        kept, report = enumerate_pratama_runs()
        print(report)
        samples = select(kept, args)
        if not samples:
            print("ERROR: no runs found; run list-samples --corpus pratama", file=sys.stderr)
            return 1
        print(f"{len(samples)} run(s) selected: {', '.join(s[0] for s in samples)}")

        inputs = build_inputs_pratama(samples, with_gpr_panel=args.with_gpr_panel,
                                      with_zenodo_comparison=args.with_zenodo_comparison)
        containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
        resource_lib = DataInstanceLibrary.Load(MLIB / "resources" / "lib")
        targets = build_targets_pratama(with_gpr_panel=args.with_gpr_panel,
                                        with_zenodo_comparison=args.with_zenodo_comparison,
                                        with_host_prediction=args.with_host_prediction,
                                        with_cellular_annotation=args.with_cellular_annotation)

        smith = (Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home_pratama"), runtime=Runtime.APPTAINER)
                 if args.dry_run else get_agent("pratama"))

        print("Planning workflow...")
        sample_views = list(inputs.AsSamples("sequences::read_metadata"))
        # Expected to be [1], not len(samples) -- every run's read_metadata is
        # parented to the shared viromics::contig_study root the viral survey
        # pools under, which collapses AsSamples to one view spanning every
        # run. See build_inputs_pratama's docstring.
        print(f"AsSamples: {len(samples)} run(s) -> {len(sample_views)} solver "
              f"sample-view(s) (pooled under viromics::contig_study)")
        task = smith.GenerateWorkflow(
            samples=sample_views,
            resources=[containers, resource_lib,
                      build_globals_pratama(with_gpr_panel=args.with_gpr_panel,
                                            with_zenodo_comparison=args.with_zenodo_comparison)],
            transforms=build_transforms_for_pratama(),
            targets=targets,
        )
        if not task.ok:
            _report_plan_failure(task)
        _assert_given_counts(task, {
            "sequences::read_metadata": len(samples),
            "sequences::read_pair": len(samples),
            "sequences::zipped_forward_short_reads": len(samples),
            "sequences::zipped_reverse_short_reads": len(samples),
        })

        steps = task.plan.steps
        print(f"Plan OK -- {len(steps)} steps over {len(samples)} runs, key={task.GetKey()}")
        for s in steps:
            prods = sorted({i.dtype_name for g in s.produces for i in g})
            print(f"  {s.order:>2}. {Path(s.transform._path).stem:28s} -> {prods}")
        if task.plan.dropped_targets:
            print(f"\ndropped: {sorted(task.plan.dropped_targets)}")
            for h in (task.plan.hints or []):
                print(f"  hint: {getattr(h, 'kind', '?')} target={getattr(h, 'target', '?')}"
                      f" msg={getattr(h, 'message', '')}")

        if args.dry_run:
            print("\n(dry-run; nothing staged or submitted)")
            return 0

        keys_file = CACHE_DIR / "task_keys.json"
        keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
        keys[args.tag or f"pratama_{len(samples)}runs"] = task.GetKey()
        keys_file.write_text(json.dumps(keys, indent=2))

        print(f"Staging workflow to {HPC_HOST}...")
        smith.StageWorkflow(task, on_exist=args.on_exist, verify_external_paths=False)
        if args.stage_only:
            print(f"\n(stage-only; staged as {task.GetKey()})")
            return 0

        config = make_slurm_config(comebin_device=args.comebin_device,
                                  comebin_time=args.comebin_time,
                                  comebin_cpus=args.comebin_cpus)
        print(f"Submitting to SLURM (config: {config})...")
        smith.RunWorkflow(
            task=task, config_file=config,
            params=dict(slurmAccount=SLURM_ACCOUNT,
                        executor=dict(queueSize=500),
                        process=dict(tries=4, array=25),
                        # Per-binner parity with nf-core/mag, audited against its
                        # conf/modules.config at the pinned 5.5.0 revision. All three
                        # binners, not just metabat2 -- auditing one of three is how an
                        # unstated confound survives.
                        #   metabat2: nf-core `-m 1500` + `--seed 1`; metabat2 itself
                        #     defaults to 2500 and a RANDOM seed. nf-core passes no `-s`,
                        #     so minClsSize is 200000 on both sides and is NOT a deviation.
                        #   semibin2: nf-core `--min-len 1500` + `--random-seed 1`.
                        #     SemiBin2 unset derives its floor from `--ratio` 0.05 relative
                        #     to 2500 bp -- a different RULE, not just a different number --
                        #     and its seed is "set by the system", so our binning was not
                        #     reproducible run to run. `--environment global` already
                        #     matched nf-core's own default.
                        #   comebin: nf-core passes no args at all; ours passes
                        #     `-b min(usable_contigs, 1024)` and COMEBin's own default is
                        #     1024, so on any real sample these are IDENTICAL. The clamp
                        #     only bites on inputs too small for COMEBin to run.
                        #
                        # CAUTION these were INERT until bootstrap.py was taught to load
                        # workflow.params.yml -- a driver's params reached nextflow only.
                        metabat2_min_contig=1500, metabat2_seed=1,
                        semibin2_min_len=1500, semibin2_seed=1),
            resource_overrides={
                "bbduk":   Resources(memory=Size.GB(64), cpus=16),
                "megahit": Resources(memory=Size.GB(128), cpus=32,
                                     duration=Duration(hours=12)),
                # A `local`-labelled step runs on a login node, where nextflow's LOCAL
                # executor admits it only if its request fits `executor.cpus`/`.memory`
                # (8 cpus, 8 GB in the slurm preset). That admission check is fatal to the
                # whole session -- it fires before errorStrategy is consulted and cancels
                # everything already running, on slurm included. downloadDramDB declares
                # 64 GB, which is the right default for a full DRAM setup but not for the
                # five databases this transform actually selects: kofam_hmm, kofam_ko_list,
                # pfam, pfam_hmm and dbcan. No uniref, so no large diamond makedb. Measured
                # requirement is unknown; 8 GB is the ceiling the local executor allows and
                # the login node's per-user cgroup is 16 GiB shared, so a larger request
                # could not be honoured there anyway. Check the product, not the exit code:
                # success is DRAM.config existing.
                "downloadDramDB": Resources(cpus=4, memory=Size.GB(8),
                                            duration=Duration(hours=12)),
            },
        )
        print(f"Submitted: {task.GetKey()}")
        return 0

    if args.with_long_read:
        # A SEPARATE launch from the plain CAMI short-read run above, not a mode
        # folded into it -- see get_agent's HPC_MSM_HOME_CAMI_LONG comment and
        # build_inputs_long_read's docstring for why (every lineage gets its
        # own read_metadata; there is no shared-sample mode left to fold into
        # a short-read run). --variant is not used by this arm --
        # build_targets_long_read always runs the three-binner + DAS Tool tail,
        # since MetaWRAP needs short reads this arm never has.
        #
        # The FULL long-read corpus (enumerate_long_read_samples), not just
        # the toy_humangut pairing -- that pairing was scoped to a now-settled
        # question (does ancestry alone separate flye_raw from a short-read
        # assembler with no mask? yes, confirmed) and leaving the arm scoped
        # to it left 151 of 152 CAMI II long-read samples unreachable, which
        # defeated the point of the orchestrator's AMBER reversal. --sample/
        # --limit govern how much of the 172-sample corpus (152 CAMI II +
        # 20 CAMI III toy_humangut_long) actually runs; the arm itself must
        # only be able to reach all of it.
        samples = select(enumerate_long_read_samples(), args)
        if not samples:
            print("ERROR: no long-read samples found; "
                  "run list-samples --corpus cami --long-read", file=sys.stderr)
            return 1
        print(f"{len(samples)} long-read sample(s): "
              f"{', '.join(s[0] for s in samples)}")

        inputs = build_inputs_long_read(samples)
        containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
        resource_lib = DataInstanceLibrary.Load(MLIB / "resources" / "lib")
        targets = build_targets_long_read()

        smith = (Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home_cami_long"),
                       runtime=Runtime.APPTAINER)
                 if args.dry_run else get_agent("cami_long"))

        print("Planning workflow...")
        # resources carries build_globals_long_read(), NOT `inputs` -- passing the
        # whole per-sample library here is the case-collapse bug documented on
        # build_globals_long_read. Getting this wrong would not show up as an
        # error: the plan would still report OK with dropped_targets empty, just
        # silently missing one shape's steps. Every sample here is independently
        # registered (its own meta, only long_reads under it -- see
        # build_inputs_long_read), so mixing CAMI II's read-type-only samples
        # with CAMI III's toy_humangut ones (which also happen to have a
        # short-read sibling elsewhere, never registered here) is still ONE
        # shape; verified by solving at full corpus size, not assumed.
        task = smith.GenerateWorkflow(
            samples=list(inputs.AsSamples("sequences::read_metadata")),
            resources=[containers, resource_lib, build_globals_long_read()],
            transforms=build_transforms_for_long_read(),
            targets=targets,
        )
        if not task.ok:
            _report_plan_failure(task)
        n_truth = sum(1 for _, _, truth, _dataset in samples if truth is not None)
        _assert_given_counts(task, {
            "sequences::long_reads": len(samples),
            "sequences::read_metadata": len(samples),
            "binning::cami_read_truth": n_truth,
        })

        steps = task.plan.steps
        print(f"Plan OK -- {len(steps)} steps across {len(samples)} "
              f"samples, key={task.GetKey()}")
        for s in steps:
            prods = sorted({i.dtype_name for g in s.produces for i in g})
            print(f"  {s.order:>2}. {Path(s.transform._path).stem:28s} -> {prods}")
        if task.plan.dropped_targets:
            print(f"\ndropped: {sorted(task.plan.dropped_targets)}")
            for h in (task.plan.hints or []):
                print(f"  hint: {getattr(h, 'kind', '?')} target={getattr(h, 'target', '?')}"
                      f" msg={getattr(h, 'message', '')}")

        if args.dry_run:
            print("\n(dry-run; nothing staged or submitted)")
            return 0

        keys_file = CACHE_DIR / "task_keys.json"
        keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
        keys[args.tag or f"cami_long_{len(samples)}samples"] = task.GetKey()
        keys_file.write_text(json.dumps(keys, indent=2))

        print(f"Staging workflow to {HPC_HOST}...")
        smith.StageWorkflow(task, on_exist=args.on_exist, verify_external_paths=False)
        if args.stage_only:
            print(f"\n(stage-only; staged as {task.GetKey()})")
            return 0

        config = make_slurm_config(comebin_device=args.comebin_device,
                                  comebin_time=args.comebin_time,
                                  comebin_cpus=args.comebin_cpus)
        print(f"Submitting to SLURM (config: {config})...")
        smith.RunWorkflow(
            task=task, config_file=config,
            params=dict(slurmAccount=SLURM_ACCOUNT,
                        executor=dict(queueSize=500),
                        process=dict(tries=4, array=25),
                        # Per-binner parity with nf-core/mag, audited against its
                        # conf/modules.config at the pinned 5.5.0 revision. All three
                        # binners, not just metabat2 -- auditing one of three is how an
                        # unstated confound survives.
                        #   metabat2: nf-core `-m 1500` + `--seed 1`; metabat2 itself
                        #     defaults to 2500 and a RANDOM seed. nf-core passes no `-s`,
                        #     so minClsSize is 200000 on both sides and is NOT a deviation.
                        #   semibin2: nf-core `--min-len 1500` + `--random-seed 1`.
                        #     SemiBin2 unset derives its floor from `--ratio` 0.05 relative
                        #     to 2500 bp -- a different RULE, not just a different number --
                        #     and its seed is "set by the system", so our binning was not
                        #     reproducible run to run. `--environment global` already
                        #     matched nf-core's own default.
                        #   comebin: nf-core passes no args at all; ours passes
                        #     `-b min(usable_contigs, 1024)` and COMEBin's own default is
                        #     1024, so on any real sample these are IDENTICAL. The clamp
                        #     only bites on inputs too small for COMEBin to run.
                        #
                        # CAUTION these were INERT until bootstrap.py was taught to load
                        # workflow.params.yml -- a driver's params reached nextflow only.
                        metabat2_min_contig=1500, metabat2_seed=1,
                        semibin2_min_len=1500, semibin2_seed=1),
            resource_overrides={
                "bbduk":   Resources(memory=Size.GB(64), cpus=16),
                "megahit": Resources(memory=Size.GB(128), cpus=32,
                                     duration=Duration(hours=12)),
            },
        )
        print(f"Submitted: {task.GetKey()}")
        return 0

    samples = select(enumerate_samples(), args)
    if not samples:
        print("ERROR: no samples found; run list-samples", file=sys.stderr)
        return 1
    print(f"{len(samples)} sample(s): {', '.join(s for s, _ in samples)}")

    inputs = build_inputs(samples)
    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    # cami_contig_truth.py requires lib::cami_gold_standard.py, so the resource
    # library has to be given as well as the env one. Without it the chain is
    # unsatisfiable, and the planner does not report that: it explores the whole
    # library and then blames every unrelated target instead, dead-ending at
    # ncbi::genome_name and sequences::background_genome. One missing resource
    # library reads as a broken driver.
    resource_lib = DataInstanceLibrary.Load(MLIB / "resources" / "lib")
    targets = build_targets(with_dedup=not args.no_dedup, variant=args.variant)

    smith = (Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home"), runtime=Runtime.APPTAINER)
             if args.dry_run else get_agent())

    print("Planning workflow...")
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[containers, resource_lib, build_globals()],
        transforms=build_transforms_for(args.variant),
        targets=targets,
    )
    if not task.ok:
        _report_plan_failure(task)
    _assert_given_counts(task, {
        "sequences::short_reads_pe": len(samples),
        "sequences::read_metadata": len(samples),
        "binning::cami_read_truth": len(samples),
    })

    steps = task.plan.steps
    print(f"Plan OK -- {len(steps)} steps across {len(samples)} samples, key={task.GetKey()}")
    for s in steps:
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {Path(s.transform._path).stem:28s} -> {prods}")

    if args.dry_run:
        print("\n(dry-run; nothing staged or submitted)")
        return 0

    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[args.tag or f"cami_{len(samples)}samples"] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))

    print(f"Staging workflow to {HPC_HOST}...")
    smith.StageWorkflow(task, on_exist=args.on_exist, verify_external_paths=False)
    if args.stage_only:
        print(f"\n(stage-only; staged as {task.GetKey()})")
        return 0

    config = make_slurm_config(comebin_device=args.comebin_device,
                              comebin_time=args.comebin_time,
                              comebin_cpus=args.comebin_cpus)
    print(f"Submitting to SLURM (config: {config})...")
    smith.RunWorkflow(
        task=task, config_file=config,
        params=dict(slurmAccount=SLURM_ACCOUNT,
                    executor=dict(queueSize=500),
                    process=dict(tries=4, array=25),
                        # Per-binner parity with nf-core/mag, audited against its
                        # conf/modules.config at the pinned 5.5.0 revision. All three
                        # binners, not just metabat2 -- auditing one of three is how an
                        # unstated confound survives.
                        #   metabat2: nf-core `-m 1500` + `--seed 1`; metabat2 itself
                        #     defaults to 2500 and a RANDOM seed. nf-core passes no `-s`,
                        #     so minClsSize is 200000 on both sides and is NOT a deviation.
                        #   semibin2: nf-core `--min-len 1500` + `--random-seed 1`.
                        #     SemiBin2 unset derives its floor from `--ratio` 0.05 relative
                        #     to 2500 bp -- a different RULE, not just a different number --
                        #     and its seed is "set by the system", so our binning was not
                        #     reproducible run to run. `--environment global` already
                        #     matched nf-core's own default.
                        #   comebin: nf-core passes no args at all; ours passes
                        #     `-b min(usable_contigs, 1024)` and COMEBin's own default is
                        #     1024, so on any real sample these are IDENTICAL. The clamp
                        #     only bites on inputs too small for COMEBin to run.
                        #
                        # CAUTION these were INERT until bootstrap.py was taught to load
                        # workflow.params.yml -- a driver's params reached nextflow only.
                        metabat2_min_contig=1500, metabat2_seed=1,
                        semibin2_min_len=1500, semibin2_seed=1),
        resource_overrides={
            "bbduk":   Resources(memory=Size.GB(64), cpus=16),
            "megahit": Resources(memory=Size.GB(128), cpus=32,
                                 duration=Duration(hours=12)),
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
            print(f"  {type(e).__name__}: {e}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("list-samples")
    p.add_argument("--corpus", default="cami", choices=["cami", "pratama"])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stage-only", action="store_true")
    p.add_argument("--allow-partial", action="store_true")
    p.add_argument("--long-read", action="store_true",
                   help="cami only: list the long-read arm's samples (toy_humangut "
                        "short+long pairs, then long-read-only rows) instead of the "
                        "normal short-read core/variant corpus.")
    p.set_defaults(fn=cmd_list_samples)

    sub.add_parser("check-dbs").set_defaults(fn=cmd_check_dbs)

    p = sub.add_parser("setup", help="pull the container images onto the cluster")
    p.add_argument("--run", action="store_true")
    p.set_defaults(fn=cmd_setup)

    p = sub.add_parser("run")
    p.add_argument("--corpus", default="cami", choices=["cami", "pratama"])
    p.add_argument("--sample", nargs="*")
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stage-only", action="store_true")
    p.add_argument("--no-dedup", action="store_true")
    p.add_argument("--on-exist", default="update", choices=["update", "clear"])
    # cpu by request, with the GPU lane one flag away. Measured on a marine sample,
    # 200 epochs of 70 iterations: 25.0 s/epoch on a 20 GB MIG slice against 145 s/epoch
    # on 64 cores, so the card is worth 5.8x and finishes the whole step in 2 h 20 m
    # against roughly 9 h. It is not worth more than that because the shipped COMEBin
    # image's PyTorch has no sm_90 cubin and JIT-compiles every kernel forward from
    # compute_50 PTX on fir's H100s.
    #
    # 24h, and that is the load-bearing part. Under the old 8 h wall the CPU lane
    # reached 191, 195 and 28 epochs of 200 across three attempts and never once
    # finished -- twice missing by about twenty minutes. `--nv` with no card present
    # warns once and trains on the CPU anyway, so an under-timed CPU request reads as
    # a GPU request that failed. The wall is the fix.
    p.add_argument("--variant", default="core", choices=["core", "variant"],
                   help="core: metaSPAdes + MetaWRAP + AMBER. variant: MEGAHIT + three binners + skANI.")
    p.add_argument("--allow-partial", action="store_true",
                   help="launch even though runs of the corpus are still downloading; the "
                        "viral clustering pools across every run, so a partial corpus defines "
                        "a different vOTU set and cannot be topped up later.")
    p.add_argument("--with-gpr-panel", action="store_true",
                   help="pratama only: add annotation::gpr_table, which pulls KOfamScan, "
                        "CLEAN, DIAMOND UniRef50 and ProteinBERT in behind it and needs "
                        "ref::mnxr_lookup and ref::label_transfer_landmarks, neither of "
                        "which is sourced yet.")
    p.add_argument("--with-zenodo-comparison", action="store_true",
                   help="pratama only: add pratama::mag_recovery_table and "
                        "pratama::votu_recovery_table (T17's skani-dist comparison "
                        "against Pratama's published MAGs and vOTU catalogue). Needs "
                        "pratama::published_mags and pratama::published_votus, neither "
                        "of which is unzipped anywhere yet -- the Zenodo record is on "
                        "fir only as the zips it arrived in.")
    p.add_argument("--with-cellular-annotation", action="store_true",
                   help="pratama only: add annotation::dram_annotations, the CELLULAR "
                        "annotation lane. OFF by default and deliberately: it answers no "
                        "acceptance criterion (the AMG calls are DRAM-v's) and measures at "
                        "~8,900 tasks of 8 cpus and 64 GB, on the order of 36,000-71,000 "
                        "core-hours, when it binds the whole assembly's 672,121 ORFs.")
    p.add_argument("--with-host-prediction", action="store_true",
                   help="pratama only: add viromics::host_prediction_genome, which "
                        "pulls gtdbtk_de_novo (32 cpus, 240 GB, a 48 h wall over "
                        "189,805 taxa) and the iPHoP chain plus its ~0.5 TB database "
                        "in behind it. OFF by default: acceptance criterion 2 asks "
                        "for a vOTU catalogue, MAGs and AMG calls, and host "
                        "prediction is not among them. The MAG chain it used to "
                        "reach is now named directly, so turning this off no longer "
                        "removes the MAG comparison -- see build_targets_pratama.")
    p.add_argument("--with-long-read", action="store_true",
                   help="cami only: a SEPARATE launch (own agent home, own task) "
                        "that plans a raw-Flye long-read arm against toy_humangut's "
                        "short+long pairing plus one long-read-only sample, instead "
                        "of the normal short-read core/variant run. Not scored -- "
                        "see build_targets_long_read.")
    p.add_argument("--comebin-device", default="cpu", choices=["cpu", "gpu"])
    p.add_argument("--comebin-time", default="3d")
    p.add_argument("--comebin-cpus", type=int, default=48)
    p.add_argument("--tag")
    p.set_defaults(fn=cmd_run)

    sub.add_parser("status").set_defaults(fn=cmd_status)

    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
