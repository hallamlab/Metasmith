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
pinned to metaSPAdes rather than MEGAHIT. That survey's cross-sample tools pool under one
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
SETUP_COMMANDS = ["module load apptainer"]

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

AGENT_IMAGE = os.environ.get(
    "MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.22.1")

CONTAINERS = [
    "seqkit", "bbtools", "megahit", "samtools", "minimap2", "bedtools",
    "pprodigal", "diamond", "kofamscan", "polars", "python_for_data_science",
    "metabat2", "semibin", "comebin", "checkm", "skani", "amber",
]


def ssh_cmd(cmd, timeout=180, check=True):
    r = subprocess.run(["ssh", HPC_HOST, cmd], capture_output=True, text=True,
                       timeout=timeout)
    if check and r.returncode != 0:
        print(f"ssh stderr: {r.stderr}", file=sys.stderr)
        raise RuntimeError(f"ssh command failed: {cmd}")
    return r.stdout.strip(), r.returncode


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


def get_agent(corpus="cami"):
    home = HPC_MSM_HOME if corpus == "cami" else HPC_MSM_HOME_PRATAMA
    return Agent(
        home=SshSource(host=HPC_HOST, path=home).AsSource(),
        container=AGENT_IMAGE,
        runtime=Runtime.APPTAINER,
        setup_commands=SETUP_COMMANDS,
    )


def enumerate_samples():
    """(sample_id, remote reads path) for every CAMI sample.

    Read from the tracked manifest research/cami/samples.tsv (229 rows across six
    datasets: marine, strain, toy_mousegut, toy_hmp_airskinurogenital,
    plant_associated, toy_hmp_gastrooral) rather than a directory glob -- a glob
    matching only marine's own nesting silently limited every downstream driver
    to 10 of the 229 samples, with no error, because the other five subtrees
    nest reads differently and a glob that finds nothing is not a glob that
    fails. `sample_id` is unique only WITHIN a dataset ("sample_0" recurs in
    several), so the id returned here is namespaced with the dataset -- without
    that, two different datasets' samples collide on one `_stable_id`.

    Every row here has has_truth=1 (spot-checked on fir across the strain and
    gastrooral subtrees too), so cami_contig_truth's bridge reaches every
    sample. Only 110 of 229 have has_binning_gs=1 -- 119 samples have no
    gold-standard bin file at all, which is why AMBER scores through the
    read-truth bridge rather than through binning_gs.tsv directly.

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
    return [(f"{r['dataset']}_{r['sample_id']}", Path(r["reads_path"])) for r in rows]


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

    inputs.Save()
    return inputs


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
        binners = ("metabat2", "semibin2", "comebin")

    bins = [t.Add(f"sequences::{b}_bin_fasta", parents=[asm]) for b in binners]
    tables = [t.Add(f"binning::{b}_contig_to_bin_table", parents=[asm]) for b in binners]
    for b in bins:
        t.Add("taxonomy::checkm_stats", parents=[b])
    # One amber target per binner, pinned to that binner's own table, exactly as
    # checkm_stats is pinned per bin set. Pinned to the assembly instead, the
    # planner satisfies the slot once and scores ONE binner -- a solve that
    # succeeds and silently answers a fraction of the question. amber emits both
    # its products in one step, so naming amber_bin_metrics too would buy nothing
    # and cost a slot.
    for tb in tables:
        t.Add("binning::amber_results", parents=[tb])
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
        inputs.AddItem(DEFERRED, "pratama::published_mags")
        inputs.AddItem(DEFERRED, "pratama::published_votus")

    inputs.Save()
    return inputs


def build_targets_pratama(with_gpr_panel=False, with_zenodo_comparison=False):
    """metaSPAdes (JGI protocol) + MetaWRAP + CheckM2, plus the viral survey.

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

    THE ASSEMBLY CONFLICT, surfaced rather than resolved: Pratama's own metaSPAdes call
    is `spades.py --meta -k 21,33,55,77 -m 190`, no error-correction pass. `asm` here is
    the ONE spades transform this library carries, and it runs the DOE JGI protocol
    instead -- bbcms error correction, `--only-assembler -k 33,55,77,99,127`, a 200 bp
    seqkit filter -- because that is what batch 1's citable-core framing requires and a
    second spades transform was rejected before (src/metasmith_libraries/AGENTS.md).
    Faithful-to-Pratama and citable-core cannot both hold for this step; batch 1 keeps
    the JGI protocol and the gap is Pratama's real assembly parameters, not a detail.

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
    asm = t.Add("sequences::spades_assembly")
    frozen = t.Add("viromics::dereplicated_candidate_virus")

    # cross-sample tools, pooled on the frozen set (viromics TARGETS[2:9] minus
    # kofamscan_descriptions -- see the docstring)
    for dtype in ("viromics::contig_length_table", "viromics::precluster_table",
                  "viromics::votu_cluster_table", "viromics::checkv_contamination",
                  "viromics::vcontact3_network",
                  "viromics::host_prediction_genome", "viromics::spacer_host_links"):
        t.Add(dtype, parents=[frozen])
    if with_zenodo_comparison:
        t.Add("pratama::votu_recovery_table", parents=[frozen])
    # per-sample viral work that nothing above reaches (viromics TARGETS[10], [12])
    per_sample = ["annotation::dramv_distill", "annotation::dram_annotations"]
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
        _assembly_without("megahit"),
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
        # A quarter of a fir compute node. They are 192-core AMD Turin (8 sockets of 24,
        # one thread per core) with 768 GB, and the `cpubase_bycore_*` partitions
        # schedule partial nodes, so this queues like a normal job. Memory is 4 GB per
        # core by entitlement, so 96 GB is free headroom rather than a larger ask.
        #
        # 48 rather than 96 because comebin's training is Amdahl-limited and the
        # allocation, not the wall, is the scarce thing. Halving the cores costs well
        # under double the wall and saves real core-hours.
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
        body = [
            f"        cpus = {comebin_cpus}",
            "        memory = '96 GB'",
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
    text = base + "\n" + "\n".join(
        ["", "process {", "    withName: '.*__comebin' {", *body, "    }", "}", "",
         "process {", "    withName: '.*_cached' {", "        array = 0", "    }", "}", ""])
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


def cmd_list_samples(args):
    if args.corpus == "pratama":
        kept, report = enumerate_pratama_runs()
        print(report)
        for run, dataset, fwd, rev in kept:
            print(f"{run:14s} {dataset:10s} {fwd}")
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
                  "(metaSPAdes + MetaWRAP + CheckM2 + viral survey); "
                  "the MEGAHIT/three-binner variant was never asked for and is "
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
                                        with_zenodo_comparison=args.with_zenodo_comparison)

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
            resources=[containers, resource_lib, inputs],
            transforms=build_transforms_for_pratama(),
            targets=targets,
        )
        if not task.ok:
            _report_plan_failure(task)

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
        print("ERROR: submission for pratama is not wired up past staging; "
              "pass --stage-only", file=sys.stderr)
        return 1

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
        resources=[containers, resource_lib, inputs],
        transforms=build_transforms_for(args.variant),
        targets=targets,
    )
    if not task.ok:
        _report_plan_failure(task)

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
                    process=dict(tries=4, array=25)),
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
