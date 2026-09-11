#!/usr/bin/env python3
"""Assembly, binning and functional annotation over CAMI samples on fir.

The CAMI reads arrive interleaved in one anonymous_reads.fq.gz per sample, which the
library takes directly: short_reads_pe extends the short_reads that bbduk requires, so
nothing deinterleaves. The metadata parity must still read "paired" -- bbduk asserts on
{single, paired} and turns "paired" into its int=t flag.

Subcommands: list-samples, check-dbs, setup, run [--dry-run], status.
"""

import argparse
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
    TargetBuilder, Runtime,
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
# One directory per CAMI subtree that has been unpacked into per-sample reads.
READS_GLOB = os.environ.get(
    "CAMI_READS_GLOB",
    str(CAMI_ROOT / "work" / "marine_short_read" / "simulation_short_read"
        / "*" / "reads" / "anonymous_reads.fq.gz"))

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
    "metabat2", "semibin", "comebin", "checkm", "skani",
]


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


def enumerate_samples():
    """(sample_id, remote reads path), read off the cluster rather than guessed."""
    out, _ = ssh_cmd(f"ls {READS_GLOB} 2>/dev/null || true")
    samples = []
    for line in sorted(p.strip() for p in out.splitlines() if p.strip()):
        # .../<timestamp>_sample_N/reads/anonymous_reads.fq.gz -> sample_N
        stem = Path(line).parent.parent.name
        m = re.search(r"(sample_\d+)$", stem)
        sid = m.group(1) if m else stem
        samples.append((sid, Path(line)))
    return samples


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


def _stable_id(*parts: str) -> str:
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
    """
    from metasmith.caching.keys import multihash_key
    return multihash_key("\x00".join(("cami",) + parts).encode("utf-8")).hex()


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
            instance_id=_stable_id("read_metadata", sid, meta_value),
        )
        inputs.RegisterItem(
            reads, "sequences::short_reads_pe", parents={meta},
            instance_id=_stable_id("short_reads_pe", sid, str(reads)),
        )

    for dtype, path in DB_PATHS.items():
        inputs.RegisterItem(path, dtype, instance_id=_stable_id("ref", dtype, str(path)))

    inputs.Save()
    return inputs


def build_transforms():
    return [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "assembly"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
    ]


def build_targets(with_dedup=True):
    """Assembly, binning and annotation only: the taxonomy lane is deliberately absent.

    Every assembly-derived target is pinned to the megahit assembly. Unpinned, spades
    also satisfies `sequences::assembly` and the planner may answer each target from a
    different assembler, running both; the cost lands on the refiner rather than the
    search. See the comment in metagenomics_from_paired_reads.py for the measurements.
    """
    t = TargetBuilder()
    asm = t.Add("sequences::megahit_assembly")
    t.Add("sequences::read_qc_stats")
    for dtype in ("sequences::orfs",
                  "sequences::gff",
                  "sequences::assembly_stats",
                  "sequences::assembly_per_contig_coverage",
                  "alignment::bam",
                  "annotation::kofamscan_results",
                  "annotation::diamond_uniref50_results"):
        t.Add(dtype, parents=[asm])

    bins = [t.Add(f"sequences::{b}_bin_fasta", parents=[asm])
            for b in ("metabat2", "semibin2", "comebin")]
    for b in ("metabat2", "semibin2", "comebin"):
        t.Add(f"binning::{b}_contig_to_bin_table", parents=[asm])
    for b in bins:
        t.Add("taxonomy::checkm_stats", parents=[b])
    if with_dedup:
        t.Add("binning_local::cluster_table", parents=[asm])
    return t


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
    samples = select(enumerate_samples(), args)
    if not samples:
        print("ERROR: no samples found; run list-samples", file=sys.stderr)
        return 1
    print(f"{len(samples)} sample(s): {', '.join(s for s, _ in samples)}")

    inputs = build_inputs(samples)
    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    targets = build_targets(with_dedup=not args.no_dedup)

    smith = (Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home"), runtime=Runtime.APPTAINER)
             if args.dry_run else get_agent())

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

    sub.add_parser("list-samples").set_defaults(fn=cmd_list_samples)
    sub.add_parser("check-dbs").set_defaults(fn=cmd_check_dbs)

    p = sub.add_parser("setup", help="pull the container images onto the cluster")
    p.add_argument("--run", action="store_true")
    p.set_defaults(fn=cmd_setup)

    p = sub.add_parser("run")
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
