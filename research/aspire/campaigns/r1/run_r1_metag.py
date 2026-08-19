import os
import re
import sys
import json
import argparse
import subprocess
from pathlib import Path

os.environ["PATH"] = f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}"

from metasmith.python_api import (  # noqa: E402
    Agent, Source, SshSource,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Runtime,
    Resources, Size, Duration,
)

ROOT = Path(__file__).resolve().parent
MLIB      = Path(os.environ.get(
    "MSM_LIB",
    str(Path(__file__).resolve().parents[4] / "src" / "metasmith_libraries")))
CACHE_DIR = Path(os.environ.get("MSM_CACHE_DIR", ROOT / ".cache"))
SAMPLES_TSV = ROOT / "samples.tsv"

HPC_HOST       = os.environ.get("MSM_HPC_HOST", "fir")
SLURM_ACCOUNT  = os.environ.get("MSM_SLURM_ACCOUNT", "rrg-shallam-ab")
GPU_ACCOUNT    = os.environ.get("MSM_GPU_ACCOUNT", "def-shallam_gpu")
SETUP_COMMANDS = ["module load apptainer"]

HPC_USER      = os.environ.get("MSM_HPC_USER", "phyberos")
HPC_SCRATCH   = Path(f"/scratch/{HPC_USER}")
HPC_MSM_HOME  = Path(os.environ.get(
    "MSM_AGENT_HOME", str(HPC_SCRATCH / "gmcf3495" / "metasmith")))
HPC_READS_DIR = Path(os.environ.get("MSM_READS_DIR", str(HPC_SCRATCH / "gmcf3495" / "reads")))

DB_ROOT = Path("/home/phyberos/project-rpp/lib")
DB_PATHS = {
    "ref::uniref50_diamond_db": DB_ROOT / "diamond" / "uniref50.dmnd",
    "ref::kofamscan_profiles":  DB_ROOT / "kofamscan" / "profiles.tgz",
    "ref::kofamscan_ko_list":   DB_ROOT / "kofamscan" / "ko_list.tsv",
    "annotation::eggnog_data":  Path("/scratch/phyberos/databases/eggnog"),
    "ref::kraken2_db":          DB_ROOT / "kraken2_2026",
    "ref::centrifuger_db":      DB_ROOT / "centrifuger_r232",
}

DB_PROBE_SUFFIX = {}

GLOBUS_SRC_EP   = "2602486c-1e0f-47a0-be15-eec1b0ff0f96"
GLOBUS_SRC_ROOT = "/Received_raw_data/GMCF_3495"
GLOBUS_DST_EP   = "8dec4129-9ab4-451d-a45f-5b4b8471f7a3"

R1_CONTAINERS = [
    "seqkit", "bbtools", "megahit", "samtools", "minimap2", "bedtools",
    "pprodigal", "diamond", "kofamscan", "eggnog-mapper",
    "proteinbert", "polars",
    "kraken2", "bracken", "centrifuger", "python_for_data_science",
    "metabat2", "semibin", "comebin", "checkm", "skani",
]


def ssh_cmd(cmd, timeout=180, check=True):
    result = subprocess.run(
        ["ssh", HPC_HOST, cmd], capture_output=True, text=True, timeout=timeout,
    )
    if check and result.returncode != 0:
        print(f"ssh stderr: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"ssh command failed: {cmd}")
    return result.stdout.strip(), result.returncode


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
    rows = []
    for line in SAMPLES_TSV.read_text().splitlines():
        if not line.strip() or line.startswith(("#", "sample_id")):
            continue
        sid, r1, r2, b1, b2 = line.split("\t")
        rows.append((sid, r1, r2, int(b1), int(b2)))
    return sorted(rows, key=lambda r: _sid_sort_key(r[0]))


def remote_reads(sid):
    return HPC_READS_DIR / f"{sid}_R1.fastq.gz", HPC_READS_DIR / f"{sid}_R2.fastq.gz"


def enumerate_samples(from_cluster=False):
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


INTERLEAVED_DIR = Path(os.environ.get(
    "MSM_INTERLEAVED_DIR", "/scratch/phyberos/gmcf3495/interleaved_backup"))
USE_INTERLEAVED = not os.environ.get("MSM_NO_INTERLEAVED")


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

        if USE_INTERLEAVED:
            inputs.AddItem(INTERLEAVED_DIR / f"{sid}.interleaved.fq.gz",
                           "sequences::short_reads", parents={pair})

    for dtype, path in DB_PATHS.items():
        inputs.AddItem(path, dtype)

    _apply_leaf_pins(inputs)
    inputs.Save()
    return inputs


LEAF_PINS = Path(os.environ.get("MSM_LEAF_PINS", ROOT / "leaf_ids_r1_0201.json"))


def cmd_dump_pins(args):
    samples = select(enumerate_samples(from_cluster=False), args)
    prev = os.environ.get("MSM_NO_LEAF_PINS")
    os.environ["MSM_NO_LEAF_PINS"] = "1"
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
    if os.environ.get("MSM_NO_LEAF_PINS"):
        print("leaf pins DISABLED (MSM_NO_LEAF_PINS set) — upstream steps will miss")
        return
    if not LEAF_PINS.exists():
        print(f"no leaf pin file at {LEAF_PINS}; ids will be freshly minted")
        return

    pins = json.loads(LEAF_PINS.read_text())["given"]

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

    t.Add("sequences::read_qc_stats")
    t.Add("sequences::megahit_assembly")
    t.Add("sequences::orfs")
    t.Add("sequences::gff")
    t.Add("sequences::assembly_stats")
    t.Add("sequences::assembly_per_contig_coverage")
    t.Add("sequences::assembly_per_bp_coverage")
    t.Add("alignment::bam")

    t.Add("annotation::diamond_uniref50_results")
    t.Add("annotation::kofamscan_results")
    t.Add("annotation::eggnog_results")
    t.Add("annotation::proteinbert_embeddings")

    t.Add("taxonomy::kraken2_report")
    t.Add("taxonomy::bracken_species")
    t.Add("taxonomy::centrifuger_kreport")
    t.Add("taxonomy::centrifuger_summary")

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
        t.Add("binning_local::cluster_table")
    return t


def make_slurm_config(comebin_device="cpu", comebin_threads=64,
                      comebin_mem="48 GB", comebin_time="8h"):
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
    smith = get_agent()
    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    logistics = TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics")

    wl = {Path(f"{n}.env") for n in R1_CONTAINERS}
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

    dag_base = args.dag_out or (CACHE_DIR / "r1_dag_current")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
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
        print(f"\n(stage-only; staged as {task.GetKey()}, nothing submitted)")
        return 0

    config = make_slurm_config(comebin_device=args.comebin_device,
                               comebin_time=args.comebin_time)
    print(f"Submitting to SLURM (config: {config})...")
    smith.RunWorkflow(
        task=task,
        config_file=config,
        params=dict(
            slurmAccount=SLURM_ACCOUNT,
            process=dict(tries=4),
        ),
        resource_overrides={
            "bbduk":   Resources(memory=Size.GB(64), cpus=16),
            "megahit": Resources(memory=Size.GB(64), cpus=32),
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
    p_run.add_argument("--comebin-time", default="8h")
    p_run.add_argument("--stage-only", action="store_true",
                       help="stage the workflow but do not submit; lets the "
                            "staged cache keys be checked against the cache first")
    p_run.add_argument("--dag-out", default=None, help="path stem for the rendered DAG")
    p_run.add_argument("--tag", default=None)
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
