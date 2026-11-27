#!/usr/bin/env python3
"""MetaPathways 4 + PathoLogic over the EngCyc OMD genomes on fir, one task per batch.

Subcommands: plan, link, setup, import, run [--dry-run], status.

`plan` deals the manifest's genomes into batches and writes batches.tsv. `link`
builds each batch on fir as a directory of hardlinks named <ENG id>.fa into the
staged FASTAs, so a batch costs no inodes and every output is named by ENG id.
Batches enter the agent's pool by import, once, under the name engcyc/omd/<batch>;
every run after that cites the name, which keeps the plan key stable for -resume.
"""

import argparse
import csv
import gzip
import json
import os
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
    "MSM_LIB", str(Path(__file__).resolve().parents[3] / "src" / "metasmith_libraries")))
CACHE_DIR = Path(os.environ.get("MSM_CACHE_DIR", ROOT / ".cache"))

HPC_HOST      = os.environ.get("MSM_HPC_HOST", "fir")
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "rrg-shallam-ab")
SETUP_COMMANDS = ["module load apptainer"]
AGENT_IMAGE = os.environ.get(
    "MSM_AGENT_IMAGE", "docker://quay.io/hallamlab/metasmith:0.24.2")

MANIFEST = Path(os.environ.get("ENGCYC_MANIFEST", "<ENGCYC_mapping.csv>"))
LISTING  = Path(os.environ.get("ENGCYC_LISTING", "<genomes-fasta listing .tsv.gz>"))

OMD_ROOT     = Path(os.environ.get("ENGCYC_OMD_ROOT", "/scratch/phyberos/engcyc"))
FASTA_DIR    = OMD_ROOT / "fasta"
BATCH_DIR    = OMD_ROOT / "batches"
HPC_MSM_HOME = Path(os.environ.get("MSM_AGENT_HOME", str(OMD_ROOT / "metasmith")))
MPDB_ROOT    = Path(os.environ.get("ENGCYC_MPDB_ROOT", "/scratch/phyberos/mpdb"))

# Pre-pulled images adopted into the agent's container store under the names the
# engine derives from each env's `container:` URI. The Pathway Tools image is
# licensed and cannot be pulled; the store directory is kept mode 700 for it.
LOCAL_IMAGES = {
    "docker..quay.io_hallamlab_metapathways..4.0.1.sif":
        Path("/scratch/phyberos/mp4/metapathways_4.0.1.sif"),
    "docker..quay.io_txyliu_pathologic..latest.sif":
        Path("/scratch/phyberos/mp4/private/pathologic.sif"),
}

# The engine's image store is $APPTAINER_CACHEDIR when it is set, and fir's
# ~/.bashrc points it at the shared cache, where the licensed image must not go.
SETUP_COMMANDS.append(f"export APPTAINER_CACHEDIR={HPC_MSM_HOME / 'container_images'}")

TYPE_LIBS = ["engcyc.yml", "lib.yml", "env.yml"]
BATCHES_TSV = CACHE_DIR / "batches.tsv"


def ssh_cmd(cmd, timeout=180, check=True, stdin=None):
    r = subprocess.run(["ssh", HPC_HOST, cmd], capture_output=True, text=True,
                       timeout=timeout, input=stdin)
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


def read_genomes():
    """(eng_id, original_file, bytes) for every manifest row."""
    for p in (MANIFEST, LISTING):
        if str(p).startswith("<"):
            sys.exit(f"ERROR: set ENGCYC_MANIFEST and ENGCYC_LISTING (unset: {p})")
    sizes = {}
    with gzip.open(LISTING, "rt") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            sizes[row["name"]] = int(row["size"])
    genomes = []
    with open(MANIFEST) as f:
        for row in csv.DictReader(f):
            eng = row["ENGCYC_file"].removesuffix(".fa")
            genomes.append((eng, row["original_file"], sizes.get(row["original_file"], -1)))
    missing = [g for g in genomes if g[2] < 0]
    if missing:
        print(f"WARNING: {len(missing)} manifest genomes absent from the listing, "
              f"e.g. {missing[0][1]}", file=sys.stderr)
    return genomes


def deal(genomes, batch_size, prefix):
    """Batches of similar total size: largest genomes dealt round-robin."""
    genomes = sorted((g for g in genomes if g[2] >= 0), key=lambda g: (-g[2], g[0]))
    n = max(1, -(-len(genomes) // batch_size))
    batches = {f"{prefix}{i:05d}": [] for i in range(n)}
    names = list(batches)
    for i, g in enumerate(genomes):
        batches[names[i % n]].append(g)
    return batches


def pilot_pick(genomes, k):
    """k genomes spread evenly over the size distribution, smallest to largest."""
    ranked = sorted((g for g in genomes if g[2] >= 0), key=lambda g: (g[2], g[0]))
    step = (len(ranked) - 1) / max(1, k - 1)
    return [ranked[round(i * step)] for i in range(k)]


def read_batches():
    if not BATCHES_TSV.exists():
        sys.exit(f"ERROR: no {BATCHES_TSV}; run `plan` first")
    batches = {}
    with open(BATCHES_TSV) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            batches.setdefault(row["batch"], []).append(
                (row["eng_id"], row["original_file"], int(row["bytes"])))
    return batches


def select(batches, args):
    names = sorted(batches)
    if getattr(args, "batch", None):
        wanted = set(args.batch)
        missing = wanted - set(names)
        if missing:
            sys.exit(f"ERROR: batch(es) not in plan: {sorted(missing)}")
        names = [b for b in names if b in wanted]
    if getattr(args, "limit", None):
        names = names[: args.limit]
    return names


def cmd_plan(args):
    genomes = read_genomes()
    if args.pilot:
        batches = deal(pilot_pick(genomes, args.pilot), args.batch_size, "pilot")
    else:
        batches = deal(genomes, args.batch_size, "b")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(BATCHES_TSV, "w") as f:
        f.write("batch\teng_id\toriginal_file\tbytes\n")
        for b, members in batches.items():
            for eng, orig, size in sorted(members):
                f.write(f"{b}\t{eng}\t{orig}\t{size}\n")
    sizes = [sum(g[2] for g in m) for m in batches.values()]
    print(f"{sum(len(m) for m in batches.values())} genomes in {len(batches)} batches "
          f"-> {BATCHES_TSV}")
    print(f"batch bytes: min {min(sizes):,} max {max(sizes):,}")
    return 0


def cmd_link(args):
    batches = read_batches()
    rows = "".join(f"{b}\t{eng}\t{orig}\n"
                   for b in select(batches, args) for eng, orig, _ in batches[b])
    script = f"""
        set -u
        missing=0 linked=0
        while IFS=$'\\t' read -r b eng orig; do
            mkdir -p "{BATCH_DIR}/$b"
            dest="{BATCH_DIR}/$b/$eng.fa"
            [ -e "$dest" ] && continue
            if [ -e "{FASTA_DIR}/$orig" ]; then
                ln "{FASTA_DIR}/$orig" "$dest" && linked=$((linked+1))
            else
                echo "MISSING $orig"; missing=$((missing+1))
            fi
        done
        echo "linked $linked, missing $missing"
    """
    out, _ = ssh_cmd(script, timeout=1800, stdin=rows)
    print(out)
    return 0


def cmd_setup(args):
    store = HPC_MSM_HOME / "container_images"
    links = "; ".join(
        f'[ -e "{store}/{name}" ] || ln "{src}" "{store}/{name}"'
        for name, src in LOCAL_IMAGES.items())
    out, _ = ssh_cmd(f'mkdir -p "{store}" && chmod 700 "{store}" && {links} && ls -la "{store}"')
    print(out)
    if args.deploy:
        get_agent().Deploy(assertive=True)
    return 0


def declare_givens(batch_names, db, smith):
    givens = smith.PoolGivens()
    for b in batch_names:
        givens.Add(BATCH_DIR / b, "engcyc::genome_batch",
                   name=f"engcyc/omd/{b}", tags=["engcyc", b])
    givens.Add(MPDB_ROOT / db, "engcyc::metapathways_db",
               name=f"engcyc/ref/mpdb_{db}", tags=["engcyc", "reference"])
    return givens


def build_inputs(batch_names, db, smith, *, ensure=False):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return declare_givens(batch_names, db, smith).Build(
        CACHE_DIR / f"omd_inputs_{db}.xgdb",
        type_library_paths=[MLIB / "data_types" / tl for tl in TYPE_LIBS],
        ensure=ensure,
    )


def make_slurm_config(smith):
    # Every cacheable step compiles to a sibling `<name>_cached` process with an
    # inline `executor 'local'` that the preset's label-keyed array exemption never
    # reaches, and Nextflow refuses a local executor with job arrays.
    base = Path(smith.GetNxfConfigPresets()["slurm"]).read_text()
    text = base + "\n" + "\n".join(
        ["", "process {", "    withName: '.*_cached' {", "        array = 0", "    }", "}", ""])
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out = CACHE_DIR / "fir_slurm_omd.config"
    out.write_text(text)
    return out


def _report_plan_failure(task):
    print("ERROR: workflow generation failed", file=sys.stderr)
    for h in getattr(task.plan, "hints", []) or []:
        print(f"  [{h.kind}] target={getattr(h, 'target', '?')}: {getattr(h, 'message', '')}")
    sys.exit(1)


def cmd_import(args):
    names = select(read_batches(), args)
    smith = get_agent()
    found = smith.EnsurePoolEntries(declare_givens(names, args.db, smith).items)
    print(f"pool holds {len(found)} entries for {len(names)} batch(es)")
    return 0


def cmd_run(args):
    names = select(read_batches(), args)
    print(f"{len(names)} batch(es), db {args.db}")
    real = get_agent()
    inputs = build_inputs(names, args.db, real, ensure=args.import_inputs)
    smith = (Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home"), runtime=Runtime.APPTAINER)
             if args.dry_run else real)
    containers = DataInstanceLibrary.Load(MLIB / "resources" / "env")
    resource_lib = DataInstanceLibrary.Load(MLIB / "resources" / "lib")
    targets = TargetBuilder()
    targets.Add("engcyc::pgdb_batch")

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("engcyc::genome_batch")),
        resources=[containers, resource_lib, inputs],
        transforms=[TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation")],
        targets=targets,
    )
    if not task.ok:
        _report_plan_failure(task)
    print(f"Plan OK -- {len(task.plan.steps)} steps, key={task.GetKey()}")
    if args.dry_run:
        print("(dry-run; nothing staged or submitted)")
        return 0

    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[args.tag or f"omd_{args.db}_{len(names)}batches"] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))

    smith.StageWorkflow(task, on_exist=args.on_exist, verify_external_paths=False)
    config = make_slurm_config(smith)
    smith.RunWorkflow(
        task=task, config_file=config,
        params=dict(slurmAccount=SLURM_ACCOUNT,
                    executor=dict(queueSize=args.queue_size),
                    process=dict(tries=2, array=args.array)),
        resource_overrides={
            "metapathways_pgdb": Resources(
                cpus=args.cpus, memory=Size.GB(args.mem_gb),
                duration=Duration(hours=args.hours)),
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

    p = sub.add_parser("plan", help="deal the manifest into batches.tsv")
    p.add_argument("--batch-size", type=int, required=True)
    p.add_argument("--pilot", type=int, help="pick this many genomes across the size range")
    p.set_defaults(fn=cmd_plan)

    for name, fn in (("link", cmd_link), ("import", cmd_import), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("--batch", nargs="*")
        p.add_argument("--limit", type=int)
        if name != "link":
            p.add_argument("--db", default="swissprot", choices=["swissprot", "uniref50"])
        p.set_defaults(fn=fn)
        if name == "run":
            p.add_argument("--dry-run", action="store_true")
            p.add_argument("--import-inputs", action="store_true")
            p.add_argument("--on-exist", default="update", choices=["update", "clear"])
            p.add_argument("--tag")
            p.add_argument("--cpus", type=int, default=16)
            p.add_argument("--mem-gb", type=int, default=64)
            p.add_argument("--hours", type=int, default=12)
            p.add_argument("--queue-size", type=int, default=800)
            p.add_argument("--array", type=int, default=0)

    p = sub.add_parser("setup", help="adopt the local images into the agent's store")
    p.add_argument("--deploy", action="store_true")
    p.set_defaults(fn=cmd_setup)

    sub.add_parser("status").set_defaults(fn=cmd_status)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
