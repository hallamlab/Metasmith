#!/usr/bin/env python3
"""Launch deep-learning protein-embedding workflows on fir via metasmith.

Patterned on cyanoverse/bgc-search/main/hpc/launch_full_runs.py:
  - Agent(home=SshSource(host="fir", path=HPC_MSM_HOME), runtime=APPTAINER)
  - DataInstanceLibrary built locally as .xgdb
  - Nextflow SLURM executor, with a custom config that adds per-transform
    GPU clusterOptions (the upstream slurm.nf has no GPU plumbing).

Inputs (per the approved plan):
  - fosmids: 4786 ORFs across all 132 fosmid contigs (default)
  - metag:   1.44M ScaDC metagenome ORFs (stretch, gated)

Targets (all GPU transforms on 3g.40gb after OOM probes; see TARGETS table):
  esmc          → annotation::esm_c_embeddings  (3g.40gb, batch=32, cap=2048)
  ankh          → annotation::ankh_embeddings   (3g.40gb, batch=8,  cap=1024)
  prott5        → annotation::prott5_embeddings (3g.40gb, batch=4,  cap=4096)
  esmfold       → sequences::predicted_structures (3g.40gb, batch=1, cap=2048, chunk_size=64)
  foldseek_3di  → sequences::structure_3di_tokens (CPU only)
  saprot        → annotation::saprot_embeddings (3g.40gb, batch=16, cap=1024; chains esmfold→foldseek)

Usage examples:
  # Smoke (5 ORFs, esmc only)
  python launch_dl_embeddings.py run --target fosmids --only esmc --test 5

  # Full fosmids, one model
  python launch_dl_embeddings.py run --target fosmids --only esmc

  # Full fosmids, all 6 models (planner resolves DAG)
  python launch_dl_embeddings.py run --target fosmids --only all

  # Show last task keys
  python launch_dl_embeddings.py status
"""
import os
import sys
import json
import argparse
import subprocess
import tempfile
from pathlib import Path

# metasmith must be importable; set MSM_SRC to a source checkout if not installed.
if os.environ.get("MSM_SRC"):
    sys.path.insert(0, os.environ["MSM_SRC"])
import metasmith
from metasmith.python_api import (
    Agent, Source, SshSource, DataInstanceLibrary, Runtime,
)

# _dl_embeddings lives with the template-authoring gate at
# src/metasmith_libraries/, a sibling of this research driver post-monorepo-move.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"))
import _dl_embeddings as DL

ROOT = Path(__file__).resolve().parent
DL_LIB = ROOT.parent
CACHE_DIR = ROOT / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ─── HPC constants (set via env vars or edit) ───────────────────────────────
HPC_HOST = os.environ.get("MSM_HPC_HOST", "fir")
HPC_SCRATCH = Path(os.environ.get("MSM_HPC_SCRATCH", "<cluster-scratch-dir>"))
HPC_MSM_HOME = HPC_SCRATCH / "metasmith"
HPC_DL = HPC_SCRATCH / "dl_work"

# GPU allocation to submit under — must have access to a GPU partition (a
# CPU-only allocation gets rejected for GPU jobs).
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "<gpu-allocation>")

# ─── Inputs (paths resolve on fir; cyanoverse pattern) ──────────────────────
HPC_INPUTS = HPC_DL / "inputs"
FOSMIDS_FAA = HPC_INPUTS / "fosmids_orfprediction.faa"
FOSMIDS_TEST5_FAA = HPC_INPUTS / "fosmids_orfprediction.head5.faa"
METAG_FAA = HPC_INPUTS / "metag.faa"
# Local source paths (used only if we need to (re-)upload)
LOCAL_DATA = Path(os.environ.get("MSM_LOCAL_DATA", "<local-data-dir>"))
LOCAL_FOSMIDS_FAA = LOCAL_DATA / "fosmids_orfprediction.faa"
LOCAL_METAG_FAA = LOCAL_DATA / "metag.faa"

# ─── HPC weights (staged out-of-band; tarballs match data_type ext: tgz) ────
# Which weights a target needs, and what type each is, live in _dl_embeddings —
# shared with the template author and the planner probe. Only where they sit on
# this cluster is local.
HPC_WEIGHTS = HPC_DL / "weights"
WEIGHT_PATHS = {k: HPC_WEIGHTS / f for k, f in DL.WEIGHT_FILES.items()}

TARGETS = DL.TARGETS


def get_agent():
    home = SshSource(host=HPC_HOST, path=HPC_MSM_HOME).AsSource()
    return Agent(
        home=home,
        runtime=Runtime.APPTAINER,
        setup_commands=["module load apptainer"],
    )


def save_task_key(name: str, key: str):
    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[name] = key
    keys_file.write_text(json.dumps(keys, indent=2))
    print(f"  saved task key: {name} = {key}")


def subsample_fasta_remote(src: Path, n: int) -> Path:
    """Create a head-N subsample of `src` on fir. Returns the HPC path.

    `src` is a path on fir (resolved via SshSource). We use awk on fir
    rather than copying the FASTA back-and-forth.
    """
    dst = src.with_name(f"{src.stem}.head{n}.faa")
    cmd = (
        f"test -e {dst} && echo EXISTS || "
        f"awk '/^>/{{n++}} n>{n}{{exit}} {{print}}' {src} > {dst} && "
        f"grep -c '^>' {dst}"
    )
    res = subprocess.run(
        ["ssh", HPC_HOST, cmd],
        capture_output=True, text=True, timeout=60,
    )
    if res.returncode != 0:
        raise RuntimeError(f"remote subsample failed: {res.stderr}")
    print(f"  remote subsample {src.name} → {dst.name} ({n} ORFs)")
    return dst


def build_input_library(
    orfs_path: Path,
    needed_weights: list[str],
    lib_name: str,
    force_rebuild: bool = False,
) -> DataInstanceLibrary:
    """Build (or reload) the .xgdb with ORFs + needed weights."""
    in_dir = CACHE_DIR / f"{lib_name}.xgdb"

    if in_dir.exists() and not force_rebuild:
        print(f"  loading existing library: {in_dir}")
        return DataInstanceLibrary.Load(in_dir)

    if in_dir.exists():
        import shutil
        shutil.rmtree(in_dir)

    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    DL.add_inputs(inputs, orfs_path, {w: WEIGHT_PATHS[w] for w in needed_weights})
    inputs.Save()
    print(f"  created library: {in_dir}")
    return inputs


def make_fir_slurm_config(transform_gpu_map: dict[str, str]) -> Path:
    """Render a custom Nextflow config = upstream slurm.nf + per-transform
    GPU clusterOptions. Returns path to the rendered .config file (in cache).

    Args:
      transform_gpu_map: { transform_name: SLURM --gpus= value or None }
        e.g. {"esm_c": "nvidia_h100_80gb_hbm3_1g.10gb:1", "foldseek_3di": None}
    """
    base = Path(metasmith.__file__).parent / "nextflow_config" / "slurm.nf"
    base_text = base.read_text()

    overrides = ["", "process {"]
    for tname, gpu_slice in transform_gpu_map.items():
        if gpu_slice is None:
            continue
        overrides += [
            f"    withName: '.*__{tname}' {{",
            f"        clusterOptions = \"--nodes=1 --ntasks=1 --account=${{params.slurmAccount}} --gpus={gpu_slice}\"",
            # Forward CUDA_VISIBLE_DEVICES through the metasmith bootstrap container's
            # --cleanenv. APPTAINERENV_* survives --cleanenv and is unprefixed inside.
            f"        beforeScript = 'export APPTAINERENV_CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES'",
            f"    }}",
        ]
    overrides += ["}", ""]

    out = CACHE_DIR / "fir_slurm_gpu.config"
    out.write_text(base_text + "\n" + "\n".join(overrides))
    print(f"  rendered nextflow config: {out}")
    return out


def run_one(
    name: str,
    orfs_path: Path,
    targets_to_run: list[str],
    on_exist: str = "clear",
    queue_size: int = 50,
    shard_size: int | None = None,
):
    """Generate, stage, and run a workflow over one set of targets."""
    print(f"\n{'='*60}\nworkflow: {name}\n{'='*60}")

    # key the gpu map by transform module name so withName: '.*__<tname>' matches
    # Nextflow's p<N>__<tname>
    needed_weights = DL.needed_weights(targets_to_run)
    transform_gpu_map = DL.gpu_map(targets_to_run)

    inputs = build_input_library(orfs_path, needed_weights, name, force_rebuild=True)
    smith = get_agent()

    print("generating workflow...")
    # MCTS seed=42 (metasmith default) lands in a local optimum for the 6-target
    # `--only all` search that adds a redundant downloadESMFoldWeights step
    # despite the weight tarball being pre-staged. Seeds 1/7/99/2024 find the
    # optimal 7-step plan; see probe_planner.py beside this file for the sweep.
    task = DL.spec(inputs, targets_to_run).Solve(seed=1)

    if not task.ok or len(task.plan.steps) == 0:
        print(f"ERROR: workflow planning failed for {name}")
        print(f"  plan: {task.plan}")
        return None
    print(f"  steps: {len(task.plan.steps)}, key={task.GetKey()}")
    save_task_key(name, task.GetKey())

    print(f"staging workflow (on_exist={on_exist})...")
    smith.StageWorkflow(task, on_exist=on_exist, verify_external_paths=False)

    config = make_fir_slurm_config(transform_gpu_map)

    print("running workflow...")
    params = dict(
        slurmAccount=SLURM_ACCOUNT,
        gpus=1,
        executor=dict(queueSize=queue_size),
        process=dict(array=20, tries=2),
    )
    if shard_size is not None:
        print(f"  WARN: --shard-size {shard_size} ignored; SHARD_SIZE is baked into shardFasta.py")
    smith.RunWorkflow(
        task=task,
        config_file=config,
        params=params,
    )
    print(f"submitted [{name}]: {task.GetKey()}")
    return task.GetKey()


# ─── CLI ────────────────────────────────────────────────────────────────────

def cmd_run(args):
    if args.target == "fosmids":
        orfs_full = FOSMIDS_FAA
        base_label = "fosmids"
    elif args.target == "metag":
        orfs_full = METAG_FAA
        base_label = "metag"
    else:
        raise ValueError(f"unknown --target {args.target}")

    # All input paths are HPC (resolved on fir). We don't validate
    # existence locally; the agent stages and the workflow either finds them or
    # fails with a clear message.
    if args.test:
        orfs_path = subsample_fasta_remote(orfs_full, args.test)
        suffix = f"test{args.test}"
    else:
        orfs_path = orfs_full
        suffix = "full"

    if args.only == "all":
        targets_to_run = list(TARGETS.keys())
    else:
        if args.only not in TARGETS:
            print(f"ERROR: unknown --only {args.only}; valid: {list(TARGETS) + ['all']}")
            sys.exit(1)
        targets_to_run = [args.only]

    name = f"{base_label}_{suffix}_{args.only}"
    run_one(name=name, orfs_path=orfs_path, targets_to_run=targets_to_run,
            shard_size=args.shard_size)


def cmd_status(args):
    keys_file = CACHE_DIR / "task_keys.json"
    if not keys_file.exists():
        print("no task keys yet")
        return
    keys = json.loads(keys_file.read_text())
    for name, key in keys.items():
        print(f"  {name:40s} {key}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("run", help="Generate + stage + run a workflow")
    pr.add_argument("--target", choices=["fosmids", "metag"], default="fosmids")
    pr.add_argument("--only", default="esmc",
                    help=f"target name or 'all'; choices: {list(TARGETS) + ['all']}")
    pr.add_argument("--test", type=int, default=0,
                    help="subsample to first N ORFs (0 = full FASTA)")
    pr.add_argument("--shard-size", type=int, default=None,
                    help="override shardFasta target seqs/shard (default 1024)")
    pr.set_defaults(func=cmd_run)

    ps = sub.add_parser("status", help="Show last task keys")
    ps.set_defaults(func=cmd_status)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
