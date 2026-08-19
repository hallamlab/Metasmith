import argparse
import importlib.util
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TRANSFORMS = REPO / "transforms" / "functionalAnnotation"
REMOTE_ROOT = "/scratch/phyberos/dl_testing_claude/oom_probe"
LOCAL_ROOT  = Path("/tmp/oom_probe_stage")

PROBES = [
    dict(
        name="esm_c_1g10gb",
        transform="esm_c.py",
        container="esmc",
        infer_script="infer_esmc.py",
        slice="nvidia_h100_80gb_hbm3_1g.10gb",
        slurm_mem="16000M",
        slurm_cpus=4,
        weights_tgz="esmc_300m.tgz",
        runner_args="--model-name esmc_300m",
    ),
    dict(
        name="esm_c_3g40gb",
        transform="esm_c.py",
        container="esmc",
        infer_script="infer_esmc.py",
        slice="nvidia_h100_80gb_hbm3_3g.40gb",
        slurm_mem="40000M",
        slurm_cpus=4,
        weights_tgz="esmc_300m.tgz",
        runner_args="--model-name esmc_300m",
    ),
    dict(
        name="ankh_1g10gb",
        transform="ankh.py",
        container="ankh",
        infer_script="infer_ankh.py",
        slice="nvidia_h100_80gb_hbm3_1g.10gb",
        slurm_mem="16000M",
        slurm_cpus=4,
        weights_tgz="ankh_base.tgz",
    ),
    dict(
        name="prott5_1g10gb",
        transform="prott5.py",
        container="prott5",
        infer_script="infer_prott5.py",
        slice="nvidia_h100_80gb_hbm3_1g.10gb",
        slurm_mem="24000M",
        slurm_cpus=4,
        weights_tgz="prott5_xl.tgz",
    ),
    dict(
        name="prott5_1g10gb_b1",
        transform="prott5.py",
        container="prott5",
        infer_script="infer_prott5.py",
        slice="nvidia_h100_80gb_hbm3_1g.10gb",
        slurm_mem="24000M",
        slurm_cpus=4,
        weights_tgz="prott5_xl.tgz",
        batch_override=1,
    ),
    dict(
        name="prott5_3g40gb",
        transform="prott5.py",
        container="prott5",
        infer_script="infer_prott5.py",
        slice="nvidia_h100_80gb_hbm3_3g.40gb",
        slurm_mem="40000M",
        slurm_cpus=4,
        weights_tgz="prott5_xl.tgz",
    ),
    dict(
        name="esmfold_3g40gb",
        transform="esmfold.py",
        container="esmfold",
        infer_script="infer_esmfold.py",
        slice="nvidia_h100_80gb_hbm3_3g.40gb",
        slurm_mem="40000M",
        slurm_cpus=4,
        weights_tgz="esmfold_v1.tgz",
    ),
    dict(
        name="saprot_3g40gb",
        transform="saprot.py",
        container="saprot",
        infer_script="infer_saprot.py",
        slice="nvidia_h100_80gb_hbm3_3g.40gb",
        slurm_mem="32000M",
        slurm_cpus=4,
        weights_tgz="saprot_650m.tgz",
    ),
]


def _read_module_constants(path: Path) -> dict:
    text = path.read_text()
    out = {}
    for key in ("BATCH_SIZE", "MAX_LEN", "CHUNK_OVERLAP", "CHUNK_SIZE"):
        m = re.search(rf"^{key}\s*=\s*(\d+)", text, re.M)
        if m:
            out[key] = int(m.group(1))
    m = re.search(r"INFERENCE\s*=\s*r'''(.*?)'''", text, re.S)
    if m:
        out["INFERENCE"] = m.group(1)
    return out


def _synth_fasta(n_seqs: int, length: int, fp: Path):
    fp.parent.mkdir(parents=True, exist_ok=True)
    seq = "G" * length
    with open(fp, "w") as f:
        for i in range(n_seqs):
            f.write(f">probe_{i:03d}\n{seq}\n")


def _synth_3di_parquet(n_seqs: int, length: int, fp: Path):
    import pandas as pd
    fp.parent.mkdir(parents=True, exist_ok=True)
    di3 = "A" * length
    df = pd.DataFrame({
        "sequence_id": [f"probe_{i:03d}" for i in range(n_seqs)],
        "di3_sequence": [di3] * n_seqs,
    })
    df.to_parquet(fp, index=False)


def _sbatch_text(spec: dict, consts: dict) -> str:
    batch = spec.get("batch_override", consts["BATCH_SIZE"])
    cap   = spec.get("cap_override", consts["MAX_LEN"])
    overlap = consts.get("CHUNK_OVERLAP", 128)
    name = spec["name"]
    container = spec["container"]
    sif_name = f"docker..quay.io_hallamlab_external_{container}..2026.05.19.sif"

    if container == "esmfold":
        infer_args = (
            f"--weights /weights "
            f"--fasta /probe/probe.faa "
            f"--out-dir /probe/structures "
            f"--device cuda "
            f"--max-len {cap} "
            f"--chunk-size {consts.get('CHUNK_SIZE', 64)}"
        )
    elif container == "saprot":
        infer_args = (
            f"--weights /weights "
            f"--fasta /probe/probe.faa "
            f"--di3 /probe/probe_3di.parquet "
            f"--out-parquet /probe/embeddings.parquet "
            f"--out-index /probe/index.csv "
            f"--device cuda "
            f"--batch-size {batch} "
            f"--max-len {cap} "
            f"--chunk-overlap {overlap}"
        )
    else:
        extra = spec.get("runner_args", "")
        infer_args = (
            f"--weights /weights "
            f"--fasta /probe/probe.faa "
            f"--out-parquet /probe/embeddings.parquet "
            f"--out-index /probe/index.csv "
            f"--device cuda "
            f"--batch-size {batch} "
            f"--max-len {cap} "
            f"--chunk-overlap {overlap} "
            f"{extra}"
        )

    return f"""#!/bin/bash
# OOM probe: {name}
# Worst-case batch = {batch} × {cap}-AA seqs (= {batch * cap} AA / batch)
#SBATCH --account=def-shallam_gpu
#SBATCH --partition=gpubase_bygpu_b1
#SBATCH --gres=gpu:{spec['slice']}:1
#SBATCH --cpus-per-task={spec['slurm_cpus']}
#SBATCH --mem={spec['slurm_mem']}
#SBATCH --time=0:30:00
#SBATCH --output={REMOTE_ROOT}/{name}/probe.out
#SBATCH --error={REMOTE_ROOT}/{name}/probe.err

set -uo pipefail
PROBE={REMOTE_ROOT}/{name}
SIF_PRIMARY=/scratch/phyberos/metasmith/container_images/{sif_name}
SIF_FALLBACK=/scratch/phyberos/dl_testing_claude/container_tests/builds/{container}.sif
if [ -f "$SIF_PRIMARY" ]; then SIF=$SIF_PRIMARY; else SIF=$SIF_FALLBACK; fi
echo "using SIF: $SIF"
WEIGHTS_TGZ=/scratch/phyberos/dl_testing_claude/weights/{spec['weights_tgz']}
module load apptainer 2>/dev/null || true

echo "=== node: $(hostname) ==="
nvidia-smi -L
echo "=== probe spec ==="
echo "name={name} batch_size={batch} max_len={cap} slice={spec['slice']}"
echo "worst-case batch AA = {batch * cap}"
echo "CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES"

# Extract weights (match the production protocol's behavior of unpacking
# the .tgz into a local ./weights dir, then binding it into the container).
mkdir -p $PROBE/weights
if [ ! -e "$PROBE/weights/.unpacked" ]; then
    tar -xzf $WEIGHTS_TGZ -C $PROBE/weights
    touch $PROBE/weights/.unpacked
fi

# Background memory poller — track only THIS job's compute-app entries on
# the MIG slice. `--query-compute-apps=pid,used_memory` is the only nvidia-smi
# query that actually filters to a MIG UUID with --id.
( while true; do
    out=$(nvidia-smi --id=$CUDA_VISIBLE_DEVICES \\
            --query-compute-apps=pid,used_memory \\
            --format=csv,noheader,nounits 2>/dev/null) || true
    if [ -n "$out" ]; then
        echo "[$(date +%H:%M:%S)] $out"
    fi
    sleep 2
  done ) > $PROBE/gpu_mem.log &
MONPID=$!

mkdir -p $PROBE/structures

apptainer exec --nv \\
  --env CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES \\
  --bind $PROBE:/probe \\
  --bind $PROBE/weights:/weights \\
  $SIF python /probe/{spec['infer_script']} {infer_args}
rc=$?

kill $MONPID 2>/dev/null
echo "--- inference exit=$rc ---"
echo "--- peak GPU mem (this job, slice-local) ---"
# Lines look like "[HH:MM:SS] PID, MEM"; take max MEM across all samples for any pid
awk -F'[ ,]' '/[0-9]+/ {{for(i=1;i<=NF;i++) if($i~/^[0-9]+$/ && $i+0>m && $i+0<200000) m=$i+0}} END {{if(m>0) print "peak="m"MiB"; else print "no MIG compute-app memory data captured"}}' $PROBE/gpu_mem.log

if [ $rc -eq 0 ]; then
    echo "[{name}] PASS"
else
    echo "[{name}] FAIL exit=$rc"
fi
exit $rc
"""


def stage(spec: dict, upload: bool):
    transform = TRANSFORMS / spec["transform"]
    consts = _read_module_constants(transform)
    if "INFERENCE" not in consts or "BATCH_SIZE" not in consts or "MAX_LEN" not in consts:
        if spec["transform"] == "esmfold.py" and "MAX_LEN" in consts:
            consts.setdefault("BATCH_SIZE", 1)
        else:
            print(f"[{spec['name']}] SKIP: missing constants in {transform}", file=sys.stderr)
            return

    name = spec["name"]
    local = LOCAL_ROOT / name
    local.mkdir(parents=True, exist_ok=True)

    n_seqs = spec.get("batch_override", consts["BATCH_SIZE"])
    length = spec.get("cap_override", consts["MAX_LEN"])

    _synth_fasta(n_seqs, length, local / "probe.faa")
    if spec["transform"] == "saprot.py":
        _synth_3di_parquet(n_seqs, length, local / "probe_3di.parquet")

    (local / spec["infer_script"]).write_text(consts["INFERENCE"])
    (local / "probe.sbatch").write_text(_sbatch_text(spec, consts))

    print(f"[{name}] staged at {local}/  (batch={n_seqs}, cap={length})")

    if upload:
        cmd = ["rsync", "-az", "--mkpath", f"{local}/",
               f"fir:{REMOTE_ROOT}/{name}/"]
        print(f"  $ {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        print(f"  $ ssh fir 'sbatch {REMOTE_ROOT}/{name}/probe.sbatch'   # to submit")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", help="run only this probe name (default: all)")
    ap.add_argument("--upload", action="store_true",
                    help="rsync the staged probe to fir under " + REMOTE_ROOT)
    args = ap.parse_args()

    todo = [p for p in PROBES if args.model is None or p["name"] == args.model]
    if not todo:
        print(f"no probes match --model {args.model}", file=sys.stderr)
        sys.exit(2)

    for spec in todo:
        stage(spec, args.upload)

    if not args.upload:
        print()
        print(f"staged locally under {LOCAL_ROOT}/")
        print(f"re-run with --upload to push to fir:{REMOTE_ROOT}/")


if __name__ == "__main__":
    main()
