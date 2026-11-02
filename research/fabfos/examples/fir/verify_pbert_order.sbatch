#!/bin/bash
#SBATCH --account=def-shallam
#SBATCH --job-name=cv_pbverify
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --time=2:00:00
#SBATCH --output=/project/rpp-shallam/phyberos/cyanoverse/gpr/audit/logs/pbverify_%j.out
# Re-embed a handful of assemblies through the SAME pinned ProteinBERT image the
# legacy pass used, then check that legacy row i really is fasta record i.
# Cosine distance between two embedding SPACES is a number with no referent, so
# the image tag is load-bearing: `external_proteinbert:2024.03.28`, the one
# `resources/env/proteinbert.env` names.
set -euo pipefail
module load apptainer

GPR=/project/rpp-shallam/phyberos/cyanoverse/gpr
ROOT=/project/rpp-shallam/phyberos/cyanoverse
ORFS=$ROOT/data/orfs_and_metabuli/sequences-open_reading_frames
PB_SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_proteinbert..2024.03.28.sif
PL_SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_polars..1.38.1.sif
PY_SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_python_for_data_science..1.2.5.sif

FRESH=$GPR/audit/pbert_fresh
WORK=${SLURM_TMPDIR:-$GPR/work}/pbverify
mkdir -p "$FRESH" "$WORK"

# The samples to check. Deliberately spread: two small, one mid, and one large
# enough that the embedder needed several chunks -- multi-chunk is exactly where
# a lexicographic concat would reorder, so a single-chunk sample proves nothing
# about the ones that matter.
SAMPLES="$*"
[ -n "$SAMPLES" ] || { echo "give sample ids" >&2; exit 2; }

for S in $SAMPLES; do
    [ -f "$FRESH/$S.parquet" ] && { echo "skip $S (already embedded)"; continue; }
    D=$WORK/$S; rm -rf "$D"; mkdir -p "$D"
    cd "$D"
    apptainer exec --nv -B /project -B /scratch -B "$D" "$PB_SIF" bash -c "
        python3 -c '
import sys
ALPHA = set(\"ACDEFGHIKLMNPQRSTUVWXY\")
src, dst = sys.argv[1], sys.argv[2]
with open(src) as fh, open(dst, \"w\") as out:
    for line in fh:
        if line.startswith(\">\"): out.write(line)
        else: out.write(\"\".join(c if c in ALPHA else \"X\" for c in line.strip().upper()) + \"\n\")
' $ORFS/$S.faa $D/_t.faa &&
        pbert run -i $D/_t.faa -o $D/pbert_output --threads 8 --protein_size 512 --model_batch 1024 -x 1"
    apptainer exec -B /project -B /scratch -B "$D" "$PL_SIF" python -c "
from pathlib import Path
import numpy as np, polars as pl
d = Path('$D/pbert_output')
npys = sorted(d.glob('*.npy'))
print('chunks:', [f.name for f in npys], flush=True)
stack = np.vstack([np.load(f) for f in npys])[:, -512:]
pl.DataFrame(stack, schema=[f'dim_{i}' for i in range(512)]).write_parquet('$FRESH/$S.parquet')
print('$S', stack.shape, flush=True)"
    rm -rf "$D"
done

apptainer exec -B /project -B /scratch "$PY_SIF" \
    python3 "$GPR/scripts/verify_pbert_order.py" "$ORFS" "$ROOT/data/annot1" "$FRESH" $SAMPLES
