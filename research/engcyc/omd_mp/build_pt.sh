#!/bin/bash
# Build MetaPathways 4's Pathway Tools SIF on a fir login node, which has the
# internet access the SRI patch download needs. Local disk holds the build tree.
# usage: build_pt.sh <installer> <image_dir>
set -euo pipefail
installer=$(realpath "$1"); out=$(realpath -m "$2")
tmp=/tmp/$USER-build_pt
mkdir -p "$out" "$tmp"
chmod 700 "$out" "$tmp"
module load apptainer/1.4.5
export APPTAINER_TMPDIR=$tmp APPTAINER_CACHEDIR=$tmp/cache
SECONDS=0
apptainer exec --unsquash --cleanenv \
    --bind /scratch/phyberos:/scratch/phyberos,$tmp:$tmp \
    --env NXF_HOME=$tmp/nxf,APPTAINER_TMPDIR=$tmp,APPTAINER_CACHEDIR=$tmp/cache \
    /scratch/phyberos/mp4/metapathways_4.0.1.sif \
    metapathways build_pt -i "$installer" -o "$out" -t 4 --work_dir "$tmp/work"
echo "build_pt_seconds $SECONDS"
ls -la "$out"
