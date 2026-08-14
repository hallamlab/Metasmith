#!/bin/bash
# fabfos runtime -- build and run the docker-native fabfos image locally.
#
# DOCKER-NATIVE, NOT A SIF. Unlike docker/ecspr_bake, this image is meant to run on
# this workstation directly -- no --sif/--sync/--push modes; docker is available here.
#
# ONE IMAGE, TWO ENVS. `fabfos` (numpy<2: CHOLMOD, cobra, assembly/annotation tools)
# and `fabfos_polars` (numpy>=2: polars) -- see dockerfile for why they can't merge.
#
# CLEAN and ProteinBERT are not part of this image -- see dockerfile's header comment.
set -euo pipefail

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
IMAGE=fabfos:local

VERIFY="from sksparse.cholmod import cho_factor; import numpy as np, scipy.sparse as sp; \
A = sp.csc_matrix(np.array([[4.0,1.0],[1.0,3.0]])); \
f = cho_factor(A); \
x = f.solve(np.array([1.0,2.0]).reshape(-1,1)).ravel(); \
assert np.allclose(x, np.linalg.solve(A.toarray(), [1.0,2.0])), 'CHOLMOD solve is wrong'; \
import cobra, pandas, pyarrow, scipy; \
print('fabfos OK: cholmod + cobra', cobra.__version__, '| numpy<2 env ready')"

case "${1:-}" in
    --build|-b)
        mkdir -p "$HERE/load"
        [ -f "$HERE/load/tini" ] || wget -q \
            https://github.com/krallin/tini/releases/download/v0.19.0/tini \
            -O "$HERE/load/tini"
        export DOCKER_BUILDKIT=1
        docker build \
            --build-arg="VERIFY=$VERIFY" \
            -t "$IMAGE" -f "$HERE/dockerfile" "$HERE"
    ;;
    --check|-c)
        docker run --rm "$IMAGE" python -c "$VERIFY"
        docker run --rm "$IMAGE" bash -lc '
            set -e
            for t in megahit spades.py flye bbduk.sh filtlong minimap2 samtools \
                     bedtools seqkit hifiasm hifiasm_meta miniasm bam2fastq blastn \
                     exec_annotation diamond gfatools; do
                command -v "$t" >/dev/null || { echo "MISSING: $t"; exit 1; }
            done
            echo "fabfos OK: all CLI tools on PATH"
        '
    ;;
    --run|-r)
        docker run -it --rm \
            --mount type=bind,source="$HERE/../..",target="/ws" --workdir="/ws" \
            "$IMAGE" /bin/bash
    ;;
    *)
        echo "usage: dev.sh [--build|--check|--run]"
        echo "typical: --build && --check"
        exit 1
    ;;
esac
