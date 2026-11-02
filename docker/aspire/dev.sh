#!/bin/bash
# ASPIRE analysis runtime -- build, verify and publish the image `resources/env/aspire.env` names.
#
# --push is one-way and public, and quay creates a new repository PRIVATE: flip
# hallamlab/aspire to public before any host pulls it anonymously.
set -euo pipefail

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VERSION=0.1.0
IMAGE=${ASPIRE_IMAGE:-quay.io/hallamlab/aspire:$VERSION}

VERIFY="Rscript -e 'suppressPackageStartupMessages({library(optparse); library(dplyr); library(purrr); \
library(readr); library(tibble); library(tidyr); library(indicspecies); library(permute)}); \
cat(\"indicspecies\", as.character(packageVersion(\"indicspecies\")), \"\\n\")' && \
python -c 'import pandas, numpy, scipy, Bio, tqdm, seaborn, matplotlib, plotly.graph_objects as go; \
fig = go.Figure(go.Sankey(node=dict(label=[\"a\", \"b\"]), link=dict(source=[0], target=[1], value=[1]))); \
fig.write_image(\"/tmp/probe.svg\"); print(\"python OK: pandas\", pandas.__version__, \"seaborn\", seaborn.__version__)'"

case "${1:-}" in
    --build|-b)
        export DOCKER_BUILDKIT=1
        docker build --build-arg="VERIFY=$VERIFY" -t "$IMAGE" -f "$HERE/dockerfile" "$HERE"
    ;;
    --check|-c)
        docker run --rm "$IMAGE" bash -c "$VERIFY"
    ;;
    --push|-p)
        "$HERE/dev.sh" --check
        docker push "$IMAGE"
        echo "record this digest in resources/env/aspire.env, with the date:"
        docker inspect --format='{{index .RepoDigests 0}}' "$IMAGE"
    ;;
    --save|-s)
        # For a host that cannot pull it: `apptainer build aspire.sif docker-archive://aspire.tar`.
        docker save -o "${2:?usage: dev.sh --save <out.tar>}" "$IMAGE"
    ;;
    *)
        echo "usage: dev.sh [--build|--check|--push|--save <out.tar>]"
        exit 1
    ;;
esac
