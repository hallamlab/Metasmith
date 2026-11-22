#!/bin/bash
# Build the geoquery image as quay.io/hallamlab/geoquery:<version>, the metasmith release it ships in.
# usage: dev.sh [-p] [docker build args]
#   -p  push the image and pin resources/env/geoquery.env to its tag and digest
set -euo pipefail
ROOT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/../.." &> /dev/null && pwd )
HERE=$ROOT/docker/geoquery
ENV=$ROOT/src/metasmith_libraries/resources/env/geoquery.env
VERSION=${GEOQUERY_VERSION:-$(cat "$ROOT/src/metasmith/version.txt")}
IMAGE=quay.io/hallamlab/geoquery:$VERSION
PUSH=0
if [[ ${1:-} == -p ]]; then PUSH=1; shift; fi
rm -rf "${HERE:?}/geoquery" && cp -r "$ROOT/research/geospatial_query/geoquery" "$HERE/geoquery"
trap 'rm -rf "${HERE:?}/geoquery"' EXIT
DOCKER_BUILDKIT=1 docker build "$@" -t "$IMAGE" -f "$HERE/dockerfile" "$HERE"
docker run --rm "$IMAGE" python -c "import geoquery.registry as r; print(len(r.specs()), 'adapters')"
(( PUSH )) || exit 0
docker push "$IMAGE"
DIGEST=$(docker inspect -f '{{index .RepoDigests 0}}' "$IMAGE" | cut -d@ -f2)
printf '# %s, pushed %s, digest %s. Imports geoquery.registry with every adapter.\ncontainer: docker://%s\n' \
    "$VERSION" "$(date +%F)" "$DIGEST" "$IMAGE" > "$ENV"
cat "$ENV"
