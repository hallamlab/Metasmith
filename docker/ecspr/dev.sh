#!/bin/bash
# ECSPr runtime -- build, check and publish the ecspr image.
#
# THE BUILD CONTEXT IS THE REPO ROOT, not this directory: the image installs the
# package and its env spec from src/ecspr, and a copy of either under docker/
# would be a second place for them to be wrong.
#
# The image is NOT what the dev loop uses. `./dev.sh -e` runs the editable install
# in the `ecspr` conda env, and `env::ecspr.env` carries a `conda:` key, so the
# transform runs under `--runtime mamba` with no image at all. This image is for
# the container runtimes.
#
# TAG. Derived, never typed: `ecspr.CONTAINER_TAG` is `version.txt` plus the
# 7-char content hash of src/ecspr, rendered `0.1.0-abc1234` because Docker
# rejects `+`. The hash is stamped IMMEDIATELY BEFORE the build, so the tag
# cannot name a source state other than the one baked into the image -- and
# --check re-reads the version out of the running image to prove it.
#
# WHAT THE IMAGE MUST BE ABLE TO DO is asserted by `ecspr selftest`, in the
# package, so the image build, this script and the conda recipe's test block all
# check the same thing. A copy of it here would be the one that goes stale.
set -euo pipefail

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO=$( cd -- "$HERE/../.." &> /dev/null && pwd )

# `ecspr` is importable from source with no install; nothing here needs its deps.
# PY is whatever python is on PATH -- but an ABSENT one must not silently yield
# an empty tag, which `${VAR:-$(...)}` would happily paste into an image name.
PY=${ECSPR_PYTHON:-python}
# An `exit` inside a command substitution only leaves the subshell, so these
# report emptiness upward and the CALLER refuses -- otherwise a missing python
# yields the image name ":" and every verb below silently addresses garbage.
_ver() { "$PY" -c "import ecspr; print(ecspr.$1)" 2>/dev/null || true; }
_image() {
    if [ -n "${ECSPR_IMAGE:-}" ]; then echo "$ECSPR_IMAGE"; return 0; fi
    local img tag
    img=$(_ver CONTAINER_IMAGE); tag=$(_ver CONTAINER_TAG)
    if [ -z "$img" ] || [ -z "$tag" ]; then
        echo "ERROR: cannot import ecspr from $REPO/src using '$PY'." >&2
        echo "  activate an env with python, or set ECSPR_PYTHON=/path/to/python." >&2
        return 1
    fi
    echo "$img:$tag"
}
_stamp() { "$PY" -m ecspr._build_hash --write >/dev/null; }
export PYTHONPATH="$REPO/src${PYTHONPATH:+:$PYTHONPATH}"

case "${1:-}" in
    --build|-b)
        _stamp
        IMAGE=$(_image)
        echo "building $IMAGE"
        mkdir -p "$HERE/load"
        # tini is the one thing the image needs that is not in the repo. Take the
        # copy docker/fabfos already downloaded when it is there.
        [ -f "$HERE/load/tini" ] || cp "$REPO/docker/fabfos/load/tini" \
            "$HERE/load/tini" 2>/dev/null || wget -q \
            https://github.com/krallin/tini/releases/download/v0.19.0/tini \
            -O "$HERE/load/tini"
        export DOCKER_BUILDKIT=1
        docker build -t "$IMAGE" -f "$HERE/dockerfile" "$REPO"
    ;;
    --check|-c)
        IMAGE=$(_image)
        echo "checking $IMAGE"
        docker run --rm "$IMAGE" ecspr selftest
        docker run --rm "$IMAGE" ecspr --where
        docker run --rm "$IMAGE" python -m pytest -q /opt/tests/ecspr
        # The tag is a claim about the code inside. Prove it rather than trust
        # the build order: a rebuilt-with-cache image can otherwise carry an
        # older tree under a freshly stamped name.
        want=$(_ver FULL_VERSION)
        got=$(docker run --rm "$IMAGE" python -c "import ecspr; print(ecspr.__version__)")
        [ "$want" = "$got" ] || {
            echo "ERROR: image reports $got but is tagged for $want" >&2
            echo "  the tag names a source state that is not the one in the image;" >&2
            echo "  rebuild with --build (no cache reuse across a source change)." >&2
            exit 1
        }
        echo "image version matches source: $got"
    ;;
    --push|-p)
        # Gated on --check re-run here rather than on a stamp file: a stamp goes
        # stale silently, and this is a public, one-way publication.
        IMAGE=$(_image)
        "$HERE/dev.sh" --check
        echo "pushing $IMAGE"
        docker push "$IMAGE"
    ;;
    --run|-r)
        IMAGE=$(_image)
        docker run -it --rm \
            --mount type=bind,source="$REPO",target="/ws" --workdir="/ws" \
            "$IMAGE" /bin/bash
    ;;
    --tag|-t) # print the tag this tree would build, without stamping
        _image
    ;;
    *)
        echo "usage: dev.sh [--build|--check|--push|--run|--tag]"
        echo "typical: --build && --check && --push"
        echo "  --push re-runs --check; it will not publish an image that fails it"
        echo "  ECSPR_IMAGE overrides the derived name:tag for a local experiment"
        exit 1
    ;;
esac
