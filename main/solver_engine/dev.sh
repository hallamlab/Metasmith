HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $HERE

DOCKER_IMAGE=joseluisq/rust-linux-darwin-builder:2.0.0-beta.1

# Where the built binaries have to land to be packaged. Unlike msm_relay --
# which runs on the agent host and is baked into the docker image -- msm_solver
# runs locally at plan time, in whatever process is planning, so it ships inside
# the pip wheel and conda package as package data.
STAGE="$HERE/../../src/metasmith/engine"

in_container() {
    echo "in container: $@"
    docker run --rm \
        --mount type=bind,source="$HERE",target="/root/src"\
        --workdir /root/src \
        $DOCKER_IMAGE \
        "$@"
}

# target triple -> the {architecture}-{system} suffix the Python side resolves,
# which is bootstrap.py's naming for the relay and is reused here so there is
# one convention rather than two.
stage_one() {
    local triple=$1 slot=$2
    local src="$HERE/target/$triple/release/msm_solver"
    [ -f "$src" ] || return 1
    mkdir -p "$STAGE"
    cp "$src" "$STAGE/msm_solver.$slot"
    chmod +x "$STAGE/msm_solver.$slot"
    echo "  staged $slot  ($(stat -c %s "$src") bytes)"
}

stage_all() {
    local kind=$1
    echo "staging to $STAGE"
    stage_one x86_64-unknown-linux-musl  x86_64-linux
    stage_one aarch64-unknown-linux-musl arm64-linux
    stage_one x86_64-apple-darwin        x86_64-darwin
    stage_one aarch64-apple-darwin       arm64-darwin
    # What produced these. `-bl` output is host-linked and correct to run here
    # but wrong to ship, and nothing about the file itself says so -- the
    # packaging guard reads this marker rather than trying to infer it.
    echo "$kind" > "$STAGE/BUILD_KIND"
}

case $1 in
    --init)
        project=msm_solver
        [[ -e ./src ]] && exit 0
        [[ -e $project ]] && rm -r $project
        in_container cargo new $project
        mv $project/* ./
        mv $project/.* ./
        rmdir $project
    ;;
    ###################################################
    # build
    -bb)
        # DOCKER_IMAGE is an *upstream* image: it carries the osxcross toolchain
        # that the two *-apple-darwin targets link against. Building a local
        # Dockerfile over this tag replaces it with a plain rust image, so `-b`
        # fails both darwin targets with "cc: unrecognized command-line option
        # '-framework'" and leaves stub binaries in target/ for the package build
        # to pick up. That is the 0.18.4 stub-relay bug. Pull, do not build.
        docker pull $DOCKER_IMAGE
    ;;
    -b)
        in_container sh ./build.sh && stage_all cross
    ;;
    -b1)
        # One target, still cross-compiled: staging is partial, so the packaging
        # guard will refuse. For the inner loop, not for a release.
        in_container sh ./build.sh --single && stage_all cross-single
    ;;
    -bl)
        # Host toolchain, host target, no docker -- seconds instead of minutes.
        # The binary is dynamically linked against this machine's libc, which is
        # exactly what `cross` output is not, hence the marker.
        cargo build --release || exit 1
        # Clear the stage first. A host build refreshes exactly one slot, and
        # leaving the other three behind from an earlier `-be` is how a stale
        # binary survives a rebuild that looked like it succeeded -- which is the
        # 0.18.4 stub-relay bug wearing different clothes.
        rm -rf "$STAGE"
        mkdir -p "$STAGE"
        cp "$HERE/target/release/msm_solver" "$STAGE/msm_solver.$(uname -m | sed 's/aarch64/arm64/')-$(uname -s | tr 'A-Z' 'a-z')"
        echo "local" > "$STAGE/BUILD_KIND"
        echo "staged a HOST-LINKED build to $STAGE -- fine to test with, refused by -bp"
    ;;
    --stage)
        stage_all cross
    ;;
    --clean-stage)
        rm -rf "$STAGE"
        echo "removed $STAGE"
    ;;
    ###################################################
    # run
    -r)
        shift
        cargo run --release -- $@
    ;;
    *)
        echo "bad option"
        echo $1
    ;;
esac
