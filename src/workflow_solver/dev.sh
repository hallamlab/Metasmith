HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $HERE

DOCKER_IMAGE=joseluisq/rust-linux-darwin-builder:2.0.0-beta.1

# Where the built binaries have to land to be packaged. Unlike msm_relay --
# which runs on the agent host and is baked into the docker image -- msm_solver
# runs locally at plan time, in whatever process is planning, so it ships inside
# the pip wheel and conda package as package data.
STAGE="$HERE/../../src/metasmith/engine"

# Cargo's build directory, deliberately OUTSIDE the worktree and shared by every
# scope on this machine. The stage above is a per-scope build artifact and has to
# stay one -- it was DVC-tracked once so siblings could skip the cross build, and
# DVC checks its outputs out read-only, which cost every plan in every worktree
# the executable bit and silently reverted the solver to the python fallback.
# Sharing the *compilation* is how that convenience comes back without the
# artifact being shared: registry deps and the four cross targets are built once,
# and only msm_solver itself recompiles per scope. Cargo locks the directory, so
# two scopes building at once block rather than corrupt.
#
# Split in two because the container runs as root and leaves root-owned files
# behind: a host `-bl` build sharing one root would fail on cargo's own
# `.rustc_info.json` at the top of it. They are different toolchains against
# different targets anyway, so nothing is lost by keeping them apart.
TARGET_DIR="${MSM_SOLVER_TARGET_DIR:-$HOME/.cache/metasmith/solver-target}"
CROSS_TARGET_DIR="$TARGET_DIR/cross"
HOST_TARGET_DIR="$TARGET_DIR/host"
mkdir -p "$CROSS_TARGET_DIR" "$HOST_TARGET_DIR"

in_container() {
    echo "in container: $@"
    # The target dir is mounted at its own host path, not at a container-local
    # one: cargo records absolute paths in its fingerprints, so the directory has
    # to be called the same thing on both sides or every build invalidates the
    # last one's work.
    #
    # `solver_witness` is mounted beside this crate because Cargo.toml depends on
    # it by the relative path `../solver_witness`. Mounting this crate alone puts
    # that path outside the container and cargo fails while resolving the
    # dependency graph, before it compiles anything.
    docker run --rm \
        ${MSM_DOCKER_DNS:+--dns "$MSM_DOCKER_DNS"} \
        --mount type=bind,source="$HERE",target="/root/src/workflow_solver"\
        --mount type=bind,source="$HERE/../solver_witness",target="/root/src/solver_witness"\
        --mount type=bind,source="$CROSS_TARGET_DIR",target="$CROSS_TARGET_DIR"\
        --env CARGO_TARGET_DIR="$CROSS_TARGET_DIR" \
        --workdir /root/src/workflow_solver \
        $DOCKER_IMAGE \
        "$@"
}

# target triple -> the {architecture}-{system} suffix the Python side resolves,
# which is bootstrap.py's naming for the relay and is reused here so there is
# one convention rather than two.
stage_one() {
    local triple=$1 slot=$2
    local src="$CROSS_TARGET_DIR/$triple/release/msm_solver"
    [ -f "$src" ] || return 1
    mkdir -p "$STAGE"
    cp "$src" "$STAGE/msm_solver.$slot"
    # An explicit mode, not `chmod +x`. `cp` writes through an existing file and
    # keeps its mode, so restaging over a 444 checkout (which is how these
    # arrived while engine/ was DVC-tracked) left 555 -- runnable, but not what
    # anyone asked for, and it makes the packaging guard's mode column lie about
    # what a fresh build produces.
    chmod 755 "$STAGE/msm_solver.$slot"
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
        export CARGO_TARGET_DIR="$HOST_TARGET_DIR"
        cargo build --release || exit 1
        # Clear the stage first. A host build refreshes exactly one slot, and
        # leaving the other three behind from an earlier `-be` is how a stale
        # binary survives a rebuild that looked like it succeeded -- which is the
        # 0.18.4 stub-relay bug wearing different clothes.
        rm -rf "$STAGE"
        mkdir -p "$STAGE"
        cp "$HOST_TARGET_DIR/release/msm_solver" "$STAGE/msm_solver.$(uname -m | sed 's/aarch64/arm64/')-$(uname -s | tr 'A-Z' 'a-z')"
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
        export CARGO_TARGET_DIR="$HOST_TARGET_DIR"
        cargo run --release -- $@
    ;;
    *)
        echo "bad option"
        echo $1
    ;;
esac
