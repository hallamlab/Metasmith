HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $HERE

USER="hallamlab"
NAME="rust_build_env"
VERSION="0.1.0"
# DOCKER_IMAGE=quay.io/$USER/$NAME:$VERSION
DOCKER_IMAGE=joseluisq/rust-linux-darwin-builder:2.0.0-beta.1

in_container() {
    echo "in container: $@"
        # -u $(id -u):$(id -g) \
    docker run --rm \
        --mount type=bind,source="$HERE",target="/root/src"\
        --workdir /root/src \
        $DOCKER_IMAGE \
        "$@"
}

case $1 in
    --init)
        project=msm_relay
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
        docker build \
            -t $DOCKER_IMAGE .
    ;;
    -b)
        in_container sh ./build.sh
    ;;
    -b1)
        in_container sh ./build.sh --single
    ;;
    ###################################################
    # run
    -rd)
        docker run -it --rm \
        -u $(id -u):$(id -g) \
            --mount type=bind,source="$HERE",target="/ws"\
            --workdir /ws \
            $BUILD_IMAGE \
            sh
    ;;
    -r)
        shift
        mkdir -p ./scratch/ws
        cp $HERE/target/x86_64-unknown-linux-musl/release/msm_relay ./scratch/ws
        cd ./scratch/ws
        ./msm_relay $@
    ;;
    ###################################################
    # run
    *)
        echo "bad option"
        echo $1
    ;;
esac
