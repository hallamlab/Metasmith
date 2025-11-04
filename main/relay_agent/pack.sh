HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $HERE

NAME=pyinstaller_builder
DOCKER_IMAGE=quay.io/txyliu/$NAME

case $1 in
    -x)
        while true; do
            sleep 1
            echo x >>x
        done
    ;;
    -b)
        export DOCKER_BUILDKIT=1
        docker build \
            --build-arg="CONDA_ENV=${NAME}_env" \
            -t $DOCKER_IMAGE .
    ;;
    -bs)
        apptainer build --force $NAME.sif docker-daemon://$DOCKER_IMAGE
    ;;
    -u)
        SRC=../../src/metasmith
        rsync -auc $SRC/coms                ./relay
        rsync -auc $SRC/serialization.py    ./relay/serialization.py
        rsync -auc $SRC/hashing.py          ./relay/hashing.py
        rsync -auc $SRC/logging.py          ./relay/logging.py
    ;;
    -p)
        $HERE/pack.sh -u
        rm -r ./dist ./build
        # use docker to force older glibc version
        docker run -it --rm \
            -u $(id -u):$(id -g) \
            --mount type=bind,source="$HERE",target="/ws"\
            --mount type=bind,source="$HERE/relay",target="/app/relay"\
            --workdir /ws \
            $DOCKER_IMAGE \
            bash -c "pyinstaller msm_relay.py \
                --exclude-module pkg_resources \
                --onefile --bootloader-ignore-signals"
    ;;
    -rd)
        docker run -it --rm \
            -u $(id -u):$(id -g) \
            --mount type=bind,source="$HERE",target="/ws"\
            --workdir /ws \
            $DOCKER_IMAGE \
            bash
    ;;
    -r)
        shift
        python -m relay $@
    ;;
    -rx)
        shift
        cd $HERE/dist
        ./msm_relay $@
    ;;
    *)
        echo "bad option"
        echo $1
    ;;
esac
