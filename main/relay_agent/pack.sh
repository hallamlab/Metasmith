HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $HERE

NAME=pyinstaller_build_env
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
    -u)
        SRC=../../src/metasmith
        mkdir -p ./relay/coms
        rsync -auc $SRC/coms/ipc.py         ./relay/coms/ipc.py
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
            --workdir /ws \
            $DOCKER_IMAGE \
            pyinstaller relay.py --onefile --bootloader-ignore-signals 
    ;;
    -t)
        docker run -it --rm \
            -u $(id -u):$(id -g) \
            --mount type=bind,source="$HERE",target="/ws"\
            --workdir /ws \
            $DOCKER_IMAGE \
            bash
    ;;
    *)
        echo "bad option"
        echo $1
    ;;
esac
