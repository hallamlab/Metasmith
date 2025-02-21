HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $HERE

NAME=pyinstaller_build_env
DOCKER_IMAGE=quay.io/txyliu/$NAME

case $1 in
    -b)

        export DOCKER_BUILDKIT=1
        docker build \
            --build-arg="CONDA_ENV=${NAME}_env" \
            -t $DOCKER_IMAGE .
    ;;
    -p)
        SRC=../../src/metasmith
        mkdir -p ./relay/coms
        cp $SRC/coms/ipc.py         ./relay/coms/ipc.py
        cp $SRC/serialization.py    ./relay/serialization.py
        cp $SRC/hashing.py          ./relay/hashing.py
        cp $SRC/logging.py          ./relay/logging.py
        rm -r ./dist ./build
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
