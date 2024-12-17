#!/bin/bash
# dev script version 1.0 

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
NAME=test_container_bounce
DEV_USER=txyliu
# VER="$(cat $HERE/version.txt).$(git branch --show-current)-$(git rev-parse --short HEAD)"
VER=dev
DOCKER_IMAGE=quay.io/$DEV_USER/$NAME

# CONDA=conda
CONDA=mamba # https://mamba.readthedocs.io/en/latest/mamba-installation.html#mamba-install
echo image: $DOCKER_IMAGE
echo ""

case $1 in
    -bd) # docker
        # pre-download requirements
        mkdir -p $HERE/lib
        cd $HERE/lib
        TINI_VERSION=v0.19.0
        ! [ -f tini ] && wget https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini
        cd $HERE

        # build the docker container locally
        export DOCKER_BUILDKIT=1
        docker build \
            --build-arg="CONDA_ENV=${NAME}_env" \
            --build-arg="PACKAGE=${NAME}" \
            --build-arg="VERSION=${VER}" \
            -t $DOCKER_IMAGE:$VER .
    ;;
    -bs) # apptainer image *from docker*
        apptainer build $NAME.sif docker-daemon://$DOCKER_IMAGE:$VER
    ;;
    
    ###################################################
    # run

    -rd) # docker
            # -e XDG_CACHE_HOME="/ws"\
        shift
        docker run -it --rm \
            -u $(id -u):$(id -g) \
            --mount type=bind,source="$HERE",target="/ws"\
            --workdir="/ws" \
            -p 56100:443 \
            $DOCKER_IMAGE:$VER /bin/bash
    ;;
    -rs) # apptainer
            # -e XDG_CACHE_HOME="/ws"\
        shift
            # --net --network-args="portmap=56100:443/tcp" \
        apptainer exec \
            --bind ./:/ws \
            --workdir /ws \
            --net \
            $NAME.sif /bin/bash
    ;;

    ###################################################
    # test

    -t)
        echo "no tests"
    ;;

    ###################################################

    *)
        echo "bad option"
        echo $1
    ;;
esac