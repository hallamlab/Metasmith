#!/bin/bash
# dev script version 1.0 

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
NAME=metasmith
DEV_USER=hallamlab
_ver_file=$(find $HERE/src | grep version.txt)
# VER="$(cat $_ver_file).$(git branch --show-current)-$(git rev-parse --short HEAD)"
VER="$(cat $_ver_file)"
DOCKER_IMAGE=quay.io/$DEV_USER/$NAME

# CONDA=conda
CONDA=mamba # https://mamba.readthedocs.io/en/latest/mamba-installation.html#mamba-install
echo image: $DOCKER_IMAGE:$VER
echo ""

# this file contains a list of commands useful for dev,
# providing automation for some build tasks
#
# example workflow 1, pip:
# dev.sh --idev # create a local conda dev env
# # add pypi api token as file to ./secrets [https://pypi.org/help/#apitoken]
# # make some changes to source
# # bump up ./src/*/version.txt
# dev.sh -bp # build the pip package
# dev.sh -up # test upload to testpypi
# dev.sh -upload-pypi # release to pypi index for pip install
#
# example workflow 2, conda:
# dev.sh --idev # create a local conda dev env
# dev.sh -bp # build the pip package
# dev.sh -bc # build conda package from pip package
# dev.sh -uc # publish to conda index
#
# example workflow 3, containerization:
# dev.sh --idev # create a local conda dev env
# dev.sh -bd # build docker image
# dev.sh -ud # publish to quay.io
# dev.sh -bs # build apptainer image from local docker image

case $1 in
    ###################################################
    # environments

    --idev) # with dev tools for packaging
        cd $HERE/envs
        echo "creating new conda env: $NAME"
        echo "WARNING: you will need to install docker and apptainer individually"
        sleep 2
        $CONDA env create --no-default-packages -n $NAME -f ./base.yml \
        && $CONDA env update -n $NAME -f ./dev.yml
    ;;
    --ibase) # base only
        cd $HERE/envs
        echo "creating new conda env: $NAME"
        sleep 2
        $CONDA env create --no-default-packages -n $NAME -f ./base.yml
    ;;

    ###################################################
    # build

    -bp) # pip
        # build pip package
        [ -d ./build ] && rm -r build
        [ -d ./dist ] && rm -r dist
        python -m build
    ;;
    -bpi) # pip - test install
        pip install $HERE/dist/$NAME-$VER-py3-none-any.whl
    ;;
    -bpx) # pip - remove package
        pip uninstall -y $NAME
    ;;
    -bc) # conda
        # requires built pip package
        rm -r $HERE/conda_build
        python ./conda_recipe/compile_recipe.py
        $HERE/conda_recipe/call_build.sh
    ;;
    -bd) # docker
        # pre-download requirements
        mkdir -p $HERE/lib
        cd $HERE/lib
        TINI_VERSION=v0.19.0
        ! [ -f tini ] && wget https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini
        if ! [ -d globusconnectpersonal-latest ]; then
            wget -continue https://downloads.globus.org/globus-connect-personal/linux/stable/globusconnectpersonal-latest.tgz
            tar -xvf globusconnectpersonal-latest.tgz
            rm globusconnectpersonal-latest.tgz
            mv globusconnectpersonal-* globusconnectpersonal-latest
        fi
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
    # upload

    -up) # pip (testpypi)
        PYPI=testpypi
        TOKEN=$(cat secrets/${PYPI}) # https://pypi.org/help/#apitoken
        python -m twine upload --repository $PYPI dist/*.whl -u __token__ -p $TOKEN
    ;;
    -upload-pypi) # pip (pypi)
        echo "not all dependencies are available on pypi, so this is not a good idea..."
        # PYPI=pypi
        # TOKEN=$(cat secrets/${PYPI}) # https://pypi.org/help/#apitoken
        # python -m twine upload --repository $PYPI dist/*.whl -u __token__ -p $TOKEN
    ;;
    -uc) # conda (personal channel)
        # run `anaconda login` first
        find ./conda_build -name *.tar.bz2 | xargs -I % anaconda upload -u $DEV_USER %
    ;;
    -ud) # docker
        # login and push image to quay.io
        # sudo docker login quay.io
	    docker push $DOCKER_IMAGE:$VER
        echo "!!!"
        echo "remember to update the \"latest\" tag"
        echo "https://$DOCKER_IMAGE?tab=tags"
    ;;
    
    ###################################################
    # run

    -r)
        shift
        export PYTHONPATH=$HERE/src:$PYTHONPATH
        python -m $NAME $@
    ;;
    -rd) # docker
            # -e XDG_CACHE_HOME="/ws"\
        shift
        mkdir -p ./scratch/docker
        cd ./scratch/docker
        # mkdir -p cache/.globus cache/.globusonline
        docker run -it --rm \
            -u $(id -u):$(id -g) \
            --mount type=bind,source="$HERE/scratch/docker",target="/ws"\
            --mount type=bind,source="$HOME/.globus",target="/.globus"\
            --mount type=bind,source="$HOME/.globusonline",target="/.globusonline"\
            --workdir="/ws" \
            $DOCKER_IMAGE:$VER /bin/bash
    ;;
    -rs) # apptainer
            # -e XDG_CACHE_HOME="/ws"\
        shift
        mkdir -p ./scratch/docker
        cd ./scratch/docker
        apptainer exec \
            --bind ./:/ws \
            --workdir /ws \
            --no-home \
            $HERE/$NAME.sif /bin/bash
            # docker-daemon://$DOCKER_IMAGE:$VER /bin/bash
    ;;

    ###################################################
    # test

    -t)
        shift
        export PYTHONPATH=$HERE/src:$PYTHONPATH
        python -m $NAME $@ deploy
    ;;

    -t2)
        cd ./main/relay_agent/scratch/cache/mock
        # mkdir -p cache/.globus cache/.globusonline
        docker run -it --rm \
            -u $(id -u):$(id -g) \
            --mount type=bind,source="./work",target="/ws"\
            --mount type=bind,source="./home",target="/msm_home"\
            --mount type=bind,source="$HOME/.globus",target="/.globus"\
            --mount type=bind,source="$HOME/.globusonline",target="/.globusonline"\
            --workdir="/ws" \
            $DOCKER_IMAGE:$VER /bin/bash
    ;;

    ###################################################

    *)
        echo "bad option"
        echo $1
    ;;
esac