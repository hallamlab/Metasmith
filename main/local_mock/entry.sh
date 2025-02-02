# bounce to container

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEV_SRC=$HERE/../../src

IMAGE=quay.io/hallamlab/metasmith:0.2.dev-5c4c599

BOUNCE=./temp.bounce.sh
echo "export PYTHONPATH=/app:\$PYTHONPATH" >>$BOUNCE
echo "python -m \$@" >>$BOUNCE
docker run --rm \
    -u $(id -u):$(id -g) \
    --mount type=bind,source="./",target="/ws"\
    --mount type=bind,source="$DEV_SRC",target="/app"\
    --workdir="/ws" \
    $IMAGE bash $BOUNCE metasmith api stage_slurm
rm $BOUNCE
