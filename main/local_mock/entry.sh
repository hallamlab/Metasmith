# bounce to container

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEV_SRC=$HERE/../../src

echo $(pwd -P)
IMAGE=quay.io/hallamlab/metasmith:0.2.dev-5c4c599

BOUNCE=./temp.bounce.sh
echo "export PYTHONPATH=/app:\$PYTHONPATH" >>$BOUNCE
echo "python -m metasmith \$@" >>$BOUNCE
docker run --rm \
    -u $(id -u):$(id -g) \
    --mount type=bind,source="./",target="/ws"\
    --mount type=bind,source="$DEV_SRC",target="/app"\
    --workdir="/ws" \
    $IMAGE bash $BOUNCE api deploy_from_container
rm $BOUNCE

bash ./lib/start_relay.sh

BOUNCE=./temp.bounce.sh
echo "export PYTHONPATH=/app:\$PYTHONPATH" >>$BOUNCE
echo "python -m metasmith \$@" >>$BOUNCE
docker run --rm \
    -u $(id -u):$(id -g) \
    --mount type=bind,source="./",target="/ws"\
    --mount type=bind,source="$DEV_SRC",target="/app"\
    --workdir="/ws" \
    $IMAGE bash $BOUNCE api run_task
rm $BOUNCE
