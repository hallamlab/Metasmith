#!/bin/bash

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# # ../../dev.sh -r deploy --config $HERE/config/local_test.yml
# ../../dev.sh -r run \
#     --work $HERE/../../scratch/test_ws \
#     --request $HERE/job_requests/1.yml

function test {
    echo "$@"
}

test "hello" "world"
