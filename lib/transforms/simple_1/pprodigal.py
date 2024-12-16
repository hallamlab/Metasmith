from pathlib import Path
from metasmith import RegisterTransform, ExecutionContext, ExecutionResult
from _common import PROTOTYPE_DEFINITIONS

def protocol(context: ExecutionContext):
    print("this is pprodigal!")
    return ExecutionResult()

RegisterTransform(
    container="docker://quay.io/hallamlab/external_pprodigal:1.0.1",
    protocol=protocol,
    prototypes=PROTOTYPE_DEFINITIONS,
    input_signature={
        "contigs",
    },
    output_signature={
        "orfs": "orfs.gff3"
    },
)
