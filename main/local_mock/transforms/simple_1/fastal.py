from pathlib import Path
from metasmith import RegisterTransform, ExecutionContext, ExecutionResult
from _common import PROTOTYPE_DEFINITIONS

def protocol(context: ExecutionContext):
    print("this is fastal!")
    return ExecutionResult()

RegisterTransform(
    container="docker://quay.io/hallamlab/fast_aligner:1.0",
    protocol=protocol,
    prototypes=PROTOTYPE_DEFINITIONS,
    input_signature={
        "metagenomics:orfs", # namespace:prototype
        "fastal_protein_reference"
    },
    output_signature={
        "orf_annotation": "annotations.csv"
    },
)
