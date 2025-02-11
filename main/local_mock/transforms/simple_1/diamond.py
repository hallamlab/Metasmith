from pathlib import Path
from metasmith import *

def protocol(context: ExecutionContext):
    Log.Info("this is diamond!")
    for k, x in context.outputs.items():
        context.Shell(f"touch {x.source}")
    return ExecutionResult(success=True)

# todo: url for more consistency
lib = DataTypeLibrary.Load("/home/tony/workspace/tools/Metasmith/main/local_mock/prototypes/metagenomics.dev3.yml")
TransformInstance(
    protocol=protocol,
    input_signature=lib.Subset([
        "oci_image_diamond",
        "orfs_faa",
        "protein_reference_diamond",
    ]),
    output_signature=DataInstanceLibrary(
        manifest={
            Path("annotations.csv"): lib["orf_annotations"],
        },
    ),
)
