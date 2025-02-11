from pathlib import Path
from metasmith import *

def protocol(context: ExecutionContext):
    Log.Info("this is pprodigal!")
    for k, x in context.outputs.items():
        context.Shell(f"touch {x.source}")
    return ExecutionResult(success=True)

# todo: url for more consistency
lib = DataTypeLibrary.Load("/home/tony/workspace/tools/Metasmith/main/local_mock/prototypes/metagenomics.dev3.yml")

TransformInstance(
    protocol=protocol,
    input_signature=lib.Subset([
        "oci_image_pprodigal",
        "contigs",
    ]),
    output_signature=DataInstanceLibrary(
        manifest={
            Path("orfs.faa"): lib["orfs_faa"],
        },
    ),
)
